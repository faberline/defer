// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-direct-k8s" tracker="#766" reason="Offline contract for Defer direct Kustomize singleton and operator-owned HA boundary."
use serde_yaml::Value;

fn yaml(input: &str) -> Value {
    serde_yaml::from_str(input).expect("manifest parses")
}

fn yaml_documents(input: &str) -> Vec<Value> {
    input
        .split("\n---\n")
        .filter(|document| !document.trim().is_empty())
        .map(yaml)
        .collect()
}

fn unique_named<'a>(objects: &'a [Value], kind: &str, name: &str) -> &'a Value {
    let matches = objects
        .iter()
        .filter(|object| object["kind"] == kind && object["metadata"]["name"] == name)
        .collect::<Vec<_>>();
    assert_eq!(matches.len(), 1, "expected exactly one {kind}/{name}");
    matches[0]
}

fn render_kustomize_raw(profile: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(profile);
    let output = std::process::Command::new("kubectl")
        .arg("kustomize")
        .arg(&path)
        .output()
        .unwrap_or_else(|error| {
            panic!("kubectl is required to render {}: {error}", path.display())
        });
    assert!(
        output.status.success(),
        "{} must render: {}",
        path.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn render_kustomize(profile: &str) -> Vec<Value> {
    yaml_documents(&String::from_utf8(render_kustomize_raw(profile)).unwrap())
}

fn assert_connected_services_and_pdb(resources: &[Value]) {
    let selector = yaml("{app: defer, role: server}");
    let statefulset = unique_named(resources, "StatefulSet", "defer");
    assert_eq!(statefulset["spec"]["selector"]["matchLabels"], selector);
    assert_eq!(
        statefulset["spec"]["template"]["metadata"]["labels"],
        statefulset["spec"]["selector"]["matchLabels"]
    );
    let workload_selector = &statefulset["spec"]["selector"]["matchLabels"];
    let headless = unique_named(resources, "Service", "defer-headless");
    assert_eq!(headless["spec"]["clusterIP"], "None");
    assert_eq!(headless["spec"]["publishNotReadyAddresses"], true);
    assert_eq!(&headless["spec"]["selector"], workload_selector);
    assert_eq!(
        headless["spec"]["ports"],
        yaml("[{name: http, port: 7141, targetPort: http, protocol: TCP}, {name: raft, port: 7142, targetPort: raft, protocol: TCP}]")
    );
    let client = unique_named(resources, "Service", "defer");
    assert_eq!(&client["spec"]["selector"], workload_selector);
    assert_eq!(
        client["spec"]["ports"],
        yaml("[{name: http, port: 7141, targetPort: http, protocol: TCP}]")
    );
    let pdb = unique_named(resources, "PodDisruptionBudget", "defer");
    assert_eq!(pdb["spec"]["maxUnavailable"], 0);
    assert_eq!(&pdb["spec"]["selector"]["matchLabels"], workload_selector);
}

#[test]
fn direct_base_is_a_restricted_durable_singleton() {
    let statefulset = yaml(include_str!("../../../../k8s/base/statefulset.yaml"));
    assert_eq!(statefulset["kind"], "StatefulSet");
    assert_eq!(statefulset["spec"]["replicas"], 1);
    assert_eq!(statefulset["spec"]["serviceName"], "defer-headless");
    assert_eq!(
        statefulset["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"],
        "10Gi"
    );
    let container = &statefulset["spec"]["template"]["spec"]["containers"][0];
    let pod_security = &statefulset["spec"]["template"]["spec"]["securityContext"];
    assert_eq!(pod_security["runAsNonRoot"], true);
    assert_eq!(pod_security["runAsUser"], 65532);
    assert_eq!(pod_security["runAsGroup"], 65532);
    assert_eq!(pod_security["seccompProfile"]["type"], "RuntimeDefault");
    assert_eq!(container["command"][0], "defer");
    assert_eq!(container["command"][1], "serve");
    assert_eq!(container["ports"][0]["containerPort"], 7141);
    assert_eq!(container["ports"][1]["containerPort"], 7142);
    assert_eq!(container["readinessProbe"]["httpGet"]["path"], "/readyz");
    assert_eq!(container["readinessProbe"]["httpGet"]["port"], "http");
    assert_eq!(container["livenessProbe"]["httpGet"]["path"], "/healthz");
    assert_eq!(container["livenessProbe"]["httpGet"]["port"], "http");
    assert_eq!(container["startupProbe"]["httpGet"]["path"], "/healthz");
    assert_eq!(container["startupProbe"]["failureThreshold"], 120);
    assert_eq!(container["securityContext"]["readOnlyRootFilesystem"], true);
    assert_eq!(
        container["securityContext"]["allowPrivilegeEscalation"],
        false
    );
    assert_eq!(
        container["securityContext"]["capabilities"]["drop"][0],
        "ALL"
    );
    let env = container["env"].as_sequence().unwrap();
    assert!(env.iter().any(|entry| entry["name"] == "DEFER_LOG_FORMAT"));
    let config = yaml(include_str!("../../../../k8s/base/configmap.yaml"));
    assert_eq!(config["data"]["DEFER_LOG_FORMAT"], "json");
    for (name, value) in [
        ("SHARD_COUNT", "1"),
        ("REPLICAS_PER_SHARD", "1"),
        ("VOTER_COUNT", "1"),
        ("DEFER_PEER_SERVICE", "defer-headless"),
        ("DEFER_RAFT_PORT", "7142"),
        ("DEFER_DATA_DIR", "/data"),
    ] {
        assert!(env
            .iter()
            .any(|entry| entry["name"] == name && entry["value"] == value));
    }

    let data_mounts = container["volumeMounts"]
        .as_sequence()
        .unwrap()
        .iter()
        .filter(|mount| mount["name"] == "data")
        .collect::<Vec<_>>();
    assert_eq!(data_mounts.len(), 1);
    assert_eq!(data_mounts[0]["mountPath"], "/data");
    let claim = &statefulset["spec"]["volumeClaimTemplates"][0];
    assert_eq!(claim["metadata"]["name"], "data");
    assert_eq!(claim["spec"]["accessModes"], yaml("[ReadWriteOnce]"));

    let services = yaml_documents(include_str!("../../../../k8s/base/service.yaml"));
    assert_eq!(services.len(), 2);
    let selector = yaml("{app: defer, role: server}");
    let headless = unique_named(&services, "Service", "defer-headless");
    assert_eq!(headless["spec"]["clusterIP"], "None");
    assert_eq!(headless["spec"]["publishNotReadyAddresses"], true);
    assert_eq!(&headless["spec"]["selector"], &selector);
    assert_eq!(
        headless["spec"]["ports"],
        yaml("[{name: http, port: 7141, targetPort: http, protocol: TCP}, {name: raft, port: 7142, targetPort: raft, protocol: TCP}]")
    );
    let client = unique_named(&services, "Service", "defer");
    assert_eq!(&client["spec"]["selector"], &selector);
    assert_eq!(
        client["spec"]["ports"],
        yaml("[{name: http, port: 7141, targetPort: http, protocol: TCP}]")
    );

    let pdb = yaml(include_str!("../../../../k8s/base/pdb.yaml"));
    assert_eq!(pdb["kind"], "PodDisruptionBudget");
    assert_eq!(pdb["spec"]["maxUnavailable"], 0);
    assert_eq!(&pdb["spec"]["selector"]["matchLabels"], &selector);
}

#[test]
fn prod_profile_uses_security_components_without_voter_hpa() {
    let prod = include_str!("../../../../k8s/overlays/prod/kustomization.yaml");
    assert!(prod.contains("DEFER_TOKEN_REGISTRY_FILE"));
    assert!(prod.contains("secretName: defer-token-registry"));
    assert!(prod.contains("readOnly: true"));
    assert!(prod.contains("../../components/observability"));
    assert!(prod.contains("../../components/network-policy"));
    assert!(!prod.contains("HorizontalPodAutoscaler"));
    assert!(!prod.contains("kind: HPA"));

    let policy = yaml(include_str!(
        "../../../../k8s/components/network-policy/networkpolicy.yaml"
    ));
    assert_eq!(policy["kind"], "NetworkPolicy");
    assert_eq!(policy["spec"]["podSelector"]["matchLabels"]["app"], "defer");
    assert_eq!(
        policy["spec"]["podSelector"]["matchLabels"]["role"],
        "server"
    );
    let policy_types = policy["spec"]["policyTypes"].as_sequence().unwrap();
    assert!(policy_types.iter().any(|value| value == "Ingress"));
    assert!(policy_types.iter().any(|value| value == "Egress"));
    let policy_yaml = serde_yaml::to_string(&policy).unwrap();
    assert!(policy_yaml.contains("port: 7141"));
    assert!(policy_yaml.contains("port: 7142"));
}

// <HANDWRITE gap="missing-generator:e2e-test:defer-rendered-prod-security" tracker="#2215" reason="Render the composed production overlay so disconnected or invalid security patches cannot pass raw-string checks.">
#[test]
fn prod_profile_renders_the_connected_security_boundary() {
    let base = render_kustomize("k8s/base");
    assert_connected_services_and_pdb(&base);
    let resources = render_kustomize("k8s/overlays/prod");
    assert!(!resources.is_empty());
    assert_connected_services_and_pdb(&resources);
    assert!(!resources
        .iter()
        .any(|resource| resource["kind"] == "HorizontalPodAutoscaler"));
    assert!(resources
        .iter()
        .any(|resource| resource["kind"] == "ServiceMonitor"));

    let statefulset = resources
        .iter()
        .find(|resource| {
            resource["kind"] == "StatefulSet" && resource["metadata"]["name"] == "defer"
        })
        .expect("rendered Defer StatefulSet");
    let container = &statefulset["spec"]["template"]["spec"]["containers"][0];
    let pod_security = &statefulset["spec"]["template"]["spec"]["securityContext"];
    assert_eq!(pod_security["runAsNonRoot"], true);
    assert_eq!(pod_security["runAsUser"], 65532);
    assert_eq!(pod_security["runAsGroup"], 65532);
    assert_eq!(pod_security["seccompProfile"]["type"], "RuntimeDefault");
    assert_eq!(container["securityContext"]["runAsNonRoot"], true);
    assert_eq!(
        container["securityContext"]["allowPrivilegeEscalation"],
        false
    );
    assert_eq!(container["securityContext"]["readOnlyRootFilesystem"], true);
    assert_eq!(
        container["securityContext"]["capabilities"]["drop"][0],
        "ALL"
    );
    let rendered_container = serde_yaml::to_string(container).unwrap();
    let rendered_statefulset = serde_yaml::to_string(statefulset).unwrap();
    assert!(rendered_container.contains("DEFER_TOKEN_REGISTRY_FILE"));
    assert!(rendered_statefulset.contains("secretName: defer-token-registry"));
    assert!(rendered_container.contains("readOnly: true"));
    assert_eq!(
        statefulset["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"],
        "100Gi"
    );

    let policy = resources
        .iter()
        .find(|resource| resource["kind"] == "NetworkPolicy")
        .expect("rendered production NetworkPolicy");
    assert_eq!(policy["spec"]["podSelector"]["matchLabels"]["app"], "defer");
    assert_eq!(
        policy["spec"]["podSelector"]["matchLabels"]["role"],
        "server"
    );
    let rendered_policy = serde_yaml::to_string(policy).unwrap();
    assert!(rendered_policy.contains("port: 7141"));
    assert!(rendered_policy.contains("port: 7142"));
}
// </HANDWRITE>

#[test]
fn operator_deployment_uses_a_restricted_security_context() {
    let operator = yaml(include_str!("../../../../k8s/operator/deployment.yaml"));
    let pod_security = &operator["spec"]["template"]["spec"]["securityContext"];
    assert_eq!(pod_security["runAsNonRoot"], true);
    assert_eq!(pod_security["seccompProfile"]["type"], "RuntimeDefault");
    let container = &operator["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(container["securityContext"]["readOnlyRootFilesystem"], true);
    assert_eq!(
        container["securityContext"]["allowPrivilegeEscalation"],
        false
    );
    assert_eq!(
        container["securityContext"]["capabilities"]["drop"][0],
        "ALL"
    );
}

fn parse_dockerfile_entrypoint(content: &str) -> Vec<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("ENTRYPOINT ") {
            let parsed: Vec<String> =
                serde_yaml::from_str(rest).expect("parse ENTRYPOINT as yaml array");
            return parsed;
        }
    }
    panic!("missing ENTRYPOINT in Dockerfile content");
}

#[test]
fn k8s_profiles_render_deterministically_with_valid_metadata_and_placeholders() {
    let profiles = [
        ("k8s/base", 7),
        ("k8s/overlays/dev", 7),
        ("k8s/overlays/staging", 10),
        ("k8s/overlays/prod", 10),
        ("k8s/overlays/template", 7),
        ("k8s/operator", 6),
    ];

    for (profile, expected_count) in profiles {
        let run1 = render_kustomize_raw(profile);
        let run2 = render_kustomize_raw(profile);
        assert_eq!(
            run1, run2,
            "profile {profile} must render byte-identically on repeated runs"
        );

        let rendered_str = String::from_utf8(run1).expect("kustomize stdout is valid utf-8");
        let docs = yaml_documents(&rendered_str);
        assert_eq!(
            docs.len(),
            expected_count,
            "profile {profile} document count mismatch"
        );
        assert!(!docs.is_empty(), "profile {profile} produced 0 documents");

        for (idx, doc) in docs.iter().enumerate() {
            let api = doc
                .get("apiVersion")
                .and_then(Value::as_str)
                .unwrap_or_else(|| {
                    panic!("profile {profile} doc #{idx} missing apiVersion: {doc:?}")
                });
            assert!(
                !api.trim().is_empty(),
                "profile {profile} doc #{idx} has empty apiVersion"
            );

            let kind = doc
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("profile {profile} doc #{idx} missing kind: {doc:?}"));
            assert!(
                !kind.trim().is_empty(),
                "profile {profile} doc #{idx} has empty kind"
            );

            let name = doc
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_else(|| {
                    panic!("profile {profile} doc #{idx} missing metadata.name: {doc:?}")
                });
            assert!(
                !name.trim().is_empty(),
                "profile {profile} doc #{idx} has empty metadata.name"
            );
        }

        if profile == "k8s/overlays/template" {
            let statefulset = unique_named(&docs, "StatefulSet", "defer");
            let image = statefulset["spec"]["template"]["spec"]["containers"][0]["image"]
                .as_str()
                .expect("template container image is string");
            assert_eq!(image, "REPLACE_ME__REGISTRY/defer:REPLACE_ME__TAG");
            assert_eq!(
                rendered_str.matches("REPLACE_ME").count(),
                2,
                "template profile must render exactly the registry and tag REPLACE_ME placeholders"
            );
        } else {
            assert!(
                !rendered_str.contains("REPLACE_ME"),
                "profile {profile} must not contain REPLACE_ME"
            );
        }
    }
}

#[test]
fn container_command_and_image_match_dockerfile_entrypoint() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let dockerfile_src = std::fs::read_to_string(root.join("Dockerfile")).expect("read Dockerfile");
    let dockerfile_rel =
        std::fs::read_to_string(root.join("Dockerfile.release")).expect("read Dockerfile.release");

    let ep_src = parse_dockerfile_entrypoint(&dockerfile_src);
    let ep_rel = parse_dockerfile_entrypoint(&dockerfile_rel);

    let expected_entrypoint = vec!["/usr/local/bin/defer".to_string()];
    assert_eq!(
        ep_src, expected_entrypoint,
        "Dockerfile ENTRYPOINT mismatch"
    );
    assert_eq!(
        ep_rel, expected_entrypoint,
        "Dockerfile.release ENTRYPOINT mismatch"
    );

    let binary_name = std::path::Path::new(&expected_entrypoint[0])
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(binary_name, "defer");

    for profile in [
        "k8s/base",
        "k8s/overlays/dev",
        "k8s/overlays/staging",
        "k8s/overlays/prod",
    ] {
        let docs = render_kustomize(profile);
        let statefulset = unique_named(&docs, "StatefulSet", "defer");
        let container = &statefulset["spec"]["template"]["spec"]["containers"][0];

        let command = container["command"]
            .as_sequence()
            .expect("container command is sequence");
        assert_eq!(
            command.len(),
            2,
            "profile {profile} expected command with 2 elements"
        );
        assert_eq!(
            command[0].as_str(),
            Some("defer"),
            "profile {profile} container command[0] must be 'defer'"
        );
        assert_eq!(
            command[1].as_str(),
            Some("serve"),
            "profile {profile} container command[1] must be 'serve'"
        );
        assert_eq!(
            command[0].as_str().unwrap(),
            binary_name,
            "profile {profile} container command first word must match binary name"
        );

        let image = container["image"]
            .as_str()
            .expect("container image is string");
        let without_tag = image.split(':').next().unwrap();
        let image_name_part = without_tag.rsplit('/').next().unwrap();
        assert_eq!(
            image_name_part, "defer",
            "profile {profile} image name part must be 'defer'"
        );
    }
}

#[test]
fn overlays_match_declared_components_and_patches() {
    // dev profile assertions
    let dev_docs = render_kustomize("k8s/overlays/dev");
    assert!(
        !dev_docs.iter().any(|r| r["kind"] == "NetworkPolicy"),
        "dev must not include NetworkPolicy"
    );
    assert!(
        !dev_docs.iter().any(|r| r["kind"] == "ServiceMonitor"),
        "dev must not include ServiceMonitor"
    );
    assert!(
        !dev_docs.iter().any(|r| r["kind"] == "PrometheusRule"),
        "dev must not include PrometheusRule"
    );
    let dev_config = unique_named(&dev_docs, "ConfigMap", "defer-config");
    assert_eq!(
        dev_config["data"]["RUST_LOG"], "debug",
        "dev ConfigMap must patch RUST_LOG to debug"
    );
    let dev_ss = unique_named(&dev_docs, "StatefulSet", "defer");
    assert_eq!(
        dev_ss["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"],
        "1Gi",
        "dev StatefulSet storage patch must be 1Gi"
    );

    // staging profile assertions
    let staging_docs = render_kustomize("k8s/overlays/staging");
    assert!(
        staging_docs.iter().any(|r| r["kind"] == "NetworkPolicy"),
        "staging must include NetworkPolicy component"
    );
    assert!(
        staging_docs.iter().any(|r| r["kind"] == "ServiceMonitor"),
        "staging must include ServiceMonitor component"
    );
    assert!(
        staging_docs.iter().any(|r| r["kind"] == "PrometheusRule"),
        "staging must include PrometheusRule component"
    );
    let staging_config = unique_named(&staging_docs, "ConfigMap", "defer-config");
    assert_eq!(
        staging_config["data"]["RUST_LOG"], "info",
        "staging ConfigMap retains default RUST_LOG info"
    );
    let staging_ss = unique_named(&staging_docs, "StatefulSet", "defer");
    assert_eq!(
        staging_ss["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"],
        "20Gi",
        "staging StatefulSet storage patch must be 20Gi"
    );
    let staging_ss_str = serde_yaml::to_string(staging_ss).unwrap();
    assert!(
        !staging_ss_str.contains("defer-token-registry"),
        "staging must not include token registry secret"
    );

    // prod profile assertions
    let prod_docs = render_kustomize("k8s/overlays/prod");
    assert!(
        prod_docs.iter().any(|r| r["kind"] == "NetworkPolicy"),
        "prod must include NetworkPolicy"
    );
    assert!(
        prod_docs.iter().any(|r| r["kind"] == "ServiceMonitor"),
        "prod must include ServiceMonitor"
    );
    assert!(
        prod_docs.iter().any(|r| r["kind"] == "PrometheusRule"),
        "prod must include PrometheusRule"
    );
    let prod_ss = unique_named(&prod_docs, "StatefulSet", "defer");
    assert_eq!(
        prod_ss["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"],
        "100Gi",
        "prod StatefulSet storage must be 100Gi"
    );
    let prod_ss_str = serde_yaml::to_string(prod_ss).unwrap();
    assert!(
        prod_ss_str.contains("defer-token-registry"),
        "prod must include token registry secret patch"
    );
}

// HANDWRITE-END
