//! Defer CRD and operator render conformance.
//!
//! isolation: needs `--features operator`; CI runs this binary by name with
//! `cargo test -p defer --features operator --test operator`.
#![cfg(feature = "operator")]
// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-operator" tracker="#766" reason="Pure CRD/operator render conformance for stateful HA, security, and scheduled backups."
use defer::operator::{self, Defer};
use serde_json::{json, Value};

fn object<'a>(objects: &'a [Value], kind: &str, name: &str) -> &'a Value {
    objects
        .iter()
        .find(|object| object["kind"] == kind && object["metadata"]["name"] == name)
        .unwrap_or_else(|| panic!("missing {kind}/{name}"))
}

fn named<'a>(array: &'a Value, name: &str) -> &'a Value {
    array
        .as_array()
        .unwrap_or_else(|| panic!("expected array while finding {name}"))
        .iter()
        .find(|value| value["name"] == name)
        .unwrap_or_else(|| panic!("missing named entry {name}"))
}

fn production() -> Defer {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "defer.dev/v1alpha1",
        "kind": "Defer",
        "metadata": {"name": "jobs", "namespace": "payments", "uid": "u-1"},
        "spec": {
            "image": "registry/defer:1",
            "imagePullPolicy": "Always",
            "replicasPerShard": 3,
            "voterCount": 3,
            "storage": "100Gi",
            "auth": "required",
            "tokensSecret": "defer-tokens",
            "targetSigningSecret": "target-signing",
            "targetSigningKeyId": "active",
            "peerTlsSecret": "peer-tls",
            "backup": {
                "schedule": "0 */6 * * *",
                "destination": "s3://backups/defer",
                "retentionSecs": 604800,
                "adminTokenSecret": "backup-token"
            },
            "resources": {"cpu": "2", "memory": "8Gi"}
        }
    }))
    .unwrap()
}

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2220" reason="Own the exact six-object production graph, three-replica StatefulSet relationships, connected security secrets, and backup CronJob oracle.">
#[test]
fn production_render_composes_shared_stateful_primitives() {
    let objects = operator::render::render(&production());
    assert_eq!(objects.len(), 6, "exact operator-owned resource graph");
    let selector = json!({
        "app.kubernetes.io/name": "defer",
        "app.kubernetes.io/instance": "jobs",
        "app.kubernetes.io/component": "server"
    });
    let stateful = object(&objects, "StatefulSet", "jobs");
    assert_eq!(stateful["metadata"]["namespace"], "payments");
    assert_eq!(stateful["spec"]["replicas"], 3);
    assert_eq!(stateful["spec"]["serviceName"], "jobs-headless");
    assert_eq!(stateful["spec"]["selector"]["matchLabels"], selector);
    assert_eq!(
        stateful["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"],
        "100Gi"
    );
    assert_eq!(
        stateful["spec"]["volumeClaimTemplates"][0]["metadata"]["name"],
        "data"
    );
    assert_eq!(
        stateful["spec"]["volumeClaimTemplates"][0]["spec"]["accessModes"],
        json!(["ReadWriteOnce"])
    );
    let container = &stateful["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(
        container["ports"],
        json!([
            {"name": "http", "containerPort": 7141, "protocol": "TCP"},
            {"name": "raft", "containerPort": 7142, "protocol": "TCP"}
        ])
    );
    assert_eq!(
        container["readinessProbe"],
        json!({"httpGet": {"path": "/readyz", "port": "http"}, "periodSeconds": 5})
    );
    assert_eq!(
        container["livenessProbe"],
        json!({"httpGet": {"path": "/healthz", "port": "http"}, "periodSeconds": 15})
    );
    assert_eq!(
        container["startupProbe"],
        json!({"httpGet": {"path": "/healthz", "port": "http"}, "periodSeconds": 5, "failureThreshold": 120})
    );
    assert_eq!(
        named(&container["volumeMounts"], "data")["mountPath"],
        "/data"
    );
    assert_eq!(named(&container["volumeMounts"], "data")["readOnly"], false);

    let headless = object(&objects, "Service", "jobs-headless");
    assert_eq!(headless["spec"]["clusterIP"], "None");
    assert_eq!(headless["spec"]["publishNotReadyAddresses"], true);
    assert_eq!(headless["spec"]["selector"], selector);
    assert_eq!(
        headless["spec"]["ports"],
        json!([
            {"name": "http", "port": 7141, "targetPort": "http", "protocol": "TCP"},
            {"name": "raft", "port": 7142, "targetPort": "raft", "protocol": "TCP"}
        ])
    );
    let client = object(&objects, "Service", "jobs");
    assert_eq!(client["spec"]["type"], "ClusterIP");
    assert_eq!(client["spec"]["selector"], selector);
    assert_eq!(
        client["spec"]["ports"],
        json!([{"name": "http", "port": 7141, "targetPort": "http", "protocol": "TCP"}])
    );
    let pdb = object(&objects, "PodDisruptionBudget", "jobs");
    assert_eq!(pdb["spec"]["maxUnavailable"], 1);
    assert_eq!(pdb["spec"]["selector"]["matchLabels"], selector);

    let volumes = &stateful["spec"]["template"]["spec"]["volumes"];
    assert_eq!(
        named(volumes, "defer-token-registry"),
        &json!({
            "name": "defer-token-registry",
            "secret": {"secretName": "defer-tokens", "items": [{"key": "token-registry.json", "path": "token-registry.json"}]}
        })
    );
    assert_eq!(
        named(&container["volumeMounts"], "defer-token-registry"),
        &json!({"name": "defer-token-registry", "mountPath": "/var/run/secrets/defer", "readOnly": true})
    );
    assert_eq!(
        named(volumes, "defer-target-signing"),
        &json!({
            "name": "defer-target-signing",
            "secret": {"secretName": "target-signing", "items": [{"key": "key", "path": "key"}]}
        })
    );
    assert_eq!(
        named(&container["volumeMounts"], "defer-target-signing"),
        &json!({"name": "defer-target-signing", "mountPath": "/var/run/secrets/defer-target", "readOnly": true})
    );
    assert_eq!(
        named(volumes, "defer-peer-tls"),
        &json!({
            "name": "defer-peer-tls",
            "secret": {"secretName": "peer-tls", "items": [
                {"key": "tls.crt", "path": "tls.crt"},
                {"key": "tls.key", "path": "tls.key"},
                {"key": "ca.crt", "path": "ca.crt"}
            ]}
        })
    );
    assert_eq!(
        named(&container["volumeMounts"], "defer-peer-tls"),
        &json!({"name": "defer-peer-tls", "mountPath": "/var/run/secrets/defer-peer", "readOnly": true})
    );
    for (name, value) in [
        ("DEFER_AUTH", "required"),
        (
            "DEFER_TOKEN_REGISTRY_FILE",
            "/var/run/secrets/defer/token-registry.json",
        ),
        ("DEFER_TARGET_SIGNING_KEY_ID", "active"),
        (
            "DEFER_TARGET_SIGNING_SECRET_FILE",
            "/var/run/secrets/defer-target/key",
        ),
        ("DEFER_PEER_MTLS", "on"),
        ("DEFER_PEER_TLS_CERT", "/var/run/secrets/defer-peer/tls.crt"),
        ("DEFER_PEER_TLS_KEY", "/var/run/secrets/defer-peer/tls.key"),
        ("DEFER_PEER_TLS_CA", "/var/run/secrets/defer-peer/ca.crt"),
    ] {
        assert_eq!(named(&container["env"], name)["value"], value, "{name}");
    }

    let cron = object(&objects, "CronJob", "jobs-backup");
    assert_eq!(cron["spec"]["schedule"], "0 */6 * * *");
    let backup = &cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(
        backup["args"],
        json!([
            "backup",
            "--url",
            "http://jobs.payments.svc.cluster.local:7141",
            "--dest",
            "s3://backups/defer",
            "--retention-secs",
            "604800"
        ])
    );
    assert_eq!(
        named(&backup["env"], "DEFER_TOKEN")["valueFrom"]["secretKeyRef"],
        json!({"name": "backup-token", "key": "token"})
    );
}
// </HANDWRITE>

#[test]
fn generated_crd_is_structural_and_includes_backup_and_signing() {
    let yaml = operator::crd_yaml();
    assert!(yaml.contains("name: defers.defer.dev"));
    assert!(yaml.contains("targetSigningSecret"));
    assert!(yaml.contains("backup"));
    assert!(yaml.contains("tokensSecretCsiDriver"));
    assert!(!yaml.contains("format: uint64"));
    assert!(!yaml.contains("format: uint32"));
}

/// `k8s/operator/crd.yaml` is what `defer k8s crd render` emits without the
/// `operator` feature, so it must stay byte-identical to the generator.
#[test]
fn committed_crd_matches_the_generator() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../k8s/operator/crd.yaml");
    let committed = std::fs::read_to_string(path).unwrap();
    let generated = format!("{}\n", operator::crd_yaml().trim_end_matches('\n'));
    assert!(
        committed == generated,
        "k8s/operator/crd.yaml drifted; regenerate with \
         `cargo run -p defer --features operator --bin defer -- k8s crd render --out k8s/operator`"
    );
}

fn placed(placement: Value, pull_secrets: Value) -> Defer {
    let mut defer = production();
    defer.spec.placement = serde_json::from_value(placement).unwrap();
    defer.spec.image_pull_secrets = serde_json::from_value(pull_secrets).unwrap();
    defer
}

/// `spec.placement` and `spec.imagePullSecrets` reach every operator-rendered
/// pod: the serving StatefulSet and the backup CronJob. The one-pod-per-node
/// anti-affinity stays.
#[test]
fn placement_and_pull_secrets_reach_every_rendered_pod() {
    let defer = placed(
        json!({
            "nodeSelector": {"faberline.dev/pool": "defer-server"},
            "tolerations": [{
                "key": "faberline.dev/pool",
                "operator": "Equal",
                "value": "defer-server",
                "effect": "NoSchedule"
            }]
        }),
        json!([{"name": "ghcr-pull"}]),
    );
    let objects = operator::render::render(&defer);
    let stateful = &object(&objects, "StatefulSet", "jobs")["spec"]["template"]["spec"];
    let cron = &object(&objects, "CronJob", "jobs-backup")["spec"]["jobTemplate"]["spec"]
        ["template"]["spec"];
    for pod in [stateful, cron] {
        assert_eq!(
            pod["nodeSelector"],
            json!({"faberline.dev/pool": "defer-server"})
        );
        assert_eq!(
            pod["tolerations"],
            json!([{
                "key": "faberline.dev/pool",
                "operator": "Equal",
                "value": "defer-server",
                "effect": "NoSchedule"
            }])
        );
        assert_eq!(pod["imagePullSecrets"], json!([{"name": "ghcr-pull"}]));
    }
    assert!(
        stateful["affinity"]["podAntiAffinity"].is_object(),
        "placement must not replace the operator-owned anti-affinity"
    );
}

/// Without `spec.placement` or `spec.imagePullSecrets` the pods carry none of
/// those fields, the previous behavior.
#[test]
fn no_placement_renders_no_selector_tolerations_or_pull_secrets() {
    let objects = operator::render::render(&production());
    let stateful = &object(&objects, "StatefulSet", "jobs")["spec"]["template"]["spec"];
    let cron = &object(&objects, "CronJob", "jobs-backup")["spec"]["jobTemplate"]["spec"]
        ["template"]["spec"];
    for pod in [stateful, cron] {
        assert!(pod.get("nodeSelector").is_none_or(Value::is_null), "{pod}");
        assert!(
            pod.get("tolerations")
                .is_none_or(|t| t.as_array().is_some_and(Vec::is_empty)),
            "{pod}"
        );
        assert!(pod.get("imagePullSecrets").is_none(), "{pod}");
    }
}

/// An empty placement block is the same as none.
#[test]
fn empty_placement_renders_no_selector() {
    let objects = operator::render::render(&placed(json!({}), json!([])));
    let stateful = &object(&objects, "StatefulSet", "jobs")["spec"]["template"]["spec"];
    assert!(stateful.get("nodeSelector").is_none_or(Value::is_null));
    assert!(stateful.get("imagePullSecrets").is_none());
}

#[test]
fn generated_crd_carries_placement_and_pull_secrets() {
    let yaml = operator::crd_yaml();
    for field in [
        "placement:",
        "nodeSelector:",
        "tolerations:",
        "tolerationSeconds:",
        "imagePullSecrets:",
    ] {
        assert!(yaml.contains(field), "CRD is missing {field}");
    }
}

fn body_limit_env(spec_extra: Value) -> Value {
    let mut spec = json!({
        "image": "registry/defer:1",
        "replicasPerShard": 3,
        "voterCount": 3
    });
    spec.as_object_mut()
        .unwrap()
        .extend(spec_extra.as_object().unwrap().clone());
    let defer: Defer = serde_json::from_value(json!({
        "apiVersion": "defer.dev/v1alpha1",
        "kind": "Defer",
        "metadata": {"name": "jobs", "namespace": "payments", "uid": "u-1"},
        "spec": spec
    }))
    .unwrap();
    let objects = operator::render::render(&defer);
    let stateful = object(&objects, "StatefulSet", "jobs");
    named(
        &stateful["spec"]["template"]["spec"]["containers"][0]["env"],
        "DEFER_BODY_LIMIT_BYTES",
    )
    .clone()
}

/// #18: `spec.bodyLimitBytes` reaches the pod, and omitting it keeps the
/// 8 MiB `defer serve` default.
#[test]
fn body_limit_bytes_reaches_the_pod_env() {
    assert_eq!(
        body_limit_env(json!({"bodyLimitBytes": 1_048_576}))["value"],
        "1048576"
    );
    assert_eq!(
        body_limit_env(json!({}))["value"],
        (8 * 1024 * 1024).to_string()
    );
}

/// GKE's managed Secrets Store add-on registers a different CSI driver name
/// than the community default (#2456/#2457); an explicit
/// `tokensSecretCsiDriver` must render that name on the pod volume.
#[test]
fn tokens_secret_csi_driver_overrides_the_default_csi_driver_name() {
    let defer: Defer = serde_json::from_value(json!({
        "apiVersion": "defer.dev/v1alpha1",
        "kind": "Defer",
        "metadata": {"name": "jobs", "namespace": "payments", "uid": "u-1"},
        "spec": {
            "image": "registry/defer:1",
            "replicasPerShard": 3,
            "voterCount": 3,
            "storage": "100Gi",
            "auth": "required",
            "tokensSecretProviderClass": "defer-registry-csi",
            "tokensSecretCsiDriver": "secrets-store-gke.csi.k8s.io"
        }
    }))
    .unwrap();
    let objects = operator::render::render(&defer);
    let stateful = object(&objects, "StatefulSet", "jobs");
    let vol = named(
        &stateful["spec"]["template"]["spec"]["volumes"],
        "defer-token-registry",
    );
    assert_eq!(vol["csi"]["driver"], "secrets-store-gke.csi.k8s.io");
    assert_eq!(
        vol["csi"]["volumeAttributes"]["secretProviderClass"],
        "defer-registry-csi"
    );
}

#[test]
fn generated_crd_carries_limits() {
    let yaml = operator::crd_yaml();
    for field in ["limits:", "cpu:", "memory:"] {
        assert!(yaml.contains(field), "CRD is missing {field}");
    }
}

/// Specifying `spec.limits` renders `resources.limits` on the StatefulSet
/// serving container while keeping the configured `requests`. Omitting
/// `limits` or providing empty entries renders no `limits` key.
#[test]
fn render_limits_on_statefulset_container() {
    // 1. When limits are set, they appear alongside existing requests.
    let mut defer = production();
    defer.spec.limits = Some(operator::DeferResourceLimits {
        cpu: Some("4".into()),
        memory: Some("16Gi".into()),
    });
    let objects = operator::render::render(&defer);
    let stateful = object(&objects, "StatefulSet", "jobs");
    let container = &stateful["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(
        container["resources"]["requests"],
        json!({"cpu": "2", "memory": "8Gi"})
    );
    assert_eq!(
        container["resources"]["limits"],
        json!({"cpu": "4", "memory": "16Gi"})
    );

    // 2. When limits is unset (production default), no limits key is rendered.
    let objects_unset = operator::render::render(&production());
    let stateful_unset = object(&objects_unset, "StatefulSet", "jobs");
    let container_unset = &stateful_unset["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(
        container_unset["resources"]["requests"],
        json!({"cpu": "2", "memory": "8Gi"})
    );
    assert!(
        container_unset["resources"].get("limits").is_none(),
        "limits key must be absent when unset"
    );

    // 3. When limits has both entries None or empty, no limits key is rendered.
    let mut defer_empty = production();
    defer_empty.spec.limits = Some(operator::DeferResourceLimits {
        cpu: None,
        memory: None,
    });
    let objects_empty = operator::render::render(&defer_empty);
    let stateful_empty = object(&objects_empty, "StatefulSet", "jobs");
    let container_empty = &stateful_empty["spec"]["template"]["spec"]["containers"][0];
    assert!(
        container_empty["resources"].get("limits").is_none(),
        "limits key must be absent when entries are None"
    );

    let mut defer_blank = production();
    defer_blank.spec.limits = Some(operator::DeferResourceLimits {
        cpu: Some("  ".into()),
        memory: Some("".into()),
    });
    let objects_blank = operator::render::render(&defer_blank);
    let stateful_blank = object(&objects_blank, "StatefulSet", "jobs");
    let container_blank = &stateful_blank["spec"]["template"]["spec"]["containers"][0];
    assert!(
        container_blank["resources"].get("limits").is_none(),
        "limits key must be absent when entries are whitespace/empty"
    );
}

/// When a limit quantity is lower than the matching request (e.g. CPU 500m < 2
/// or memory 4Gi < 8Gi), the operator does not render the lower limit silently:
/// it raises the limit to the request quantity.
#[test]
fn limits_lower_than_requests_are_raised_to_requests() {
    let mut defer = production();
    // Requests in production() are cpu: "2" (2000m) and memory: "8Gi"
    defer.spec.limits = Some(operator::DeferResourceLimits {
        cpu: Some("500m".into()),
        memory: Some("4Gi".into()),
    });
    let objects = operator::render::render(&defer);
    let stateful = object(&objects, "StatefulSet", "jobs");
    let container = &stateful["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(
        container["resources"]["requests"],
        json!({"cpu": "2", "memory": "8Gi"})
    );
    assert_eq!(
        container["resources"]["limits"],
        json!({"cpu": "2", "memory": "8Gi"}),
        "limits lower than requests must be raised to match the request"
    );

    // Also verify plain cores form: limit 1 core < request 2 cores
    let mut defer_cores = production();
    defer_cores.spec.limits = Some(operator::DeferResourceLimits {
        cpu: Some("1".into()),
        memory: Some("16Gi".into()),
    });
    let objects_cores = operator::render::render(&defer_cores);
    let stateful_cores = object(&objects_cores, "StatefulSet", "jobs");
    let container_cores = &stateful_cores["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(container_cores["resources"]["limits"]["cpu"], "2");
    assert_eq!(container_cores["resources"]["limits"]["memory"], "16Gi");
}
// HANDWRITE-END
