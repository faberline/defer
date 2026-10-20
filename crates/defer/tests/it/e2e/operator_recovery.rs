//! Suite `operator_recovery`: the Defer CR `defer` of k8s/overlays/gke-test,
//! reconciled by the shipped operator into StatefulSet/defer and its one pod
//! defer-0. It checks that the operator renders the expected topology on the
//! placed pool, the image under test runs in every pod, and the queue and
//! task state survives the replacement of the serving pod with its PVC kept.

use std::time::Duration;

use serde_json::{json, Value};

use super::{
    assert_pods_run, endpoint, evidence, kubectl, kubectl_json, only_container, try_kubectl,
    wait_for, Expected,
};

const SUITE: &str = "operator_recovery";
const DEFER: &str = "defer";
const POD: &str = "defer-0";
const PVC: &str = "data-defer-0";
const POOL: &str = "defer-server";
const POOL_LABEL: &str = "faberline.dev/pool";
const SERVER_LABEL: &str =
    "app.kubernetes.io/name=defer,app.kubernetes.io/instance=defer,app.kubernetes.io/component=server";
const OPERATOR_LABEL: &str = "app.kubernetes.io/name=defer-operator";
const QUEUE: &str = "jobs";
const TASK_A: &str = "before-restart-a";
const TASK_B: &str = "before-restart-b";

/// The run waits only for the operator rollout. The operator creates the
/// StatefulSet after that, and pool-defer-server may still have to add a node
/// for defer-0.
const FIRST_POD_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const REPLACEMENT_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const API_TIMEOUT: Duration = Duration::from_secs(2 * 60);

fn selector() -> Value {
    json!({
        "app.kubernetes.io/name": "defer",
        "app.kubernetes.io/instance": "defer",
        "app.kubernetes.io/component": "server"
    })
}

/// The operator renders StatefulSet/defer, its two Services, and its PDB as
/// the shipped contract says, places defer-0 on pool-defer-server, binds a
/// PVC of at least the requested 1Gi, and runs the image under test in the
/// operator and in defer-0.
#[test]
#[ignore = "e2e: needs E2E_ENDPOINT"]
fn operator_renders_the_placed_topology() {
    endpoint();
    let expected = Expected::from_env();

    let uid = wait_for_ready_pod(None, FIRST_POD_TIMEOUT);
    evidence(SUITE, "pod ready", json!({ "pod": POD, "uid": uid }));

    let pvc = wait_for("PVC data-defer-0 Bound", API_TIMEOUT, || {
        let pvc = kubectl_json(&["get", "pvc", PVC]);
        if pvc["status"]["phase"] == "Bound" {
            Ok(pvc)
        } else {
            Err(format!("phase {}", pvc["status"]["phase"]))
        }
    });
    assert_eq!(pvc["spec"]["resources"]["requests"]["storage"], "1Gi");
    let capacity = pvc["status"]["capacity"]["storage"]
        .as_str()
        .unwrap_or_default();
    // A local-path provisioner gives exactly 1Gi; a cloud disk may round up,
    // so the floor is the contract.
    assert!(
        gibibytes(capacity).is_some_and(|gi| gi >= 1),
        "PVC {PVC} capacity {capacity:?} is under the requested 1Gi"
    );
    evidence(
        SUITE,
        "PVC bound",
        json!({ "pvc": PVC, "request": "1Gi", "capacity": capacity }),
    );

    assert_statefulset();
    assert_services_and_pdb();
    evidence(
        SUITE,
        "operator topology",
        json!({ "statefulset": DEFER, "services": [DEFER, "defer-headless"], "pdb": DEFER }),
    );

    let pod = kubectl_json(&["get", "pod", POD]);
    let spec = &pod["spec"];
    assert_eq!(
        spec["nodeSelector"][POOL_LABEL], POOL,
        "defer-0 nodeSelector"
    );
    assert!(
        spec["tolerations"]
            .as_array()
            .is_some_and(|all| all.iter().any(|t| {
                t["key"] == POOL_LABEL && t["value"] == POOL && t["effect"] == "NoSchedule"
            })),
        "defer-0 has no toleration {POOL_LABEL}={POOL}:NoSchedule: {}",
        spec["tolerations"]
    );
    assert_eq!(spec["imagePullSecrets"], json!([{ "name": "ghcr-pull" }]));
    // The Job's Role is namespaced, so it cannot read Node labels; the
    // nodeSelector above is what pins the pool.
    let node = spec["nodeName"].as_str().unwrap_or_default().to_owned();
    assert!(!node.is_empty(), "defer-0 is Ready but has no nodeName");
    evidence(
        SUITE,
        "placement",
        json!({ "pod": POD, "node": node, "pool": POOL, "image_pull_secret": "ghcr-pull" }),
    );

    let status = wait_for("Defer/defer status.phase Ready", API_TIMEOUT, || {
        let status = kubectl_json(&["get", "defer", DEFER])["status"].clone();
        if status["phase"] == "Ready" {
            Ok(status)
        } else {
            Err(status.to_string())
        }
    });
    assert_eq!(status["readyReplicas"], 1);
    assert_eq!(status["desiredReplicas"], 1);
    evidence(SUITE, "CR status", json!({ "status": status }));

    assert_identity(&expected);
}

/// Queue config and two scheduled tasks committed before the serving pod is
/// replaced are all there after it, on the same PVC; pause and cancel still
/// commit after the recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn queue_state_survives_the_serving_pod_replacement() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    let first_uid = wait_for_ready_pod(None, FIRST_POD_TIMEOUT);
    wait_healthy(&http, &base).await;

    let queue = send(http.put(format!("{base}/v1/queues/{QUEUE}")).json(&json!({
        "max_in_flight": 100,
        "max_dispatch_per_tick": 100,
        "max_dispatches_per_second": 100,
        "max_burst_size": 100,
        "lease_ttl_ms": 30000,
        "retry_backoff_ms": 1000
    })))
    .await;
    assert_eq!(queue["queue"], QUEUE, "{queue}");
    assert_eq!(queue["control_state"], "Running", "{queue}");
    assert_eq!(queue["task_count"], 0, "{queue}");
    evidence(SUITE, "queue configured", json!({ "queue": QUEUE }));

    let created = send(
        http.post(format!("{base}/v1/queues/{QUEUE}/tasks:batch"))
            .json(&json!({ "tasks": [task(TASK_A, "a", 10), task(TASK_B, "b", 20)] })),
    )
    .await;
    assert_eq!(created["created"], 2, "{created}");
    assert_task(&http, &base, TASK_A, "Scheduled").await;
    assert_queue(&http, &base, "Running", 0).await;
    evidence(
        SUITE,
        "tasks scheduled",
        json!({ "tasks": [TASK_A, TASK_B], "status": "Scheduled" }),
    );

    let kept_pvc = pvc_uid();
    kubectl(&["delete", "pod", POD, "--wait=false"]);
    let replacement_uid = wait_for_ready_pod(Some(&first_uid), REPLACEMENT_TIMEOUT);
    assert_ne!(
        replacement_uid, first_uid,
        "the StatefulSet did not replace {POD}"
    );
    assert_eq!(pvc_uid(), kept_pvc, "PVC {PVC} was replaced, not kept");
    assert_eq!(
        kubectl_json(&["get", "pvc", PVC])["status"]["phase"],
        "Bound"
    );
    evidence(
        SUITE,
        "pod replaced, PVC kept",
        json!({ "old_uid": first_uid, "new_uid": replacement_uid, "pvc_uid": kept_pvc }),
    );

    wait_healthy(&http, &base).await;
    assert_queue(&http, &base, "Running", 0).await;
    assert_task(&http, &base, TASK_B, "Scheduled").await;
    evidence(
        SUITE,
        "state recovered",
        json!({ "queue": QUEUE, "task_count": 2, "terminal_count": 0 }),
    );

    let paused = send(
        http.post(format!("{base}/v1/queues/{QUEUE}/control"))
            .json(&json!({ "state": "Paused" })),
    )
    .await;
    assert_eq!(paused["control_state"], "Paused", "{paused}");
    let cancel = http
        .delete(format!("{base}/v1/queues/{QUEUE}/tasks/{TASK_A}"))
        .send()
        .await
        .expect("DELETE task");
    assert_eq!(cancel.status().as_u16(), 204, "cancel {TASK_A}");
    assert_task(&http, &base, TASK_A, "Canceled").await;
    assert_queue(&http, &base, "Paused", 1).await;
    evidence(
        SUITE,
        "pause and cancel after recovery",
        json!({ "control_state": "Paused", "canceled": TASK_A, "terminal_count": 1 }),
    );
}

fn task(id: &str, kind: &str, priority: u32) -> Value {
    json!({
        "task_id": id,
        "target": { "url": format!("http://example.invalid/{kind}"), "method": "POST", "headers": {} },
        "payload": { "kind": kind },
        "schedule_at": "2099-01-01T00:00:00Z",
        "priority": priority,
        "max_attempts": 3
    })
}

/// Sends a request that must succeed and returns its JSON body.
async fn send(request: reqwest::RequestBuilder) -> Value {
    let response = request.send().await.expect("request");
    let status = response.status();
    let url = response.url().clone();
    let body = response.text().await.expect("body");
    assert!(status.is_success(), "{url}: {status}: {body}");
    serde_json::from_str(&body).unwrap_or_else(|err| panic!("{url}: not JSON: {err}: {body}"))
}

async fn assert_queue(http: &reqwest::Client, base: &str, state: &str, terminal: u64) {
    let queue = send(http.get(format!("{base}/v1/queues/{QUEUE}"))).await;
    assert_eq!(queue["control_state"], state, "{queue}");
    assert_eq!(queue["task_count"], 2, "{queue}");
    assert_eq!(queue["terminal_count"], terminal, "{queue}");
}

async fn assert_task(http: &reqwest::Client, base: &str, id: &str, status: &str) {
    let task = send(http.get(format!("{base}/v1/queues/{QUEUE}/tasks/{id}"))).await;
    assert_eq!(task["task_id"], id, "{task}");
    assert_eq!(task["status"], status, "{task}");
}

/// Waits until `GET /healthz` answers 2xx through Service/defer.
async fn wait_healthy(http: &reqwest::Client, base: &str) {
    let deadline = tokio::time::Instant::now() + API_TIMEOUT;
    loop {
        let last = match http.get(format!("{base}/healthz")).send().await {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => response.status().to_string(),
            Err(err) => err.to_string(),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "{base}/healthz: not healthy within {API_TIMEOUT:?}; last saw: {last}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Waits until defer-0 exists with a uid other than `previous` and is Ready,
/// and returns its uid.
fn wait_for_ready_pod(previous: Option<&str>, timeout: Duration) -> String {
    wait_for("pod defer-0 Ready", timeout, || {
        let out = try_kubectl(&["get", "pod", POD, "-o", "json"])?;
        let pod: Value = serde_json::from_str(&out).map_err(|err| err.to_string())?;
        let uid = pod["metadata"]["uid"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let ready = pod["status"]["conditions"].as_array().is_some_and(|all| {
            all.iter()
                .any(|c| c["type"] == "Ready" && c["status"] == "True")
        });
        if !uid.is_empty() && Some(uid.as_str()) != previous && ready {
            Ok(uid)
        } else {
            Err(format!(
                "uid {uid:?} ready {ready} phase {}",
                pod["status"]["phase"]
            ))
        }
    })
}

fn pvc_uid() -> String {
    let uid = kubectl_json(&["get", "pvc", PVC])["metadata"]["uid"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(!uid.is_empty(), "PVC {PVC} has no uid");
    uid
}

/// Whole GiB of a Kubernetes quantity in `Gi`, `Ti`, or `G` (rounded down).
fn gibibytes(quantity: &str) -> Option<u64> {
    if let Some(n) = quantity.strip_suffix("Gi") {
        n.parse().ok()
    } else if let Some(n) = quantity.strip_suffix("Ti") {
        n.parse::<u64>().ok().map(|n| n * 1024)
    } else if let Some(n) = quantity.strip_suffix('G') {
        n.parse::<u64>().ok().map(|n| n * 1_000_000_000 / (1 << 30))
    } else {
        None
    }
}

fn assert_statefulset() {
    let sts = kubectl_json(&["get", "statefulset", DEFER]);
    let spec = &sts["spec"];
    assert_eq!(spec["replicas"], 1);
    assert_eq!(spec["serviceName"], "defer-headless");
    assert_eq!(spec["selector"]["matchLabels"], selector());
    let labels = &spec["template"]["metadata"]["labels"];
    for (key, value) in selector().as_object().unwrap() {
        assert_eq!(&labels[key], value, "template label {key}");
    }
    let container = only_container(&spec["template"]["spec"]);
    let probe = |name: &str, path: &str, period: u64| {
        let probe = &container[name];
        assert_eq!(probe["httpGet"]["path"], path, "{name}");
        assert_eq!(probe["httpGet"]["port"], "http", "{name}");
        assert_eq!(probe["periodSeconds"], period, "{name}");
    };
    probe("readinessProbe", "/readyz", 5);
    probe("livenessProbe", "/healthz", 15);
    probe("startupProbe", "/healthz", 5);
    assert_eq!(container["startupProbe"]["failureThreshold"], 120);
    let data_mounts: Vec<&Value> = container["volumeMounts"]
        .as_array()
        .map(|all| {
            all.iter()
                .filter(|m| {
                    m["name"] == "data"
                        && m["mountPath"] == "/data"
                        && !m["readOnly"].as_bool().unwrap_or(false)
                })
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(data_mounts.len(), 1, "one writable data mount at /data");
    let claim = &spec["volumeClaimTemplates"][0];
    assert_eq!(claim["metadata"]["name"], "data");
    assert_eq!(claim["spec"]["resources"]["requests"]["storage"], "1Gi");
}

fn assert_services_and_pdb() {
    let ports = |service: &Value| -> Value {
        service["spec"]["ports"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                json!({
                    "name": p["name"], "port": p["port"],
                    "targetPort": p["targetPort"], "protocol": p["protocol"]
                })
            })
            .collect()
    };
    let headless = kubectl_json(&["get", "service", "defer-headless"]);
    assert_eq!(headless["spec"]["clusterIP"], "None");
    assert_eq!(headless["spec"]["publishNotReadyAddresses"], true);
    assert_eq!(headless["spec"]["selector"], selector());
    assert_eq!(
        ports(&headless),
        json!([
            { "name": "http", "port": 7141, "targetPort": "http", "protocol": "TCP" },
            { "name": "raft", "port": 7142, "targetPort": "raft", "protocol": "TCP" }
        ])
    );
    let client = kubectl_json(&["get", "service", DEFER]);
    assert_eq!(client["spec"]["type"], "ClusterIP");
    assert_eq!(client["spec"]["selector"], selector());
    assert_eq!(
        ports(&client),
        json!([{ "name": "http", "port": 7141, "targetPort": "http", "protocol": "TCP" }])
    );
    let pdb = kubectl_json(&["get", "poddisruptionbudget", DEFER]);
    assert_eq!(pdb["spec"]["maxUnavailable"], 1);
    assert_eq!(pdb["spec"]["selector"]["matchLabels"], selector());
}

/// The operator and defer-0 run the image under test, pinned by digest, and
/// the binary in it reports the expected version.
fn assert_identity(expected: &Expected) {
    let operator = assert_pods_run(expected, OPERATOR_LABEL, 1);
    let server = assert_pods_run(expected, SERVER_LABEL, 1);
    let version = kubectl(&["exec", POD, "--", "defer", "--version"]);
    let version = version.trim();
    assert_eq!(
        version,
        format!("defer {}", expected.version),
        "defer --version in {POD}"
    );
    evidence(
        SUITE,
        "identity",
        json!({
            "image": expected.image,
            "runtime_digest": expected.runtime_digest,
            "git_sha": expected.git_sha,
            "version": version,
            "operator_image_ids": operator,
            "server_image_ids": server,
        }),
    );
}

#[test]
fn gibibytes_reads_cloud_and_local_capacities() {
    assert_eq!(gibibytes("1Gi"), Some(1));
    assert_eq!(gibibytes("10Gi"), Some(10));
    assert_eq!(gibibytes("1Ti"), Some(1024));
    assert_eq!(gibibytes("2G"), Some(1));
    assert_eq!(gibibytes("512Mi"), None);
}
