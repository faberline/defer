//! Suite `operator_recovery`: the Defer CR `defer` of k8s/overlays/gke-test,
//! reconciled by the shipped operator into StatefulSet/defer with three pods,
//! defer-0 to defer-2. It checks that the operator renders the expected
//! topology on the placed pool with one pod per node, the image under test runs
//! in every pod, and the queue and task state survives the replacement of one
//! pod with its PVC kept.

use std::collections::HashSet;
use std::time::Duration;

use serde_json::{json, Value};

use super::{
    assert_pods_run, endpoint, evidence, kubectl, kubectl_json, only_container, try_kubectl,
    wait_for, Expected,
};

const SUITE: &str = "operator_recovery";
const DEFER: &str = "defer";
/// The StatefulSet has REPLICAS pods, `defer-0` to `defer-2`, each with its own
/// PVC `data-defer-0` to `data-defer-2`.
const REPLICAS: usize = 3;
/// The pod that the recovery test deletes. The test does not assume it leads.
const KILLED: usize = 1;
/// The PVC request in k8s/overlays/gke-test/defer-cr.yaml. A
/// hyperdisk-balanced disk on the N4 test pools is at least 4 GB.
const STORAGE: &str = "4Gi";
const STORAGE_GI: u64 = 4;
const POOL: &str = "defer-server";
const POOL_LABEL: &str = "faberline.dev/pool";
const SERVER_LABEL: &str =
    "app.kubernetes.io/name=defer,app.kubernetes.io/instance=defer,app.kubernetes.io/component=server";
const OPERATOR_LABEL: &str = "app.kubernetes.io/name=defer-operator";
const QUEUE: &str = "jobs";
const TASK_A: &str = "before-restart-a";
const TASK_B: &str = "before-restart-b";

/// The run waits only for the operator rollout. The operator creates the
/// StatefulSet after that, and pool-defer-server may still have to add nodes
/// for the three defer pods.
const FIRST_POD_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const REPLACEMENT_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const API_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const SCALED_REPLICAS: usize = 2;
const SCALE_QUEUE: &str = "scale-jobs";
const SCALE_TASK: &str = "scale-task";
const TOKEN_SECRET: &str = "defer-admin-token";
const SCALE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// The name of the StatefulSet pod with ordinal `i`.
fn pod(i: usize) -> String {
    format!("defer-{i}")
}

/// The name of the PVC of the StatefulSet pod with ordinal `i`.
fn pvc(i: usize) -> String {
    format!("data-defer-{i}")
}

fn selector() -> Value {
    json!({
        "app.kubernetes.io/name": "defer",
        "app.kubernetes.io/instance": "defer",
        "app.kubernetes.io/component": "server"
    })
}

/// The operator renders StatefulSet/defer, its two Services, and its PDB as
/// the shipped contract says, places the three defer pods on pool-defer-server
/// with one pod per node, binds a PVC of at least the requested 4Gi for each,
/// and runs the image under test in the operator and in every defer pod.
#[test]
#[ignore = "e2e: needs E2E_ENDPOINT"]
fn operator_renders_the_placed_topology() {
    endpoint();
    let expected = Expected::from_env();

    let mut pods = Vec::new();
    for i in 0..REPLICAS {
        let name = pod(i);
        let uid = wait_for_ready_pod(&name, None, FIRST_POD_TIMEOUT);
        evidence(SUITE, "pod ready", json!({ "pod": name, "uid": uid }));
        pods.push(name);
    }

    for i in 0..REPLICAS {
        let claim = pvc(i);
        let bound = wait_for(&format!("PVC {claim} Bound"), API_TIMEOUT, || {
            let pvc = kubectl_json(&["get", "pvc", claim.as_str()]);
            if pvc["status"]["phase"] == "Bound" {
                Ok(pvc)
            } else {
                Err(format!("phase {}", pvc["status"]["phase"]))
            }
        });
        assert_eq!(bound["spec"]["resources"]["requests"]["storage"], STORAGE);
        let capacity = bound["status"]["capacity"]["storage"]
            .as_str()
            .unwrap_or_default();
        // A cloud disk may round up, so the floor is the contract.
        assert!(
            gibibytes(capacity).is_some_and(|gi| gi >= STORAGE_GI),
            "PVC {claim} capacity {capacity:?} is under the requested {STORAGE}"
        );
        evidence(
            SUITE,
            "PVC bound",
            json!({ "pvc": claim, "request": STORAGE, "capacity": capacity }),
        );
    }

    assert_statefulset();
    assert_services_and_pdb();
    evidence(
        SUITE,
        "operator topology",
        json!({ "statefulset": DEFER, "services": [DEFER, "defer-headless"], "pdb": DEFER }),
    );

    let mut nodes = Vec::new();
    for name in &pods {
        let pod = kubectl_json(&["get", "pod", name.as_str()]);
        let spec = &pod["spec"];
        assert_eq!(
            spec["nodeSelector"][POOL_LABEL], POOL,
            "{name} nodeSelector"
        );
        assert!(
            spec["tolerations"]
                .as_array()
                .is_some_and(|all| all.iter().any(|t| {
                    t["key"] == POOL_LABEL && t["value"] == POOL && t["effect"] == "NoSchedule"
                })),
            "{name} has no toleration {POOL_LABEL}={POOL}:NoSchedule: {}",
            spec["tolerations"]
        );
        assert_eq!(
            spec["imagePullSecrets"],
            json!([{ "name": "ghcr-pull" }]),
            "{name} imagePullSecrets"
        );
        // The Job's Role is namespaced, so it cannot read Node labels; the
        // nodeSelector above is what pins the pool.
        let node = spec["nodeName"].as_str().unwrap_or_default().to_owned();
        assert!(!node.is_empty(), "{name} is Ready but has no nodeName");
        evidence(
            SUITE,
            "placement",
            json!({ "pod": name, "node": node, "pool": POOL, "image_pull_secret": "ghcr-pull" }),
        );
        nodes.push(node);
    }
    let distinct: HashSet<&String> = nodes.iter().collect();
    assert_eq!(
        distinct.len(),
        REPLICAS,
        "defer pods share a node (one pod per node): {nodes:?}"
    );

    let status = wait_for("Defer/defer status.phase Ready", API_TIMEOUT, || {
        let status = kubectl_json(&["get", "defer", DEFER])["status"].clone();
        if status["phase"] == "Ready" {
            Ok(status)
        } else {
            Err(status.to_string())
        }
    });
    assert_eq!(status["readyReplicas"], REPLICAS as u64);
    assert_eq!(status["desiredReplicas"], REPLICAS as u64);
    evidence(SUITE, "CR status", json!({ "status": status }));

    assert_identity(&expected);
}

/// Queue config and two scheduled tasks committed before one pod of the group
/// is replaced are all there after it recovers, with its PVC kept; pause and
/// cancel still commit after the recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn queue_state_survives_a_pod_loss_in_a_three_replica_group() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    let uids: Vec<String> = (0..REPLICAS)
        .map(|i| wait_for_ready_pod(&pod(i), None, FIRST_POD_TIMEOUT))
        .collect();
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

    let victim = pod(KILLED);
    let victim_pvc = pvc(KILLED);
    let first_uid = &uids[KILLED];
    let kept_pvc = pvc_uid(&victim_pvc);
    kubectl(&["delete", "pod", victim.as_str(), "--wait=false"]);
    let replacement_uid = wait_for_ready_pod(&victim, Some(first_uid), REPLACEMENT_TIMEOUT);
    assert_ne!(
        &replacement_uid, first_uid,
        "the StatefulSet did not replace {victim}"
    );
    assert_eq!(
        pvc_uid(&victim_pvc),
        kept_pvc,
        "PVC {victim_pvc} was replaced, not kept"
    );
    assert_eq!(
        kubectl_json(&["get", "pvc", victim_pvc.as_str()])["status"]["phase"],
        "Bound"
    );
    evidence(
        SUITE,
        "pod replaced, PVC kept",
        json!({
            "pod": victim,
            "pvc": victim_pvc,
            "old_uid": first_uid,
            "new_uid": replacement_uid,
            "pvc_uid": kept_pvc
        }),
    );

    for i in (0..REPLICAS).filter(|i| *i != KILLED) {
        wait_for_ready_pod(&pod(i), None, REPLACEMENT_TIMEOUT);
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn scales_replicas_per_shard_by_one_layer_and_back() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    for i in 0..REPLICAS {
        wait_for_ready_pod(&pod(i), None, FIRST_POD_TIMEOUT);
    }
    wait_healthy(&http, &base).await;

    // Auth is off, so the token is ignored by the server; the operator only
    // needs the Secret to exist and to hold the key `token`.
    kubectl(&[
        "create",
        "secret",
        "generic",
        TOKEN_SECRET,
        "--from-literal=token=e2e-auth-is-off",
    ]);
    let token = admin_token_patch().to_string();
    kubectl(&[
        "patch",
        "defer",
        DEFER,
        "--type",
        "merge",
        "-p",
        token.as_str(),
    ]);
    wait_for_replicas(REPLICAS);
    evidence(
        SUITE,
        "operator token ready",
        json!({ "secret": TOKEN_SECRET }),
    );

    let queue = put_scale_queue_when_ready(&http, &base).await;
    assert_eq!(queue["queue"], SCALE_QUEUE, "{queue}");
    let created = send(
        http.post(format!("{base}/v1/queues/{SCALE_QUEUE}/tasks:batch"))
            .json(&json!({ "tasks": [task(SCALE_TASK, "s", 10)] })),
    )
    .await;
    assert_eq!(created["created"], 1, "{created}");
    let fetched =
        send(http.get(format!("{base}/v1/queues/{SCALE_QUEUE}/tasks/{SCALE_TASK}"))).await;
    assert_eq!(fetched["status"], "Scheduled", "{fetched}");

    let kept_pvc = pvc_uid(&pvc(REPLICAS - 1));
    let shrink = replicas_patch(SCALED_REPLICAS).to_string();
    kubectl(&[
        "patch",
        "defer",
        DEFER,
        "--type",
        "merge",
        "-p",
        shrink.as_str(),
    ]);
    wait_for_replicas(SCALED_REPLICAS);
    let gone = pod(REPLICAS - 1);
    wait_for(
        &format!("pod {gone} removed"),
        API_TIMEOUT,
        || match try_kubectl(&["get", "pod", gone.as_str()]) {
            Err(err) if err.contains("NotFound") => Ok(()),
            Err(err) => Err(err),
            Ok(_) => Err("still exists".to_owned()),
        },
    );
    assert_eq!(
        pvc_uid(&pvc(REPLICAS - 1)),
        kept_pvc,
        "PVC {} was replaced, not kept",
        pvc(REPLICAS - 1)
    );
    wait_healthy(&http, &base).await;
    put_scale_queue_when_ready(&http, &base).await;
    let after_in = send(http.get(format!("{base}/v1/queues/{SCALE_QUEUE}"))).await;
    assert_eq!(after_in["task_count"], 1, "{after_in}");
    let still = send(http.get(format!("{base}/v1/queues/{SCALE_QUEUE}/tasks/{SCALE_TASK}"))).await;
    assert_eq!(still["status"], "Scheduled", "{still}");
    evidence(
        SUITE,
        "scaled in",
        json!({ "replicas": SCALED_REPLICAS, "pvc_kept": true }),
    );

    let grow = replicas_patch(REPLICAS).to_string();
    kubectl(&[
        "patch",
        "defer",
        DEFER,
        "--type",
        "merge",
        "-p",
        grow.as_str(),
    ]);
    wait_for_replicas(REPLICAS);
    wait_for_ready_pod(&pod(REPLICAS - 1), None, SCALE_TIMEOUT);
    wait_healthy(&http, &base).await;
    put_scale_queue_when_ready(&http, &base).await;
    let after_out = send(http.get(format!("{base}/v1/queues/{SCALE_QUEUE}"))).await;
    assert_eq!(after_out["task_count"], 1, "{after_out}");
    let second = send(
        http.post(format!("{base}/v1/queues/{SCALE_QUEUE}/tasks:batch"))
            .json(&json!({ "tasks": [task("scale-task-after", "s2", 10)] })),
    )
    .await;
    assert_eq!(second["created"], 1, "{second}");
    let after_write = send(http.get(format!("{base}/v1/queues/{SCALE_QUEUE}"))).await;
    assert_eq!(after_write["task_count"], 2, "{after_write}");
    evidence(
        SUITE,
        "scaled out",
        json!({ "replicas": REPLICAS, "task_count": 2 }),
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

/// Sends `PUT /v1/queues/{SCALE_QUEUE}` (idempotent) until it answers 2xx, and
/// returns the JSON body. A 5xx such as `raft: no leader elected` or a
/// connection error is retried every 2 s until `API_TIMEOUT`; a 4xx fails at once.
async fn put_scale_queue_when_ready(http: &reqwest::Client, base: &str) -> Value {
    let url = format!("{base}/v1/queues/{SCALE_QUEUE}");
    let deadline = tokio::time::Instant::now() + API_TIMEOUT;
    loop {
        let request = http.put(&url).json(&json!({
            "max_in_flight": 100,
            "max_dispatch_per_tick": 100,
            "max_dispatches_per_second": 100,
            "max_burst_size": 100,
            "lease_ttl_ms": 30000,
            "retry_backoff_ms": 1000
        }));
        let last = match request.send().await {
            Ok(response) if response.status().is_success() => {
                let body = response.text().await.expect("body");
                return serde_json::from_str(&body)
                    .unwrap_or_else(|err| panic!("{url}: not JSON: {err}: {body}"));
            }
            Ok(response) if response.status().is_client_error() => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                panic!("{url}: {status}: {body}");
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                format!("{status}: {body}")
            }
            Err(err) => err.to_string(),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "{url}: no write accepted within {API_TIMEOUT:?}; last saw: {last}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
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

/// Waits until the pod `name` exists with a uid other than `previous` and is
/// Ready, and returns its uid.
fn wait_for_ready_pod(name: &str, previous: Option<&str>, timeout: Duration) -> String {
    wait_for(&format!("pod {name} Ready"), timeout, || {
        let out = try_kubectl(&["get", "pod", name, "-o", "json"])?;
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

fn pvc_uid(name: &str) -> String {
    let uid = kubectl_json(&["get", "pvc", name])["metadata"]["uid"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(!uid.is_empty(), "PVC {name} has no uid");
    uid
}

fn replicas_patch(replicas: usize) -> Value {
    json!({ "spec": { "replicasPerShard": replicas } })
}

fn admin_token_patch() -> Value {
    json!({ "spec": { "adminTokenSecret": TOKEN_SECRET } })
}

/// Waits until the StatefulSet and the Defer CR status both show `want`
/// replicas, all of them ready.
fn wait_for_replicas(want: usize) {
    wait_for(
        &format!("Defer/{DEFER} settled at {want} replicas"),
        SCALE_TIMEOUT,
        || {
            let sts = kubectl_json(&["get", "statefulset", DEFER]);
            let cr = kubectl_json(&["get", "defer", DEFER])["status"].clone();
            let ok = sts["spec"]["replicas"] == want
                && sts["status"]["readyReplicas"] == want
                && cr["settledReplicas"] == want
                && cr["readyReplicas"] == want
                && cr["desiredReplicas"] == want;
            if ok {
                Ok(())
            } else {
                Err(format!(
                    "sts spec {} ready {}; cr {cr}",
                    sts["spec"]["replicas"], sts["status"]["readyReplicas"]
                ))
            }
        },
    )
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
    assert_eq!(spec["replicas"], REPLICAS as u64);
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
    assert_eq!(claim["spec"]["resources"]["requests"]["storage"], STORAGE);
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

/// The operator and every defer pod run the image under test, pinned by
/// digest, and the binary in each pod reports the expected version.
fn assert_identity(expected: &Expected) {
    let operator = assert_pods_run(expected, OPERATOR_LABEL, 1);
    let server = assert_pods_run(expected, SERVER_LABEL, REPLICAS as u64);
    let mut versions = Vec::new();
    for i in 0..REPLICAS {
        let name = pod(i);
        let version = kubectl(&["exec", name.as_str(), "--", "defer", "--version"]);
        let version = version.trim().to_owned();
        assert_eq!(
            version,
            format!("defer {}", expected.version),
            "defer --version in {name}"
        );
        versions.push(version);
    }
    evidence(
        SUITE,
        "identity",
        json!({
            "image": expected.image,
            "runtime_digest": expected.runtime_digest,
            "git_sha": expected.git_sha,
            "versions": versions,
            "operator_image_ids": operator,
            "server_image_ids": server,
        }),
    );
}

#[test]
fn pod_and_pvc_names_follow_the_statefulset_ordinal() {
    assert_eq!(pod(0), "defer-0");
    assert_eq!(pod(2), "defer-2");
    assert_eq!(pvc(1), "data-defer-1");
    assert_eq!(pvc(0), "data-defer-0");
}

#[test]
fn killed_pod_is_one_of_the_replicas() {
    assert_eq!(REPLICAS, 3);
    assert!((0..REPLICAS).contains(&KILLED));
}

#[test]
fn replicas_patch_sets_replicas_per_shard() {
    assert_eq!(replicas_patch(2), json!({"spec": {"replicasPerShard": 2}}));
}

#[test]
fn admin_token_patch_names_the_secret() {
    assert_eq!(
        admin_token_patch(),
        json!({"spec": {"adminTokenSecret": "defer-admin-token"}})
    );
}

#[test]
fn scale_in_target_fits_the_pool() {
    assert!((1..REPLICAS).contains(&SCALED_REPLICAS));
}

#[test]
fn gibibytes_reads_cloud_and_local_capacities() {
    assert_eq!(gibibytes("1Gi"), Some(1));
    assert_eq!(gibibytes("10Gi"), Some(10));
    assert_eq!(gibibytes("1Ti"), Some(1024));
    assert_eq!(gibibytes("2G"), Some(1));
    assert_eq!(gibibytes("512Mi"), None);
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::{self, JoinHandle};

    use super::put_scale_queue_when_ready;

    /// Reads one HTTP request (headers and body) from `stream`.
    fn read_request(stream: &mut std::net::TcpStream) {
        let mut buf = Vec::new();
        let mut chunk = [0_u8; 1024];
        let header_end = loop {
            let n = stream.read(&mut chunk).expect("read request");
            assert!(n > 0, "client closed before the headers ended");
            buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let headers = String::from_utf8_lossy(&buf[..header_end]).to_ascii_lowercase();
        let length: usize = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .map(|value| value.trim().parse().expect("content-length"))
            .unwrap_or(0);
        while buf.len() < header_end + length {
            let n = stream.read(&mut chunk).expect("read body");
            assert!(n > 0, "client closed before the body ended");
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Starts a server on 127.0.0.1 that answers one request per `(status, body)`
    /// in `replies`, then stops. It returns the address and a join handle whose
    /// value is the number of requests it served.
    fn serve(replies: Vec<(&'static str, &'static str)>) -> (String, JoinHandle<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = format!("http://{}", listener.local_addr().expect("addr"));
        let handle = thread::spawn(move || {
            let mut served = 0;
            for (status, body) in replies {
                let (mut stream, _) = listener.accept().expect("accept");
                read_request(&mut stream);
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).expect("write");
                served += 1;
            }
            served
        });
        (addr, handle)
    }

    #[tokio::test]
    async fn put_scale_queue_when_ready_retries_a_500_then_returns_the_body() {
        let (base, server) = serve(vec![
            (
                "500 Internal Server Error",
                r#"{"error":"internal","message":"raft: no leader elected (cluster not ready)"}"#,
            ),
            ("200 OK", r#"{"queue":"scale-jobs"}"#),
        ]);
        let http = reqwest::Client::new();

        let queue = put_scale_queue_when_ready(&http, &base).await;

        assert_eq!(queue["queue"], "scale-jobs", "{queue}");
        assert_eq!(server.join().expect("server thread"), 2);
    }

    #[test]
    fn put_scale_queue_when_ready_fails_at_once_on_a_4xx() {
        let (base, server) = serve(vec![("400 Bad Request", r#"{"error":"bad_request"}"#)]);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let http = reqwest::Client::new();
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(put_scale_queue_when_ready(&http, &base))
        }))
        .is_err();

        assert!(panicked, "a 400 must panic");
        assert_eq!(server.join().expect("server thread"), 1);
    }
}
