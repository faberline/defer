//! Suite `shard_split`, part 1: start a one-shard group with an admin token
//! Secret, then create 6 queues with 400 due tasks each. It never dispatches
//! anything and no split runs here; part 2 raises `shardCount` on this group.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};

use super::{endpoint, evidence, kubectl, kubectl_json, required_env, try_kubectl, wait_for};

const SUITE: &str = "shard_split";
const DEFER: &str = "defer";
const TOKEN_SECRET: &str = "defer-admin-token";
const QUEUES: usize = 6;
const TASKS_PER_QUEUE: usize = 400;
const GROUP_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const DELETE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const API_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// `split-<q>`, for example `split-0`.
fn queue_name(q: usize) -> String {
    format!("split-{q}")
}

/// `s<q>-<i padded to 4 digits>`, for example `s0-0007`.
fn task_id(q: usize, i: usize) -> String {
    format!("s{q}-{i:04}")
}

/// Body of POST /v1/queues/<queue>/tasks. A due task, so the queue's rate
/// limit decides when it runs. A 2xx from `/healthz` acks it.
fn task_body(id: &str) -> Value {
    json!({
        "task_id": id,
        "target": {
            "url": "http://defer:7141/healthz",
            "method": "GET",
            "headers": {}
        },
        "payload": { "suite": SUITE, "id": id },
        "schedule_at": "2020-01-01T00:00:00Z",
        "priority": 10,
        "max_attempts": 3
    })
}

/// The live CR with the start spec for the split: one shard, one replica per
/// shard, one voter, and the admin token Secret. The test pool has 4 nodes
/// with one pod per node, so two shards of one replica fit and two shards of
/// three do not.
fn cr_for_start(live: &Value) -> Value {
    let mut spec = live["spec"].clone();
    if let Some(fields) = spec.as_object_mut() {
        fields.insert("shardCount".to_owned(), json!(1));
        fields.insert("replicasPerShard".to_owned(), json!(1));
        fields.insert("voterCount".to_owned(), json!(1));
        fields.insert("adminTokenSecret".to_owned(), json!(TOKEN_SECRET));
    }
    json!({
        "apiVersion": live["apiVersion"],
        "kind": live["kind"],
        "metadata": { "name": DEFER },
        "spec": spec
    })
}

/// PUT /v1/queues/<queue> body. One task a second for each queue, so 400
/// tasks last more than six minutes, longer than a split.
fn queue_policy() -> Value {
    json!({
        "max_in_flight": 1,
        "max_dispatch_per_tick": 1,
        "max_dispatches_per_second": 1,
        "max_burst_size": 1,
        "lease_ttl_ms": 30000,
        "retry_backoff_ms": 250
    })
}

/// `data-defer-0` and `data-defer-1`.
fn pvc_names() -> [String; 2] {
    [0, 1].map(|i| format!("data-defer-{i}"))
}

/// Whether a pod name is one of the group's pods (`defer-0`, `defer-1`, ...),
/// and not the operator or the test Job.
fn is_group_pod(name: &str) -> bool {
    name.strip_prefix("defer-")
        .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
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

/// Waits until `GET /healthz` answers 2xx.
async fn wait_healthy(http: &reqwest::Client, base: &str) {
    let deadline = tokio::time::Instant::now() + GROUP_TIMEOUT;
    loop {
        let last = match http.get(format!("{base}/healthz")).send().await {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => response.status().to_string(),
            Err(err) => err.to_string(),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "{base}/healthz: not healthy within {GROUP_TIMEOUT:?}; last saw: {last}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Waits until StatefulSet `defer` has `want` ready replicas.
fn wait_for_group_ready(want: u64, timeout: Duration) {
    wait_for(
        &format!("StatefulSet {DEFER} has {want} ready replicas"),
        timeout,
        || {
            let out = try_kubectl(&["get", "statefulset", DEFER, "-o", "json"])?;
            let sts: Value =
                serde_json::from_str(&out).map_err(|err| format!("not JSON: {err}"))?;
            let ready = sts["status"]["readyReplicas"].clone();
            if ready == want {
                Ok(())
            } else {
                Err(format!("readyReplicas {ready}"))
            }
        },
    );
}

/// Deletes the admin token Secret, the Defer CR and the group's PVCs, and
/// waits until no group pod and no `data-defer-` PVC is left. Every delete
/// ignores a missing object, so cleanup also works on a half-built state.
fn remove_group_and_pvcs() {
    let _ = try_kubectl(&["delete", "secret", TOKEN_SECRET, "--ignore-not-found"]);
    kubectl(&[
        "delete",
        "defer",
        DEFER,
        "--ignore-not-found",
        "--wait=true",
    ]);
    wait_for("old group pods gone", DELETE_TIMEOUT, || {
        let pods = kubectl_json(&["get", "pods"]);
        let left = names_where(&pods, is_group_pod);
        if left.is_empty() {
            Ok(())
        } else {
            Err(format!("pods left: {left:?}"))
        }
    });
    for name in pvc_names() {
        kubectl(&[
            "delete",
            "pvc",
            name.as_str(),
            "--ignore-not-found",
            "--wait=true",
        ]);
    }
    wait_for("PVCs gone", DELETE_TIMEOUT, || {
        let list = kubectl_json(&["get", "pvc"]);
        let left = names_where(&list, |name| name.starts_with("data-defer-"));
        if left.is_empty() {
            Ok(())
        } else {
            Err(format!("PVCs left: {left:?}"))
        }
    });
}

/// Removes the group on drop, so a panic in the test leaves no group behind.
struct Cleanup;

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(remove_group_and_pvcs));
    }
}

/// The names of the `items` of a kubectl list that match `keep`.
fn names_where(list: &Value, keep: impl Fn(&str) -> bool) -> Vec<String> {
    list["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item["metadata"]["name"].as_str())
                .filter(|name| keep(name))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn shard_split_group_starts_on_one_shard() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(API_TIMEOUT)
        .build()
        .unwrap();

    // 1. The start spec, written before the old group goes.
    let live = kubectl_json(&["get", "defer", DEFER]);
    let tmp = PathBuf::from(required_env("TMPDIR"));
    let start_cr = tmp.join("defer-shard-split.json");
    std::fs::write(&start_cr, cr_for_start(&live).to_string()).expect("write start-spec CR");
    evidence(
        SUITE,
        "start spec written",
        json!({ "shardCount": 1, "replicasPerShard": 1 }),
    );

    // 2. A clean slate. The guard removes the group, its PVCs and the Secret
    // at the end, also after a panic.
    let _cleanup = Cleanup;
    remove_group_and_pvcs();

    // 3. The admin token Secret first, then the one-shard group.
    kubectl(&[
        "create",
        "secret",
        "generic",
        TOKEN_SECRET,
        "--from-literal=token=e2e-auth-is-off",
    ]);
    kubectl(&["apply", "-f", start_cr.to_str().expect("UTF-8 path")]);
    wait_for_group_ready(1, GROUP_TIMEOUT);
    wait_healthy(&http, &base).await;
    evidence(SUITE, "one shard ready", json!({ "replicas": 1 }));

    // 4. Six queues with the drain policy.
    for q in 0..QUEUES {
        send(
            http.put(format!("{base}/v1/queues/{}", queue_name(q)))
                .json(&queue_policy()),
        )
        .await;
    }
    evidence(SUITE, "queues put", json!({ "queues": QUEUES }));

    // 5. 400 due tasks in each queue.
    for q in 0..QUEUES {
        for i in 0..TASKS_PER_QUEUE {
            let id = task_id(q, i);
            let response = http
                .post(format!("{base}/v1/queues/{}/tasks", queue_name(q)))
                .json(&task_body(&id))
                .send()
                .await
                .expect("create task request");
            assert_eq!(response.status().as_u16(), 201, "create task {id} status");
        }
    }
    evidence(
        SUITE,
        "tasks created",
        json!({
            "queues": QUEUES,
            "tasks_per_queue": TASKS_PER_QUEUE,
            "total": QUEUES * TASKS_PER_QUEUE
        }),
    );

    // 6. The first task reads back with its id.
    let first = task_id(0, 0);
    let task = send(http.get(format!("{base}/v1/queues/{}/tasks/{first}", queue_name(0)))).await;
    assert_eq!(task["task_id"], first.as_str());
    evidence(SUITE, "task readable", json!({ "task_id": first }));
}

#[cfg(test)]
mod tests {
    use super::{
        cr_for_start, is_group_pod, pvc_names, queue_name, queue_policy, task_body, task_id,
    };
    use serde_json::json;

    #[test]
    fn queue_and_task_names_are_stable() {
        assert_eq!(queue_name(3), "split-3");
        assert_eq!(task_id(2, 7), "s2-0007");
        assert_eq!(task_id(0, 399), "s0-0399");
    }

    #[test]
    fn task_body_is_a_due_healthz_get() {
        let body = task_body("s0-0000");
        assert_eq!(body["task_id"], "s0-0000");
        assert_eq!(body["target"]["url"], "http://defer:7141/healthz");
        assert_eq!(body["target"]["method"], "GET");
        assert_eq!(body["schedule_at"], "2020-01-01T00:00:00Z");
        assert_eq!(body["max_attempts"], 3);
    }

    #[test]
    fn queue_policy_drains_one_task_a_second() {
        let policy = queue_policy();
        assert_eq!(policy["max_dispatches_per_second"], 1);
        assert_eq!(policy["max_in_flight"], 1);
        assert_eq!(policy["max_burst_size"], 1);
    }

    #[test]
    fn cr_for_start_sets_one_shard_one_replica_and_the_token() {
        let live = json!({
            "apiVersion": "defer.dev/v1alpha1",
            "kind": "Defer",
            "metadata": { "name": "defer", "uid": "u", "resourceVersion": "7" },
            "spec": {
                "image": "img",
                "shardCount": 2,
                "replicasPerShard": 3,
                "voterCount": 3,
                "storage": "4Gi"
            },
            "status": { "phase": "Ready" }
        });
        let cr = cr_for_start(&live);
        assert_eq!(cr["spec"]["shardCount"], 1);
        assert_eq!(cr["spec"]["replicasPerShard"], 1);
        assert_eq!(cr["spec"]["voterCount"], 1);
        assert_eq!(cr["spec"]["adminTokenSecret"], "defer-admin-token");
        assert_eq!(cr["spec"]["image"], "img");
        assert_eq!(cr["spec"]["storage"], "4Gi");
        assert_eq!(cr["metadata"], json!({ "name": "defer" }));
        assert!(cr["status"].is_null());
    }

    #[test]
    fn pvc_names_cover_two_shards() {
        assert_eq!(pvc_names(), ["data-defer-0", "data-defer-1"]);
    }

    #[test]
    fn is_group_pod_skips_the_operator() {
        assert!(is_group_pod("defer-0"));
        assert!(is_group_pod("defer-1"));
        assert!(!is_group_pod("defer-operator-5f7d"));
        assert!(!is_group_pod("defer-e2e-abc"));
    }
}
