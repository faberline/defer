//! Suite `shard_split`, part 2: start a one-shard group with an admin token
//! Secret, create 6 queues with 400 due tasks each, then raise `shardCount`
//! to 2 while a writer keeps creating tasks. It proves every task reaches
//! `Succeeded` and that no lease settles twice.

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
const DURING_TASKS_PER_QUEUE: usize = 20;
const SPLIT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// `split-<q>`, for example `split-0`.
fn queue_name(q: usize) -> String {
    format!("split-{q}")
}

/// `s<q>-<i padded to 4 digits>`, for example `s0-0007`.
fn task_id(q: usize, i: usize) -> String {
    format!("s{q}-{i:04}")
}

/// `d<q>-<i padded to 4 digits>`, for example `d1-0003`. A second id range
/// for the tasks that the writer creates during the split.
fn during_task_id(q: usize, i: usize) -> String {
    format!("d{q}-{i:04}")
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

/// Merge patch that raises `shardCount` of the CR from 1 to 2.
fn split_patch() -> Value {
    json!({"spec": {"shardCount": 2}})
}

/// True only when the CR status reports the finished move from 1 shard to 2
/// (`queueMove.step` is `done`) and both pods of the group are ready.
fn split_done(status: &Value, pods_ready: u64) -> bool {
    status["queueMove"] == json!({"fromShards": 1, "toShards": 2, "step": "done"})
        && pods_ready == 2
}

/// Where a task stands, read from its `status` text.
#[derive(Debug, PartialEq, Eq)]
enum Progress {
    Done,
    Waiting,
    Failed,
}

/// `Succeeded` is done; `DeadLettered` and `Canceled` can never succeed; any
/// other text is still in progress.
fn classify(status: &str) -> Progress {
    match status {
        "Succeeded" => Progress::Done,
        "DeadLettered" | "Canceled" => Progress::Failed,
        _ => Progress::Waiting,
    }
}

/// Checks that every created task settled exactly once: no lease lost its
/// owner, and the acked count equals the created count.
fn settled_once(acked: u64, lost: u64, created: u64) -> Result<(), String> {
    if lost > 0 {
        return Err(format!("{lost} leases lost ownership, want 0"));
    }
    if acked > created {
        return Err(format!(
            "acked {acked}, created {created}: a lease settled twice"
        ));
    }
    if acked < created {
        return Err(format!(
            "acked {acked}, created {created}: a task never settled"
        ));
    }
    Ok(())
}

/// The value of one counter in a Prometheus text body, or `None`.
fn parse_counter(metrics: &str, name: &str) -> Option<u64> {
    for line in metrics.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix(name) {
            if rest.starts_with(' ') || rest.starts_with('\t') || rest.starts_with('{') {
                let rest = rest.trim();
                let val_str = if let Some((_, val)) = rest.split_once('}') {
                    val.trim()
                } else {
                    rest
                };
                if let Ok(val) = val_str.parse::<u64>() {
                    return Some(val);
                }
            }
        }
    }
    None
}

/// The base URL of each of the 2 replicas, read through the headless Service:
/// `defer-<i>.defer-headless.<namespace>`.
fn replica_bases(namespace: &str) -> Vec<String> {
    (0..2)
        .map(|i| format!("http://defer-{i}.defer-headless.{namespace}.svc.cluster.local:7141"))
        .collect()
}

/// Reads `defer_dispatch_acked_total` and `defer_dispatch_lost_ownership_total`
/// from `GET /metrics` of every base, and returns the sums. The counters are
/// per process, so the sums cover the whole group. A failing base fails the
/// whole read.
async fn read_settle_counters(
    http: &reqwest::Client,
    bases: &[String],
) -> Result<(u64, u64), String> {
    let mut acked_total = 0;
    let mut lost_total = 0;
    for base in bases {
        let response = http
            .get(format!("{base}/metrics"))
            .send()
            .await
            .map_err(|err| format!("{base}: {err}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("{base}/metrics: {status}"));
        }
        let text = response
            .text()
            .await
            .map_err(|err| format!("{base}: {err}"))?;
        let acked = parse_counter(&text, "defer_dispatch_acked_total")
            .ok_or_else(|| format!("{base}: missing defer_dispatch_acked_total"))?;
        let lost = parse_counter(&text, "defer_dispatch_lost_ownership_total")
            .ok_or_else(|| format!("{base}: missing defer_dispatch_lost_ownership_total"))?;
        acked_total += acked;
        lost_total += lost;
    }
    Ok((acked_total, lost_total))
}

/// Creates `DURING_TASKS_PER_QUEUE` tasks in each queue while the split runs.
/// A 201 or a 409 (an id this writer already sent) counts as created. A
/// connection error or a 5xx is retried every `POLL_INTERVAL` until
/// `API_TIMEOUT` passes for that task. Returns the ids of all created tasks.
async fn write_during_tasks(http: reqwest::Client, base: String) -> Vec<String> {
    let mut created = Vec::new();
    for q in 0..QUEUES {
        for i in 0..DURING_TASKS_PER_QUEUE {
            let id = during_task_id(q, i);
            let deadline = tokio::time::Instant::now() + API_TIMEOUT;
            loop {
                let last = match http
                    .post(format!("{base}/v1/queues/{}/tasks", queue_name(q)))
                    .json(&task_body(&id))
                    .send()
                    .await
                {
                    Ok(response) => {
                        let status = response.status().as_u16();
                        match status {
                            201 | 409 => {
                                created.push(id.clone());
                                break;
                            }
                            500..=599 => format!("status {status}"),
                            _ => panic!("create task {id}: status {status}"),
                        }
                    }
                    Err(err) => err.to_string(),
                };
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "create task {id}: not created within {API_TIMEOUT:?}; last error: {last}"
                );
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    }
    created
}

/// Polls every `(queue index, task id)` pair until all read `Succeeded`. A
/// 404, `DeadLettered` or `Canceled` fails at once, with the id. A connection
/// error or a 5xx is read as still waiting.
async fn wait_all_succeeded(http: &reqwest::Client, base: &str, ids: &[(usize, String)]) {
    let deadline = tokio::time::Instant::now() + SETTLE_TIMEOUT;
    let mut done = vec![false; ids.len()];
    loop {
        for (k, (q, id)) in ids.iter().enumerate() {
            if done[k] {
                continue;
            }
            let response = match http
                .get(format!("{base}/v1/queues/{}/tasks/{id}", queue_name(*q)))
                .send()
                .await
            {
                Ok(response) => response,
                Err(_) => continue,
            };
            let status = response.status().as_u16();
            match status {
                404 => panic!("task {id}: 404 after the split"),
                200 => {
                    let body: Value = response.json().await.unwrap_or(Value::Null);
                    let text = body["status"].as_str().unwrap_or("");
                    match classify(text) {
                        Progress::Failed => panic!("task {id}: {text} after the split"),
                        Progress::Done => done[k] = true,
                        Progress::Waiting => {}
                    }
                }
                500..=599 => {}
                _ => panic!("task {id}: status {status} after the split"),
            }
        }
        let left = done.iter().filter(|d| !**d).count();
        if left == 0 {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            let first: Vec<&str> = ids
                .iter()
                .zip(&done)
                .filter(|(_, d)| !**d)
                .take(5)
                .map(|((_, id), _)| id.as_str())
                .collect();
            panic!("{left} tasks not Succeeded within {SETTLE_TIMEOUT:?}; first left: {first:?}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn shard_split_under_load_settles_every_task_once() {
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

    // 7. Start the writer, then raise shardCount to 2 while it runs.
    let writer = tokio::spawn(write_during_tasks(http.clone(), base.clone()));
    let patch = split_patch().to_string();
    kubectl(&[
        "patch",
        "defer",
        DEFER,
        "--type",
        "merge",
        "-p",
        patch.as_str(),
    ]);
    evidence(SUITE, "split started", json!({ "shardCount": 2 }));

    // 8. Wait for the queue move to finish and both pods to be ready.
    wait_for("queue move done and 2 pods ready", SPLIT_TIMEOUT, || {
        let status = kubectl_json(&["get", "defer", DEFER])["status"].clone();
        let out = try_kubectl(&["get", "statefulset", DEFER, "-o", "json"])?;
        let sts: Value = serde_json::from_str(&out).map_err(|err| format!("not JSON: {err}"))?;
        let ready = sts["status"]["readyReplicas"].as_u64().unwrap_or(0);
        if split_done(&status, ready) {
            Ok(())
        } else {
            Err(format!("pods ready {ready}; status {status}"))
        }
    });
    wait_healthy(&http, &base).await;
    evidence(SUITE, "split done", json!({ "toShards": 2 }));

    // 9. The writer created every task it planned.
    let during = writer.await.expect("writer task");
    assert_eq!(
        during.len(),
        QUEUES * DURING_TASKS_PER_QUEUE,
        "tasks created during the split"
    );
    evidence(
        SUITE,
        "during tasks created",
        json!({ "total": during.len() }),
    );

    // 10. Every task, before and during the split, reaches Succeeded.
    let mut pairs: Vec<(usize, String)> = Vec::new();
    for q in 0..QUEUES {
        for i in 0..TASKS_PER_QUEUE {
            pairs.push((q, task_id(q, i)));
        }
        for i in 0..DURING_TASKS_PER_QUEUE {
            pairs.push((q, during_task_id(q, i)));
        }
    }
    wait_all_succeeded(&http, &base, &pairs).await;
    let created = QUEUES * TASKS_PER_QUEUE + during.len();
    evidence(SUITE, "all tasks succeeded", json!({ "total": created }));

    // 11. Each lease settled exactly once, on either replica.
    let bases = replica_bases(&required_env("NAMESPACE"));
    let deadline = tokio::time::Instant::now() + API_TIMEOUT;
    let (acked, lost) = loop {
        match read_settle_counters(&http, &bases).await {
            Ok(counts) => break counts,
            Err(err) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "read settle counters: {err}"
                );
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    };
    settled_once(acked, lost, created as u64).unwrap_or_else(|err| panic!("{err}"));
    evidence(
        SUITE,
        "each lease settled once",
        json!({ "acked": acked, "lost_ownership": lost, "created": created }),
    );
}

#[cfg(test)]
mod tests {
    use super::{
        classify, cr_for_start, during_task_id, is_group_pod, parse_counter, pvc_names, queue_name,
        queue_policy, replica_bases, settled_once, split_done, split_patch, task_body, task_id,
        Progress,
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

    #[test]
    fn during_task_id_uses_its_own_range() {
        assert_eq!(during_task_id(1, 3), "d1-0003");
        assert_eq!(during_task_id(0, 19), "d0-0019");
    }

    #[test]
    fn split_patch_raises_shard_count_to_two() {
        assert_eq!(split_patch(), json!({"spec": {"shardCount": 2}}));
    }

    #[test]
    fn split_done_needs_the_done_step_and_two_pods() {
        let done = json!({"queueMove": {"fromShards": 1, "toShards": 2, "step": "done"}});
        assert!(split_done(&done, 2));
        // Negative cases: each one must be false.
        assert!(!split_done(&done, 1));
        assert!(!split_done(&done, 3));
        let moving = json!({"queueMove": {"fromShards": 1, "toShards": 2, "step": "moving"}});
        assert!(!split_done(&moving, 2));
        let pending = json!({"queueMove": {"fromShards": 1, "toShards": 2, "step": "pending"}});
        assert!(!split_done(&pending, 2));
        let from_two = json!({"queueMove": {"fromShards": 2, "toShards": 2, "step": "done"}});
        assert!(!split_done(&from_two, 2));
        let to_three = json!({"queueMove": {"fromShards": 1, "toShards": 3, "step": "done"}});
        assert!(!split_done(&to_three, 2));
        assert!(!split_done(&json!({}), 2));
    }

    #[test]
    fn classify_reads_task_status() {
        assert_eq!(classify("Succeeded"), Progress::Done);
        assert_eq!(classify("DeadLettered"), Progress::Failed);
        assert_eq!(classify("Canceled"), Progress::Failed);
        assert_eq!(classify("Pending"), Progress::Waiting);
        assert_eq!(classify("Running"), Progress::Waiting);
        assert_eq!(classify("Scheduled"), Progress::Waiting);
        assert_eq!(classify(""), Progress::Waiting);
    }

    #[test]
    fn settled_once_accepts_equal_counts() {
        assert_eq!(settled_once(2520, 0, 2520), Ok(()));
    }

    #[test]
    fn settled_once_rejects_a_double_settle() {
        let err = settled_once(2521, 0, 2520).expect_err("acked above created");
        assert!(err.contains("settled twice"), "{err}");
    }

    #[test]
    fn settled_once_rejects_lost_ownership() {
        let err = settled_once(2520, 1, 2520).expect_err("a lost lease");
        assert!(err.contains("lost ownership"), "{err}");
    }

    #[test]
    fn settled_once_rejects_a_missing_settle() {
        let err = settled_once(2519, 0, 2520).expect_err("acked below created");
        assert!(err.contains("never settled"), "{err}");
    }

    #[test]
    fn parse_counter_reads_plain_and_labelled_lines() {
        assert_eq!(parse_counter("# HELP x\nx 5\n", "x"), Some(5));
        assert_eq!(parse_counter("x{a=\"b\"} 7\n", "x"), Some(7));
        assert_eq!(parse_counter("y 5\n", "x"), None);
    }

    #[test]
    fn replica_bases_names_two_pods() {
        assert_eq!(
            replica_bases("ns"),
            [
                "http://defer-0.defer-headless.ns.svc.cluster.local:7141",
                "http://defer-1.defer-headless.ns.svc.cluster.local:7141",
            ]
        );
    }
}
