//! Suite `backup_restore`: back up a running 3-replica group through
//! `GET /admin/backup`, delete the Defer CR and its PVCs, start a new group
//! from a CR whose `bootstrapSeedConfigMap` holds that backup, and check that
//! the queue policy and every task status match the ones before the backup.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};

use super::{endpoint, evidence, kubectl, kubectl_json, required_env, wait_for};

const SUITE: &str = "backup_restore";
const DEFER: &str = "defer";
const QUEUE: &str = "backup-restore";
const SEED_CONFIGMAP: &str = "defer-restore-seed";
const TASKS: usize = 200;
const GROUP_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const DELETE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const API_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// The ConfigMap limit is 1 MiB; the snapshot must stay clear of it.
const SNAPSHOT_LIMIT: usize = 900 * 1024;

fn task_id(i: usize) -> String {
    format!("restore-{i:03}")
}

/// A task that never runs in this suite: its schedule is far in the future,
/// so every status stays stable and comparable across the restore.
fn task_body(id: &str) -> Value {
    json!({
        "task_id": id,
        "target": {
            "url": "http://defer:7141/healthz",
            "method": "GET",
            "headers": {}
        },
        "payload": { "suite": SUITE, "id": id },
        "schedule_at": "2099-01-01T00:00:00Z",
        "priority": 10,
        "max_attempts": 3
    })
}

/// The CR that starts a new group from the ConfigMap seed: the live spec with
/// the seed URI replaced, and no identity or status fields of the live object.
fn restore_cr(live: &Value) -> Value {
    let mut spec = live["spec"].clone();
    if let Some(fields) = spec.as_object_mut() {
        fields.remove("bootstrapSeedUri");
        fields.insert("bootstrapSeedConfigMap".to_owned(), json!(SEED_CONFIGMAP));
    }
    json!({
        "apiVersion": live["apiVersion"],
        "kind": live["kind"],
        "metadata": { "name": DEFER },
        "spec": spec
    })
}

fn pvc_names() -> [String; 3] {
    [0, 1, 2].map(|i| format!("data-defer-{i}"))
}

/// Whether a pod name is one of the group's pods (`defer-0`, `defer-1`, ...),
/// and not the operator or the test Job.
fn is_group_pod(name: &str) -> bool {
    name.strip_prefix("defer-")
        .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
}

/// Checks that the snapshot is JSON and holds the first and the last of
/// `want` tasks. This guards against an empty or short backup before the
/// group is deleted.
fn snapshot_has_tasks(bytes: &[u8], want: usize) -> Result<(), String> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|err| format!("snapshot is not JSON: {err}"))?;
    let text = value.to_string();
    for id in [task_id(0), task_id(want.saturating_sub(1))] {
        if !text.contains(&id) {
            return Err(format!("snapshot lacks task {id}"));
        }
    }
    Ok(())
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

/// Waits until the StatefulSet `defer` has 3 ready replicas.
fn wait_for_group_ready(timeout: Duration) {
    wait_for(
        &format!("StatefulSet {DEFER} has 3 ready replicas"),
        timeout,
        || {
            let sts = kubectl_json(&["get", "statefulset", DEFER]);
            let ready = sts["status"]["readyReplicas"].clone();
            if ready == 3 {
                Ok(())
            } else {
                Err(format!("readyReplicas {ready}"))
            }
        },
    );
}

/// Deletes the Defer CR and the three group PVCs, and waits until no group
/// pod and no `data-defer-*` PVC is left. The pool holds 4 nodes, so the old
/// group must be gone before a new one starts.
fn remove_group_and_pvcs() {
    kubectl(&["delete", "defer", DEFER, "--wait=true"]);
    wait_for("old group pods gone", DELETE_TIMEOUT, || {
        let pods = kubectl_json(&["get", "pods"]);
        let left = names_where(&pods, is_group_pod);
        if left.is_empty() {
            Ok(())
        } else {
            Err(format!("pods left: {left:?}"))
        }
    });
    let pvcs = pvc_names();
    kubectl(&[
        "delete",
        "pvc",
        pvcs[0].as_str(),
        pvcs[1].as_str(),
        pvcs[2].as_str(),
        "--wait=true",
    ]);
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

/// Removes the seed ConfigMap if the run failed before the test removed it.
/// It never panics: a panic here during an unwind would abort the process,
/// and the test's own failure must stay the reported one.
struct Cleanup;

impl Drop for Cleanup {
    fn drop(&mut self) {
        let Ok(namespace) = std::env::var("NAMESPACE") else {
            return;
        };
        let _ = std::process::Command::new("kubectl")
            .args(["-n", &namespace, "delete", "configmap", SEED_CONFIGMAP])
            .arg("--ignore-not-found")
            .output();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn backup_and_restore_keeps_queue_and_tasks() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(API_TIMEOUT)
        .build()
        .unwrap();

    // 1. The old group is up and healthy; keep its CR for the restore.
    wait_for_group_ready(GROUP_TIMEOUT);
    wait_healthy(&http, &base).await;
    let live = kubectl_json(&["get", "defer", DEFER]);
    evidence(SUITE, "group ready", json!({ "replicas": 3 }));
    let _cleanup = Cleanup;

    // 3. A queue and its tasks, then the state before the backup.
    let queue_url = format!("{base}/v1/queues/{QUEUE}");
    send(http.put(&queue_url).json(&json!({
        "max_in_flight": 100,
        "max_dispatch_per_tick": 100,
        "max_dispatches_per_second": 100,
        "max_burst_size": 100,
        "lease_ttl_ms": 30000,
        "retry_backoff_ms": 250
    })))
    .await;
    for i in 0..TASKS {
        let id = task_id(i);
        let resp = http
            .post(format!("{base}/v1/queues/{QUEUE}/tasks"))
            .json(&task_body(&id))
            .send()
            .await
            .expect("create task request");
        assert_eq!(resp.status().as_u16(), 201, "create task {id} status");
    }
    let queue_before = send(http.get(&queue_url)).await;
    let mut tasks_before = Vec::with_capacity(TASKS);
    for i in 0..TASKS {
        let id = task_id(i);
        let task = send(http.get(format!("{base}/v1/queues/{QUEUE}/tasks/{id}"))).await;
        tasks_before.push((id, task));
    }
    let first_status = tasks_before[0].1["status"].clone();
    assert_ne!(first_status, "Succeeded", "far-future tasks must not run");
    for (id, task) in &tasks_before {
        assert_eq!(
            task["status"], first_status,
            "task {id} status before backup"
        );
    }
    evidence(
        SUITE,
        "queue and tasks created",
        json!({ "queue": QUEUE, "tasks": TASKS }),
    );

    // 4. The backup goes into the seed ConfigMap, through the work volume.
    let bytes = http
        .get(format!("{base}/admin/backup"))
        .send()
        .await
        .expect("GET /admin/backup")
        .error_for_status()
        .expect("GET /admin/backup status")
        .bytes()
        .await
        .expect("backup body");
    snapshot_has_tasks(&bytes, TASKS).unwrap_or_else(|err| panic!("{err}"));
    assert!(
        bytes.len() < SNAPSHOT_LIMIT,
        "snapshot {} bytes is near the 1 MiB ConfigMap limit",
        bytes.len()
    );
    let tmp = PathBuf::from(required_env("TMPDIR"));
    let snapshot = tmp.join("snapshot.json");
    std::fs::write(&snapshot, &bytes).expect("write snapshot");
    let from_file = format!("--from-file=snapshot.json={}", snapshot.display());
    kubectl(&["create", "configmap", SEED_CONFIGMAP, from_file.as_str()]);
    evidence(
        SUITE,
        "backup taken",
        json!({ "bytes": bytes.len(), "tasks": TASKS }),
    );

    // 5. Save the restore CR, then remove the old group and its volumes.
    let cr_file = tmp.join("defer-restore.json");
    std::fs::write(&cr_file, restore_cr(&live).to_string()).expect("write restore CR");
    remove_group_and_pvcs();
    evidence(SUITE, "old group deleted", json!({ "pvcs": pvc_names() }));

    // 6. The new group starts from the seed.
    let cr_path = cr_file.to_str().expect("UTF-8 path");
    kubectl(&["apply", "-f", cr_path]);
    wait_for_group_ready(GROUP_TIMEOUT);
    wait_healthy(&http, &base).await;
    evidence(
        SUITE,
        "restored group applied",
        json!({ "seed": SEED_CONFIGMAP }),
    );

    // 7. The queue policy and every task status match the backup's state.
    let queue_after = send(http.get(&queue_url)).await;
    assert_eq!(queue_after, queue_before, "queue policy after restore");
    let mut mismatches = Vec::new();
    for (id, before) in &tasks_before {
        let after = send(http.get(format!("{base}/v1/queues/{QUEUE}/tasks/{id}"))).await;
        if after != *before {
            mismatches.push(id.clone());
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} tasks differ after restore, first five: {:?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(5)]
    );
    evidence(
        SUITE,
        "restore compared",
        json!({ "queue": QUEUE, "tasks": TASKS, "mismatches": 0 }),
    );

    // 8. Remove the restored group, its volumes and the seed.
    remove_group_and_pvcs();
    kubectl(&["delete", "configmap", SEED_CONFIGMAP, "--ignore-not-found"]);
    evidence(SUITE, "cleaned up", json!({ "pvcs": 0 }));
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        is_group_pod, pvc_names, restore_cr, snapshot_has_tasks, task_body, task_id, SEED_CONFIGMAP,
    };

    #[test]
    fn restore_cr_sets_the_configmap_and_drops_the_uri() {
        let live = json!({
            "apiVersion": "defer.dev/v1alpha1",
            "kind": "Defer",
            "metadata": {
                "name": "defer",
                "uid": "u",
                "resourceVersion": "7",
                "namespace": "defer"
            },
            "spec": { "replicasPerShard": 3, "bootstrapSeedUri": "file:///x" },
            "status": { "phase": "Ready" }
        });
        let cr = restore_cr(&live);
        assert_eq!(cr["spec"]["bootstrapSeedConfigMap"], SEED_CONFIGMAP);
        assert!(cr["spec"]["bootstrapSeedUri"].is_null());
        assert_eq!(cr["spec"]["replicasPerShard"], 3);
        assert_eq!(cr["metadata"], json!({"name": "defer"}));
        assert!(cr["status"].is_null());
    }

    #[test]
    fn snapshot_has_tasks_accepts_a_full_snapshot() {
        let bytes = br#"{"tasks":["restore-000","restore-199"]}"#;
        assert_eq!(snapshot_has_tasks(bytes, 200), Ok(()));
    }

    #[test]
    fn snapshot_has_tasks_refuses_a_short_snapshot() {
        let bytes = br#"{"tasks":["restore-000"]}"#;
        let err = snapshot_has_tasks(bytes, 200).expect_err("a short snapshot");
        assert!(err.contains("restore-199"), "{err}");
    }

    #[test]
    fn snapshot_has_tasks_refuses_non_json() {
        let err = snapshot_has_tasks(b"not json", 200).expect_err("not JSON");
        assert!(err.starts_with("snapshot is not JSON"), "{err}");
    }

    #[test]
    fn task_ids_are_zero_padded() {
        assert_eq!(task_id(7), "restore-007");
    }

    #[test]
    fn task_body_is_far_in_the_future() {
        let body = task_body("restore-001");
        assert_eq!(body["schedule_at"], "2099-01-01T00:00:00Z");
        assert_eq!(body["task_id"], "restore-001");
    }

    #[test]
    fn pvc_names_are_the_three_group_volumes() {
        assert_eq!(
            pvc_names(),
            ["data-defer-0", "data-defer-1", "data-defer-2"].map(String::from)
        );
    }

    #[test]
    fn group_pod_names_match_only_defer_pods() {
        assert!(is_group_pod("defer-0"));
        assert!(is_group_pod("defer-2"));
        assert!(!is_group_pod("defer-operator-5f7d"));
        assert!(!is_group_pod("defer-e2e-abcde"));
        assert!(!is_group_pod("defer"));
    }
}
