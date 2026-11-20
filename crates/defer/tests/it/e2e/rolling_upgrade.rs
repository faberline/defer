//! Suite `rolling_upgrade`: start a 3-replica group on a previous release
//! image, write a queue and 200 tasks, change `spec.image` of the Defer CR to
//! the current image, and check that the StatefulSet rolls one replica at a
//! time and that the queue policy and every task are unchanged afterwards.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};

use super::{
    assert_pods_run, endpoint, evidence, kubectl, kubectl_json, required_env, try_kubectl,
    wait_for, Expected,
};

const SUITE: &str = "rolling_upgrade";
const DEFER: &str = "defer";
const QUEUE: &str = "rolling-upgrade";
const TASKS: usize = 200;
const SERVER_LABEL: &str =
    "app.kubernetes.io/name=defer,app.kubernetes.io/instance=defer,app.kubernetes.io/component=server";
const GROUP_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const DELETE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const ROLL_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const API_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

fn task_id(i: usize) -> String {
    format!("upgrade-{i:03}")
}

/// A task that never runs in this suite: its schedule is far in the future,
/// so every status stays stable and comparable across the roll.
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

/// The previous-image CR: the live spec with `spec.image` replaced, and no
/// identity or status fields of the live object.
fn cr_with_image(live: &Value, image: &str) -> Value {
    let mut spec = live["spec"].clone();
    if let Some(fields) = spec.as_object_mut() {
        fields.insert("image".to_owned(), json!(image));
    }
    json!({
        "apiVersion": live["apiVersion"],
        "kind": live["kind"],
        "metadata": { "name": DEFER },
        "spec": spec
    })
}

/// Checks that `previous` is a digest reference of the defer image and is not
/// the image under test. A tag is refused: it can move, so the run could not
/// tell which image it started from.
fn check_previous_image(previous: &str, current: &str) -> Result<(), String> {
    let digest_ok = previous
        .strip_prefix("ghcr.io/faberline/defer@sha256:")
        .is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        });
    if !digest_ok {
        return Err(format!(
            "E2E_PREVIOUS_IMAGE must be ghcr.io/faberline/defer@sha256:<64 hex>, got {previous}"
        ));
    }
    if previous == current {
        return Err(format!(
            "E2E_PREVIOUS_IMAGE is the same as the current image {current}; a roll needs an older image"
        ));
    }
    Ok(())
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
            let out = try_kubectl(&["get", "statefulset", DEFER, "-o", "json"])?;
            let sts: Value =
                serde_json::from_str(&out).map_err(|err| format!("not JSON: {err}"))?;
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

/// Replicas the StatefulSet wants but does not have ready. Kubernetes omits
/// `readyReplicas` at 0, so a missing value counts as 0.
fn unavailable(sts: &Value) -> u64 {
    let want = sts["spec"]["replicas"].as_u64().unwrap_or(0);
    let ready = sts["status"]["readyReplicas"].as_u64().unwrap_or(0);
    want.saturating_sub(ready)
}

/// The roll is done when all 3 replicas run the new revision and are ready.
fn roll_done(sts: &Value) -> bool {
    let status = &sts["status"];
    let current = status["currentRevision"].as_str().unwrap_or_default();
    status["updatedReplicas"].as_u64() == Some(3)
        && status["readyReplicas"].as_u64() == Some(3)
        && !current.is_empty()
        && status["updateRevision"].as_str() == Some(current)
}

/// The image of each group pod's one container, sorted. Terminating pods and
/// pods that are not group pods are left out.
fn group_images(pods: &Value) -> Vec<String> {
    let mut images: Vec<String> = pods["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter(|pod| {
                    is_group_pod(pod["metadata"]["name"].as_str().unwrap_or_default())
                        && pod["metadata"]["deletionTimestamp"].is_null()
                })
                .filter_map(|pod| pod["spec"]["containers"][0]["image"].as_str())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    images.sort();
    images
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn rolling_upgrade_keeps_queue_and_tasks() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(API_TIMEOUT)
        .build()
        .unwrap();

    // 1. The previous image and the image under test must differ.
    let expected = Expected::from_env();
    let previous = required_env("E2E_PREVIOUS_IMAGE");
    check_previous_image(&previous, &expected.image).unwrap_or_else(|err| panic!("{err}"));
    evidence(
        SUITE,
        "images checked",
        json!({ "previous": previous, "current": expected.image }),
    );

    // 2. A healthy group on the previous image. The live CR keeps its fields.
    let live = kubectl_json(&["get", "defer", DEFER]);
    let tmp = PathBuf::from(required_env("TMPDIR"));
    let previous_cr = tmp.join("defer-previous.json");
    std::fs::write(&previous_cr, cr_with_image(&live, &previous).to_string())
        .expect("write previous-image CR");
    remove_group_and_pvcs();
    let previous_path = previous_cr.to_str().expect("UTF-8 path");
    kubectl(&["apply", "-f", previous_path]);
    wait_for_group_ready(GROUP_TIMEOUT);
    wait_healthy(&http, &base).await;
    assert_eq!(
        group_images(&kubectl_json(&["get", "pods"])),
        vec![previous.clone(); 3],
        "group runs the previous image"
    );
    evidence(SUITE, "previous group ready", json!({ "replicas": 3 }));

    // 3. A queue and its tasks, then the state before the roll.
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
        assert_eq!(task["status"], first_status, "task {id} status before roll");
    }
    evidence(
        SUITE,
        "queue and tasks created",
        json!({ "queue": QUEUE, "tasks": TASKS }),
    );

    // 4. Patch the CR to the current image. The StatefulSet rolls one replica
    //    at a time; no poll may see more than one replica unavailable.
    let revision_before =
        kubectl_json(&["get", "statefulset", DEFER])["status"]["updateRevision"].clone();
    let patch = json!({ "spec": { "image": expected.image } }).to_string();
    kubectl(&[
        "patch",
        "defer",
        DEFER,
        "--type",
        "merge",
        "-p",
        patch.as_str(),
    ]);
    let mut max_unavailable = 0;
    let mut polls = 0u64;
    let sts = wait_for("StatefulSet roll finished", ROLL_TIMEOUT, || {
        let sts = kubectl_json(&["get", "statefulset", DEFER]);
        polls += 1;
        max_unavailable = max_unavailable.max(unavailable(&sts));
        // A new revision must appear first: right after the patch the old
        // status may still look done.
        let rolled = sts["status"]["updateRevision"] != revision_before;
        if unavailable(&sts) > 1 || (rolled && roll_done(&sts)) {
            Ok(sts)
        } else {
            Err(format!("status {}", sts["status"]))
        }
    });
    assert!(
        unavailable(&sts) <= 1,
        "more than one replica unavailable during the roll: {}",
        sts["status"]
    );
    assert!(roll_done(&sts), "roll did not finish: {}", sts["status"]);
    evidence(
        SUITE,
        "roll finished",
        json!({ "max_unavailable": max_unavailable, "polls": polls }),
    );

    // 5. Every replica runs the current image and binds its digest.
    wait_for_group_ready(GROUP_TIMEOUT);
    wait_healthy(&http, &base).await;
    assert_pods_run(&expected, SERVER_LABEL, 3);
    evidence(
        SUITE,
        "group on current image",
        json!({ "replicas": 3, "image": expected.image }),
    );

    // 6. The queue policy and every task status match the state before the roll.
    let queue_after = send(http.get(&queue_url)).await;
    assert_eq!(queue_after, queue_before, "queue policy after roll");
    let mut mismatches = Vec::new();
    for (id, before) in &tasks_before {
        let after = send(http.get(format!("{base}/v1/queues/{QUEUE}/tasks/{id}"))).await;
        if after != *before {
            mismatches.push(id.clone());
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} tasks differ after the roll, first five: {:?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(5)]
    );
    evidence(
        SUITE,
        "roll compared",
        json!({ "queue": QUEUE, "tasks": TASKS, "mismatches": 0 }),
    );

    // 7. Remove the group and its volumes.
    remove_group_and_pvcs();
    evidence(SUITE, "cleaned up", json!({ "pvcs": 0 }));
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        check_previous_image, cr_with_image, group_images, roll_done, task_id, unavailable,
    };

    fn digest(c: char) -> String {
        format!(
            "ghcr.io/faberline/defer@sha256:{}",
            c.to_string().repeat(64)
        )
    }

    #[test]
    fn check_previous_image_accepts_a_digest_reference() {
        assert_eq!(check_previous_image(&digest('a'), &digest('b')), Ok(()));
    }

    #[test]
    fn check_previous_image_refuses_a_tag() {
        let err = check_previous_image("ghcr.io/faberline/defer:0.4.13", &digest('b'))
            .expect_err("a tag is not a digest");
        assert!(err.contains("E2E_PREVIOUS_IMAGE"), "{err}");
    }

    #[test]
    fn check_previous_image_refuses_the_current_image() {
        let err = check_previous_image(&digest('b'), &digest('b'))
            .expect_err("the same image is no roll");
        assert!(err.contains("same as the current image"), "{err}");
    }

    #[test]
    fn cr_with_image_replaces_only_the_image() {
        let live = json!({
            "apiVersion": "defer.dev/v1alpha1",
            "kind": "Defer",
            "metadata": { "name": "defer", "uid": "u", "resourceVersion": "7" },
            "spec": { "image": "old", "replicasPerShard": 3 },
            "status": { "phase": "Ready" }
        });
        let cr = cr_with_image(&live, "new");
        assert_eq!(cr["spec"]["image"], "new");
        assert_eq!(cr["spec"]["replicasPerShard"], 3);
        assert_eq!(cr["metadata"], json!({ "name": "defer" }));
        assert!(cr["status"].is_null());
    }

    #[test]
    fn unavailable_counts_missing_ready_replicas_as_zero() {
        assert_eq!(
            unavailable(&json!({"spec": {"replicas": 3}, "status": {}})),
            3
        );
        assert_eq!(
            unavailable(&json!({"spec": {"replicas": 3}, "status": {"readyReplicas": 2}})),
            1
        );
        assert_eq!(
            unavailable(&json!({"spec": {"replicas": 3}, "status": {"readyReplicas": 3}})),
            0
        );
    }

    #[test]
    fn roll_done_needs_all_replicas_updated_ready_and_one_revision() {
        let done = json!({"status": {
            "updatedReplicas": 3, "readyReplicas": 3,
            "currentRevision": "r2", "updateRevision": "r2"
        }});
        assert!(roll_done(&done));
        let mut two_updated = done.clone();
        two_updated["status"]["updatedReplicas"] = json!(2);
        assert!(!roll_done(&two_updated));
        let mut two_ready = done.clone();
        two_ready["status"]["readyReplicas"] = json!(2);
        assert!(!roll_done(&two_ready));
        let mut revisions_differ = done;
        revisions_differ["status"]["currentRevision"] = json!("r1");
        assert!(!roll_done(&revisions_differ));
    }

    #[test]
    fn group_images_skips_other_pods_and_terminating_pods() {
        let pods = json!({"items": [
            {"metadata": {"name": "defer-0"}, "spec": {"containers": [{"image": "a"}]}},
            {"metadata": {"name": "defer-1"}, "spec": {"containers": [{"image": "b"}]}},
            {"metadata": {"name": "defer-2", "deletionTimestamp": "2026-11-20T00:00:00Z"},
             "spec": {"containers": [{"image": "b"}]}},
            {"metadata": {"name": "defer-operator-5f7d"}, "spec": {"containers": [{"image": "c"}]}}
        ]});
        assert_eq!(group_images(&pods), vec!["a".to_owned(), "b".to_owned()]);
    }

    #[test]
    fn task_ids_are_zero_padded() {
        assert_eq!(task_id(7), "upgrade-007");
    }
}
