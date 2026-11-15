// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-http-api" tracker="#766" reason="Real h2c service shell, OpenAPI, metrics, auth, and domain-route integration."

use super::common::{http1_client, raft, start_with_body_limit};
use defer::{AuthConfig, QueuePolicy};

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2215" reason="Own the required-auth h2c oracle for tokenless operational routes, protected task/admin routes, queue-scoped RBAC, and cross-queue tenant denial.">
/// #2556: a task creation body over the shared data-plane body cap is rejected
/// with 413 rather than buffered/accepted, guarding against unbounded
/// request bodies on the data plane (probes stay exempt/unbounded).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_task_body_is_rejected_with_413() {
    let (_dir, raft) = raft().await;
    let auth = AuthConfig::resolve("off", None, None).unwrap();
    let (url, _server) = start_with_body_limit(raft, auth, 1024).await;
    let client = http1_client();

    // Prepare queue
    client
        .put(format!("{url}/v1/queues/jobs"))
        .json(&QueuePolicy::default())
        .send()
        .await
        .unwrap();

    // One byte over the 1 KiB data-plane cap to make it easy to test.
    let oversized_payload = "a".repeat(1024 + 1);
    let result = client
        .post(format!("{url}/v1/queues/jobs/tasks"))
        .header("content-type", "application/json")
        .body(format!(
            r#"{{"task_id":"oversized","target":{{"url":"http://127.0.0.1/","method":"POST","headers":{{}}}},"payload":"{oversized_payload}","schedule_at":"2024-01-01T00:00:00Z","priority":10,"max_attempts":1}}"#
        ))
        .send()
        .await;

    // The Content-Length short-circuit is the point of the layer: the server
    // answers and closes without reading the full body. The client is still
    // uploading when that happens, so it races between reading the 413 and
    // having its own write fail with ECONNRESET — both outcomes are the same
    // refusal, and asserting only on the status code makes this test flaky
    // (~1 run in 5).
    match result {
        Ok(resp) => {
            assert_eq!(resp.status(), 413, "oversized task must be refused");
            // Verify the structured error envelope
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["error"], "payload_too_large");
        }
        Err(err) => assert!(
            err.is_request(),
            "expected a transport-level refusal from the early close, got: {err}"
        ),
    }

    // Race-free invariant, and the one that actually matters: whichever way
    // the client observed the refusal, the oversized body never reached the
    // queue. A regression that buffered and accepted it would show up here
    // even if the status assertion above were satisfied.
    let snapshot: serde_json::Value = client
        .get(format!("{url}/v1/queues/jobs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let task_count = snapshot.get("task_count").and_then(|v| v.as_u64());
    assert_eq!(
        task_count,
        Some(0),
        "the refused body must not be queued: {snapshot}"
    );
}

/// #2556: an undersized task creation body still succeeds, proving the cap
/// isn't just "reject everything".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undersized_task_body_succeeds() {
    let (_dir, raft) = raft().await;
    let auth = AuthConfig::resolve("off", None, None).unwrap();
    let (url, _server) = start_with_body_limit(raft, auth, 8 * 1024 * 1024).await;
    let client = http1_client();

    // Prepare queue
    client
        .put(format!("{url}/v1/queues/jobs"))
        .json(&QueuePolicy::default())
        .send()
        .await
        .unwrap();

    // Create a task well under the 8 MiB cap
    let resp = client
        .post(format!("{url}/v1/queues/jobs/tasks"))
        .json(&serde_json::json!({
            "task_id": "normal",
            "target": {"url": "http://127.0.0.1/", "method": "POST", "headers": {}},
            "payload": {"data": "small payload"},
            "schedule_at": "2024-01-01T00:00:00Z",
            "priority": 10,
            "max_attempts": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "undersized task must be accepted");

    // Verify the task was actually created
    let status: serde_json::Value = client
        .get(format!("{url}/v1/queues/jobs/tasks/normal"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["status"], "Scheduled");
}
// </HANDWRITE>
// HANDWRITE-END
