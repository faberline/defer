// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-http-api" tracker="#766" reason="Real h2c service shell, OpenAPI, metrics, auth, and domain-route integration."

use super::common::{assert_api_error, http1_client, raft, start, task_body};
use axum::http::StatusCode;
use chrono::Utc;
use defer::{AuthConfig, QueuePolicy};

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2215" reason="Own the required-auth h2c oracle for tokenless operational routes, protected task/admin routes, queue-scoped RBAC, and cross-queue tenant denial.">
/// faberline/defer#19: write paths map a committed `SchedulerError` to its
/// typed status instead of 500.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_paths_return_typed_scheduler_errors() {
    let (_dir, raft) = raft().await;
    let (url, _server) = start(raft, AuthConfig::open()).await;
    let client = http1_client();

    let missing = client
        .post(format!("{url}/v1/queues/missing/tasks"))
        .json(&task_body("t"))
        .send()
        .await
        .unwrap();
    assert_api_error(missing, StatusCode::NOT_FOUND, "not_found").await;
    let missing = client
        .post(format!("{url}/v1/queues/missing/tasks:batch"))
        .json(&serde_json::json!({"tasks": [task_body("t")]}))
        .send()
        .await
        .unwrap();
    assert_api_error(missing, StatusCode::NOT_FOUND, "not_found").await;
    let missing = client
        .post(format!("{url}/v1/queues/missing/control"))
        .json(&serde_json::json!({"state": "Paused"}))
        .send()
        .await
        .unwrap();
    assert_api_error(missing, StatusCode::NOT_FOUND, "not_found").await;
    let missing = client
        .delete(format!("{url}/v1/queues/missing/tasks/t"))
        .send()
        .await
        .unwrap();
    assert_api_error(missing, StatusCode::NOT_FOUND, "not_found").await;

    client
        .put(format!("{url}/v1/queues/jobs"))
        .json(&QueuePolicy::default())
        .send()
        .await
        .unwrap();
    let created = client
        .post(format!("{url}/v1/queues/jobs/tasks"))
        .json(&task_body("dup"))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let duplicate = client
        .post(format!("{url}/v1/queues/jobs/tasks"))
        .json(&task_body("dup"))
        .send()
        .await
        .unwrap();
    assert_api_error(duplicate, StatusCode::CONFLICT, "conflict").await;
    let duplicate = client
        .post(format!("{url}/v1/queues/jobs/tasks:batch"))
        .json(&serde_json::json!({"tasks": [task_body("dup")]}))
        .send()
        .await
        .unwrap();
    assert_api_error(duplicate, StatusCode::CONFLICT, "conflict").await;

    let disabled = client
        .post(format!("{url}/v1/queues/jobs/control"))
        .json(&serde_json::json!({"state": "Disabled"}))
        .send()
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::OK);
    let refused = client
        .post(format!("{url}/v1/queues/jobs/tasks"))
        .json(&task_body("after-disable"))
        .send()
        .await
        .unwrap();
    assert_api_error(refused, StatusCode::CONFLICT, "queue_disabled").await;
}

/// faberline/defer#113: `terminal_retention_ms` round-trips on the HTTP API,
/// defaults to no pruning, and a duplicate `task_id` stays refused while its
/// terminal record is inside the retention.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_retention_round_trips_and_keeps_duplicate_ids_refused() {
    let (_dir, raft) = raft().await;
    let (url, _server) = start(raft, AuthConfig::open()).await;
    let client = http1_client();

    let put = client
        .put(format!("{url}/v1/queues/jobs"))
        .json(&QueuePolicy {
            terminal_retention_ms: Some(3_600_000),
            ..QueuePolicy::default()
        })
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let body: serde_json::Value = client
        .get(format!("{url}/v1/queues/jobs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["policy"]["terminal_retention_ms"], 3_600_000);

    let put = client
        .put(format!("{url}/v1/queues/plain"))
        .json(&QueuePolicy::default())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let body: serde_json::Value = client
        .get(format!("{url}/v1/queues/plain"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(body["policy"]["terminal_retention_ms"].is_null());

    let created = client
        .post(format!("{url}/v1/queues/jobs/tasks"))
        .json(&task_body("dup"))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let canceled = client
        .delete(format!("{url}/v1/queues/jobs/tasks/dup"))
        .send()
        .await
        .unwrap();
    assert!(canceled.status().is_success());
    let duplicate = client
        .post(format!("{url}/v1/queues/jobs/tasks"))
        .json(&task_body("dup"))
        .send()
        .await
        .unwrap();
    assert_api_error(duplicate, StatusCode::CONFLICT, "conflict").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_list_paged_filtered_and_authorized() {
    let (_dir, raft) = raft().await;
    let (url, _server) = start(raft.clone(), AuthConfig::open()).await;
    let client = http1_client();

    // 1. Missing queue answers 404
    let res = client
        .get(format!("{url}/v1/queues/nonexistent/tasks"))
        .send()
        .await
        .unwrap();
    assert_api_error(res, StatusCode::NOT_FOUND, "not_found").await;

    // Create queue
    let res = client
        .put(format!("{url}/v1/queues/jobs"))
        .json(&QueuePolicy::default())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 2. An empty queue lists nothing
    let res = client
        .get(format!("{url}/v1/queues/jobs/tasks"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["tasks"], serde_json::json!([]));
    assert!(body.get("next_cursor").is_none() || body["next_cursor"].is_null());

    // 3. Bad limit answers 400
    let res = client
        .get(format!("{url}/v1/queues/jobs/tasks?limit=0"))
        .send()
        .await
        .unwrap();
    assert_api_error(res, StatusCode::BAD_REQUEST, "invalid").await;

    let res = client
        .get(format!("{url}/v1/queues/jobs/tasks?limit=1001"))
        .send()
        .await
        .unwrap();
    assert_api_error(res, StatusCode::BAD_REQUEST, "invalid").await;

    // 4. Bad state answers 400
    let res = client
        .get(format!("{url}/v1/queues/jobs/tasks?state=unknown"))
        .send()
        .await
        .unwrap();
    assert_api_error(res, StatusCode::BAD_REQUEST, "invalid").await;

    // 5. Populate tasks with mixed states
    let now = Utc::now();
    // task-1: Scheduled (future)
    client
        .post(format!("{url}/v1/queues/jobs/tasks"))
        .json(&serde_json::json!({
            "task_id": "task-1",
            "target": {"url": "http://127.0.0.1/", "method": "POST", "headers": {}},
            "payload": {},
            "schedule_at": now + chrono::Duration::hours(1),
            "priority": 10,
            "max_attempts": 1
        }))
        .send()
        .await
        .unwrap();

    // task-2, task-3, task-4: Due now
    for id in ["task-2", "task-3", "task-4"] {
        client
            .post(format!("{url}/v1/queues/jobs/tasks"))
            .json(&serde_json::json!({
                "task_id": id,
                "target": {"url": "http://127.0.0.1/", "method": "POST", "headers": {}},
                "payload": {},
                "schedule_at": now - chrono::Duration::seconds(10),
                "priority": 10,
                "max_attempts": 1
            }))
            .send()
            .await
            .unwrap();
    }

    // task-5: Future (will cancel)
    client
        .post(format!("{url}/v1/queues/jobs/tasks"))
        .json(&serde_json::json!({
            "task_id": "task-5",
            "target": {"url": "http://127.0.0.1/", "method": "POST", "headers": {}},
            "payload": {},
            "schedule_at": now + chrono::Duration::hours(1),
            "priority": 10,
            "max_attempts": 1
        }))
        .send()
        .await
        .unwrap();

    // Transition states:
    {
        let sched_arc = raft.scheduler();
        let mut sched = sched_arc.lock().unwrap();
        // Lease task-2
        let l2 = sched.lease_due("jobs", now, 1).unwrap();
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].task_id, "task-2");

        // Lease task-3 and ack -> Succeeded
        let l3 = sched.lease_due("jobs", now, 1).unwrap();
        assert_eq!(l3.len(), 1);
        assert_eq!(l3[0].task_id, "task-3");
        assert!(sched.ack("jobs", &l3[0].attempt_id).unwrap());

        // Lease task-4 and nack -> DeadLettered (max_attempts = 1)
        let l4 = sched.lease_due("jobs", now, 1).unwrap();
        assert_eq!(l4.len(), 1);
        assert_eq!(l4[0].task_id, "task-4");
        assert_eq!(
            sched.nack("jobs", &l4[0].attempt_id, now).unwrap(),
            Some(defer::NackOutcome::DeadLettered)
        );
    }
    // Cancel task-5 via DELETE
    let res = client
        .delete(format!("{url}/v1/queues/jobs/tasks/task-5"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // 6. Mixed queue lists every state and each filter returns only its state
    let res = client
        .get(format!("{url}/v1/queues/jobs/tasks"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let all: serde_json::Value = res.json().await.unwrap();
    let tasks = all["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 5);
    assert_eq!(tasks[0]["task_id"], "task-1");
    assert_eq!(tasks[1]["task_id"], "task-2");
    assert_eq!(tasks[2]["task_id"], "task-3");
    assert_eq!(tasks[3]["task_id"], "task-4");
    assert_eq!(tasks[4]["task_id"], "task-5");

    for (state, expected_id) in [
        ("scheduled", "task-1"),
        ("leased", "task-2"),
        ("succeeded", "task-3"),
        ("dead_lettered", "task-4"),
        ("canceled", "task-5"),
    ] {
        let res = client
            .get(format!("{url}/v1/queues/jobs/tasks?state={state}"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value = res.json().await.unwrap();
        let tasks = body["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 1, "expected 1 task for state={state}");
        assert_eq!(tasks[0]["task_id"], expected_id);
    }

    // 7. Paging with limit=2 walks all tasks exactly once in order
    let res = client
        .get(format!("{url}/v1/queues/jobs/tasks?limit=2"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let p1: serde_json::Value = res.json().await.unwrap();
    let t1 = p1["tasks"].as_array().unwrap();
    assert_eq!(t1.len(), 2);
    assert_eq!(t1[0]["task_id"], "task-1");
    assert_eq!(t1[1]["task_id"], "task-2");
    assert_eq!(p1["next_cursor"], "task-2");

    let res = client
        .get(format!("{url}/v1/queues/jobs/tasks?limit=2&cursor=task-2"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let p2: serde_json::Value = res.json().await.unwrap();
    let t2 = p2["tasks"].as_array().unwrap();
    assert_eq!(t2.len(), 2);
    assert_eq!(t2[0]["task_id"], "task-3");
    assert_eq!(t2[1]["task_id"], "task-4");
    assert_eq!(p2["next_cursor"], "task-4");

    let res = client
        .get(format!("{url}/v1/queues/jobs/tasks?limit=2&cursor=task-4"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let p3: serde_json::Value = res.json().await.unwrap();
    let t3 = p3["tasks"].as_array().unwrap();
    assert_eq!(t3.len(), 1);
    assert_eq!(t3[0]["task_id"], "task-5");
    assert!(p3.get("next_cursor").is_none() || p3["next_cursor"].is_null());

    // 8. With --auth required a token without read on the queue is refused
    let registry = serde_json::json!({
        "reader": {"subject": "reader", "roles": {"jobs": "read"}},
        "writer": {"subject": "writer", "roles": {"jobs": "write"}},
        "other":  {"subject": "other",  "roles": {"other_queue": "read"}}
    })
    .to_string();
    let auth = AuthConfig::resolve("required", None, Some(&registry)).unwrap();
    let (auth_url, _auth_server) = start(raft, auth).await;

    // No token -> 401
    let res = client
        .get(format!("{auth_url}/v1/queues/jobs/tasks"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Writer token (write role implies read) -> 200
    let res = client
        .get(format!("{auth_url}/v1/queues/jobs/tasks"))
        .bearer_auth("writer")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Other token (read on different queue) -> 403
    let res = client
        .get(format!("{auth_url}/v1/queues/jobs/tasks"))
        .bearer_auth("other")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Reader token -> 200
    let res = client
        .get(format!("{auth_url}/v1/queues/jobs/tasks"))
        .bearer_auth("reader")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["tasks"].as_array().unwrap().len(), 5);
}
// </HANDWRITE>
// HANDWRITE-END
