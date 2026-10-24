// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-http-api" tracker="#766" reason="Real h2c service shell, OpenAPI, metrics, auth, and domain-route integration."
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use chrono::Utc;
use defer::{AuthConfig, DeferRaft, DeferScheduler, HttpDispatcher, QueuePolicy};
use raft_runtime::Membership;

async fn raft() -> (tempfile::TempDir, Arc<DeferRaft>) {
    let dir = tempfile::tempdir().unwrap();
    let raft = Arc::new(
        DeferRaft::spawn(
            Arc::new(Mutex::new(DeferScheduler::new())),
            &dir.path().join("raft"),
            0,
            Membership {
                voters: vec![0],
                learners: vec![],
            },
            HashMap::new(),
            DeferRaft::host_config(8),
        )
        .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(4);
    while !raft.is_leader().await {
        assert!(
            Instant::now() < deadline,
            "single node did not elect itself"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    (dir, raft)
}

async fn start(raft: Arc<DeferRaft>, auth: AuthConfig) -> (String, tokio::task::JoinHandle<()>) {
    start_with_body_limit(raft, auth, 8 * 1024 * 1024).await
}

async fn start_with_body_limit(
    raft: Arc<DeferRaft>,
    auth: AuthConfig,
    body_limit_bytes: usize,
) -> (String, tokio::task::JoinHandle<()>) {
    let state = defer::server::AppState::new(
        raft,
        HttpDispatcher::new(Duration::from_secs(2), None).unwrap(),
        auth,
        body_limit_bytes,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let serve = tokio::spawn(service_http::serve(
        listener,
        defer::server::router(state),
        std::future::pending(),
    ));
    (url, serve)
}

fn h2c_client() -> reqwest::Client {
    reqwest::Client::builder()
        .http2_prior_knowledge()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap()
}

fn http1_client() -> reqwest::Client {
    reqwest::Client::builder()
        .http1_only()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap()
}

const EXPECTED_DOMAIN_OPERATIONS: &[&str] = &[
    "DELETE /v1/queues/{queue}/tasks/{task_id}",
    "GET /admin/backup",
    "GET /v1/queues/{queue}",
    "GET /v1/queues/{queue}/tasks",
    "GET /v1/queues/{queue}/tasks/{task_id}",
    "POST /v1/queues/{queue}/control",
    "POST /v1/queues/{queue}/dispatch",
    "POST /v1/queues/{queue}/tasks",
    "POST /v1/queues/{queue}/tasks:batch",
    "PUT /v1/queues/{queue}",
];

fn assert_exact_domain_operations(spec: &serde_json::Value) {
    let paths = spec["paths"].as_object().expect("OpenAPI paths object");
    let mut actual = Vec::new();
    for (path, item) in paths {
        let item = item.as_object().expect("OpenAPI path item");
        for method in [
            "delete", "get", "head", "options", "patch", "post", "put", "trace",
        ] {
            if item.contains_key(method) {
                actual.push(format!("{} {path}", method.to_ascii_uppercase()));
            }
        }
    }
    actual.sort();
    assert_eq!(
        actual,
        EXPECTED_DOMAIN_OPERATIONS
            .iter()
            .map(|operation| (*operation).to_string())
            .collect::<Vec<_>>()
    );
}

/// #2490: every response carries a `Server-Timing: app;dur=<ms>` baseline
/// from the shared `service_http::server_timing_middleware` layer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_carry_server_timing_header() {
    let (_dir, raft) = raft().await;
    let (url, server) = start(raft, AuthConfig::open()).await;
    let client = h2c_client();
    let resp = client.get(format!("{url}/healthz")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let header = resp
        .headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        header.starts_with("app;dur="),
        "Server-Timing header must start with app;dur=, got {header:?}"
    );
    let digit = header
        .strip_prefix("app;dur=")
        .and_then(|rest| rest.chars().next());
    assert!(
        digit.is_some_and(|c| c.is_ascii_digit()),
        "app;dur= must be followed by a digit, got {header:?}"
    );
    server.abort();
}

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2215" reason="Own the required-auth h2c oracle for tokenless operational routes, protected task/admin routes, queue-scoped RBAC, and cross-queue tenant denial.">
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2c_routes_probes_openapi_metrics_dispatch_and_auth_are_live() {
    let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_url = format!("http://{}/target", target_listener.local_addr().unwrap());
    let target = tokio::spawn(async move {
        axum::serve(
            target_listener,
            Router::new().route("/target", post(|| async { StatusCode::NO_CONTENT })),
        )
        .await
        .unwrap();
    });

    let (_dir, raft) = raft().await;
    let (url, open_server) = start(raft.clone(), AuthConfig::open()).await;
    let client = h2c_client();
    let http1 = http1_client();
    for (protocol, protocol_client) in [("h2c", &client), ("http/1.1", &http1)] {
        for path in ["/healthz", "/readyz", "/docs", "/openapi.json", "/metrics"] {
            assert_eq!(
                protocol_client
                    .get(format!("{url}{path}"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK,
                "{protocol} {path}"
            );
        }
    }
    let h2c_spec = client
        .get(format!("{url}/openapi.json"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let http1_spec = http1
        .get(format!("{url}/openapi.json"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        h2c_spec, http1_spec,
        "the one-port HTTP/1.1 and h2c OpenAPI responses must be byte-identical"
    );
    let served_spec: serde_json::Value = serde_json::from_slice(&h2c_spec).unwrap();
    assert_eq!(
        served_spec,
        serde_json::to_value(defer::openapi::openapi()).unwrap(),
        "served OpenAPI must equal the canonical IR"
    );
    assert_exact_domain_operations(&served_spec);

    assert_eq!(
        client
            .put(format!("{url}/v1/queues/jobs"))
            .json(&QueuePolicy::default())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        http1
            .get(format!("{url}/v1/queues/jobs"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK,
        "domain routes share the same HTTP/1.1+h2c listener"
    );
    for state in ["Paused", "Running"] {
        let response = http1
            .post(format!("{url}/v1/queues/jobs/control"))
            .json(&serde_json::json!({"state": state}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["control_state"],
            state
        );
    }
    assert_eq!(
        client
            .post(format!("{url}/v1/queues/jobs/tasks"))
            .json(&serde_json::json!({
                "task_id": "hello",
                "target": {"url": target_url, "method": "POST", "headers": {}},
                "payload": {"hello": "world"},
                "schedule_at": Utc::now(),
                "priority": 10,
                "max_attempts": 2
            }))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );
    let batch = http1
        .post(format!("{url}/v1/queues/jobs/tasks:batch"))
        .json(&serde_json::json!({
            "tasks": [{
                "task_id": "batch-through-http1",
                "target": {"url": "http://127.0.0.1/", "method": "POST", "headers": {}},
                "payload": {"contract": "http1-batch-route"},
                "schedule_at": Utc::now() + chrono::Duration::minutes(5),
                "priority": 10,
                "max_attempts": 1
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), StatusCode::CREATED);
    assert_eq!(
        batch.json::<serde_json::Value>().await.unwrap()["created"],
        1
    );
    let dispatched: serde_json::Value = client
        .post(format!("{url}/v1/queues/jobs/dispatch"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dispatched["target_status"], 204);
    let status: serde_json::Value = client
        .get(format!("{url}/v1/queues/jobs/tasks/hello"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["status"], "Succeeded");
    let metrics = client
        .get(format!("{url}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("defer_dispatch_acked_total 1"));

    let snapshot = http1
        .get(format!("{url}/admin/backup"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let seed = tempfile::tempdir().unwrap();
    defer::raft::prepare_bootstrap_seed(seed.path(), 0, &snapshot).unwrap();
    let recovered = Arc::new(Mutex::new(DeferScheduler::new()));
    let recovered_raft = DeferRaft::spawn(
        recovered.clone(),
        &seed.path().join("raft"),
        0,
        Membership {
            voters: vec![0],
            learners: vec![],
        },
        HashMap::new(),
        DeferRaft::host_config(8),
    )
    .unwrap();
    assert!(recovered_raft.applied_index() > 0);
    assert!(matches!(
        recovered.lock().unwrap().status("jobs", "hello").unwrap(),
        Some(defer::TaskStatus::Succeeded)
    ));
    open_server.abort();

    let registry = serde_json::json!({
        "reader": {"subject": "reader", "roles": {"jobs": "read"}},
        "admin": {"subject": "admin", "roles": {"*": "admin"}}
    })
    .to_string();
    let auth = AuthConfig::resolve("required", None, Some(&registry)).unwrap();
    let (auth_url, auth_server) = start(raft, auth).await;
    for path in ["/healthz", "/readyz", "/docs", "/openapi.json", "/metrics"] {
        assert_eq!(
            client
                .get(format!("{auth_url}{path}"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK,
            "required-auth service must leave probe/spec route {path} tokenless"
        );
    }
    assert_eq!(
        client
            .get(format!("{auth_url}/v1/queues/jobs"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(format!("{auth_url}/v1/queues/jobs"))
            .bearer_auth("reader")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .post(format!("{auth_url}/v1/queues/jobs/tasks"))
            .json(&serde_json::json!({
                "task_id": "unauthenticated",
                "target": {"url": "http://127.0.0.1/", "method": "POST", "headers": {}},
                "payload": {},
                "schedule_at": Utc::now(),
                "priority": 10,
                "max_attempts": 1
            }))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(format!("{auth_url}/admin/backup"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(format!("{auth_url}/admin/backup"))
            .bearer_auth("reader")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .put(format!("{auth_url}/v1/queues/jobs"))
            .bearer_auth("reader")
            .json(&QueuePolicy::default())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .put(format!("{auth_url}/v1/queues/jobs"))
            .bearer_auth("admin")
            .json(&QueuePolicy::default())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .put(format!("{auth_url}/v1/queues/other-tenant"))
            .bearer_auth("admin")
            .json(&QueuePolicy::default())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("{auth_url}/v1/queues/other-tenant"))
            .bearer_auth("reader")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN,
        "a queue-scoped credential must not cross a tenant boundary"
    );
    assert_eq!(
        client
            .post(format!("{auth_url}/v1/queues/jobs/tasks"))
            .bearer_auth("admin")
            .json(&serde_json::json!({
                "task_id": "cancel-through-api",
                "target": {"url": "http://127.0.0.1/", "method": "POST", "headers": {}},
                "payload": {"contract": "public-cancel-inspect"},
                "schedule_at": Utc::now() + chrono::Duration::minutes(5),
                "priority": 10,
                "max_attempts": 1
            }))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );
    assert_eq!(
        client
            .delete(format!(
                "{auth_url}/v1/queues/jobs/tasks/cancel-through-api"
            ))
            .bearer_auth("reader")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN,
        "read access must not grant task cancellation"
    );
    let still_scheduled: serde_json::Value = client
        .get(format!(
            "{auth_url}/v1/queues/jobs/tasks/cancel-through-api"
        ))
        .bearer_auth("reader")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        still_scheduled["status"], "Scheduled",
        "a rejected cancellation must not mutate task state"
    );
    assert_eq!(
        client
            .delete(format!(
                "{auth_url}/v1/queues/jobs/tasks/cancel-through-api"
            ))
            .bearer_auth("admin")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    let canceled: serde_json::Value = client
        .get(format!(
            "{auth_url}/v1/queues/jobs/tasks/cancel-through-api"
        ))
        .bearer_auth("reader")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(canceled["status"], "Canceled");

    auth_server.abort();
    target.abort();
}

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

async fn assert_api_error(response: reqwest::Response, status: StatusCode, kind: &str) {
    assert_eq!(response.status(), status);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], kind, "unexpected error envelope: {body}");
}

fn task_body(task_id: &str) -> serde_json::Value {
    serde_json::json!({
        "task_id": task_id,
        "target": {"url": "http://127.0.0.1/", "method": "POST", "headers": {}},
        "payload": {},
        "schedule_at": Utc::now() + chrono::Duration::minutes(5),
        "priority": 10,
        "max_attempts": 1
    })
}

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
