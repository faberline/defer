// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-http-api" tracker="#766" reason="Real h2c service shell, OpenAPI, metrics, auth, and domain-route integration."

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use chrono::Utc;
use defer::{AuthConfig, DeferRaft, DeferScheduler, HttpDispatcher};
use raft_runtime::{Membership, NodeId};

pub(super) async fn raft() -> (tempfile::TempDir, Arc<DeferRaft>) {
    let dir = tempfile::tempdir().unwrap();
    let raft = Arc::new(
        DeferRaft::spawn(
            Arc::new(Mutex::new(DeferScheduler::new())),
            &dir.path().join("raft"),
            NodeId::new(0),
            Membership::new(vec![NodeId::new(0)], vec![]),
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

pub(super) async fn start(
    raft: Arc<DeferRaft>,
    auth: AuthConfig,
) -> (String, tokio::task::JoinHandle<()>) {
    start_with_body_limit(raft, auth, 8 * 1024 * 1024).await
}

pub(super) async fn start_with_body_limit(
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

pub(super) fn h2c_client() -> reqwest::Client {
    reqwest::Client::builder()
        .http2_prior_knowledge()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap()
}

pub(super) fn http1_client() -> reqwest::Client {
    reqwest::Client::builder()
        .http1_only()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap()
}

pub(super) async fn assert_api_error(response: reqwest::Response, status: StatusCode, kind: &str) {
    assert_eq!(response.status(), status);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], kind, "unexpected error envelope: {body}");
}

pub(super) fn task_body(task_id: &str) -> serde_json::Value {
    serde_json::json!({
        "task_id": task_id,
        "target": {"url": "http://127.0.0.1/", "method": "POST", "headers": {}},
        "payload": {},
        "schedule_at": Utc::now() + chrono::Duration::minutes(5),
        "priority": 10,
        "max_attempts": 1
    })
}
// HANDWRITE-END
