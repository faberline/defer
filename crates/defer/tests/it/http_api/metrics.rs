// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-http-api" tracker="#766" reason="Real h2c service shell, OpenAPI, metrics, auth, and domain-route integration."

use super::common::{h2c_client, raft, start, task_body};
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use chrono::Utc;
use defer::{AuthConfig, QueuePolicy};

// <HANDWRITE gap="missing-generator:unit-test" tracker="#766" reason="Assert per-queue depth gauges on /metrics across queue creation, task creation, and dispatch.">
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_expose_per_queue_task_depth_gauges() {
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

    let q1 = client
        .put(format!("{url}/v1/queues/queue-one"))
        .json(&QueuePolicy::default())
        .send()
        .await
        .unwrap();
    assert_eq!(q1.status(), StatusCode::OK);

    let q2 = client
        .put(format!("{url}/v1/queues/queue-two"))
        .json(&QueuePolicy::default())
        .send()
        .await
        .unwrap();
    assert_eq!(q2.status(), StatusCode::OK);

    let created_due = client
        .post(format!("{url}/v1/queues/queue-one/tasks"))
        .json(&serde_json::json!({
            "task_id": "task-q1-due",
            "target": {"url": target_url, "method": "POST", "headers": {}},
            "payload": {"test": "metric"},
            "schedule_at": Utc::now(),
            "priority": 10,
            "max_attempts": 2
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created_due.status(), StatusCode::CREATED);

    let created_future1 = client
        .post(format!("{url}/v1/queues/queue-one/tasks"))
        .json(&task_body("task-q1-future"))
        .send()
        .await
        .unwrap();
    assert_eq!(created_future1.status(), StatusCode::CREATED);

    let created_future2 = client
        .post(format!("{url}/v1/queues/queue-two/tasks"))
        .json(&task_body("task-q2-future"))
        .send()
        .await
        .unwrap();
    assert_eq!(created_future2.status(), StatusCode::CREATED);

    let dispatched: serde_json::Value = client
        .post(format!("{url}/v1/queues/queue-one/dispatch"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dispatched["target_status"], 204);

    let metrics = client
        .get(format!("{url}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("defer_queue_tasks{queue=\"queue-one\",state=\"scheduled\"} 1"));
    assert!(metrics.contains("defer_queue_tasks{queue=\"queue-one\",state=\"in_flight\"} 0"));
    assert!(metrics.contains("defer_queue_tasks{queue=\"queue-one\",state=\"terminal\"} 1"));
    assert!(metrics.contains("defer_queue_tasks{queue=\"queue-two\",state=\"scheduled\"} 1"));
    assert!(metrics.contains("defer_queue_tasks{queue=\"queue-two\",state=\"in_flight\"} 0"));
    assert!(metrics.contains("defer_queue_tasks{queue=\"queue-two\",state=\"terminal\"} 0"));

    target.abort();
    open_server.abort();
}
// </HANDWRITE>
// HANDWRITE-END
