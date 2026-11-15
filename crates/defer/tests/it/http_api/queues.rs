// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-http-api" tracker="#766" reason="Real h2c service shell, OpenAPI, metrics, auth, and domain-route integration."

use super::common::{assert_api_error, http1_client, raft, start};
use axum::http::StatusCode;
use defer::{AuthConfig, QueuePolicy};

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2215" reason="Own the required-auth h2c oracle for tokenless operational routes, protected task/admin routes, queue-scoped RBAC, and cross-queue tenant denial.">
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_list_paged_and_read_filtered() {
    let (_dir, raft) = raft().await;
    let (url, _server) = start(raft.clone(), AuthConfig::open()).await;
    let client = http1_client();

    let res = client.get(format!("{url}/v1/queues")).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["queues"], serde_json::json!([]));
    assert!(body.get("next_page_token").is_none() || body["next_page_token"].is_null());

    let res = client
        .get(format!("{url}/v1/queues?page_size=0"))
        .send()
        .await
        .unwrap();
    assert_api_error(res, StatusCode::BAD_REQUEST, "bad_request").await;

    let res = client
        .get(format!("{url}/v1/queues?page_size=1001"))
        .send()
        .await
        .unwrap();
    assert_api_error(res, StatusCode::BAD_REQUEST, "bad_request").await;

    for q in ["q1", "q2", "q3", "q4", "q5"] {
        let res = client
            .put(format!("{url}/v1/queues/{q}"))
            .json(&QueuePolicy::default())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    let res = client
        .get(format!("{url}/v1/queues?page_size=2"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let p1: serde_json::Value = res.json().await.unwrap();
    let q1 = p1["queues"].as_array().unwrap();
    assert_eq!(q1.len(), 2);
    assert_eq!(q1[0]["queue"], "q1");
    assert_eq!(q1[1]["queue"], "q2");
    assert_eq!(p1["next_page_token"], "q2");

    let res = client
        .get(format!("{url}/v1/queues?page_size=2&page_token=q2"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let p2: serde_json::Value = res.json().await.unwrap();
    let q2 = p2["queues"].as_array().unwrap();
    assert_eq!(q2.len(), 2);
    assert_eq!(q2[0]["queue"], "q3");
    assert_eq!(q2[1]["queue"], "q4");
    assert_eq!(p2["next_page_token"], "q4");

    let res = client
        .get(format!("{url}/v1/queues?page_size=2&page_token=q4"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let p3: serde_json::Value = res.json().await.unwrap();
    let q3 = p3["queues"].as_array().unwrap();
    assert_eq!(q3.len(), 1);
    assert_eq!(q3[0]["queue"], "q5");
    assert!(p3.get("next_page_token").is_none() || p3["next_page_token"].is_null());

    let registry = serde_json::json!({
        "reader": {
            "subject": "reader",
            "roles": {
                "q2": "read",
                "q4": "read"
            }
        }
    })
    .to_string();
    let auth = AuthConfig::resolve("required", None, Some(&registry)).unwrap();
    let (auth_url, _auth_server) = start(raft, auth).await;

    let res = client
        .get(format!("{auth_url}/v1/queues"))
        .bearer_auth("reader")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    let queues = body["queues"].as_array().unwrap();
    assert_eq!(queues.len(), 2);
    assert_eq!(queues[0]["queue"], "q2");
    assert_eq!(queues[1]["queue"], "q4");
    assert!(body.get("next_page_token").is_none() || body["next_page_token"].is_null());
}
// </HANDWRITE>
// HANDWRITE-END
