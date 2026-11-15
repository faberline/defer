// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-http-api" tracker="#766" reason="Real h2c service shell, OpenAPI, metrics, auth, and domain-route integration."

use super::common::{assert_api_error, h2c_client, raft, start};
use axum::http::StatusCode;
use defer::AuthConfig;

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2215" reason="Own the required-auth h2c oracle for tokenless operational routes, protected task/admin routes, queue-scoped RBAC, and cross-queue tenant denial.">
/// #86 AC4: the membership route needs the `Admin` role on `*`. No token is
/// 401 and a reader token is 403.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_requires_admin_role() {
    let (_dir, raft) = raft().await;
    let registry = serde_json::json!({
        "reader": {"subject": "reader", "roles": {"jobs": "read"}},
        "admin": {"subject": "admin", "roles": {"*": "admin"}}
    })
    .to_string();
    let auth = AuthConfig::resolve("required", None, Some(&registry)).unwrap();
    let (url, server) = start(raft, auth).await;
    let client = h2c_client();
    let body = serde_json::json!({"op": "demote", "node": 0});

    let anonymous = client
        .post(format!("{url}/v1/admin/membership"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);

    let reader = client
        .post(format!("{url}/v1/admin/membership"))
        .bearer_auth("reader")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(reader.status(), StatusCode::FORBIDDEN);

    server.abort();
}

/// #86 AC2/AC3: `add_learner` without a `url` is a 400 `invalid_argument`.
/// Without the route this is a 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_add_learner_without_url_is_400() {
    let (_dir, raft) = raft().await;
    let (url, server) = start(raft, AuthConfig::resolve("off", None, None).unwrap()).await;
    let response = h2c_client()
        .post(format!("{url}/v1/admin/membership"))
        .json(&serde_json::json!({"op": "add_learner", "node": 1}))
        .send()
        .await
        .unwrap();
    assert_api_error(response, StatusCode::BAD_REQUEST, "invalid_argument").await;
    server.abort();
}

/// #86 AC3: the single voter is the leader, so demoting it is refused with
/// 422 `membership_refused`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_demote_leader_is_422() {
    let (_dir, raft) = raft().await;
    let (url, server) = start(raft, AuthConfig::resolve("off", None, None).unwrap()).await;
    let response = h2c_client()
        .post(format!("{url}/v1/admin/membership"))
        .json(&serde_json::json!({"op": "demote", "node": 0}))
        .send()
        .await
        .unwrap();
    assert_api_error(
        response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "membership_refused",
    )
    .await;
    server.abort();
}

/// #86 AC2: `add_learner` with a `url` registers the peer, admits it as a
/// learner and returns the log `index` as a number.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_add_learner_returns_index() {
    let (_dir, raft) = raft().await;
    let (url, server) = start(raft, AuthConfig::resolve("off", None, None).unwrap()).await;
    let response = h2c_client()
        .post(format!("{url}/v1/admin/membership"))
        .json(&serde_json::json!({
            "op": "add_learner",
            "node": 1,
            "url": "http://127.0.0.1:1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["index"].is_u64(), "unexpected body: {body}");
    server.abort();
}

/// #98 AC3: `remove` of the single voter (the leader) is refused with 422
/// `membership_refused`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_remove_leader_is_422() {
    let (_dir, raft) = raft().await;
    let (url, server) = start(raft, AuthConfig::resolve("off", None, None).unwrap()).await;
    let response = h2c_client()
        .post(format!("{url}/v1/admin/membership"))
        .json(&serde_json::json!({"op": "remove", "node": 0}))
        .send()
        .await
        .unwrap();
    assert_api_error(
        response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "membership_refused",
    )
    .await;
    server.abort();
}

/// #98 AC3: `remove` of a node that is not a member is refused with 422
/// `membership_refused`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_remove_unknown_node_is_422() {
    let (_dir, raft) = raft().await;
    let (url, server) = start(raft, AuthConfig::resolve("off", None, None).unwrap()).await;
    let response = h2c_client()
        .post(format!("{url}/v1/admin/membership"))
        .json(&serde_json::json!({"op": "remove", "node": 9}))
        .send()
        .await
        .unwrap();
    assert_api_error(
        response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "membership_refused",
    )
    .await;
    server.abort();
}

/// #98 AC1 and AC3: a learner added with `add_learner` is removed with 200
/// and the log `index` as a number. A 409 (leader change or a transition in
/// flight) is retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_remove_learner_returns_index() {
    let (_dir, raft) = raft().await;
    let (url, server) = start(raft, AuthConfig::resolve("off", None, None).unwrap()).await;
    let client = h2c_client();
    let added = client
        .post(format!("{url}/v1/admin/membership"))
        .json(&serde_json::json!({
            "op": "add_learner",
            "node": 1,
            "url": "http://127.0.0.1:1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(added.status(), StatusCode::OK);

    let mut final_response = None;
    for _ in 0..50 {
        let response = client
            .post(format!("{url}/v1/admin/membership"))
            .json(&serde_json::json!({"op": "remove", "node": 1}))
            .send()
            .await
            .unwrap();
        // 422 is also retried: `add_learner` returns before the learner is
        // applied to the membership, so the first remove sees `NotAMember`.
        if response.status() != StatusCode::CONFLICT
            && response.status() != StatusCode::UNPROCESSABLE_ENTITY
        {
            final_response = Some(response);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let response = final_response.expect("remove stayed 409 for 50 attempts");
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["index"].is_u64(), "unexpected body: {body}");
    server.abort();
}
// </HANDWRITE>
// HANDWRITE-END
