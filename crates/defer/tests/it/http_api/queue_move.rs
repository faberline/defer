// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-http-api" tracker="#283" reason="Real h2c service shell, OpenAPI, metrics, auth, and domain-route integration."

use super::common::{assert_api_error, h2c_client, raft, start, task_body};
use axum::http::StatusCode;
use defer::{AuthConfig, QueuePolicy};

async fn make_queue(url: &str, client: &reqwest::Client, queue: &str) {
    let response = client
        .put(format!("{url}/v1/queues/{queue}"))
        .json(&QueuePolicy::default())
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "create {queue}: {}",
        response.status()
    );
}

async fn make_task(url: &str, client: &reqwest::Client, queue: &str, task_id: &str) {
    let response = client
        .post(format!("{url}/v1/queues/{queue}/tasks"))
        .json(&task_body(task_id))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "create task {task_id}: {}",
        response.status()
    );
}

// <HANDWRITE gap="missing-generator:unit-test" tracker="#283" reason="Queue move between two raft groups, admin guard, and error mapping for export, import and remove.">
/// #283 AC1: export, import and remove move a queue and its tasks between
/// two raft groups. Export does not remove the source queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_export_import_remove_moves_a_queue_between_two_groups() {
    let auth = || AuthConfig::resolve("off", None, None).unwrap();
    let (_dir_a, raft_a) = raft().await;
    let (url_a, server_a) = start(raft_a, auth()).await;
    let (_dir_b, raft_b) = raft().await;
    let (url_b, server_b) = start(raft_b, auth()).await;
    let client = h2c_client();

    make_queue(&url_a, &client, "jobs").await;
    make_task(&url_a, &client, "jobs", "t1").await;

    let export = client
        .get(format!("{url_a}/v1/admin/queues/jobs/export"))
        .send()
        .await
        .unwrap();
    assert_eq!(export.status(), StatusCode::OK);
    let export_body: serde_json::Value = export.json().await.unwrap();

    let import = client
        .put(format!("{url_b}/v1/admin/queues/jobs/import"))
        .json(&export_body)
        .send()
        .await
        .unwrap();
    assert_eq!(import.status(), StatusCode::NO_CONTENT);

    let moved = client
        .get(format!("{url_b}/v1/queues/jobs/tasks/t1"))
        .send()
        .await
        .unwrap();
    assert_eq!(moved.status(), StatusCode::OK, "task must be on the target");

    let still_here = client
        .get(format!("{url_a}/v1/queues/jobs"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        still_here.status(),
        StatusCode::OK,
        "export must not remove"
    );

    let remove = client
        .delete(format!("{url_a}/v1/admin/queues/jobs/export"))
        .send()
        .await
        .unwrap();
    assert_eq!(remove.status(), StatusCode::NO_CONTENT);

    let gone = client
        .get(format!("{url_a}/v1/queues/jobs"))
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), StatusCode::NOT_FOUND);

    let target_task = client
        .get(format!("{url_b}/v1/queues/jobs/tasks/t1"))
        .send()
        .await
        .unwrap();
    assert_eq!(target_task.status(), StatusCode::OK);

    server_a.abort();
    server_b.abort();
}

/// #283 AC2: importing a queue that already exists is 409 and changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_import_of_an_existing_queue_is_409() {
    let (_dir, raft) = raft().await;
    let (url, server) = start(raft, AuthConfig::resolve("off", None, None).unwrap()).await;
    let client = h2c_client();

    make_queue(&url, &client, "jobs").await;
    let export: serde_json::Value = client
        .get(format!("{url}/v1/admin/queues/jobs/export"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let response = client
        .put(format!("{url}/v1/admin/queues/jobs/import"))
        .json(&export)
        .send()
        .await
        .unwrap();
    assert_api_error(response, StatusCode::CONFLICT, "conflict").await;

    server.abort();
}

/// #283 AC2: an import whose path name differs from the export name is 400
/// and imports nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_import_with_a_mismatched_name_is_400() {
    let auth = || AuthConfig::resolve("off", None, None).unwrap();
    let (_dir_a, raft_a) = raft().await;
    let (url_a, server_a) = start(raft_a, auth()).await;
    let (_dir_b, raft_b) = raft().await;
    let (url_b, server_b) = start(raft_b, auth()).await;
    let client = h2c_client();

    make_queue(&url_a, &client, "jobs").await;
    let export: serde_json::Value = client
        .get(format!("{url_a}/v1/admin/queues/jobs/export"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let response = client
        .put(format!("{url_b}/v1/admin/queues/other/import"))
        .json(&export)
        .send()
        .await
        .unwrap();
    assert_api_error(response, StatusCode::BAD_REQUEST, "invalid_argument").await;

    for queue in ["jobs", "other"] {
        let check = client
            .get(format!("{url_b}/v1/queues/{queue}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            check.status(),
            StatusCode::NOT_FOUND,
            "{queue} must not exist"
        );
    }

    server_a.abort();
    server_b.abort();
}

/// #283 AC3: export and remove of a queue that does not exist are 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_export_and_remove_of_a_missing_queue_are_404() {
    let (_dir, raft) = raft().await;
    let (url, server) = start(raft, AuthConfig::resolve("off", None, None).unwrap()).await;
    let client = h2c_client();

    let export = client
        .get(format!("{url}/v1/admin/queues/nope/export"))
        .send()
        .await
        .unwrap();
    assert_api_error(export, StatusCode::NOT_FOUND, "not_found").await;

    let remove = client
        .delete(format!("{url}/v1/admin/queues/nope/export"))
        .send()
        .await
        .unwrap();
    assert_api_error(remove, StatusCode::NOT_FOUND, "not_found").await;

    server.abort();
}

/// #283 AC4: all three routes need the admin role on `*`. No token is 401,
/// a reader token is 403, and the admin token passes the guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_move_routes_require_the_admin_role() {
    let (_dir_src, raft_src) = raft().await;
    let (url_src, server_src) =
        start(raft_src, AuthConfig::resolve("off", None, None).unwrap()).await;
    let client = h2c_client();
    make_queue(&url_src, &client, "jobs").await;
    let export: serde_json::Value = client
        .get(format!("{url_src}/v1/admin/queues/jobs/export"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    server_src.abort();

    let (_dir, raft) = raft().await;
    let registry = serde_json::json!({
        "reader": {"subject": "reader", "roles": {"jobs": "read"}},
        "admin": {"subject": "admin", "roles": {"*": "admin"}}
    })
    .to_string();
    let auth = AuthConfig::resolve("required", None, Some(&registry)).unwrap();
    let (url, server) = start(raft, auth).await;

    for (method, path) in [
        ("GET", "/v1/admin/queues/nope/export"),
        ("DELETE", "/v1/admin/queues/nope/export"),
    ] {
        let request = |token: Option<&str>| {
            let builder = match method {
                "GET" => client.get(format!("{url}{path}")),
                _ => client.delete(format!("{url}{path}")),
            };
            match token {
                Some(token) => builder.bearer_auth(token),
                None => builder,
            }
        };

        let anonymous = request(None).send().await.unwrap();
        assert_eq!(
            anonymous.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {path}"
        );

        let reader = request(Some("reader")).send().await.unwrap();
        assert_eq!(reader.status(), StatusCode::FORBIDDEN, "{method} {path}");

        let admin = request(Some("admin")).send().await.unwrap();
        assert_eq!(admin.status(), StatusCode::NOT_FOUND, "{method} {path}");
    }

    let import = client
        .put(format!("{url}/v1/admin/queues/jobs/import"))
        .json(&export);
    assert_eq!(
        import.try_clone().unwrap().send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let reader_import = import
        .try_clone()
        .unwrap()
        .bearer_auth("reader")
        .send()
        .await
        .unwrap();
    assert_eq!(reader_import.status(), StatusCode::FORBIDDEN);
    let admin_import = import.bearer_auth("admin").send().await.unwrap();
    assert_eq!(admin_import.status(), StatusCode::NO_CONTENT);

    server.abort();
}
// </HANDWRITE>
// HANDWRITE-END
