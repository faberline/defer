// HANDWRITE-BEGIN gap="missing-generator:logic:defer-http-api" tracker="#766" reason="Shared service-http/auth shell around Defer's Raft-backed domain commands."
//! The defer HTTP router and its handlers. The handlers stay in this module:
//! utoipa derives each operation's default OpenAPI tag (`crate::server`) from
//! it.

mod dto;
mod state;

use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use service_auth::{AuditedRoleMapPrincipal, ReloadableRoleMapVerifier, Role};
use service_http::{ApiErr, MetricsProvider};

use defer_access as auth;
use defer_dispatch::DispatchDisposition;
use defer_replication::ProposeError;
use defer_shared_kernel::{CreateTask, QueuePolicy, SchedulerError};

pub use dto::{
    CreateTasksRequest, CreateTasksResponse, QueueControlRequest, QueueListResponse,
    TaskStatusResponse,
};
pub use state::AppState;

pub fn router(state: AppState) -> Router {
    router_with_admission(state, None)
}

pub fn router_without_raft_routes(state: AppState) -> Router {
    router_without_raft_routes_with_admission(state, None)
}

pub fn router_with_admission(
    state: AppState,
    admission: Option<service_http::AdmissionController>,
) -> Router {
    router_inner(state, true, admission)
}

pub fn router_without_raft_routes_with_admission(
    state: AppState,
    admission: Option<service_http::AdmissionController>,
) -> Router {
    router_inner(state, false, admission)
}

fn router_inner(
    state: AppState,
    include_raft_routes: bool,
    admission: Option<service_http::AdmissionController>,
) -> Router {
    let verifier = state.verifier.clone();
    let body_limit = state.body_limit_bytes();
    let data = Router::new()
        .route("/v1/queues", get(queue_list))
        .route("/v1/queues/{queue}", get(queue_get).put(queue_put))
        .route("/v1/queues/{queue}/control", post(queue_control))
        .route("/v1/queues/{queue}/tasks", post(task_create))
        .route("/v1/queues/{queue}/tasks:batch", post(task_create_batch))
        .route(
            "/v1/queues/{queue}/tasks/{task_id}",
            get(task_status).delete(task_cancel),
        )
        .route("/v1/queues/{queue}/dispatch", post(dispatch_one))
        .route("/admin/backup", get(admin_backup))
        .route_layer(from_fn_with_state(
            verifier,
            service_auth::auth_middleware::<ReloadableRoleMapVerifier>,
        ))
        .with_state(state.clone())
        // Data-plane-only request body cap (#2556); probes stay
        // unbounded, matching `service_http`'s documented probe behavior.
        // Enforces the configured body_limit_bytes with a structured 413 envelope.
        .layer(service_http::body_limit_layer(body_limit));
    let data = match admission {
        Some(controller) => data.route_layer(from_fn_with_state(
            service_http::AdmissionMiddleware::new(controller, |request| {
                let path = request.uri().path();
                let class = if path.starts_with("/admin/") || path.ends_with("/dispatch") {
                    "defer.admin"
                } else if *request.method() == Method::GET {
                    "defer.read"
                } else {
                    "defer.write"
                };
                let key = request
                    .headers()
                    .get(header::AUTHORIZATION)
                    .map(|value| value.as_bytes())
                    .unwrap_or(b"anonymous");
                Some(service_http::AdmissionInput::new(class, key))
            }),
            service_http::admission_middleware,
        )),
        None => data,
    };

    let probe_state = Arc::new(state.clone());
    let metrics: Arc<dyn MetricsProvider> = probe_state.clone();
    let app =
        service_http::standard_probe_routes(probe_state, Some(metrics), crate::openapi::openapi)
            .merge(data)
            .layer(service_http::trace_layer())
            // Per-request Server-Timing response attribution, composed at
            // the same outermost position as trace_layer() above (#2490).
            .layer(axum::middleware::from_fn(
                service_http::server_timing_middleware,
            ));
    if include_raft_routes {
        app.merge(state.raft.router())
    } else {
        app
    }
}

fn authorize(
    principal: &AuditedRoleMapPrincipal,
    queue: &str,
    role: Role,
) -> Result<(), service_auth::AuthError> {
    auth::authorize(principal, queue, role)
}

fn scheduler_error(error: SchedulerError) -> Response {
    let (status, kind) = match error {
        SchedulerError::QueueMissing(_) | SchedulerError::TaskMissing(_) => {
            (StatusCode::NOT_FOUND, "not_found")
        }
        SchedulerError::TaskExists(_) => (StatusCode::CONFLICT, "conflict"),
        SchedulerError::QueueDisabled(_) => (StatusCode::CONFLICT, "queue_disabled"),
        SchedulerError::AttemptMissing(_) => (StatusCode::NOT_FOUND, "not_found"),
    };
    ApiErr::new(status, kind, error.to_string()).into_response()
}

/// A committed scheduler refusal keeps its typed status; anything else is internal.
fn propose_error(error: ProposeError) -> Response {
    match error {
        ProposeError::Scheduler(error) => scheduler_error(error),
        ProposeError::Infra(error) => internal(error),
    }
}

fn internal(error: impl std::fmt::Display) -> Response {
    ApiErr::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        error.to_string(),
    )
    .into_response()
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct QueueListQuery {
    pub page_size: Option<String>,
    pub page_token: Option<String>,
}

#[utoipa::path(
    get,
    path = "/v1/queues",
    params(
        ("page_size" = Option<usize>, Query, description = "Maximum number of queues to return (1..=1000, default 100)"),
        ("page_token" = Option<String>, Query, description = "Page token of previous page"),
    ),
    responses(
        (status = 200, body = QueueListResponse),
        (status = 400, body = service_http::ErrorEnvelope)
    )
)]
pub async fn queue_list(
    State(state): State<AppState>,
    Extension(principal): Extension<AuditedRoleMapPrincipal>,
    Query(query): Query<QueueListQuery>,
) -> Response {
    let page_size = match query.page_size.as_deref() {
        Some(raw) => match raw.parse::<i64>() {
            Ok(size) if (1..=1000).contains(&size) => size as usize,
            _ => {
                return ApiErr::new(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "page_size must be in range 1..=1000",
                )
                .into_response();
            }
        },
        None => 100,
    };

    state.metrics.requests.incr();

    let principal_inner = principal.into_inner();
    let scheduler_arc = state.raft.scheduler();
    let scheduler = scheduler_arc.lock().unwrap();
    let all_queues = scheduler.queue_names();
    let visible_queues: Vec<_> = all_queues
        .into_iter()
        .filter(|q| principal_inner.ensure(q, Role::Read).is_ok())
        .collect();

    let start_idx = match query.page_token.as_deref() {
        Some(token) if !token.is_empty() => visible_queues
            .iter()
            .position(|q| q.as_str() > token)
            .unwrap_or(visible_queues.len()),
        _ => 0,
    };

    let remaining = &visible_queues[start_idx..];
    let (page_queues, next_page_token) = if remaining.len() > page_size {
        (
            &remaining[..page_size],
            Some(remaining[page_size - 1].clone()),
        )
    } else {
        (remaining, None)
    };

    let mut queues = Vec::with_capacity(page_queues.len());
    for name in page_queues {
        if let Ok(snapshot) = scheduler.queue_snapshot(name) {
            queues.push(snapshot);
        }
    }

    Json(QueueListResponse {
        queues,
        next_page_token,
    })
    .into_response()
}

#[utoipa::path(
    get,
    path = "/v1/queues/{queue}",
    params(("queue" = String, Path, description = "Queue name")),
    responses((status = 200, body = QueueSnapshot), (status = 404, body = service_http::ErrorEnvelope))
)]
pub async fn queue_get(
    State(state): State<AppState>,
    Extension(principal): Extension<AuditedRoleMapPrincipal>,
    Path(queue): Path<String>,
) -> Response {
    if let Err(error) = authorize(&principal, &queue, Role::Read) {
        return error.into_response();
    }
    state.metrics.requests.incr();
    match state
        .raft
        .scheduler()
        .lock()
        .unwrap()
        .queue_snapshot(&queue)
    {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => scheduler_error(error),
    }
}

#[utoipa::path(
    put,
    path = "/v1/queues/{queue}",
    params(("queue" = String, Path, description = "Queue name")),
    request_body = QueuePolicy,
    responses((status = 200, body = QueueSnapshot), (status = 400, body = service_http::ErrorEnvelope))
)]
pub async fn queue_put(
    State(state): State<AppState>,
    Extension(principal): Extension<AuditedRoleMapPrincipal>,
    Path(queue): Path<String>,
    Json(policy): Json<QueuePolicy>,
) -> Response {
    if let Err(error) = authorize(&principal, &queue, Role::Write) {
        return error.into_response();
    }
    state.metrics.requests.incr();
    match state.raft.configure_queue(queue, policy).await {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => propose_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/v1/queues/{queue}/control",
    params(("queue" = String, Path, description = "Queue name")),
    request_body = QueueControlRequest,
    responses((status = 200, body = QueueSnapshot), (status = 404, body = service_http::ErrorEnvelope))
)]
pub async fn queue_control(
    State(state): State<AppState>,
    Extension(principal): Extension<AuditedRoleMapPrincipal>,
    Path(queue): Path<String>,
    Json(request): Json<QueueControlRequest>,
) -> Response {
    if let Err(error) = authorize(&principal, &queue, Role::Write) {
        return error.into_response();
    }
    state.metrics.requests.incr();
    match state.raft.set_queue_control(queue, request.state).await {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => propose_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/v1/queues/{queue}/tasks",
    params(("queue" = String, Path, description = "Queue name")),
    request_body = CreateTask,
    responses((status = 201), (status = 404, body = service_http::ErrorEnvelope), (status = 409, body = service_http::ErrorEnvelope))
)]
pub async fn task_create(
    State(state): State<AppState>,
    Extension(principal): Extension<AuditedRoleMapPrincipal>,
    Path(queue): Path<String>,
    Json(task): Json<CreateTask>,
) -> Response {
    if let Err(error) = authorize(&principal, &queue, Role::Write) {
        return error.into_response();
    }
    state.metrics.requests.incr();
    match state.raft.create_task(queue, task).await {
        Ok(()) => StatusCode::CREATED.into_response(),
        Err(error) => propose_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/v1/queues/{queue}/tasks:batch",
    params(("queue" = String, Path, description = "Queue name")),
    request_body = CreateTasksRequest,
    responses((status = 201, body = CreateTasksResponse), (status = 404, body = service_http::ErrorEnvelope), (status = 409, body = service_http::ErrorEnvelope))
)]
pub async fn task_create_batch(
    State(state): State<AppState>,
    Extension(principal): Extension<AuditedRoleMapPrincipal>,
    Path(queue): Path<String>,
    Json(request): Json<CreateTasksRequest>,
) -> Response {
    if let Err(error) = authorize(&principal, &queue, Role::Write) {
        return error.into_response();
    }
    state.metrics.requests.incr();
    match state.raft.create_tasks(queue, request.tasks).await {
        Ok(created) => (StatusCode::CREATED, Json(CreateTasksResponse { created })).into_response(),
        Err(error) => propose_error(error),
    }
}

#[utoipa::path(
    get,
    path = "/v1/queues/{queue}/tasks/{task_id}",
    params(
        ("queue" = String, Path, description = "Queue name"),
        ("task_id" = String, Path, description = "Task id")
    ),
    responses((status = 200, body = TaskStatusResponse), (status = 404, body = service_http::ErrorEnvelope))
)]
pub async fn task_status(
    State(state): State<AppState>,
    Extension(principal): Extension<AuditedRoleMapPrincipal>,
    Path((queue, task_id)): Path<(String, String)>,
) -> Response {
    if let Err(error) = authorize(&principal, &queue, Role::Read) {
        return error.into_response();
    }
    state.metrics.requests.incr();
    match state
        .raft
        .scheduler()
        .lock()
        .unwrap()
        .status(&queue, &task_id)
    {
        Ok(Some(status)) => Json(TaskStatusResponse { task_id, status }).into_response(),
        Ok(None) => {
            ApiErr::new(StatusCode::NOT_FOUND, "not_found", "task not found").into_response()
        }
        Err(error) => scheduler_error(error),
    }
}

#[utoipa::path(
    delete,
    path = "/v1/queues/{queue}/tasks/{task_id}",
    params(
        ("queue" = String, Path, description = "Queue name"),
        ("task_id" = String, Path, description = "Task id")
    ),
    responses((status = 204), (status = 404, body = service_http::ErrorEnvelope))
)]
pub async fn task_cancel(
    State(state): State<AppState>,
    Extension(principal): Extension<AuditedRoleMapPrincipal>,
    Path((queue, task_id)): Path<(String, String)>,
) -> Response {
    if let Err(error) = authorize(&principal, &queue, Role::Write) {
        return error.into_response();
    }
    state.metrics.requests.incr();
    match state.raft.cancel(queue, task_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => ApiErr::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "task is missing or terminal",
        )
        .into_response(),
        Err(error) => propose_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/v1/queues/{queue}/dispatch",
    params(("queue" = String, Path, description = "Queue name")),
    responses((status = 200, body = crate::DispatchReport), (status = 204))
)]
pub async fn dispatch_one(
    State(state): State<AppState>,
    Extension(principal): Extension<AuditedRoleMapPrincipal>,
    Path(queue): Path<String>,
) -> Response {
    if let Err(error) = authorize(&principal, &queue, Role::Admin) {
        return error.into_response();
    }
    state.metrics.requests.incr();
    match state
        .dispatcher
        .dispatch_one(&state.raft, &queue, chrono::Utc::now())
        .await
    {
        Ok(Some(report)) => {
            match report.disposition {
                DispatchDisposition::Acked => state.metrics.dispatch_acked.incr(),
                DispatchDisposition::Retried { .. } => state.metrics.dispatch_retried.incr(),
                DispatchDisposition::DeadLettered => state.metrics.dispatch_dead_lettered.incr(),
                DispatchDisposition::LostOwnership => state.metrics.dispatch_lost_ownership.incr(),
            }
            Json(report).into_response()
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => internal(error),
    }
}

#[utoipa::path(
    get,
    path = "/admin/backup",
    responses((status = 200, description = "Exact Defer state-machine snapshot bytes"))
)]
pub async fn admin_backup(
    State(state): State<AppState>,
    Extension(principal): Extension<AuditedRoleMapPrincipal>,
) -> Response {
    if let Err(error) = authorize(&principal, "*", Role::Admin) {
        return error.into_response();
    }
    match state.raft.snapshot_bytes() {
        Ok(bytes) => ([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response(),
        Err(error) => internal(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn propose_error_keeps_scheduler_status_and_hides_infra_failures() {
        let missing = propose_error(SchedulerError::QueueMissing("jobs".into()).into());
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let disabled = propose_error(SchedulerError::QueueDisabled("jobs".into()).into());
        assert_eq!(disabled.status(), StatusCode::CONFLICT);
        let infra = propose_error(anyhow::anyhow!("commit timed out").into());
        assert_eq!(infra.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn queue_list_response_construction() {
        let response = QueueListResponse {
            queues: vec![],
            next_page_token: None,
        };
        assert!(response.queues.is_empty());
        assert_eq!(response.next_page_token, None);

        let response_with_token = QueueListResponse {
            queues: vec![],
            next_page_token: Some("q2".into()),
        };
        assert_eq!(response_with_token.next_page_token.as_deref(), Some("q2"));
    }
}
// HANDWRITE-END
