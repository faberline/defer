// HANDWRITE-BEGIN gap="missing-generator:logic:defer-request-audit" tracker="#261" reason="Request audit record: one structured log event per mutating request and per failed authentication."
//! Request audit records: one `defer.audit` log event for each mutating
//! request and for each failed authentication. No payload, header or token is
//! written.

use axum::extract::{MatchedPath, Request};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use service_auth::AuditedRoleMapPrincipal;

/// The queue a path names: the first segment after `/v1/queues/`, or `-` for
/// any other path (and for a path with no queue name).
pub(crate) fn queue_of(path: &str) -> &str {
    let Some(rest) = path.strip_prefix("/v1/queues/") else {
        return "-";
    };
    let queue = rest.split(['/', ':']).next().unwrap_or_default();
    if queue.is_empty() {
        "-"
    } else {
        queue
    }
}

/// Whether a method changes state. GET, HEAD and OPTIONS do not.
pub(crate) fn is_mutating(method: &Method) -> bool {
    *method != Method::GET && *method != Method::HEAD && *method != Method::OPTIONS
}

/// The result name of a response status.
pub(crate) fn result_of(status: StatusCode) -> &'static str {
    match status.as_u16() {
        401 => "unauthenticated",
        403 => "forbidden",
        200..=299 => "success",
        400..=499 => "rejected",
        _ => "error",
    }
}

/// The matched route pattern of a request, for example `/v1/queues/{queue}`.
fn route_of(request: &Request) -> String {
    request
        .extensions()
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str)
        .to_owned()
}

/// Writes one `defer.audit` event. A refusal (401 or 403) is logged at warn.
fn emit(principal: &str, method: &Method, route: &str, queue: &str, status: StatusCode) {
    let operation = format!("{method} {route}");
    let result = result_of(status);
    let time = chrono::Utc::now().to_rfc3339();
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        tracing::warn!(target: "defer.audit", event = "request_audit", time = %time, principal, operation = %operation, queue, result, status = status.as_u16());
    } else {
        tracing::info!(target: "defer.audit", event = "request_audit", time = %time, principal, operation = %operation, queue, result, status = status.as_u16());
    }
}

/// Audits a mutating request that passed authentication. It runs inside the
/// auth layer, so the request holds the verified principal.
pub(crate) async fn audit_authenticated(request: Request, next: Next) -> Response {
    let principal = request
        .extensions()
        .get::<AuditedRoleMapPrincipal>()
        .and_then(|principal| principal.subject())
        .unwrap_or("anonymous")
        .to_owned();
    let method = request.method().clone();
    let route = route_of(&request);
    let queue = queue_of(request.uri().path()).to_owned();
    let response = next.run(request).await;
    if is_mutating(&method) {
        emit(&principal, &method, &route, &queue, response.status());
    }
    response
}

/// Audits a failed authentication (HTTP 401) for any method. It runs outside
/// the auth layer, so it sees the refusal.
pub(crate) async fn audit_unauthenticated(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let route = route_of(&request);
    let queue = queue_of(request.uri().path()).to_owned();
    let response = next.run(request).await;
    if response.status() == StatusCode::UNAUTHORIZED {
        emit("anonymous", &method, &route, &queue, response.status());
    }
    response
}

#[cfg(test)]
mod tests {
    use axum::http::{Method, StatusCode};

    use super::{is_mutating, queue_of, result_of};

    #[test]
    fn queue_of_reads_the_queue_segment() {
        assert_eq!(queue_of("/v1/queues/jobs"), "jobs");
        assert_eq!(queue_of("/v1/queues/jobs/tasks/t1"), "jobs");
        assert_eq!(queue_of("/v1/queues/jobs/tasks:batch"), "jobs");
        assert_eq!(queue_of("/v1/queues/jobs:x"), "jobs");
    }

    #[test]
    fn queue_of_is_dash_without_a_queue() {
        for path in [
            "/v1/queues",
            "/v1/queues/",
            "/admin/backup",
            "/v1/admin/membership",
            "/healthz",
        ] {
            assert_eq!(queue_of(path), "-", "{path}");
        }
    }

    #[test]
    fn only_writes_are_mutating() {
        for method in [Method::POST, Method::PUT, Method::DELETE, Method::PATCH] {
            assert!(is_mutating(&method), "{method}");
        }
        for method in [Method::GET, Method::HEAD, Method::OPTIONS] {
            assert!(!is_mutating(&method), "{method}");
        }
    }

    #[test]
    fn result_names_follow_the_status() {
        assert_eq!(result_of(StatusCode::CREATED), "success");
        assert_eq!(result_of(StatusCode::UNAUTHORIZED), "unauthenticated");
        assert_eq!(result_of(StatusCode::FORBIDDEN), "forbidden");
        assert_eq!(result_of(StatusCode::NOT_FOUND), "rejected");
        assert_eq!(result_of(StatusCode::CONFLICT), "rejected");
        assert_eq!(result_of(StatusCode::INTERNAL_SERVER_ERROR), "error");
    }
}
// HANDWRITE-END
