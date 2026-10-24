// HANDWRITE-BEGIN gap="missing-generator:logic:defer-openapi" tracker="#766" reason="One generated OpenAPI document shared by spec, server, docs, and client codegen."
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Defer HTTP API",
        description = "Raft-backed delayed HTTP push queue over HTTP/1.1 and h2c."
    ),
    paths(
        crate::server::queue_list,
        crate::server::queue_get,
        crate::server::queue_put,
        crate::server::queue_control,
        crate::server::task_create,
        crate::server::task_create_batch,
        crate::server::task_status,
        crate::server::task_cancel,
        crate::server::dispatch_one,
        crate::server::admin_backup,
    ),
    components(schemas(
        defer_shared_kernel::Target,
        defer_shared_kernel::CreateTask,
        defer_shared_kernel::QueuePolicy,
        defer_shared_kernel::QueueControlState,
        defer_shared_kernel::QueueSnapshot,
        defer_shared_kernel::TaskStatus,
        defer_dispatch::DispatchDisposition,
        defer_dispatch::DispatchReport,
        crate::server::QueueControlRequest,
        crate::server::CreateTasksRequest,
        crate::server::CreateTasksResponse,
        crate::server::QueueListResponse,
        crate::server::TaskStatusResponse,
        service_http::ErrorEnvelope,
    ))
)]
pub struct ApiDoc;

pub fn openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

#[cfg(test)]
mod tests {
    /// `clients/openapi.json` is a committed snapshot of this document, the
    /// input for client generation. It must match the live document byte for
    /// byte. Regenerate it with
    /// `DEFER_WRITE_OPENAPI=1 cargo test -p defer-queue --lib openapi::`.
    /// `info.version` is fixed to `snapshot`, so a release bump does not
    /// change the file.
    #[test]
    fn committed_snapshot_matches_live_document() {
        let mut doc = super::openapi();
        doc.info.version = "snapshot".to_string();
        let live = format!("{}\n", doc.to_pretty_json().unwrap());
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../clients/openapi.json");
        if std::env::var_os("DEFER_WRITE_OPENAPI").is_some() {
            std::fs::write(path, &live).unwrap();
        }
        let committed = std::fs::read_to_string(path).unwrap_or_default();
        assert!(
            committed == live,
            "clients/openapi.json is stale: run DEFER_WRITE_OPENAPI=1 cargo test -p defer-queue --lib openapi::"
        );
    }
}
// HANDWRITE-END
