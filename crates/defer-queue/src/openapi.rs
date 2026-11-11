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
        crate::server::queue_delete,
        crate::server::queue_control,
        crate::server::queue_purge,
        crate::server::task_create,
        crate::server::task_create_batch,
        crate::server::task_status,
        crate::server::task_cancel,
        crate::server::task_list,
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
        crate::server::PurgeQueueResponse,
        crate::server::TaskListResponse,
        service_http::ErrorEnvelope,
    ))
)]
pub struct ApiDoc;

pub fn openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

/// Every documented operation as `METHOD path`, sorted. The routes output of
/// `defer spec` and the tests read this list, so a route is added in `paths(...)`
/// only.
pub fn operations() -> Vec<String> {
    const METHODS: [&str; 8] = [
        "delete", "get", "head", "options", "patch", "post", "put", "trace",
    ];
    let value = serde_json::to_value(openapi()).expect("OpenAPI document serializes");
    let mut ops = Vec::new();
    if let Some(paths) = value["paths"].as_object() {
        for (path, item) in paths {
            let Some(item) = item.as_object() else {
                continue;
            };
            for method in METHODS {
                if item.contains_key(method) {
                    ops.push(format!("{} {path}", method.to_ascii_uppercase()));
                }
            }
        }
    }
    ops.sort();
    ops
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

    /// Every `#/components/schemas/<name>` reference in the live document
    /// must name a registered component. A dangling reference breaks client
    /// generation.
    #[test]
    fn every_schema_ref_resolves_to_a_component() {
        const PREFIX: &str = "\"#/components/schemas/";
        let doc = super::openapi();
        let json = doc.to_json().unwrap();
        let components = doc
            .components
            .as_ref()
            .map(|components| &components.schemas);
        let mut dangling = std::collections::BTreeSet::new();
        for (start, _) in json.match_indices(PREFIX) {
            let rest = &json[start + PREFIX.len()..];
            let name = &rest[..rest.find('"').unwrap()];
            if !components.is_some_and(|schemas| schemas.contains_key(name)) {
                dangling.insert(name.to_string());
            }
        }
        assert!(
            dangling.is_empty(),
            "OpenAPI refs without a component: {dangling:?}"
        );
    }

    /// The route list is the sorted `METHOD path` pairs of the document, and
    /// it names every documented operation.
    #[test]
    fn operations_are_sorted_and_cover_every_documented_path() {
        let ops = super::operations();
        let mut sorted = ops.clone();
        sorted.sort();
        assert_eq!(ops, sorted, "operations() must be sorted");
        let expected: Vec<String> = [
            "DELETE /v1/queues/{queue}",
            "DELETE /v1/queues/{queue}/tasks/{task_id}",
            "GET /admin/backup",
            "GET /v1/queues",
            "GET /v1/queues/{queue}",
            "GET /v1/queues/{queue}/tasks",
            "GET /v1/queues/{queue}/tasks/{task_id}",
            "POST /v1/queues/{queue}/control",
            "POST /v1/queues/{queue}/dispatch",
            "POST /v1/queues/{queue}/purge",
            "POST /v1/queues/{queue}/tasks",
            "POST /v1/queues/{queue}/tasks:batch",
            "PUT /v1/queues/{queue}",
        ]
        .iter()
        .map(|op| (*op).to_string())
        .collect();
        assert_eq!(ops, expected);
    }

    /// A `#[utoipa::path]` body must name the bare component
    /// (`ErrorEnvelope`). A module path leaves a `$ref` with no component.
    #[test]
    fn utoipa_paths_use_the_bare_error_envelope() {
        let source = include_str!("server.rs");
        for line in source.lines() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            for (start, _) in line.match_indices("body = ") {
                let rest = &line[start + "body = ".len()..];
                let end = rest.find([',', ')']).unwrap_or(rest.len());
                assert!(
                    !rest[..end].contains("::"),
                    "#[utoipa::path] body must use the bare component name (ErrorEnvelope), not a module path: {line}"
                );
            }
        }
    }
}
// HANDWRITE-END
