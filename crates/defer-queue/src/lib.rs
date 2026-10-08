// HANDWRITE-BEGIN gap="missing-generator:logic:defer-queue" tracker="#766" reason="Defer queue HTTP API: router, handlers, OpenAPI document, and metric names."
//! The defer HTTP API. `server` and `openapi` reference each other (the
//! document lists the handlers; the router serves the document), so they live
//! in one crate. The module is named `server` because utoipa derives each
//! operation's default tag from it (`crate::server`), which is part of the
//! published OpenAPI contract.

pub mod metrics;
pub mod openapi;
pub mod server;

// `dispatch_one` names its body `crate::DispatchReport`; utoipa 4 prints that
// path into the published `$ref`, so the root re-export keeps the spec stable.
pub use defer_dispatch::DispatchReport;
pub use server::AppState;
// HANDWRITE-END
