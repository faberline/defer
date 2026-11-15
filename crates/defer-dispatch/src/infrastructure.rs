// HANDWRITE-BEGIN gap="missing-generator:logic:defer-http-dispatch" tracker="#766" reason="Committed-lease HTTP target executor with stable idempotency and optional HMAC signing."
//! Adapters of the dispatch context: signing, report types and the HTTP
//! dispatcher.

mod http_dispatcher;
mod report;
mod signing;

pub use http_dispatcher::HttpDispatcher;
pub use report::{DispatchDisposition, DispatchReport};
pub use signing::TargetSigningKey;
// HANDWRITE-END
