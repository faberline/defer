// HANDWRITE-BEGIN gap="missing-generator:logic:defer-http-dispatch" tracker="#766" reason="Committed-lease HTTP target executor with stable idempotency and optional HMAC signing."
//! HTTP push execution after committed lease ownership.

mod infrastructure;

pub use infrastructure::{DispatchDisposition, DispatchReport, HttpDispatcher, TargetSigningKey};
// HANDWRITE-END
