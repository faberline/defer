// SPEC-MANAGED: tech-design/logic/core-scheduler-priority-rate-dispatch.md#logic
// HANDWRITE-BEGIN gap="missing-generator:logic:defer-http-dispatch" tracker="#766" reason="Committed-lease HTTP target executor with stable idempotency and optional HMAC signing."
//! HTTP push execution after committed lease ownership.

mod dispatcher;

pub use dispatcher::{DispatchDisposition, DispatchReport, HttpDispatcher, TargetSigningKey};
// HANDWRITE-END
