// HANDWRITE-BEGIN gap="missing-generator:logic:defer-http-dispatch" tracker="#766" reason="Committed-lease HTTP target executor with stable idempotency and optional HMAC signing."
//! Wire types of one dispatch report.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum DispatchDisposition {
    Acked,
    Retried { next_at: DateTime<Utc> },
    DeadLettered,
    LostOwnership,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DispatchReport {
    pub task_id: String,
    pub attempt_id: String,
    pub target_status: Option<u16>,
    pub transport_error: Option<String>,
    pub disposition: DispatchDisposition,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{DispatchDisposition, DispatchReport};

    #[test]
    fn disposition_serializes_with_the_current_wire_shape() {
        assert_eq!(
            serde_json::to_value(DispatchDisposition::Acked).unwrap(),
            json!("Acked")
        );
        assert_eq!(
            serde_json::to_value(DispatchDisposition::DeadLettered).unwrap(),
            json!("DeadLettered")
        );
    }

    #[test]
    fn report_round_trips() {
        let report = DispatchReport {
            task_id: "task-1".to_string(),
            attempt_id: "attempt-1".to_string(),
            target_status: Some(204),
            transport_error: None,
            disposition: DispatchDisposition::LostOwnership,
        };
        let value = serde_json::to_value(&report).unwrap();
        let back: DispatchReport = serde_json::from_value(value).unwrap();
        assert_eq!(back, report);
    }
}
// HANDWRITE-END
