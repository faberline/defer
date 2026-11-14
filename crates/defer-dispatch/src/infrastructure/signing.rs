// HANDWRITE-BEGIN gap="missing-generator:logic:defer-http-dispatch" tracker="#766" reason="Committed-lease HTTP target executor with stable idempotency and optional HMAC signing."
//! HMAC signing of target requests.

use anyhow::Result;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use defer_shared_kernel::DispatchLease;

#[derive(Clone)]
pub struct TargetSigningKey {
    pub key_id: String,
    secret: Vec<u8>,
}

impl TargetSigningKey {
    pub fn new(key_id: impl Into<String>, secret: impl Into<Vec<u8>>) -> Result<Self> {
        let key_id = key_id.into();
        let secret = secret.into();
        anyhow::ensure!(!key_id.trim().is_empty(), "target signing key id is empty");
        anyhow::ensure!(
            secret.len() >= 32,
            "target signing secret must be at least 32 bytes"
        );
        Ok(Self { key_id, secret })
    }

    /// `v1` HMAC over the immutable delivery identity and exact JSON bytes.
    pub fn signature(&self, lease: &DispatchLease, timestamp_ms: i64, body: &[u8]) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.secret).expect("HMAC accepts arbitrary-size keys");
        let timestamp = timestamp_ms.to_string();
        for field in [
            lease.idempotency_key.as_bytes(),
            lease.attempt_id.as_bytes(),
            lease.target.url.as_bytes(),
            timestamp.as_bytes(),
            body,
        ] {
            mac.update(&(field.len() as u64).to_be_bytes());
            mac.update(field);
        }
        format!(
            "v1={}",
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease() -> DispatchLease {
        serde_json::from_value(serde_json::json!({
            "attempt_id": "attempt-1",
            "task_id": "task-1",
            "queue": "q",
            "target": {"url": "https://example.test/hook", "method": "POST", "headers": {}},
            "payload": {"a": 1},
            "priority": 5,
            "attempt": 1,
            "idempotency_key": "idem-1",
            "executor_node": 1,
            "epoch": 1,
            "leased_at": "2023-11-14T22:13:20Z",
            "expires_at": "2023-11-14T22:14:20Z",
            "dispatch_deadline_ms": null
        }))
        .expect("lease fixture")
    }

    #[test]
    fn signature_matches_known_vector() {
        let key = TargetSigningKey::new("k1", vec![b'k'; 32]).expect("key");
        assert_eq!(
            key.signature(&lease(), 1_700_000_000_000, br#"{"a":1}"#),
            "v1=L9Zp862O7lIGP95lxNBTtGPGFMvyVZgdHPhN9dCa7vk="
        );
    }

    #[test]
    fn signature_changes_when_the_body_changes() {
        let key = TargetSigningKey::new("k1", vec![b'k'; 32]).expect("key");
        assert_ne!(
            key.signature(&lease(), 1_700_000_000_000, br#"{"a":2}"#),
            "v1=L9Zp862O7lIGP95lxNBTtGPGFMvyVZgdHPhN9dCa7vk="
        );
    }

    #[test]
    fn new_rejects_empty_key_id() {
        assert!(TargetSigningKey::new("  ", vec![b'k'; 32]).is_err());
    }

    #[test]
    fn new_rejects_short_secret() {
        assert!(TargetSigningKey::new("k1", vec![b'k'; 31]).is_err());
    }
}
// HANDWRITE-END
