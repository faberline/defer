// HANDWRITE-BEGIN gap="missing-generator:logic:defer-http-dispatch" tracker="#766" reason="Committed-lease HTTP target executor with stable idempotency and optional HMAC signing."
//! [`HttpDispatcher`]: one signed target request per committed lease, then
//! the committed ack or nack.

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use defer_replication::DeferRaft;
use defer_shared_kernel::{AttemptSettlement, DispatchLease, NackOutcome, SettlementOutcome};

use super::report::{DispatchDisposition, DispatchReport};
use super::signing::TargetSigningKey;

const IDEMPOTENCY_HEADER: &str = "idempotency-key";
const ATTEMPT_HEADER: &str = "x-defer-attempt-id";
const ATTEMPT_NUMBER_HEADER: &str = "x-defer-attempt";
const EPOCH_HEADER: &str = "x-defer-fence-epoch";
const TIMESTAMP_HEADER: &str = "x-defer-timestamp-ms";
const KEY_ID_HEADER: &str = "x-defer-key-id";
const SIGNATURE_HEADER: &str = "x-defer-signature";

pub struct HttpDispatcher {
    client: reqwest::Client,
    timeout: Duration,
    signing_key: Option<TargetSigningKey>,
}

struct ExecutedAttempt {
    lease: DispatchLease,
    target_status: Option<u16>,
    transport_error: Option<String>,
    success: bool,
    completed_at: DateTime<Utc>,
}

impl HttpDispatcher {
    pub fn new(timeout: Duration, signing_key: Option<TargetSigningKey>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .http2_adaptive_window(true)
            .timeout(timeout)
            .build()
            .context("build Defer target HTTP client")?;
        Ok(Self {
            client,
            timeout,
            signing_key,
        })
    }

    /// Lease and execute at most one due task. `None` means the queue had no
    /// eligible work. HTTP success is any 2xx status; transport errors and
    /// non-2xx responses commit nack/retry/DLQ. A false ack/nack means this
    /// executor lost the committed fence while the external effect was in
    /// flight and is reported as `LostOwnership`.
    pub async fn dispatch_one(
        &self,
        raft: &DeferRaft,
        queue: &str,
        lease_at: DateTime<Utc>,
    ) -> Result<Option<DispatchReport>> {
        Ok(self
            .dispatch_batch(raft, queue, lease_at, 1, 1)
            .await?
            .pop())
    }

    /// Commit one lease batch, execute the HTTP effects with a shared bounded
    /// runner, then commit one fenced settlement batch. This keeps ownership
    /// and terminal state authoritative without paying one Raft/fsync round
    /// trip per task transition.
    pub async fn dispatch_batch(
        &self,
        raft: &DeferRaft,
        queue: &str,
        lease_at: DateTime<Utc>,
        requested: usize,
        max_concurrency: usize,
    ) -> Result<Vec<DispatchReport>> {
        let leases = raft
            .lease_due(queue.to_string(), lease_at, requested)
            .await?;
        if leases.is_empty() {
            return Ok(Vec::new());
        }
        let executions = service_executor::run_bounded(leases, max_concurrency, |lease| {
            self.execute_lease(lease, lease_at)
        })
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        let attempts = executions
            .iter()
            .map(|execution| AttemptSettlement {
                attempt_id: execution.lease.attempt_id.clone(),
                epoch: execution.lease.epoch,
                completed_at: execution.completed_at,
                success: execution.success,
            })
            .collect();
        let outcomes = raft.settle_batch(queue.to_string(), attempts).await?;
        anyhow::ensure!(
            outcomes.len() == executions.len(),
            "settlement outcome count mismatch"
        );
        Ok(executions
            .into_iter()
            .zip(outcomes)
            .map(|(execution, outcome)| {
                let disposition = match outcome {
                    SettlementOutcome::Acked(true) => DispatchDisposition::Acked,
                    SettlementOutcome::Acked(false) => DispatchDisposition::LostOwnership,
                    SettlementOutcome::Nacked(Some(NackOutcome::Retried { next_at })) => {
                        DispatchDisposition::Retried { next_at }
                    }
                    SettlementOutcome::Nacked(Some(NackOutcome::DeadLettered)) => {
                        DispatchDisposition::DeadLettered
                    }
                    SettlementOutcome::Nacked(None) => DispatchDisposition::LostOwnership,
                };
                DispatchReport {
                    task_id: execution.lease.task_id,
                    attempt_id: execution.lease.attempt_id,
                    target_status: execution.target_status,
                    transport_error: execution.transport_error,
                    disposition,
                }
            })
            .collect())
    }

    async fn execute_lease(
        &self,
        lease: DispatchLease,
        lease_at: DateTime<Utc>,
    ) -> Result<ExecutedAttempt> {
        let body = serde_json::to_vec(&lease.payload).context("encode Defer target payload")?;
        let method = reqwest::Method::from_bytes(lease.target.method.as_bytes());

        let response = match method {
            Ok(method) => {
                let request_timeout = match lease.dispatch_deadline_ms {
                    Some(ms) => std::cmp::min(Duration::from_millis(ms), self.timeout),
                    None => self.timeout,
                };
                let mut request = self
                    .client
                    .request(method, &lease.target.url)
                    .timeout(request_timeout)
                    .header(IDEMPOTENCY_HEADER, &lease.idempotency_key)
                    .header(ATTEMPT_HEADER, &lease.attempt_id)
                    .header(ATTEMPT_NUMBER_HEADER, lease.attempt.to_string())
                    .header(EPOCH_HEADER, lease.epoch.to_string())
                    .header("content-type", "application/json");
                for (name, value) in &lease.target.headers {
                    request = request.header(name, value);
                }
                if let Some(key) = &self.signing_key {
                    let timestamp_ms = lease_at.timestamp_millis();
                    request = request
                        .header(TIMESTAMP_HEADER, timestamp_ms.to_string())
                        .header(KEY_ID_HEADER, &key.key_id)
                        .header(SIGNATURE_HEADER, key.signature(&lease, timestamp_ms, &body));
                }
                request
                    .body(body)
                    .send()
                    .await
                    .map_err(|error| error.to_string())
            }
            Err(error) => Err(error.to_string()),
        };

        let (target_status, transport_error, success) = match response {
            Ok(response) => {
                let status = response.status();
                (Some(status.as_u16()), None, status.is_success())
            }
            Err(error) => (None, Some(error), false),
        };

        // Tests and catch-up workers may propose a due time just ahead of the
        // local wall clock; never commit a completion timestamp before the
        // committed lease timestamp.
        let completed_at = std::cmp::max(Utc::now(), lease_at);
        Ok(ExecutedAttempt {
            lease,
            target_status,
            transport_error,
            success,
            completed_at,
        })
    }
}
// HANDWRITE-END
