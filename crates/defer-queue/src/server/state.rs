// HANDWRITE-BEGIN gap="missing-generator:logic:defer-http-api" tracker="#766" reason="Shared service-http/auth shell around Defer's Raft-backed domain commands."
//! [`AppState`]: the shared handler state, drain controller, and dispatch tick.

use std::sync::Arc;

use defer_access::AuthConfig;
use defer_dispatch::{DispatchDisposition, HttpDispatcher};
use defer_replication::DeferRaft;
use server_lifecycle::{DrainController, DrainSignal, LifecycleController};
use service_auth::ReloadableRoleMapVerifier;
use service_http::MetricsProvider;

use crate::metrics::DeferMetrics;

#[derive(Clone)]
pub struct AppState {
    pub(super) raft: Arc<DeferRaft>,
    pub(super) dispatcher: Arc<HttpDispatcher>,
    pub(super) verifier: Arc<ReloadableRoleMapVerifier>,
    pub(super) metrics: Arc<DeferMetrics>,
    pub(super) drain: DrainController,
    pub(super) body_limit_bytes: usize,
}

impl AppState {
    pub fn new(
        raft: Arc<DeferRaft>,
        dispatcher: HttpDispatcher,
        auth: AuthConfig,
        body_limit_bytes: usize,
    ) -> Self {
        Self {
            raft,
            dispatcher: Arc::new(dispatcher),
            verifier: Arc::new(auth.verifier()),
            metrics: Arc::new(DeferMetrics::default()),
            drain: DrainController::new(),
            body_limit_bytes,
        }
    }

    /// Bind readiness and admission to the process lifecycle, so a
    /// supervised shutdown reports draining the moment it starts.
    pub fn with_lifecycle(mut self, lifecycle: LifecycleController) -> Self {
        self.drain = DrainController::from_lifecycle(lifecycle);
        self
    }

    pub fn start_drain(&self) {
        self.drain.start_drain();
    }

    pub fn is_draining(&self) -> bool {
        self.drain.is_draining()
    }

    /// A signal that resolves once this state starts draining.
    pub fn drain_signal(&self) -> DrainSignal {
        self.drain.signal()
    }

    pub fn raft(&self) -> Arc<DeferRaft> {
        self.raft.clone()
    }

    pub fn verifier(&self) -> Arc<ReloadableRoleMapVerifier> {
        self.verifier.clone()
    }

    /// The configured data-plane request body size limit (bytes).
    pub fn body_limit_bytes(&self) -> usize {
        self.body_limit_bytes
    }

    /// Run one bounded dispatcher pass across the committed queue inventory.
    /// Each queue drains at most `max_per_queue` tasks so one hot queue cannot
    /// starve later queue names in the same tick.
    pub async fn dispatch_tick(
        &self,
        max_per_queue: usize,
        max_concurrency: usize,
    ) -> anyhow::Result<usize> {
        let queues = self.raft.scheduler().lock().unwrap().queue_names();
        let mut dispatched = 0;
        for queue in queues {
            let reports = self
                .dispatcher
                .dispatch_batch(
                    &self.raft,
                    &queue,
                    chrono::Utc::now(),
                    max_per_queue,
                    max_concurrency,
                )
                .await?;
            for report in reports {
                match report.disposition {
                    DispatchDisposition::Acked => self.metrics.dispatch_acked.incr(),
                    DispatchDisposition::Retried { .. } => self.metrics.dispatch_retried.incr(),
                    DispatchDisposition::DeadLettered => self.metrics.dispatch_dead_lettered.incr(),
                    DispatchDisposition::LostOwnership => {
                        self.metrics.dispatch_lost_ownership.incr()
                    }
                }
                dispatched += 1;
            }
        }
        Ok(dispatched)
    }
}

impl service_http::ReadinessHook for AppState {
    fn is_draining(&self) -> bool {
        self.drain.is_draining()
    }
}

impl MetricsProvider for AppState {
    fn render_metrics(&self) -> String {
        self.metrics.render()
    }
}
// HANDWRITE-END
