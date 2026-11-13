// HANDWRITE-BEGIN gap="missing-generator:logic:defer-replica-reconcile" tracker="#140" reason="Pure one-layer replica planner."

use crate::membership_driver::{MembershipActuator, RetryPolicy};
use crate::replica_controller::next_replica_layer;

/// The live state of one shard, as one reconcile sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaInput {
    pub live_replicas: u32,
    pub ready_replicas: u32,
    pub settled: Option<u32>,
    pub desired: u32,
}

/// What one reconcile sets: the StatefulSet size and the settled count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconcileDecision {
    pub statefulset_replicas: u32,
    pub settled: u32,
    pub message: Option<String>,
}

/// Moves the shard at most one replica layer per reconcile.
///
/// Never returns an error: a failed step keeps the old live size and settled count.
pub async fn decide_replicas_for_reconcile<A: MembershipActuator>(
    actuator: &A,
    input: ReplicaInput,
    retry: RetryPolicy,
) -> ReconcileDecision {
    let n = input.live_replicas;
    let s = input.settled.unwrap_or(n).min(n);
    let d = input.desired;
    let keep = |message: Option<String>| ReconcileDecision {
        statefulset_replicas: n,
        settled: s,
        message,
    };

    if d == 0 {
        return keep(Some("desired replicas must be at least 1".into()));
    }
    if s < n {
        if input.ready_replicas < n {
            return keep(None);
        }
        return match next_replica_layer(actuator, s, n, retry).await {
            Ok(outcome) => ReconcileDecision {
                statefulset_replicas: n,
                settled: outcome.statefulset_replicas,
                message: None,
            },
            Err(error) => keep(Some(error.to_string())),
        };
    }
    if d == n {
        return keep(None);
    }
    if d > n {
        return ReconcileDecision {
            statefulset_replicas: n + 1,
            settled: n,
            message: None,
        };
    }
    match next_replica_layer(actuator, n, d, retry).await {
        Ok(outcome) => ReconcileDecision {
            statefulset_replicas: outcome.statefulset_replicas,
            settled: outcome.statefulset_replicas,
            message: None,
        },
        Err(error) => keep(Some(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::{decide_replicas_for_reconcile, ReconcileDecision, ReplicaInput};
    use crate::membership_driver::{ActuatorError, MembershipActuator, RetryPolicy};
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use std::time::Duration;

    struct FakeActuator {
        calls: Mutex<Vec<String>>,
        voters: Mutex<BTreeSet<u32>>,
        fail_promote: bool,
        fail_demote: bool,
    }

    impl FakeActuator {
        fn new(current: u32, fail_promote: bool, fail_demote: bool) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                voters: Mutex::new((0..current).collect()),
                fail_promote,
                fail_demote,
            }
        }

        fn record(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        fn voters(&self) -> BTreeSet<u32> {
            self.voters.lock().unwrap().clone()
        }
    }

    impl MembershipActuator for FakeActuator {
        async fn add_learner(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.record(format!("add_learner({ordinal})"));
            Ok(())
        }

        async fn wait_catch_up(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.record(format!("wait_catch_up({ordinal})"));
            Ok(())
        }

        async fn promote(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.record(format!("promote({ordinal})"));
            if self.fail_promote {
                return Err(ActuatorError::Hard("x".into()));
            }
            self.voters.lock().unwrap().insert(ordinal);
            Ok(())
        }

        async fn demote(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.record(format!("demote({ordinal})"));
            if self.fail_demote {
                return Err(ActuatorError::Hard("x".into()));
            }
            self.voters.lock().unwrap().remove(&ordinal);
            Ok(())
        }

        async fn remove(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.record(format!("remove({ordinal})"));
            Ok(())
        }
    }

    fn retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 1,
            delay: Duration::ZERO,
        }
    }

    fn input(live: u32, ready: u32, settled: Option<u32>, desired: u32) -> ReplicaInput {
        ReplicaInput {
            live_replicas: live,
            ready_replicas: ready,
            settled,
            desired,
        }
    }

    #[tokio::test]
    async fn scale_out_grows_statefulset_first() {
        let actuator = FakeActuator::new(3, false, false);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(3, 3, Some(3), 4), retry()).await;
        assert_eq!(
            decision,
            ReconcileDecision {
                statefulset_replicas: 4,
                settled: 3,
                message: None,
            }
        );
        assert!(actuator.calls().is_empty());
    }

    #[tokio::test]
    async fn scale_out_runs_the_layer_when_pod_ready() {
        let actuator = FakeActuator::new(3, false, false);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(4, 4, Some(3), 4), retry()).await;
        assert_eq!(
            actuator.calls(),
            vec!["add_learner(3)", "wait_catch_up(3)", "promote(3)"]
        );
        assert_eq!(
            decision,
            ReconcileDecision {
                statefulset_replicas: 4,
                settled: 4,
                message: None,
            }
        );
    }

    #[tokio::test]
    async fn scale_out_waits_for_the_pod() {
        let actuator = FakeActuator::new(3, false, false);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(4, 3, Some(3), 4), retry()).await;
        assert!(actuator.calls().is_empty());
        assert_eq!(
            decision,
            ReconcileDecision {
                statefulset_replicas: 4,
                settled: 3,
                message: None,
            }
        );
    }

    #[tokio::test]
    async fn scale_in_runs_demote_and_remove() {
        let actuator = FakeActuator::new(4, false, false);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(4, 4, Some(4), 3), retry()).await;
        assert_eq!(actuator.calls(), vec!["demote(3)", "remove(3)"]);
        assert_eq!(actuator.voters(), BTreeSet::from([0, 1, 2]));
        assert_eq!(
            decision,
            ReconcileDecision {
                statefulset_replicas: 3,
                settled: 3,
                message: None,
            }
        );
    }

    #[tokio::test]
    async fn failing_promote_keeps_old_values() {
        let actuator = FakeActuator::new(3, true, false);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(4, 4, Some(3), 4), retry()).await;
        assert_eq!(decision.statefulset_replicas, 4);
        assert_eq!(decision.settled, 3);
        assert!(decision.message.as_deref().is_some_and(|m| m.contains('x')));
    }

    #[tokio::test]
    async fn failing_demote_does_not_shrink() {
        // Negative test: a version that shrinks the StatefulSet before the
        // demote succeeds returns 3 here and fails.
        let actuator = FakeActuator::new(4, false, true);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(4, 4, Some(4), 3), retry()).await;
        assert_eq!(decision.statefulset_replicas, 4);
        assert_eq!(decision.settled, 4);
        assert!(decision.message.is_some());
    }

    #[tokio::test]
    async fn missing_settled_means_live() {
        let actuator = FakeActuator::new(4, false, false);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(4, 4, None, 4), retry()).await;
        assert_eq!(
            decision,
            ReconcileDecision {
                statefulset_replicas: 4,
                settled: 4,
                message: None,
            }
        );
        assert!(actuator.calls().is_empty());

        let actuator = FakeActuator::new(3, false, false);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(3, 3, None, 4), retry()).await;
        assert_eq!(
            decision,
            ReconcileDecision {
                statefulset_replicas: 4,
                settled: 3,
                message: None,
            }
        );
        assert!(actuator.calls().is_empty());
    }

    #[tokio::test]
    async fn equal_desired_makes_no_call() {
        let actuator = FakeActuator::new(3, false, false);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(3, 3, Some(3), 3), retry()).await;
        assert_eq!(
            decision,
            ReconcileDecision {
                statefulset_replicas: 3,
                settled: 3,
                message: None,
            }
        );
        assert!(actuator.calls().is_empty());
    }

    #[tokio::test]
    async fn zero_desired_returns_old_values_with_message() {
        let actuator = FakeActuator::new(3, false, false);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(3, 3, Some(3), 0), retry()).await;
        assert_eq!(decision.statefulset_replicas, 3);
        assert_eq!(decision.settled, 3);
        assert!(decision.message.is_some());
        assert!(actuator.calls().is_empty());
    }

    #[tokio::test]
    async fn settled_above_live_is_clamped() {
        let actuator = FakeActuator::new(3, false, false);
        let decision =
            decide_replicas_for_reconcile(&actuator, input(3, 3, Some(5), 3), retry()).await;
        assert_eq!(
            decision,
            ReconcileDecision {
                statefulset_replicas: 3,
                settled: 3,
                message: None,
            }
        );
        assert!(actuator.calls().is_empty());
    }
}

// HANDWRITE-END
