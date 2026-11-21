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

/// The live state of all shards of one Defer, as one reconcile sees it (#270).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardsInput {
    /// Pods of the StatefulSet (`spec.replicas`).
    pub live_replicas: u32,
    /// Ready pods of the StatefulSet.
    pub ready_replicas: u32,
    /// Settled count of each live shard. `None` means every live shard is settled.
    pub settled_shards: Option<Vec<u32>>,
    pub desired_shards: u32,
    pub replicas_per_shard: u32,
}

/// What one reconcile sets for all shards (#270).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardsDecision {
    pub statefulset_replicas: u32,
    pub settled_shards: Vec<u32>,
    pub message: Option<String>,
}

/// Plans all shards of one Defer. Never returns an error (#270).
///
/// `actuator_for_shard` gets the first ordinal of a shard (`shard * replicas_per_shard`).
/// Growing adds shards with one voter each; lowering is held; each reconcile
/// moves at most one shard's settled count.
pub async fn decide_shards_for_reconcile<A: MembershipActuator>(
    actuator_for_shard: impl Fn(u32) -> A,
    input: ShardsInput,
    retry: RetryPolicy,
) -> ShardsDecision {
    let r = input.replicas_per_shard;
    let d = input.desired_shards;
    let live = input.live_replicas;
    let n = live.checked_div(r).unwrap_or(0);
    let mut settled: Vec<u32> = input
        .settled_shards
        .clone()
        .unwrap_or_else(|| vec![r; n as usize]);
    settled.truncate(n as usize);
    while settled.len() < n as usize {
        settled.push(r);
    }
    for value in settled.iter_mut() {
        *value = (*value).min(r);
    }
    let keep = |message: Option<String>, settled: Vec<u32>| ShardsDecision {
        statefulset_replicas: live,
        settled_shards: settled,
        message,
    };

    if r == 0 || d == 0 {
        return keep(
            Some("shardCount and replicasPerShard must be at least 1".into()),
            settled,
        );
    }
    if !live.is_multiple_of(r) {
        return keep(
            Some("live pods are not a whole number of shards; replicasPerShard cannot change while shardCount is above 1".into()),
            settled,
        );
    }
    if d < n {
        return keep(
            Some("lowering shardCount is not supported yet".into()),
            settled,
        );
    }
    if d > n {
        let mut grown = settled;
        grown.extend(std::iter::repeat_n(1, (d - n) as usize));
        return ShardsDecision {
            statefulset_replicas: d * r,
            settled_shards: grown,
            message: None,
        };
    }
    let mut message = None;
    for shard in 0..n {
        let index = shard as usize;
        let ready = input.ready_replicas.saturating_sub(shard * r).min(r);
        let decision = decide_replicas_for_reconcile(
            &actuator_for_shard(shard * r),
            ReplicaInput {
                live_replicas: r,
                ready_replicas: ready,
                settled: Some(settled[index]),
                desired: r,
            },
            retry,
        )
        .await;
        let changed = decision.settled != settled[index];
        settled[index] = decision.settled;
        if message.is_none() {
            message = decision.message;
        }
        if changed {
            break;
        }
    }
    ShardsDecision {
        statefulset_replicas: live,
        settled_shards: settled,
        message,
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

#[cfg(test)]
mod shard_tests {
    use super::{decide_shards_for_reconcile, ShardsInput};
    use crate::membership_driver::{ActuatorError, MembershipActuator, RetryPolicy};
    use std::sync::Mutex;
    use std::time::Duration;

    struct FakeActuator {
        calls: Mutex<Vec<String>>,
    }

    impl FakeActuator {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
            }
        }

        fn record(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
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
            Ok(())
        }

        async fn demote(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.record(format!("demote({ordinal})"));
            Ok(())
        }

        async fn remove(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.record(format!("remove({ordinal})"));
            Ok(())
        }
    }

    /// The actuator of one shard: it adds the shard's first ordinal to each call.
    struct Shard<'a> {
        inner: &'a FakeActuator,
        base: u32,
    }

    impl MembershipActuator for Shard<'_> {
        async fn add_learner(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.inner.add_learner(self.base + ordinal).await
        }

        async fn wait_catch_up(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.inner.wait_catch_up(self.base + ordinal).await
        }

        async fn promote(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.inner.promote(self.base + ordinal).await
        }

        async fn demote(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.inner.demote(self.base + ordinal).await
        }

        async fn remove(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.inner.remove(self.base + ordinal).await
        }
    }

    fn retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 1,
            delay: Duration::ZERO,
        }
    }

    fn input(
        live: u32,
        ready: u32,
        settled: Option<Vec<u32>>,
        desired_shards: u32,
        replicas_per_shard: u32,
    ) -> ShardsInput {
        ShardsInput {
            live_replicas: live,
            ready_replicas: ready,
            settled_shards: settled,
            desired_shards,
            replicas_per_shard,
        }
    }

    #[tokio::test]
    async fn shards_default_is_one_shard_per_replica_group_and_idle() {
        let actuator = FakeActuator::new();
        let decision = decide_shards_for_reconcile(
            |base| Shard {
                inner: &actuator,
                base,
            },
            input(3, 3, None, 1, 3),
            retry(),
        )
        .await;
        assert_eq!(decision.statefulset_replicas, 3);
        assert_eq!(decision.settled_shards, vec![3]);
        assert_eq!(decision.message, None);
        assert!(actuator.calls().is_empty());
    }

    #[tokio::test]
    async fn shards_grow_from_one_to_two_starts_new_shard_at_one_voter() {
        let actuator = FakeActuator::new();
        let decision = decide_shards_for_reconcile(
            |base| Shard {
                inner: &actuator,
                base,
            },
            input(3, 3, Some(vec![3]), 2, 3),
            retry(),
        )
        .await;
        assert_eq!(decision.statefulset_replicas, 6);
        assert_eq!(decision.settled_shards, vec![3, 1]);
        assert_eq!(decision.message, None);
        assert!(actuator.calls().is_empty());
    }

    #[tokio::test]
    async fn new_shard_is_not_settled_until_ready() {
        // Shard 1 has only one ready pod: it stays at one voter and makes no call.
        let actuator = FakeActuator::new();
        let decision = decide_shards_for_reconcile(
            |base| Shard {
                inner: &actuator,
                base,
            },
            input(6, 4, Some(vec![3, 1]), 2, 3),
            retry(),
        )
        .await;
        assert_eq!(decision.settled_shards, vec![3, 1]);
        assert!(actuator.calls().is_empty());

        // All six pods ready: shard 1 takes one step and its ordinals start at 3.
        let actuator = FakeActuator::new();
        let decision = decide_shards_for_reconcile(
            |base| Shard {
                inner: &actuator,
                base,
            },
            input(6, 6, Some(vec![3, 1]), 2, 3),
            retry(),
        )
        .await;
        assert_eq!(decision.settled_shards, vec![3, 2]);
        assert_eq!(
            actuator.calls(),
            vec!["add_learner(4)", "wait_catch_up(4)", "promote(4)"]
        );
    }

    #[tokio::test]
    async fn shards_lowering_is_held() {
        let actuator = FakeActuator::new();
        let decision = decide_shards_for_reconcile(
            |base| Shard {
                inner: &actuator,
                base,
            },
            input(6, 6, Some(vec![3, 3]), 1, 3),
            retry(),
        )
        .await;
        assert_eq!(decision.statefulset_replicas, 6);
        assert_eq!(decision.settled_shards, vec![3, 3]);
        let message = decision.message.expect("lowering is held with a message");
        assert!(message.contains("lowering shardCount"), "got: {message}");
        assert!(actuator.calls().is_empty());
    }

    #[tokio::test]
    async fn shards_whole_number_check() {
        // 4 pods with 3 per shard is not a whole number of shards: the size is kept.
        let actuator = FakeActuator::new();
        let decision = decide_shards_for_reconcile(
            |base| Shard {
                inner: &actuator,
                base,
            },
            input(4, 4, None, 2, 3),
            retry(),
        )
        .await;
        assert_eq!(decision.statefulset_replicas, 4);
        let message = decision
            .message
            .expect("the odd size is held with a message");
        assert!(message.contains("whole number of shards"), "got: {message}");
        assert!(actuator.calls().is_empty());
    }
}
// HANDWRITE-END
