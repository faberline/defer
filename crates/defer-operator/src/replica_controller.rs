// HANDWRITE-BEGIN gap="missing-generator:logic:defer-replica-controller" tracker="#137" reason="Replica-layer controller over the membership actuator."

use crate::membership_driver::{run_steps, ActuatorError, MembershipActuator, RetryPolicy};
use crate::topology_plan::{plan_replica_change, MemberSet, PlanError, Step};

/// One layer of a replica change for one shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayerOutcome {
    pub statefulset_replicas: u32,
    pub steps_run: Vec<Step>,
}

/// Why one layer of a replica change failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicaError {
    Plan(PlanError),
    Actuator(ActuatorError),
}

impl std::fmt::Display for ReplicaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plan(error) => write!(f, "{error}"),
            Self::Actuator(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ReplicaError {}

/// Moves the shard one replica towards `desired`.
///
/// `current` is the live StatefulSet size. The caller keeps the old size
/// when this returns an error.
pub async fn next_replica_layer<A: MembershipActuator>(
    actuator: &A,
    current: u32,
    desired: u32,
    retry: RetryPolicy,
) -> Result<LayerOutcome, ReplicaError> {
    if desired == 0 {
        return Err(ReplicaError::Plan(PlanError::ZeroReplicas));
    }
    if desired == current {
        return Ok(LayerOutcome {
            statefulset_replicas: current,
            steps_run: Vec::new(),
        });
    }

    let members = MemberSet {
        voters: (0..current).collect(),
        learners: Vec::new(),
    };
    let target = if desired > current {
        current + 1
    } else {
        current - 1
    };
    let steps = plan_replica_change(&members, target).map_err(ReplicaError::Plan)?;
    run_steps(actuator, &steps, retry)
        .await
        .map_err(ReplicaError::Actuator)?;
    Ok(LayerOutcome {
        statefulset_replicas: target,
        steps_run: steps,
    })
}

/// HTTP (client) URL of one replica pod through the headless service.
pub fn pod_http_url(name: &str, namespace: &str, ordinal: u32) -> String {
    format!(
        "http://{name}-{ordinal}.{name}-headless.{namespace}.svc:{}",
        crate::render::CLIENT_PORT
    )
}

/// Raft peer URL of one replica pod through the headless service.
pub fn pod_peer_url(name: &str, namespace: &str, ordinal: u32) -> String {
    format!(
        "http://{name}-{ordinal}.{name}-headless.{namespace}.svc:{}",
        crate::render::RAFT_PORT
    )
}

/// Parses the `x-defer-leader` header, the leader node id, which equals the ordinal.
pub fn leader_ordinal_from_hint(header: Option<&str>) -> Option<u32> {
    header?.trim().parse::<u32>().ok()
}

#[cfg(test)]
mod tests {
    use super::{next_replica_layer, ReplicaError};
    use crate::membership_driver::{ActuatorError, MembershipActuator, RetryPolicy};
    use crate::topology_plan::{PlanError, Step};
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use std::time::Duration;

    struct FakeActuator {
        calls: Mutex<Vec<String>>,
        voters: Mutex<BTreeSet<u32>>,
        fail_promote: bool,
    }

    impl FakeActuator {
        fn new(current: u32, fail_promote: bool) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                voters: Mutex::new((0..current).collect()),
                fail_promote,
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

    #[tokio::test]
    async fn scales_out_3_to_4_one_layer() {
        let actuator = FakeActuator::new(3, false);
        let outcome = next_replica_layer(&actuator, 3, 4, retry()).await.unwrap();
        assert_eq!(
            actuator.calls(),
            ["add_learner(3)", "wait_catch_up(3)", "promote(3)"]
        );
        assert_eq!(actuator.voters(), BTreeSet::from([0, 1, 2, 3]));
        assert_eq!(
            outcome.steps_run,
            vec![Step::AddLearner(3), Step::WaitCatchUp(3), Step::Promote(3)]
        );
        assert_eq!(outcome.statefulset_replicas, 4);
    }

    #[tokio::test]
    async fn scales_in_4_to_3_one_layer() {
        let actuator = FakeActuator::new(4, false);
        let outcome = next_replica_layer(&actuator, 4, 3, retry()).await.unwrap();
        assert_eq!(actuator.calls(), ["demote(3)", "remove(3)"]);
        assert_eq!(actuator.voters(), BTreeSet::from([0, 1, 2]));
        assert_eq!(outcome.steps_run, vec![Step::Demote(3), Step::Remove(3)]);
        assert_eq!(outcome.statefulset_replicas, 3);
    }

    #[tokio::test]
    async fn moves_one_layer_per_call() {
        let actuator = FakeActuator::new(3, false);
        let outcome = next_replica_layer(&actuator, 3, 6, retry()).await.unwrap();
        assert_eq!(outcome.statefulset_replicas, 4);
        assert_eq!(actuator.calls().len(), 3);
    }

    #[tokio::test]
    async fn equal_count_does_not_call_actuator() {
        let actuator = FakeActuator::new(3, false);
        let outcome = next_replica_layer(&actuator, 3, 3, retry()).await.unwrap();
        assert!(actuator.calls().is_empty());
        assert!(outcome.steps_run.is_empty());
        assert_eq!(outcome.statefulset_replicas, 3);
    }

    #[tokio::test]
    async fn zero_desired_is_refused() {
        let actuator = FakeActuator::new(3, false);
        let result = next_replica_layer(&actuator, 3, 0, retry()).await;
        assert_eq!(result, Err(ReplicaError::Plan(PlanError::ZeroReplicas)));
        assert!(actuator.calls().is_empty());
    }

    #[tokio::test]
    async fn actuator_failure_keeps_old_size() {
        let actuator = FakeActuator::new(3, true);
        let result = next_replica_layer(&actuator, 3, 4, retry()).await;
        assert_eq!(
            result,
            Err(ReplicaError::Actuator(ActuatorError::Hard("x".into())))
        );
    }

    #[test]
    fn pod_urls_use_headless_dns() {
        assert_eq!(
            super::pod_http_url("q", "ns", 2),
            "http://q-2.q-headless.ns.svc:7141"
        );
        assert_eq!(
            super::pod_peer_url("q", "ns", 2),
            "http://q-2.q-headless.ns.svc:7142"
        );
    }

    #[test]
    fn leader_hint_parses() {
        assert_eq!(super::leader_ordinal_from_hint(Some("2")), Some(2));
        assert_eq!(super::leader_ordinal_from_hint(Some("x")), None);
        assert_eq!(super::leader_ordinal_from_hint(Some("-1")), None);
        assert_eq!(super::leader_ordinal_from_hint(None), None);
    }
}

// HANDWRITE-END
