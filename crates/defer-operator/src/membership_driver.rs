// HANDWRITE-BEGIN gap="missing-generator:logic:defer-membership-driver" tracker="#87" reason="Operator membership step driver for a replicasPerShard change."
//! Runs a membership plan through an actuator.
//!
//! `run_steps` keeps no state. After a crash the caller builds the current
//! `MemberSet` again, calls `plan_replica_change` and runs the new plan, so a
//! pending learner gives `[WaitCatchUp, Promote]` and the steps resume.

use crate::topology_plan::Step;
use std::time::Duration;

/// Why a membership step failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActuatorError {
    /// The learner has not caught up yet. The step may be retried.
    NotCaughtUp,
    /// Any other failure. The driver stops at once.
    Hard(String),
}

impl std::fmt::Display for ActuatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotCaughtUp => write!(f, "the learner has not caught up yet."),
            Self::Hard(reason) => write!(f, "membership step failed: {reason}"),
        }
    }
}

impl std::error::Error for ActuatorError {}

/// One membership change on the leader, one method per `Step` variant.
#[allow(async_fn_in_trait)]
pub trait MembershipActuator {
    async fn add_learner(&self, ordinal: u32) -> Result<(), ActuatorError>;
    async fn wait_catch_up(&self, ordinal: u32) -> Result<(), ActuatorError>;
    async fn promote(&self, ordinal: u32) -> Result<(), ActuatorError>;
    async fn demote(&self, ordinal: u32) -> Result<(), ActuatorError>;
    async fn remove(&self, ordinal: u32) -> Result<(), ActuatorError>;
}

/// How often a step that returns `NotCaughtUp` is tried.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 60,
            delay: Duration::from_secs(5),
        }
    }
}

/// Runs `steps` in order through `actuator`.
///
/// `WaitCatchUp` and `Promote` retry on `NotCaughtUp` up to
/// `retry.max_attempts` times in total. Any other error, or `NotCaughtUp`
/// from another step, returns at once and later steps do not run.
pub async fn run_steps<A: MembershipActuator>(
    actuator: &A,
    steps: &[Step],
    retry: RetryPolicy,
) -> Result<(), ActuatorError> {
    for step in steps {
        let mut attempt = 1;
        loop {
            let result = match step {
                Step::AddLearner(o) => actuator.add_learner(*o).await,
                Step::WaitCatchUp(o) => actuator.wait_catch_up(*o).await,
                Step::Promote(o) => actuator.promote(*o).await,
                Step::Demote(o) => actuator.demote(*o).await,
                Step::Remove(o) => actuator.remove(*o).await,
            };
            match result {
                Ok(()) => break,
                Err(ActuatorError::NotCaughtUp)
                    if matches!(step, Step::WaitCatchUp(_) | Step::Promote(_))
                        && attempt < retry.max_attempts =>
                {
                    attempt += 1;
                    tokio::time::sleep(retry.delay).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::membership_driver::{run_steps, ActuatorError, MembershipActuator, RetryPolicy};
    use crate::topology_plan::{plan_replica_change, MemberSet, Step};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Default)]
    struct Fake {
        calls: Mutex<Vec<String>>,
        wait_results: Mutex<VecDeque<Result<(), ActuatorError>>>,
        promote_results: Mutex<VecDeque<Result<(), ActuatorError>>>,
    }

    impl Fake {
        fn record(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        fn with_wait(results: Vec<Result<(), ActuatorError>>) -> Self {
            Self {
                wait_results: Mutex::new(results.into()),
                ..Self::default()
            }
        }

        fn with_promote(results: Vec<Result<(), ActuatorError>>) -> Self {
            Self {
                promote_results: Mutex::new(results.into()),
                ..Self::default()
            }
        }
    }

    fn next(queue: &Mutex<VecDeque<Result<(), ActuatorError>>>) -> Result<(), ActuatorError> {
        queue.lock().unwrap().pop_front().unwrap_or(Ok(()))
    }

    impl MembershipActuator for Fake {
        async fn add_learner(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.record(format!("add_learner({ordinal})"));
            Ok(())
        }

        async fn wait_catch_up(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.record(format!("wait_catch_up({ordinal})"));
            next(&self.wait_results)
        }

        async fn promote(&self, ordinal: u32) -> Result<(), ActuatorError> {
            self.record(format!("promote({ordinal})"));
            next(&self.promote_results)
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

    fn policy(max_attempts: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            delay: Duration::ZERO,
        }
    }

    fn not_caught_up() -> Result<(), ActuatorError> {
        Err(ActuatorError::NotCaughtUp)
    }

    #[tokio::test]
    async fn scale_out_runs_add_wait_promote_in_order() {
        let fake = Fake::default();
        let steps = [Step::AddLearner(2), Step::WaitCatchUp(2), Step::Promote(2)];

        let result = run_steps(&fake, &steps, policy(3)).await;

        assert_eq!(result, Ok(()));
        assert_eq!(
            fake.calls(),
            vec!["add_learner(2)", "wait_catch_up(2)", "promote(2)"]
        );
    }

    #[tokio::test]
    async fn scale_in_runs_demote_then_remove() {
        let fake = Fake::default();
        let steps = [Step::Demote(2), Step::Remove(2)];

        let result = run_steps(&fake, &steps, policy(3)).await;

        assert_eq!(result, Ok(()));
        assert_eq!(fake.calls(), vec!["demote(2)", "remove(2)"]);
    }

    #[tokio::test]
    async fn wait_catch_up_retries_on_not_caught_up() {
        let fake = Fake::with_wait(vec![not_caught_up(), not_caught_up(), Ok(())]);
        let steps = [Step::WaitCatchUp(2), Step::Promote(2)];

        let result = run_steps(&fake, &steps, policy(3)).await;

        assert_eq!(result, Ok(()));
        assert_eq!(
            fake.calls(),
            vec![
                "wait_catch_up(2)",
                "wait_catch_up(2)",
                "wait_catch_up(2)",
                "promote(2)"
            ]
        );
    }

    #[tokio::test]
    async fn promote_retries_on_not_caught_up() {
        let fake = Fake::with_promote(vec![not_caught_up(), Ok(())]);
        let steps = [Step::Promote(2)];

        let result = run_steps(&fake, &steps, policy(3)).await;

        assert_eq!(result, Ok(()));
        assert_eq!(fake.calls(), vec!["promote(2)", "promote(2)"]);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let fake = Fake::with_wait(vec![
            not_caught_up(),
            not_caught_up(),
            not_caught_up(),
            not_caught_up(),
            not_caught_up(),
        ]);
        let steps = [Step::WaitCatchUp(2), Step::Promote(2)];

        let result = run_steps(&fake, &steps, policy(3)).await;

        assert_eq!(result, Err(ActuatorError::NotCaughtUp));
        let waits = fake
            .calls()
            .iter()
            .filter(|c| c.as_str() == "wait_catch_up(2)")
            .count();
        assert_eq!(waits, 3);
        assert!(!fake.calls().contains(&"promote(2)".to_string()));
    }

    #[tokio::test]
    async fn hard_error_stops_at_once() {
        let fake = Fake::with_promote(vec![Err(ActuatorError::Hard("boom".to_string()))]);
        let steps = [Step::Promote(2), Step::Remove(2)];

        let result = run_steps(&fake, &steps, policy(3)).await;

        assert_eq!(result, Err(ActuatorError::Hard("boom".to_string())));
        assert_eq!(fake.calls(), vec!["promote(2)"]);
    }

    #[tokio::test]
    async fn restart_after_crash_resumes_the_pending_learner() {
        let fake = Fake::default();
        let current = MemberSet {
            voters: vec![0, 1],
            learners: vec![2],
        };
        let steps = plan_replica_change(&current, 3).unwrap();

        let result = run_steps(&fake, &steps, policy(3)).await;

        assert_eq!(result, Ok(()));
        assert_eq!(fake.calls(), vec!["wait_catch_up(2)", "promote(2)"]);
    }

    #[tokio::test]
    async fn empty_plan_calls_nothing() {
        let fake = Fake::default();

        let result = run_steps(&fake, &[], policy(3)).await;

        assert_eq!(result, Ok(()));
        assert!(fake.calls().is_empty());
    }
}

// HANDWRITE-END
