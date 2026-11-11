// HANDWRITE-BEGIN gap="missing-generator:logic:defer-topology-plan" tracker="#10" reason="Pure replica-change plan for the Defer operator."
//! Pure plan for a replica-count change of one shard.
//!
//! The plan does no I/O. It turns the current members and the desired replica
//! count into an ordered list of membership steps. A call changes at most one
//! replica; the operator calls it again after the next generation.

/// Members of one shard, as StatefulSet ordinals.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemberSet {
    pub voters: Vec<u32>,
    pub learners: Vec<u32>,
}

/// One ordered membership step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    AddLearner(u32),
    WaitCatchUp(u32),
    Promote(u32),
    Demote(u32),
    Remove(u32),
}

/// Why a replica-change plan is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanError {
    ZeroReplicas,
    NoVoters,
    NonContiguous,
    LearnerNotLast,
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroReplicas => write!(f, "the desired replica count is zero."),
            Self::NoVoters => write!(f, "the shard has no voter."),
            Self::NonContiguous => write!(f, "the member ordinals are not 0..n."),
            Self::LearnerNotLast => write!(f, "the only learner must have the highest ordinal."),
        }
    }
}

impl std::error::Error for PlanError {}

/// Plans the next membership steps for `desired_replicas`.
///
/// Scale out adds a learner at the next ordinal, waits for it to catch up and
/// promotes it. Scale in demotes the highest voter and then removes it. A
/// pending learner is resumed on scale out and aborted on scale in.
pub fn plan_replica_change(
    current: &MemberSet,
    desired_replicas: u32,
) -> Result<Vec<Step>, PlanError> {
    if desired_replicas == 0 {
        return Err(PlanError::ZeroReplicas);
    }
    if current.voters.is_empty() {
        return Err(PlanError::NoVoters);
    }

    let mut voters = current.voters.clone();
    voters.sort_unstable();
    let mut learners = current.learners.clone();
    learners.sort_unstable();

    let mut all: Vec<u32> = voters.iter().chain(learners.iter()).copied().collect();
    all.sort_unstable();
    let n = u32::try_from(all.len()).map_err(|_| PlanError::NonContiguous)?;
    if all.iter().zip(0u32..).any(|(&id, expected)| id != expected) {
        return Err(PlanError::NonContiguous);
    }

    match learners.as_slice() {
        [] => {}
        [learner] if *learner == n - 1 => {
            let x = *learner;
            return Ok(if desired_replicas >= n {
                vec![Step::WaitCatchUp(x), Step::Promote(x)]
            } else {
                vec![Step::Remove(x)]
            });
        }
        _ => return Err(PlanError::LearnerNotLast),
    }

    if desired_replicas == n {
        Ok(Vec::new())
    } else if desired_replicas > n {
        Ok(vec![
            Step::AddLearner(n),
            Step::WaitCatchUp(n),
            Step::Promote(n),
        ])
    } else {
        let top = n - 1;
        Ok(vec![Step::Demote(top), Step::Remove(top)])
    }
}

#[cfg(test)]
mod tests {
    use super::{plan_replica_change, MemberSet, PlanError, Step};

    fn m(v: &[u32], l: &[u32]) -> MemberSet {
        MemberSet {
            voters: v.to_vec(),
            learners: l.to_vec(),
        }
    }

    #[test]
    fn same_count_is_empty() {
        assert_eq!(plan_replica_change(&m(&[0, 1, 2], &[]), 3), Ok(vec![]));
    }

    #[test]
    fn scale_out_adds_learner_waits_then_promotes() {
        assert_eq!(
            plan_replica_change(&m(&[0], &[]), 2),
            Ok(vec![
                Step::AddLearner(1),
                Step::WaitCatchUp(1),
                Step::Promote(1)
            ])
        );
    }

    #[test]
    fn scale_out_changes_one_replica_per_call() {
        assert_eq!(
            plan_replica_change(&m(&[0], &[]), 3),
            Ok(vec![
                Step::AddLearner(1),
                Step::WaitCatchUp(1),
                Step::Promote(1)
            ])
        );
    }

    #[test]
    fn scale_in_demotes_then_removes_highest() {
        assert_eq!(
            plan_replica_change(&m(&[0, 1, 2], &[]), 2),
            Ok(vec![Step::Demote(2), Step::Remove(2)])
        );
    }

    #[test]
    fn scale_in_changes_one_replica_per_call() {
        assert_eq!(
            plan_replica_change(&m(&[0, 1, 2], &[]), 1),
            Ok(vec![Step::Demote(2), Step::Remove(2)])
        );
    }

    #[test]
    fn unordered_input_is_sorted() {
        assert_eq!(
            plan_replica_change(&m(&[2, 0, 1], &[]), 2),
            Ok(vec![Step::Demote(2), Step::Remove(2)])
        );
    }

    #[test]
    fn pending_learner_is_resumed() {
        assert_eq!(
            plan_replica_change(&m(&[0], &[1]), 3),
            Ok(vec![Step::WaitCatchUp(1), Step::Promote(1)])
        );
    }

    #[test]
    fn pending_learner_is_aborted_on_scale_in() {
        assert_eq!(
            plan_replica_change(&m(&[0, 1], &[2]), 2),
            Ok(vec![Step::Remove(2)])
        );
    }

    #[test]
    fn zero_replicas_is_refused() {
        assert_eq!(
            plan_replica_change(&m(&[0], &[]), 0),
            Err(PlanError::ZeroReplicas)
        );
    }

    #[test]
    fn no_voters_is_refused() {
        assert_eq!(
            plan_replica_change(&m(&[], &[0]), 1),
            Err(PlanError::NoVoters)
        );
    }

    #[test]
    fn gap_is_refused() {
        assert_eq!(
            plan_replica_change(&m(&[0, 2], &[]), 2),
            Err(PlanError::NonContiguous)
        );
    }

    #[test]
    fn duplicate_is_refused() {
        assert_eq!(
            plan_replica_change(&m(&[0, 0], &[]), 2),
            Err(PlanError::NonContiguous)
        );
    }

    #[test]
    fn learner_not_last_is_refused() {
        assert_eq!(
            plan_replica_change(&m(&[0, 2], &[1]), 3),
            Err(PlanError::LearnerNotLast)
        );
    }

    #[test]
    fn two_learners_are_refused() {
        assert_eq!(
            plan_replica_change(&m(&[0], &[1, 2]), 3),
            Err(PlanError::LearnerNotLast)
        );
    }

    #[test]
    fn never_removes_last_voter() {
        assert_eq!(plan_replica_change(&m(&[0], &[]), 1), Ok(vec![]));
        for n in 1..=5u32 {
            let voters: Vec<u32> = (0..n).collect();
            for desired in 1..=6u32 {
                let plan = plan_replica_change(&m(&voters, &[]), desired).unwrap();
                assert!(
                    !plan
                        .iter()
                        .any(|s| matches!(s, Step::Remove(0) | Step::Demote(0))),
                    "n={n} desired={desired} plan={plan:?}"
                );
            }
        }
    }
}

// HANDWRITE-END
