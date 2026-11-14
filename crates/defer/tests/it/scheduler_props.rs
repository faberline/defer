//! Property tests for the scheduler state machine: leasing and fencing.
//! Tests only; the scheduler code is unchanged. Fixed time keeps them stable.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, TimeZone, Utc};
use defer::{CreateTask, DeferScheduler, QueuePolicy, Target, TaskStatus};
use proptest::prelude::*;

fn base() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap()
}

fn policy() -> QueuePolicy {
    QueuePolicy {
        max_in_flight: 1000,
        max_dispatch_per_tick: 1000,
        max_dispatches_per_second: 1000,
        max_burst_size: 1000,
        lease_ttl_ms: 1_000,
        retry_backoff_ms: 10,
        ..QueuePolicy::default()
    }
}

fn task(id: &str, max_attempts: u32) -> CreateTask {
    CreateTask {
        task_id: id.into(),
        target: Target {
            url: "http://example.test/hook".into(),
            method: "POST".into(),
            headers: Default::default(),
        },
        payload: serde_json::Value::Null,
        schedule_at: base() - Duration::seconds(1),
        priority: 10,
        max_attempts,
        dispatch_deadline_ms: None,
    }
}

fn scheduler_with(task_count: usize, max_attempts: u32, policy: QueuePolicy) -> DeferScheduler {
    let mut s = DeferScheduler::new();
    s.configure_queue("q", policy);
    let tasks = (0..task_count)
        .map(|i| task(&format!("t{i}"), max_attempts))
        .collect();
    s.create_tasks("q", tasks).unwrap();
    s
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn epoch_rises_and_a_leased_task_is_never_leased_again(
        task_count in 1usize..6,
        rounds in 1usize..8,
        nack_mask in proptest::collection::vec(any::<bool>(), 8),
    ) {
        // max_attempts is high so no task dead-letters during the run.
        let mut s = scheduler_with(task_count, 1000, policy());
        let mut last_epoch: HashMap<String, u64> = HashMap::new();
        let mut attempts: HashSet<String> = HashSet::new();
        for (round, &nack) in nack_mask.iter().enumerate().take(rounds) {
            let now = base() + Duration::milliseconds(2_000 * round as i64);
            let leased_before: HashSet<String> = (0..task_count)
                .map(|i| format!("t{i}"))
                .filter(|id| matches!(s.status("q", id), Ok(Some(TaskStatus::Leased { .. }))))
                .collect();
            let leases = s.lease_due_on_node("q", 1, now, 100).unwrap();
            let mut round_tasks: HashSet<String> = HashSet::new();
            for lease in &leases {
                prop_assert!(
                    !leased_before.contains(&lease.task_id),
                    "task {} was leased again while still leased",
                    lease.task_id
                );
                prop_assert!(
                    round_tasks.insert(lease.task_id.clone()),
                    "task {} was leased twice in one round",
                    lease.task_id
                );
                if let Some(prev) = last_epoch.insert(lease.task_id.clone(), lease.epoch) {
                    prop_assert!(
                        lease.epoch > prev,
                        "epoch of task {} did not rise: {} then {}",
                        lease.task_id,
                        prev,
                        lease.epoch
                    );
                }
                prop_assert!(
                    attempts.insert(lease.attempt_id.clone()),
                    "attempt id {} was reused",
                    lease.attempt_id
                );
            }
            if nack {
                for lease in &leases {
                    let outcome = s
                        .nack_on_node("q", &lease.attempt_id, 1, lease.epoch, now)
                        .unwrap();
                    prop_assert!(outcome.is_some(), "nack of a live lease returned None");
                }
            } else {
                s.reclaim_expired("q", now + Duration::milliseconds(1_500)).unwrap();
            }
        }
    }

    #[test]
    fn a_stale_or_foreign_fence_never_changes_state(
        node_delta in 1u64..5,
        epoch_delta in 1u64..5,
        add in any::<bool>(),
    ) {
        let mut s = scheduler_with(1, 1000, policy());
        let lease = s.lease_due_on_node("q", 1, base(), 1).unwrap().remove(0);
        let live = TaskStatus::Leased {
            attempt_id: lease.attempt_id.clone(),
            executor_node: 1,
            epoch: lease.epoch,
            expires_at: lease.expires_at,
        };
        // (node, epoch) pairs that do not match the live lease.
        let stale = (1u64, lease.epoch + epoch_delta);
        let foreign = (1 + node_delta, lease.epoch);
        let pairs = if add { [stale, foreign] } else { [foreign, stale] };
        for (node, epoch) in pairs {
            let acked = s
                .ack_on_node("q", &lease.attempt_id, node, epoch, base())
                .unwrap();
            prop_assert!(!acked, "ack with a wrong fence was accepted");
            prop_assert_eq!(s.status("q", "t0").unwrap(), Some(live.clone()));

            let nacked = s
                .nack_on_node("q", &lease.attempt_id, node, epoch, base())
                .unwrap();
            prop_assert!(nacked.is_none(), "nack with a wrong fence was accepted");
            prop_assert_eq!(s.status("q", "t0").unwrap(), Some(live.clone()));
        }
    }
}

/// Control case, not a property: after reassignment the old epoch is refused
/// and the new epoch acks. Fails if the fence check in `ack_on_node` is removed.
#[test]
fn the_live_fence_acks_and_the_old_epoch_is_refused_after_reassignment() {
    let mut s = scheduler_with(1, 1000, policy());
    let a = s.lease_due_on_node("q", 1, base(), 1).unwrap().remove(0);

    let reclaimed = s
        .reclaim_expired("q", base() + Duration::seconds(2))
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0], "t0");

    let b = s
        .lease_due_on_node("q", 2, base() + Duration::seconds(3), 1)
        .unwrap()
        .remove(0);
    assert!(b.epoch > a.epoch);

    let t3 = base() + Duration::seconds(3);
    assert!(!s.ack_on_node("q", &a.attempt_id, 1, a.epoch, t3).unwrap());
    assert!(matches!(
        s.status("q", "t0").unwrap(),
        Some(TaskStatus::Leased { epoch, .. }) if epoch == b.epoch
    ));

    assert!(s.ack_on_node("q", &b.attempt_id, 2, b.epoch, t3).unwrap());
    assert_eq!(s.status("q", "t0").unwrap(), Some(TaskStatus::Succeeded));
}
