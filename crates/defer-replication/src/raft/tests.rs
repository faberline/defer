//! State-machine snapshot/restore and proposal dedupe.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use defer_shared_kernel::{CreateTask, DeferScheduler, QueuePolicy, Target};
use raft_runtime::RaftStateMachine;

use super::command::{DeferEnvelope, ProposalId};
use super::{DeferCommand, DeferOutcome, DeferStateMachine};

fn task(id: &str, at: DateTime<Utc>) -> CreateTask {
    CreateTask {
        task_id: id.into(),
        target: Target {
            url: "http://example.test/dispatch".into(),
            method: "POST".into(),
            headers: Default::default(),
        },
        payload: serde_json::json!({"id": id}),
        schedule_at: at,
        priority: 10,
        max_attempts: 3,
    }
}

fn envelope(sequence: u64, command: DeferCommand) -> Vec<u8> {
    serde_json::to_vec(&DeferEnvelope {
        proposal_id: ProposalId {
            node: 1,
            session: 9,
            sequence,
        },
        command,
    })
    .unwrap()
}

#[test]
fn snapshot_round_trip_preserves_fenced_attempt_and_dedupe() {
    let now = Utc.timestamp_millis_opt(1_000).unwrap();
    let scheduler = Arc::new(Mutex::new(DeferScheduler::new()));
    let sm = DeferStateMachine::new(scheduler.clone(), false);
    sm.apply(
        1,
        &envelope(
            1,
            DeferCommand::ConfigureQueue {
                queue: "jobs".into(),
                policy: QueuePolicy::default(),
            },
        ),
    )
    .unwrap();
    sm.apply(
        2,
        &envelope(
            2,
            DeferCommand::CreateTask {
                queue: "jobs".into(),
                task: task("one", now),
            },
        ),
    )
    .unwrap();
    let lease_command = DeferCommand::LeaseDue {
        queue: "jobs".into(),
        executor_node: 7,
        now,
        requested: 1,
    };
    let lease_bytes = envelope(3, lease_command);
    sm.apply(3, &lease_bytes).unwrap();
    let lease = match sm.claim_outcome(3).unwrap() {
        DeferOutcome::Leased(Ok(mut leases)) => leases.remove(0),
        other => panic!("{other:?}"),
    };

    let restored_scheduler = Arc::new(Mutex::new(DeferScheduler::new()));
    let restored = DeferStateMachine::new(restored_scheduler.clone(), false);
    let mut snap_bytes = Vec::new();
    sm.snapshot(&mut snap_bytes).unwrap();
    restored
        .restore(&mut std::io::Cursor::new(&snap_bytes))
        .unwrap();
    restored.apply(4, &lease_bytes).unwrap();
    let repeated = match restored.claim_outcome(4).unwrap() {
        DeferOutcome::Leased(Ok(mut leases)) => leases.remove(0),
        other => panic!("{other:?}"),
    };
    assert_eq!(lease.attempt_id, repeated.attempt_id);
    assert_eq!(lease.epoch, repeated.epoch);
    assert_eq!(
        restored_scheduler
            .lock()
            .unwrap()
            .queue_snapshot("jobs")
            .unwrap()
            .in_flight_count,
        1
    );
}
