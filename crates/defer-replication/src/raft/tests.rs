//! State-machine snapshot/restore and proposal dedupe.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use defer_shared_kernel::{CreateTask, DeferScheduler, QueuePolicy, Target};
use raft_runtime::{Index, NodeId, RaftStateMachine};

use super::command::{DeferEnvelope, ProposalId, SchedulerSnapshot, FORMAT_VERSION};
use super::{DeferCommand, DeferOutcome, DeferStateMachine};

const SNAPSHOT_V1: &str = include_str!("fixtures/snapshot_v1.json");
const ENVELOPE_V1: &str = include_str!("fixtures/envelope_v1.json");

/// The deterministic state of the format fixtures: one queue, one task.
/// Returns the state machine and the bytes of the two envelopes applied.
fn fixture_state_machine() -> (Arc<DeferStateMachine>, Vec<u8>, Vec<u8>) {
    let now = Utc.timestamp_millis_opt(1_000).unwrap();
    let sm = DeferStateMachine::new(Arc::new(Mutex::new(DeferScheduler::new())), false);
    let configure = envelope(
        1,
        DeferCommand::ConfigureQueue {
            queue: "jobs".into(),
            policy: QueuePolicy::default(),
        },
    );
    sm.apply(Index::new(1), &configure).unwrap();
    let create = envelope(
        2,
        DeferCommand::CreateTask {
            queue: "jobs".into(),
            task: task("one", now),
        },
    );
    sm.apply(Index::new(2), &create).unwrap();
    (sm, configure, create)
}

/// Writes the committed fixtures. Run once when FORMAT_VERSION changes.
#[test]
#[ignore = "writes the committed fixtures; run once when FORMAT_VERSION changes"]
fn write_format_fixtures() {
    let (sm, _, create) = fixture_state_machine();
    let mut bytes = Vec::new();
    sm.snapshot(&mut bytes).unwrap();
    let snapshot: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let envelope_value: serde_json::Value = serde_json::from_slice(&create).unwrap();
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/raft/fixtures");
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(
        format!("{dir}/snapshot_v1.json"),
        format!("{}\n", serde_json::to_string_pretty(&snapshot).unwrap()),
    )
    .unwrap();
    std::fs::write(
        format!("{dir}/envelope_v1.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&envelope_value).unwrap()
        ),
    )
    .unwrap();
}

#[test]
fn snapshot_v1_fixture_restores() {
    let scheduler = Arc::new(Mutex::new(DeferScheduler::new()));
    let sm = DeferStateMachine::new(scheduler.clone(), false);
    sm.restore(&mut std::io::Cursor::new(SNAPSHOT_V1.as_bytes()))
        .unwrap();
    assert_eq!(sm.applied_index(), Index::new(2));
    assert_eq!(
        scheduler
            .lock()
            .unwrap()
            .queue_snapshot("jobs")
            .unwrap()
            .task_count,
        1
    );
}

#[test]
fn envelope_v1_fixture_applies() {
    let scheduler = Arc::new(Mutex::new(DeferScheduler::new()));
    let sm = DeferStateMachine::new(scheduler.clone(), false);
    sm.apply(
        Index::new(1),
        &envelope(
            1,
            DeferCommand::ConfigureQueue {
                queue: "jobs".into(),
                policy: QueuePolicy::default(),
            },
        ),
    )
    .unwrap();
    sm.apply(Index::new(2), ENVELOPE_V1.as_bytes()).unwrap();
    assert_eq!(sm.applied_index(), Index::new(2));
    assert_eq!(
        scheduler
            .lock()
            .unwrap()
            .queue_snapshot("jobs")
            .unwrap()
            .task_count,
        1
    );
}

#[test]
fn current_snapshot_matches_the_fixture_shape() {
    let (sm, _, _) = fixture_state_machine();
    let mut bytes = Vec::new();
    sm.snapshot(&mut bytes).unwrap();
    let current: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(SNAPSHOT_V1).unwrap();
    assert_eq!(current, fixture);
}

#[test]
fn snapshot_without_format_version_loads_as_version_0() {
    let mut value: serde_json::Value = serde_json::from_str(SNAPSHOT_V1).unwrap();
    value.as_object_mut().unwrap().remove("format_version");
    let bytes = serde_json::to_vec(&value).unwrap();
    let sm = DeferStateMachine::new(Arc::new(Mutex::new(DeferScheduler::new())), false);
    sm.restore(&mut std::io::Cursor::new(&bytes)).unwrap();
    assert_eq!(sm.applied_index(), Index::new(2));
}

#[test]
fn snapshot_from_a_newer_version_is_refused() {
    let mut value: serde_json::Value = serde_json::from_str(SNAPSHOT_V1).unwrap();
    value["format_version"] = serde_json::json!(FORMAT_VERSION + 1);
    let bytes = serde_json::to_vec(&value).unwrap();
    let sm = DeferStateMachine::new(Arc::new(Mutex::new(DeferScheduler::new())), false);
    let err = sm.restore(&mut std::io::Cursor::new(&bytes)).unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains(&format!("format_version {}", FORMAT_VERSION + 1)),
        "{text}"
    );
    assert!(text.contains("snapshot"), "{text}");
}

#[test]
fn log_entry_from_a_newer_version_is_refused() {
    let mut value: serde_json::Value = serde_json::from_str(ENVELOPE_V1).unwrap();
    value["format_version"] = serde_json::json!(FORMAT_VERSION + 1);
    let bytes = serde_json::to_vec(&value).unwrap();
    let sm = DeferStateMachine::new(Arc::new(Mutex::new(DeferScheduler::new())), false);
    let err = sm.apply(Index::new(1), &bytes).unwrap_err();
    assert!(err.to_string().contains("log entry"), "{err}");
}

#[test]
fn unknown_command_variant_is_refused() {
    let mut value: serde_json::Value = serde_json::from_str(ENVELOPE_V1).unwrap();
    value["command"] = serde_json::json!({"op": "not_a_command"});
    let bytes = serde_json::to_vec(&value).unwrap();
    let sm = DeferStateMachine::new(Arc::new(Mutex::new(DeferScheduler::new())), false);
    assert!(sm.apply(Index::new(1), &bytes).is_err());
}

#[test]
fn snapshot_decode_refuses_non_json() {
    let err = SchedulerSnapshot::decode(b"x").unwrap_err();
    assert!(err.to_string().starts_with("malformed snapshot"), "{err}");
}

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
        dispatch_deadline_ms: None,
    }
}

fn envelope(sequence: u64, command: DeferCommand) -> Vec<u8> {
    serde_json::to_vec(&DeferEnvelope {
        format_version: FORMAT_VERSION,
        proposal_id: ProposalId {
            node: NodeId::new(1),
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
        Index::new(1),
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
        Index::new(2),
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
        executor_node: NodeId::new(7),
        now,
        requested: 1,
    };
    let lease_bytes = envelope(3, lease_command);
    sm.apply(Index::new(3), &lease_bytes).unwrap();
    let lease = match sm.claim_outcome(Index::new(3)).unwrap() {
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
    restored.apply(Index::new(4), &lease_bytes).unwrap();
    let repeated = match restored.claim_outcome(Index::new(4)).unwrap() {
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
