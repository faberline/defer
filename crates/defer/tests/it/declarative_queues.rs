// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-declarative-queues" tracker="#766" reason="Integration tests for declarative queue provisioning."
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use defer::{
    ensure_declared_queues, parse_declared_queues, DeclaredQueue, DeclaredQueueDecision,
    DeclaredQueueOutcome, DeferRaft, DeferScheduler, QueueControlState, QueuePolicy,
};
use raft_runtime::Membership;

async fn test_raft() -> (tempfile::TempDir, Arc<DeferRaft>) {
    let dir = tempfile::tempdir().unwrap();
    let raft = Arc::new(
        DeferRaft::spawn(
            Arc::new(Mutex::new(DeferScheduler::new())),
            &defer::storage::raft_dir(dir.path()),
            0,
            Membership {
                voters: vec![0],
                learners: vec![],
            },
            HashMap::new(),
            DeferRaft::host_config(8),
        )
        .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(4);
    while !raft.is_leader().await {
        assert!(
            Instant::now() < deadline,
            "single node did not elect itself within deadline"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    (dir, raft)
}

#[tokio::test]
async fn test_parse_declared_queues_validation() {
    // Empty, whitespace, or empty array returns empty list
    assert!(parse_declared_queues("").unwrap().is_empty());
    assert!(parse_declared_queues("   ").unwrap().is_empty());
    assert!(parse_declared_queues("[]").unwrap().is_empty());
    assert!(parse_declared_queues("  []  ").unwrap().is_empty());

    // Malformed JSON
    assert!(parse_declared_queues("not-json").is_err());
    assert!(parse_declared_queues("{").is_err());

    // Non-array JSON
    assert!(parse_declared_queues(r#"{"name": "jobs"}"#).is_err());

    // Empty queue name
    assert!(parse_declared_queues(r#"[{"name": ""}]"#).is_err());
    assert!(parse_declared_queues(r#"[{"name": "   "}]"#).is_err());

    // Duplicate queue name
    assert!(parse_declared_queues(r#"[{"name": "jobs"}, {"name": "jobs"}]"#).is_err());

    // Unknown control value
    assert!(parse_declared_queues(r#"[{"name": "jobs", "control": "Unknown"}]"#).is_err());
    assert!(parse_declared_queues(r#"[{"name": "jobs", "control": "Stopped"}]"#).is_err());
    assert!(parse_declared_queues(r#"[{"name": "jobs", "control": 123}]"#).is_err());

    // Policy field that PUT /v1/queues/{queue} would refuse
    assert!(parse_declared_queues(r#"[{"name": "jobs", "policy": "not-an-object"}]"#).is_err());
    assert!(parse_declared_queues(
        r#"[{"name": "jobs", "policy": {"max_in_flight": "not-a-number"}}]"#
    )
    .is_err());
    assert!(
        parse_declared_queues(r#"[{"name": "jobs", "policy": {"lease_ttl_ms": -100}}]"#).is_err()
    );
    assert!(
        parse_declared_queues(r#"[{"name": "jobs", "policy": {"max_burst_size": []}}]"#).is_err()
    );

    // Valid declaration with omitted policy and control
    let parsed = parse_declared_queues(r#"[{"name": "jobs"}]"#).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].name, "jobs");
    assert_eq!(parsed[0].policy, QueuePolicy::default());
    assert_eq!(parsed[0].control, QueueControlState::Running);

    // Valid declaration with partial policy and control
    let parsed = parse_declared_queues(
        r#"[
            {
                "name": "high-throughput",
                "policy": {
                    "max_in_flight": 500,
                    "max_burst_size": 200
                },
                "control": "Paused"
            }
        ]"#,
    )
    .unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].name, "high-throughput");
    assert_eq!(parsed[0].policy.max_in_flight, 500);
    assert_eq!(parsed[0].policy.max_burst_size, 200);
    // Other fields default to QueuePolicy::default()
    assert_eq!(
        parsed[0].policy.max_dispatch_per_tick,
        QueuePolicy::default().max_dispatch_per_tick
    );
    assert_eq!(
        parsed[0].policy.max_dispatches_per_second,
        QueuePolicy::default().max_dispatches_per_second
    );
    assert_eq!(
        parsed[0].policy.lease_ttl_ms,
        QueuePolicy::default().lease_ttl_ms
    );
    assert_eq!(
        parsed[0].policy.retry_backoff_ms,
        QueuePolicy::default().retry_backoff_ms
    );
    assert_eq!(parsed[0].control, QueueControlState::Paused);
}

#[tokio::test]
async fn test_declared_queues_lifecycle_and_idempotence() {
    let (_dir, raft) = test_raft().await;

    let custom_policy = QueuePolicy {
        max_in_flight: 42,
        max_dispatch_per_tick: 50,
        max_dispatches_per_second: 60,
        max_burst_size: 70,
        lease_ttl_ms: 15_000,
        retry_backoff_ms: 500,
    };
    let declared = vec![DeclaredQueue {
        name: "declared-queue".to_string(),
        policy: custom_policy.clone(),
        control: QueueControlState::Paused,
    }];

    // 1. Missing queue is created with declared policy and control
    let outcomes = ensure_declared_queues(&raft, &declared).await.unwrap();
    assert_eq!(
        outcomes,
        vec![DeclaredQueueOutcome {
            name: "declared-queue".to_string(),
            decision: DeclaredQueueDecision::Created,
        }]
    );
    let snapshot = raft
        .scheduler()
        .lock()
        .unwrap()
        .queue_snapshot("declared-queue")
        .unwrap();
    assert_eq!(snapshot.policy, custom_policy);
    assert_eq!(snapshot.control_state, QueueControlState::Paused);

    // 2. Two ensures in a row are idempotent (decision: Unchanged)
    let outcomes_second = ensure_declared_queues(&raft, &declared).await.unwrap();
    assert_eq!(
        outcomes_second,
        vec![DeclaredQueueOutcome {
            name: "declared-queue".to_string(),
            decision: DeclaredQueueDecision::Unchanged,
        }]
    );
    let snapshot_second = raft
        .scheduler()
        .lock()
        .unwrap()
        .queue_snapshot("declared-queue")
        .unwrap();
    assert_eq!(snapshot_second.policy, custom_policy);
    assert_eq!(snapshot_second.control_state, QueueControlState::Paused);

    // 3. Undeclared queue is created at runtime and left untouched
    let runtime_policy = QueuePolicy {
        max_in_flight: 99,
        ..QueuePolicy::default()
    };
    raft.configure_queue("undeclared-queue".to_string(), runtime_policy.clone())
        .await
        .unwrap();
    assert!(raft
        .scheduler()
        .lock()
        .unwrap()
        .queue_snapshot("undeclared-queue")
        .is_ok());

    let outcomes_third = ensure_declared_queues(&raft, &declared).await.unwrap();
    assert_eq!(
        outcomes_third,
        vec![DeclaredQueueOutcome {
            name: "declared-queue".to_string(),
            decision: DeclaredQueueDecision::Unchanged,
        }]
    );
    // Verify undeclared queue is untouched
    let undeclared_snapshot = raft
        .scheduler()
        .lock()
        .unwrap()
        .queue_snapshot("undeclared-queue")
        .unwrap();
    assert_eq!(undeclared_snapshot.policy, runtime_policy);
    assert_eq!(
        undeclared_snapshot.control_state,
        QueueControlState::Running
    );

    // 4. Queue PUT at runtime with a different policy is brought back to declared policy
    let mutated_policy = QueuePolicy {
        max_in_flight: 888,
        ..QueuePolicy::default()
    };
    raft.configure_queue("declared-queue".to_string(), mutated_policy)
        .await
        .unwrap();
    let mutated_snapshot = raft
        .scheduler()
        .lock()
        .unwrap()
        .queue_snapshot("declared-queue")
        .unwrap();
    assert_eq!(mutated_snapshot.policy.max_in_flight, 888);

    let outcomes_reconciled = ensure_declared_queues(&raft, &declared).await.unwrap();
    assert_eq!(
        outcomes_reconciled,
        vec![DeclaredQueueOutcome {
            name: "declared-queue".to_string(),
            decision: DeclaredQueueDecision::Updated,
        }]
    );
    let restored_snapshot = raft
        .scheduler()
        .lock()
        .unwrap()
        .queue_snapshot("declared-queue")
        .unwrap();
    assert_eq!(restored_snapshot.policy, custom_policy);
    assert_eq!(restored_snapshot.control_state, QueueControlState::Paused);
}

#[tokio::test]
async fn test_follower_skips_without_error() {
    let dir = tempfile::tempdir().unwrap();
    // Node 0 is spawned with voters = [99], so node 0 will never become leader
    let non_leader_raft = Arc::new(
        DeferRaft::spawn(
            Arc::new(Mutex::new(DeferScheduler::new())),
            &defer::storage::raft_dir(dir.path()),
            0,
            Membership {
                voters: vec![99],
                learners: vec![],
            },
            HashMap::new(),
            DeferRaft::host_config(8),
        )
        .unwrap(),
    );

    assert!(!non_leader_raft.is_leader().await);

    let declared = vec![DeclaredQueue {
        name: "test-queue".to_string(),
        policy: QueuePolicy::default(),
        control: QueueControlState::Running,
    }];

    // Follower skips without error and returns an empty list
    let outcomes = ensure_declared_queues(&non_leader_raft, &declared)
        .await
        .unwrap();
    assert!(outcomes.is_empty());
}
// HANDWRITE-END
