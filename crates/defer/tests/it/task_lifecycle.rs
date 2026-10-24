// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-task-lifecycle" tracker="#766" reason="Conformance tests for Defer's ETA-first, priority-aware push queue scheduler core."
use chrono::{Duration, Utc};
use defer::{
    CreateTask, DeferScheduler, NackOutcome, QueuePolicy, Target, TaskStateFilter, TaskStatus,
    DEFAULT_PRIORITY,
};

fn target() -> Target {
    Target {
        url: "http://worker.local/task".into(),
        method: "POST".into(),
        headers: Default::default(),
    }
}

fn task(id: &str, due_offset_ms: i64, priority: u8, max_attempts: u32) -> CreateTask {
    CreateTask {
        task_id: id.into(),
        target: target(),
        payload: serde_json::json!({ "id": id }),
        schedule_at: Utc::now() + Duration::milliseconds(due_offset_ms),
        priority,
        max_attempts,
    }
}

fn scheduler(policy: QueuePolicy) -> DeferScheduler {
    let mut s = DeferScheduler::new();
    s.configure_queue("q", policy);
    s
}

#[test]
fn eta_is_checked_before_priority() {
    let now = Utc::now();
    let mut s = scheduler(QueuePolicy::default());
    let mut future_high = task("future-high", 0, 250, 3);
    future_high.schedule_at = now + Duration::seconds(30);
    let mut due_low = task("due-low", 0, 1, 3);
    due_low.schedule_at = now;

    s.create_task("q", future_high).unwrap();
    s.create_task("q", due_low).unwrap();

    let leases = s.lease_due("q", now, 10).unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].task_id, "due-low");
}

#[test]
fn priority_orders_due_tasks_and_same_priority_uses_creation_fifo() {
    let now = Utc::now();
    let mut s = scheduler(QueuePolicy::default());
    for (id, priority) in [
        ("normal-a", DEFAULT_PRIORITY),
        ("high", 200),
        ("normal-b", DEFAULT_PRIORITY),
        ("low", 1),
    ] {
        let mut t = task(id, 0, priority, 3);
        t.schedule_at = now;
        s.create_task("q", t).unwrap();
    }

    let ids: Vec<_> = s
        .lease_due("q", now, 10)
        .unwrap()
        .into_iter()
        .map(|l| l.task_id)
        .collect();
    assert_eq!(ids, vec!["high", "normal-a", "normal-b", "low"]);
}

#[test]
fn defer_owns_dispatch_budget_and_concurrency() {
    let now = Utc::now();
    let mut s = scheduler(QueuePolicy {
        max_in_flight: 2,
        max_dispatch_per_tick: 5,
        ..QueuePolicy::default()
    });
    for id in ["a", "b", "c"] {
        let mut t = task(id, 0, DEFAULT_PRIORITY, 3);
        t.schedule_at = now;
        s.create_task("q", t).unwrap();
    }

    let first = s.lease_due("q", now, 10).unwrap();
    assert_eq!(first.len(), 2, "concurrency cap limits dispatch");
    assert!(s.lease_due("q", now, 10).unwrap().is_empty());

    assert!(s.ack("q", &first[0].attempt_id).unwrap());
    let second = s.lease_due("q", now, 10).unwrap();
    assert_eq!(second.len(), 1, "ack frees one dispatch slot");
    assert_eq!(second[0].task_id, "c");
}

#[test]
fn nack_reschedules_then_dead_letters_after_max_attempts() {
    let now = Utc::now();
    let mut s = scheduler(QueuePolicy {
        retry_backoff_ms: 1_000,
        ..QueuePolicy::default()
    });
    let mut t = task("retry", 0, DEFAULT_PRIORITY, 2);
    t.schedule_at = now;
    s.create_task("q", t).unwrap();

    let first = s.lease_due("q", now, 1).unwrap().remove(0);
    let outcome = s.nack("q", &first.attempt_id, now).unwrap().unwrap();
    let next_at = match outcome {
        NackOutcome::Retried { next_at } => next_at,
        NackOutcome::DeadLettered => panic!("first failure should retry"),
    };
    assert!(s
        .lease_due("q", next_at - Duration::milliseconds(1), 1)
        .unwrap()
        .is_empty());

    let second = s.lease_due("q", next_at, 1).unwrap().remove(0);
    assert_eq!(second.attempt, 2);
    assert_eq!(
        s.nack("q", &second.attempt_id, next_at).unwrap(),
        Some(NackOutcome::DeadLettered)
    );
    assert_eq!(
        s.status("q", "retry").unwrap(),
        Some(TaskStatus::DeadLettered)
    );
}

#[test]
fn cancel_prevents_dispatch() {
    let now = Utc::now();
    let mut s = scheduler(QueuePolicy::default());
    let mut t = task("cancel-me", 0, DEFAULT_PRIORITY, 3);
    t.schedule_at = now;
    s.create_task("q", t).unwrap();
    assert!(s.cancel("q", "cancel-me").unwrap());
    assert!(s.lease_due("q", now, 1).unwrap().is_empty());
    assert_eq!(
        s.status("q", "cancel-me").unwrap(),
        Some(TaskStatus::Canceled)
    );
}

#[test]
fn list_tasks_ordering_cursor_and_state_filtering() {
    let now = Utc::now();
    let mut s = scheduler(QueuePolicy {
        retry_backoff_ms: 1_000,
        ..QueuePolicy::default()
    });

    // Verify missing queue returns SchedulerError::QueueMissing
    assert!(matches!(
        s.list_tasks("missing", None, None, 10),
        Err(defer::SchedulerError::QueueMissing(q)) if q == "missing"
    ));

    // Verify empty queue returns empty list
    let (tasks, has_more) = s.list_tasks("q", None, None, 10).unwrap();
    assert!(tasks.is_empty());
    assert!(!has_more);

    // Create 5 tasks:
    // task-1: Scheduled (future ETA, not due)
    let t1 = task("task-1", 3_600_000, DEFAULT_PRIORITY, 3);
    s.create_task("q", t1).unwrap();

    // task-2: Leased (due now)
    let mut t2 = task("task-2", 0, DEFAULT_PRIORITY, 3);
    t2.schedule_at = now;
    s.create_task("q", t2).unwrap();

    // task-3: Succeeded (due now)
    let mut t3 = task("task-3", 0, DEFAULT_PRIORITY, 3);
    t3.schedule_at = now;
    s.create_task("q", t3).unwrap();

    // task-4: DeadLettered (due now, max_attempts 1)
    let mut t4 = task("task-4", 0, DEFAULT_PRIORITY, 1);
    t4.schedule_at = now;
    s.create_task("q", t4).unwrap();

    // task-5: Canceled (future ETA)
    let t5 = task("task-5", 3_600_000, DEFAULT_PRIORITY, 3);
    s.create_task("q", t5).unwrap();

    // Lease task-2
    let l2 = s.lease_due("q", now, 1).unwrap();
    assert_eq!(l2.len(), 1);
    assert_eq!(l2[0].task_id, "task-2");

    // Lease task-3 and ack -> Succeeded
    let l3 = s.lease_due("q", now, 1).unwrap();
    assert_eq!(l3.len(), 1);
    assert_eq!(l3[0].task_id, "task-3");
    assert!(s.ack("q", &l3[0].attempt_id).unwrap());

    // Lease task-4 and nack -> DeadLettered (max_attempts = 1)
    let l4 = s.lease_due("q", now, 1).unwrap();
    assert_eq!(l4.len(), 1);
    assert_eq!(l4[0].task_id, "task-4");
    assert_eq!(
        s.nack("q", &l4[0].attempt_id, now).unwrap(),
        Some(NackOutcome::DeadLettered)
    );

    // Cancel task-5 -> Canceled
    assert!(s.cancel("q", "task-5").unwrap());

    // Verify ordering across all tasks (ascending task_id)
    let (all, has_more) = s.list_tasks("q", None, None, 10).unwrap();
    assert!(!has_more);
    assert_eq!(all.len(), 5);
    assert_eq!(all[0].0, "task-1");
    assert!(matches!(all[0].1, TaskStatus::Scheduled));
    assert_eq!(all[1].0, "task-2");
    assert!(matches!(all[1].1, TaskStatus::Leased { .. }));
    assert_eq!(all[2].0, "task-3");
    assert!(matches!(all[2].1, TaskStatus::Succeeded));
    assert_eq!(all[3].0, "task-4");
    assert!(matches!(all[3].1, TaskStatus::DeadLettered));
    assert_eq!(all[4].0, "task-5");
    assert!(matches!(all[4].1, TaskStatus::Canceled));

    // Verify cursor paging with limit = 2
    let (p1, more1) = s.list_tasks("q", None, None, 2).unwrap();
    assert!(more1);
    assert_eq!(p1.len(), 2);
    assert_eq!(p1[0].0, "task-1");
    assert_eq!(p1[1].0, "task-2");

    let (p2, more2) = s.list_tasks("q", None, Some("task-2"), 2).unwrap();
    assert!(more2);
    assert_eq!(p2.len(), 2);
    assert_eq!(p2[0].0, "task-3");
    assert_eq!(p2[1].0, "task-4");

    let (p3, more3) = s.list_tasks("q", None, Some("task-4"), 2).unwrap();
    assert!(!more3);
    assert_eq!(p3.len(), 1);
    assert_eq!(p3[0].0, "task-5");

    let (p4, more4) = s.list_tasks("q", None, Some("task-5"), 2).unwrap();
    assert!(!more4);
    assert!(p4.is_empty());

    // Verify filtering by each state
    for (filter, expected_id) in [
        (TaskStateFilter::Scheduled, "task-1"),
        (TaskStateFilter::Leased, "task-2"),
        (TaskStateFilter::Succeeded, "task-3"),
        (TaskStateFilter::DeadLettered, "task-4"),
        (TaskStateFilter::Canceled, "task-5"),
    ] {
        let (filtered, more) = s.list_tasks("q", Some(filter), None, 10).unwrap();
        assert!(!more);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0, expected_id);
    }
}
// HANDWRITE-END
