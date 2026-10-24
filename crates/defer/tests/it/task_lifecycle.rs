// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-task-lifecycle" tracker="#766" reason="Conformance tests for Defer's ETA-first, priority-aware push queue scheduler core."
use chrono::{Duration, Utc};
use defer::{
    CreateTask, DeferScheduler, NackOutcome, QueuePolicy, Target, TaskStatus, DEFAULT_PRIORITY,
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
fn retry_backoff_bounds_honor_max_doublings_and_max_backoff() {
    let now = Utc::now();
    let mut s = scheduler(QueuePolicy {
        retry_backoff_ms: 1_000,
        max_backoff_ms: Some(3_000),
        max_doublings: Some(2),
        ..QueuePolicy::default()
    });
    let mut t = task("bounded-retry", 0, DEFAULT_PRIORITY, 5);
    t.schedule_at = now;
    s.create_task("q", t).unwrap();

    // attempt 1: backoff is 1000ms
    let first = s.lease_due("q", now, 1).unwrap().remove(0);
    let outcome = s.nack("q", &first.attempt_id, now).unwrap().unwrap();
    let next_at = match outcome {
        NackOutcome::Retried { next_at } => next_at,
        NackOutcome::DeadLettered => panic!("attempt 1 should retry"),
    };
    assert_eq!(next_at, now + Duration::milliseconds(1_000));

    // attempt 2: backoff is 2000ms
    let second = s.lease_due("q", next_at, 1).unwrap().remove(0);
    let outcome2 = s.nack("q", &second.attempt_id, next_at).unwrap().unwrap();
    let next_at2 = match outcome2 {
        NackOutcome::Retried { next_at } => next_at,
        NackOutcome::DeadLettered => panic!("attempt 2 should retry"),
    };
    assert_eq!(next_at2, next_at + Duration::milliseconds(2_000));

    // attempt 3: 1000 * 2^2 = 4000ms, capped at max_backoff_ms (3000ms)
    let third = s.lease_due("q", next_at2, 1).unwrap().remove(0);
    let outcome3 = s.nack("q", &third.attempt_id, next_at2).unwrap().unwrap();
    let next_at3 = match outcome3 {
        NackOutcome::Retried { next_at } => next_at,
        NackOutcome::DeadLettered => panic!("attempt 3 should retry"),
    };
    assert_eq!(next_at3, next_at2 + Duration::milliseconds(3_000));

    // attempt 4: max_doublings = 2 stops growth, capped at 3000ms
    let fourth = s.lease_due("q", next_at3, 1).unwrap().remove(0);
    let outcome4 = s.nack("q", &fourth.attempt_id, next_at3).unwrap().unwrap();
    let next_at4 = match outcome4 {
        NackOutcome::Retried { next_at } => next_at,
        NackOutcome::DeadLettered => panic!("attempt 4 should retry"),
    };
    assert_eq!(next_at4, next_at3 + Duration::milliseconds(3_000));
}

#[test]
fn retry_backoff_cap_below_base_applied_without_panic() {
    let now = Utc::now();
    let mut s = scheduler(QueuePolicy {
        retry_backoff_ms: 1_000,
        max_backoff_ms: Some(250),
        ..QueuePolicy::default()
    });
    let mut t = task("low-cap", 0, DEFAULT_PRIORITY, 3);
    t.schedule_at = now;
    s.create_task("q", t).unwrap();

    let first = s.lease_due("q", now, 1).unwrap().remove(0);
    let outcome = s.nack("q", &first.attempt_id, now).unwrap().unwrap();
    let next_at = match outcome {
        NackOutcome::Retried { next_at } => next_at,
        NackOutcome::DeadLettered => panic!("should retry"),
    };
    assert_eq!(next_at, now + Duration::milliseconds(250));
}

#[test]
fn reclaim_expired_honors_retry_backoff_bounds() {
    let now = Utc::now();
    let mut s = scheduler(QueuePolicy {
        lease_ttl_ms: 1_000,
        retry_backoff_ms: 1_000,
        max_backoff_ms: Some(1_500),
        max_doublings: Some(1),
        ..QueuePolicy::default()
    });
    let mut t = task("expired-task", 0, DEFAULT_PRIORITY, 3);
    t.schedule_at = now;
    s.create_task("q", t).unwrap();

    let first = s.lease_due("q", now, 1).unwrap().remove(0);
    assert_eq!(first.attempt, 1);

    // Expire attempt 1
    let expire_time = now + Duration::milliseconds(1_001);
    let reclaimed = s.reclaim_expired("q", expire_time).unwrap();
    assert_eq!(reclaimed, vec!["expired-task"]);

    // Attempt 1 backoff: 1000ms
    let scheduled_at = expire_time + Duration::milliseconds(1_000);
    assert!(s
        .lease_due("q", scheduled_at - Duration::milliseconds(1), 1)
        .unwrap()
        .is_empty());

    let second = s.lease_due("q", scheduled_at, 1).unwrap().remove(0);
    assert_eq!(second.attempt, 2);

    // Expire attempt 2
    let expire_time2 = scheduled_at + Duration::milliseconds(1_001);
    let reclaimed2 = s.reclaim_expired("q", expire_time2).unwrap();
    assert_eq!(reclaimed2, vec!["expired-task"]);

    // Attempt 2 backoff: 1000 * 2^1 = 2000ms, capped at max_backoff_ms (1500ms)
    let scheduled_at2 = expire_time2 + Duration::milliseconds(1_500);
    assert!(s
        .lease_due("q", scheduled_at2 - Duration::milliseconds(1), 1)
        .unwrap()
        .is_empty());
    let third = s.lease_due("q", scheduled_at2, 1).unwrap().remove(0);
    assert_eq!(third.attempt, 3);
}
// HANDWRITE-END
