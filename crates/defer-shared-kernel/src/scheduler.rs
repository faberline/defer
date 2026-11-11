// HANDWRITE-BEGIN gap="missing-generator:logic:defer-core-scheduler" tracker="#766" reason="In-memory delayed push-queue scheduler core."
use chrono::{DateTime, Utc};
use raft_runtime::FencedAssignment;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::types::{
    AttemptId, CreateTask, QueueControlState, QueueName, QueuePolicy, QueueSnapshot,
    SchedulerError, SchedulerResult, TaskId, TaskStateFilter, TaskStatus,
};

mod lease;

const PRIORITY_BANDS: usize = u8::MAX as usize + 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskRecord {
    queue: QueueName,
    create: CreateTask,
    created_seq: u64,
    attempts: u32,
    status: TaskStatus,
    #[serde(default)]
    assignment: FencedAssignment,
    #[serde(default)]
    terminal_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueState {
    policy: QueuePolicy,
    control_state: QueueControlState,
    tasks: HashMap<TaskId, TaskRecord>,
    due: BinaryHeap<Reverse<(i64, u64, TaskId)>>,
    ready: Vec<BinaryHeap<Reverse<(u64, TaskId)>>>,
    in_flight: HashMap<AttemptId, TaskId>,
    next_seq: u64,
    next_attempt_seq: u64,
    dispatch_tokens_milli: u64,
    last_refill_at: Option<DateTime<Utc>>,
}

impl QueueState {
    fn new(policy: QueuePolicy) -> Self {
        Self {
            dispatch_tokens_milli: burst_tokens_milli(policy.max_burst_size),
            policy,
            control_state: QueueControlState::Running,
            tasks: HashMap::new(),
            due: BinaryHeap::new(),
            ready: (0..PRIORITY_BANDS).map(|_| BinaryHeap::new()).collect(),
            in_flight: HashMap::new(),
            next_seq: 0,
            next_attempt_seq: 0,
            last_refill_at: None,
        }
    }

    fn update_policy(&mut self, policy: QueuePolicy) {
        self.dispatch_tokens_milli = self
            .dispatch_tokens_milli
            .min(burst_tokens_milli(policy.max_burst_size));
        self.policy = policy;
    }

    fn in_flight_capacity(&self) -> usize {
        self.policy
            .max_in_flight
            .saturating_sub(self.in_flight.len())
    }

    fn push_due(&mut self, task_id: &str) {
        if let Some(task) = self.tasks.get(task_id) {
            self.due.push(Reverse((
                task.create.schedule_at.timestamp_millis(),
                task.created_seq,
                task_id.to_string(),
            )));
        }
    }

    fn promote_due(&mut self, now: DateTime<Utc>) {
        let cutoff = now.timestamp_millis();
        while let Some(Reverse((at, _, task_id))) = self.due.peek().cloned() {
            if at > cutoff {
                break;
            }
            self.due.pop();
            let Some(task) = self.tasks.get(&task_id) else {
                continue;
            };
            if task.create.schedule_at.timestamp_millis() != at {
                continue;
            }
            if matches!(task.status, TaskStatus::Scheduled) {
                self.ready[task.create.priority as usize]
                    .push(Reverse((task.created_seq, task_id)));
            }
        }
    }

    fn refill_dispatch_tokens(&mut self, now: DateTime<Utc>) {
        let Some(last_refill_at) = self.last_refill_at else {
            self.last_refill_at = Some(now);
            return;
        };
        if now <= last_refill_at {
            return;
        }
        let elapsed_ms = (now - last_refill_at).num_milliseconds().max(0) as u64;
        let added = elapsed_ms.saturating_mul(self.policy.max_dispatches_per_second as u64);
        self.dispatch_tokens_milli = self
            .dispatch_tokens_milli
            .saturating_add(added)
            .min(burst_tokens_milli(self.policy.max_burst_size));
        self.last_refill_at = Some(now);
    }

    fn rate_capacity(&self) -> usize {
        (self.dispatch_tokens_milli / 1_000) as usize
    }

    fn consume_dispatch_tokens(&mut self, count: usize) {
        self.dispatch_tokens_milli = self
            .dispatch_tokens_milli
            .saturating_sub((count as u64).saturating_mul(1_000));
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeferScheduler {
    queues: HashMap<QueueName, QueueState>,
}

impl DeferScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Deterministic queue inventory for dispatcher/admin loops.
    pub fn queue_names(&self) -> Vec<QueueName> {
        let mut queues: Vec<_> = self.queues.keys().cloned().collect();
        queues.sort();
        queues
    }

    pub fn configure_queue(&mut self, queue: impl Into<String>, policy: QueuePolicy) {
        let queue = queue.into();
        if let Some(state) = self.queues.get_mut(&queue) {
            state.update_policy(policy);
        } else {
            self.queues.insert(queue, QueueState::new(policy));
        }
    }

    pub fn update_queue_policy(&mut self, queue: &str, policy: QueuePolicy) -> SchedulerResult<()> {
        let state = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        state.update_policy(policy);
        Ok(())
    }

    pub fn pause_queue(&mut self, queue: &str) -> SchedulerResult<()> {
        self.set_queue_control_state(queue, QueueControlState::Paused)
    }

    pub fn resume_queue(&mut self, queue: &str) -> SchedulerResult<()> {
        self.set_queue_control_state(queue, QueueControlState::Running)
    }

    pub fn disable_queue(&mut self, queue: &str) -> SchedulerResult<()> {
        self.set_queue_control_state(queue, QueueControlState::Disabled)
    }

    pub fn queue_snapshot(&self, queue: &str) -> SchedulerResult<QueueSnapshot> {
        let state = self
            .queues
            .get(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        let scheduled_count = state
            .tasks
            .values()
            .filter(|task| matches!(task.status, TaskStatus::Scheduled))
            .count();
        let terminal_count = state
            .tasks
            .values()
            .filter(|task| {
                matches!(
                    task.status,
                    TaskStatus::Succeeded | TaskStatus::DeadLettered | TaskStatus::Canceled
                )
            })
            .count();
        Ok(QueueSnapshot {
            queue: queue.to_string(),
            control_state: state.control_state,
            policy: state.policy.clone(),
            task_count: state.tasks.len(),
            scheduled_count,
            in_flight_count: state.in_flight.len(),
            terminal_count,
        })
    }

    pub fn create_task(
        &mut self,
        queue: impl Into<String>,
        task: CreateTask,
    ) -> SchedulerResult<()> {
        self.create_tasks(queue, vec![task])
    }

    /// Atomically validate and insert a task batch. A duplicate in either the
    /// existing queue or the request rejects the whole command.
    pub fn create_tasks(
        &mut self,
        queue: impl Into<String>,
        tasks: Vec<CreateTask>,
    ) -> SchedulerResult<()> {
        let queue = queue.into();
        let state = self
            .queues
            .get_mut(&queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.clone()))?;
        if state.control_state == QueueControlState::Disabled {
            return Err(SchedulerError::QueueDisabled(queue));
        }
        let mut request_ids = HashSet::with_capacity(tasks.len());
        for task in &tasks {
            if state.tasks.contains_key(&task.task_id) || !request_ids.insert(task.task_id.clone())
            {
                return Err(SchedulerError::TaskExists(task.task_id.clone()));
            }
        }
        for task in tasks {
            let created_seq = state.next_seq;
            state.next_seq += 1;
            let task_id = task.task_id.clone();
            state.tasks.insert(
                task_id.clone(),
                TaskRecord {
                    queue: queue.clone(),
                    create: task,
                    created_seq,
                    attempts: 0,
                    status: TaskStatus::Scheduled,
                    assignment: FencedAssignment::idle(),
                    terminal_at: None,
                },
            );
            state.push_due(&task_id);
        }
        Ok(())
    }

    pub fn status(&self, queue: &str, task_id: &str) -> SchedulerResult<Option<TaskStatus>> {
        let state = self
            .queues
            .get(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        Ok(state.tasks.get(task_id).map(|t| t.status.clone()))
    }

    pub fn list_tasks(
        &self,
        queue: &str,
        state_filter: Option<TaskStateFilter>,
        after: Option<&str>,
        limit: usize,
    ) -> SchedulerResult<(Vec<(TaskId, TaskStatus)>, bool)> {
        let state = self
            .queues
            .get(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;

        let mut matching: Vec<(&TaskId, &TaskRecord)> = state
            .tasks
            .iter()
            .filter(|(task_id, task)| {
                if let Some(cursor) = after {
                    if !cursor.is_empty() && task_id.as_str() <= cursor {
                        return false;
                    }
                }
                if let Some(filter) = state_filter {
                    if !filter.matches(&task.status) {
                        return false;
                    }
                }
                true
            })
            .collect();

        matching.sort_by_key(|(task_id, _)| *task_id);

        let has_more = matching.len() > limit;
        if has_more {
            matching.truncate(limit);
        }

        let tasks = matching
            .into_iter()
            .map(|(task_id, task)| (task_id.clone(), task.status.clone()))
            .collect();

        Ok((tasks, has_more))
    }

    fn set_queue_control_state(
        &mut self,
        queue: &str,
        control_state: QueueControlState,
    ) -> SchedulerResult<()> {
        let state = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        state.control_state = control_state;
        Ok(())
    }

    pub fn purge_queue(&mut self, queue: &str) -> SchedulerResult<usize> {
        let state = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        let count = state.tasks.len();
        state.tasks.clear();
        state.due.clear();
        for ready in &mut state.ready {
            ready.clear();
        }
        state.in_flight.clear();
        Ok(count)
    }

    /// Remove terminal records whose retention has ended. The first call that
    /// sees a terminal record without `terminal_at` stamps it with `now`; a
    /// record is removed when `terminal_at + retention <= now`. `None`
    /// retention prunes nothing and stamps nothing.
    pub fn prune_terminal(&mut self, queue: &str, now: DateTime<Utc>) -> SchedulerResult<usize> {
        let state = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        let Some(retention_ms) = state.policy.terminal_retention_ms else {
            return Ok(0);
        };
        let retention = chrono::Duration::milliseconds(retention_ms.min(i64::MAX as u64) as i64);
        let mut expired = Vec::new();
        for (task_id, task) in state.tasks.iter_mut() {
            if !matches!(
                task.status,
                TaskStatus::Succeeded | TaskStatus::DeadLettered | TaskStatus::Canceled
            ) {
                continue;
            }
            let terminal_at = *task.terminal_at.get_or_insert(now);
            // Overflow keeps the record.
            let Some(due_at) = terminal_at.checked_add_signed(retention) else {
                continue;
            };
            if due_at <= now {
                expired.push(task_id.clone());
            }
        }
        for task_id in &expired {
            state.tasks.remove(task_id);
        }
        Ok(expired.len())
    }

    pub fn delete_queue(&mut self, queue: &str) -> SchedulerResult<()> {
        if self.queues.remove(queue).is_some() {
            Ok(())
        } else {
            Err(SchedulerError::QueueMissing(queue.to_string()))
        }
    }
}

fn pick_ready(state: &mut QueueState) -> Option<TaskId> {
    for priority in (0..PRIORITY_BANDS).rev() {
        while let Some(Reverse((_, task_id))) = state.ready[priority].pop() {
            let Some(task) = state.tasks.get(&task_id) else {
                continue;
            };
            if matches!(task.status, TaskStatus::Scheduled) {
                return Some(task_id);
            }
        }
    }
    None
}

fn burst_tokens_milli(max_burst_size: usize) -> u64 {
    (max_burst_size as u64).saturating_mul(1_000)
}

fn millis(value: DateTime<Utc>) -> u64 {
    value.timestamp_millis().max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CreateTask, Target, DEFAULT_PRIORITY};
    use chrono::{Duration, TimeZone, Utc};

    fn test_target() -> Target {
        Target {
            url: "http://example.com/worker".into(),
            method: "POST".into(),
            headers: Default::default(),
        }
    }

    fn test_task(id: &str, due_offset_ms: i64, max_attempts: u32) -> CreateTask {
        CreateTask {
            task_id: id.into(),
            target: test_target(),
            payload: serde_json::json!({ "id": id }),
            schedule_at: Utc::now() + Duration::milliseconds(due_offset_ms),
            priority: DEFAULT_PRIORITY,
            max_attempts,
            dispatch_deadline_ms: None,
        }
    }

    fn sample_task(id: &str, at: DateTime<Utc>) -> CreateTask {
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

    #[test]
    fn purge_queue_removes_tasks_in_every_state_and_preserves_policy_and_control() {
        let now = Utc::now();
        let mut s = DeferScheduler::new();
        let custom_policy = QueuePolicy {
            max_in_flight: 5,
            max_dispatch_per_tick: 5,
            max_dispatches_per_second: 50,
            max_burst_size: 20,
            lease_ttl_ms: 10_000,
            retry_backoff_ms: 500,
            max_backoff_ms: None,
            max_doublings: None,
            terminal_retention_ms: None,
        };
        s.configure_queue("jobs", custom_policy.clone());

        // 1. Scheduled task
        s.create_task("jobs", test_task("t-sched", 0, 3)).unwrap();

        // 2. Leased task
        s.create_task("jobs", test_task("t-leased", 0, 3)).unwrap();
        let leased = s.lease_due("jobs", now, 1).unwrap().remove(0);
        let leased_attempt_id = leased.attempt_id.clone();
        assert_eq!(leased.task_id, "t-sched"); // t-sched was first

        // Lease the second one as well
        let leased2 = s.lease_due("jobs", now, 1).unwrap().remove(0);
        assert_eq!(leased2.task_id, "t-leased");

        // 3. Succeeded task (ack leased2)
        assert!(s.ack("jobs", &leased2.attempt_id).unwrap());
        assert_eq!(
            s.status("jobs", "t-leased").unwrap(),
            Some(TaskStatus::Succeeded)
        );

        // 4. DeadLettered task (create with max_attempts=1, lease, nack)
        s.create_task("jobs", test_task("t-dead", 0, 1)).unwrap();
        let leased_dead = s.lease_due("jobs", now, 1).unwrap().remove(0);
        assert_eq!(leased_dead.task_id, "t-dead");
        s.nack("jobs", &leased_dead.attempt_id, now).unwrap();
        assert_eq!(
            s.status("jobs", "t-dead").unwrap(),
            Some(TaskStatus::DeadLettered)
        );

        // 5. Canceled task
        s.create_task("jobs", test_task("t-cancel", 0, 3)).unwrap();
        assert!(s.cancel("jobs", "t-cancel").unwrap());
        assert_eq!(
            s.status("jobs", "t-cancel").unwrap(),
            Some(TaskStatus::Canceled)
        );

        // Pause the queue to test control state preservation
        s.pause_queue("jobs").unwrap();

        // Verify snapshot before purge has 4 tasks (t-sched, t-leased, t-dead, t-cancel)
        let snap_before = s.queue_snapshot("jobs").unwrap();
        assert_eq!(snap_before.task_count, 4);
        assert_eq!(snap_before.control_state, QueueControlState::Paused);
        assert_eq!(snap_before.policy, custom_policy);

        // Consume tokens / record token state before purge
        let tokens_before = s.queues.get("jobs").unwrap().dispatch_tokens_milli;

        // Perform purge
        let count = s.purge_queue("jobs").unwrap();
        assert_eq!(count, 4);

        // Snapshot after purge
        let snap_after = s.queue_snapshot("jobs").unwrap();
        assert_eq!(snap_after.task_count, 0);
        assert_eq!(snap_after.scheduled_count, 0);
        assert_eq!(snap_after.in_flight_count, 0);
        assert_eq!(snap_after.terminal_count, 0);
        assert_eq!(snap_after.control_state, QueueControlState::Paused);
        assert_eq!(snap_after.policy, custom_policy);

        // Rate limit bucket is preserved
        let tokens_after = s.queues.get("jobs").unwrap().dispatch_tokens_milli;
        assert_eq!(tokens_after, tokens_before);

        // All task statuses are None
        for id in ["t-sched", "t-leased", "t-dead", "t-cancel"] {
            assert_eq!(s.status("jobs", id).unwrap(), None);
        }

        // A purged task_id can be created again
        s.resume_queue("jobs").unwrap();
        assert!(s.create_task("jobs", test_task("t-sched", 0, 3)).is_ok());
        assert_eq!(
            s.status("jobs", "t-sched").unwrap(),
            Some(TaskStatus::Scheduled)
        );

        // Purge on an unknown queue returns QueueMissing
        assert!(matches!(
            s.purge_queue("nonexistent"),
            Err(SchedulerError::QueueMissing(ref q)) if q == "nonexistent"
        ));

        // Ack / nack of a previously leased attempt after purge returns false/None without panic
        assert!(!s.ack("jobs", &leased_attempt_id).unwrap());
        assert_eq!(s.nack("jobs", &leased_attempt_id, now).unwrap(), None);
    }

    #[test]
    fn delete_queue_removes_queue_and_tasks_and_allows_clean_recreation() {
        let now = Utc.timestamp_millis_opt(10_000).unwrap();
        let mut scheduler = DeferScheduler::new();

        // Unknown queue returns QueueMissing
        assert_eq!(
            scheduler.delete_queue("missing"),
            Err(SchedulerError::QueueMissing("missing".to_string()))
        );

        // Configure queue and create tasks
        scheduler.configure_queue("jobs", QueuePolicy::default());
        scheduler
            .create_task("jobs", sample_task("t1", now))
            .unwrap();
        scheduler
            .create_task("jobs", sample_task("t2", now))
            .unwrap();

        assert_eq!(scheduler.queue_snapshot("jobs").unwrap().task_count, 2);

        // Lease an in-flight attempt
        let leases = scheduler.lease_due("jobs", now, 1).unwrap();
        assert_eq!(leases.len(), 1);
        let attempt_id = leases[0].attempt_id.clone();
        assert_eq!(scheduler.queue_snapshot("jobs").unwrap().in_flight_count, 1);

        // Delete queue succeeds while an attempt is in-flight
        assert_eq!(scheduler.delete_queue("jobs"), Ok(()));

        // Queue snapshot is gone
        assert_eq!(
            scheduler.queue_snapshot("jobs"),
            Err(SchedulerError::QueueMissing("jobs".to_string()))
        );

        // Subsequent delete returns QueueMissing
        assert_eq!(
            scheduler.delete_queue("jobs"),
            Err(SchedulerError::QueueMissing("jobs".to_string()))
        );

        // Later ack or nack of the in-flight attempt returns QueueMissing and does not panic
        assert_eq!(
            scheduler.ack("jobs", &attempt_id),
            Err(SchedulerError::QueueMissing("jobs".to_string()))
        );
        assert_eq!(
            scheduler.nack("jobs", &attempt_id, now),
            Err(SchedulerError::QueueMissing("jobs".to_string()))
        );

        // Recreate queue with configure_queue (equivalent to PUT /v1/queues/{queue})
        scheduler.configure_queue("jobs", QueuePolicy::default());
        let snapshot = scheduler.queue_snapshot("jobs").unwrap();
        assert_eq!(snapshot.task_count, 0);
        assert_eq!(snapshot.scheduled_count, 0);
        assert_eq!(snapshot.in_flight_count, 0);
        assert_eq!(snapshot.terminal_count, 0);
    }

    fn retention_policy(terminal_retention_ms: Option<u64>) -> QueuePolicy {
        QueuePolicy {
            terminal_retention_ms,
            ..QueuePolicy::default()
        }
    }

    #[test]
    fn prune_terminal_keeps_records_before_retention_and_removes_them_after() {
        let mut s = DeferScheduler::new();
        s.configure_queue("jobs", retention_policy(Some(1_000)));
        s.create_task("jobs", test_task("a", -1_000, 3)).unwrap();
        s.create_task("jobs", test_task("b", -1_000, 3)).unwrap();
        let leased = s.lease_due("jobs", Utc::now(), 1).unwrap().remove(0);
        assert_eq!(leased.task_id, "a");
        assert!(s.ack("jobs", &leased.attempt_id).unwrap());
        assert!(s.cancel("jobs", "b").unwrap());

        let t0 = Utc::now();
        // The first prune call stamps terminal_at on both records and removes nothing.
        assert_eq!(s.prune_terminal("jobs", t0).unwrap(), 0);
        assert_eq!(
            s.prune_terminal("jobs", t0 + Duration::milliseconds(999))
                .unwrap(),
            0
        );
        assert_eq!(s.queue_snapshot("jobs").unwrap().task_count, 2);
        assert_eq!(
            s.prune_terminal("jobs", t0 + Duration::milliseconds(1_000))
                .unwrap(),
            2
        );
        assert_eq!(s.queue_snapshot("jobs").unwrap().task_count, 0);
    }

    #[test]
    fn prune_terminal_never_removes_live_tasks() {
        let mut s = DeferScheduler::new();
        s.configure_queue("jobs", retention_policy(Some(1_000)));
        s.create_task("jobs", test_task("scheduled", 60_000, 3))
            .unwrap();
        s.create_task("jobs", test_task("leased", -1_000, 3))
            .unwrap();
        let leased = s.lease_due("jobs", Utc::now(), 1).unwrap().remove(0);
        assert_eq!(leased.task_id, "leased");

        assert_eq!(
            s.prune_terminal("jobs", Utc::now() + Duration::days(1))
                .unwrap(),
            0
        );
        assert_eq!(s.queue_snapshot("jobs").unwrap().task_count, 2);
        assert_eq!(
            s.status("jobs", "scheduled").unwrap(),
            Some(TaskStatus::Scheduled)
        );
        assert!(matches!(
            s.status("jobs", "leased").unwrap(),
            Some(TaskStatus::Leased { .. })
        ));
    }

    #[test]
    fn prune_terminal_with_no_retention_prunes_nothing() {
        let mut s = DeferScheduler::new();
        s.configure_queue("jobs", QueuePolicy::default());
        s.create_task("jobs", test_task("a", -1_000, 3)).unwrap();
        let leased = s.lease_due("jobs", Utc::now(), 1).unwrap().remove(0);
        assert!(s.ack("jobs", &leased.attempt_id).unwrap());

        assert_eq!(
            s.prune_terminal("jobs", Utc::now() + Duration::days(365))
                .unwrap(),
            0
        );
        assert_eq!(s.status("jobs", "a").unwrap(), Some(TaskStatus::Succeeded));
    }

    #[test]
    fn pruned_id_can_be_created_again_and_kept_id_is_refused() {
        let mut s = DeferScheduler::new();
        s.configure_queue("jobs", retention_policy(Some(1_000)));
        s.create_task("jobs", test_task("a", -1_000, 3)).unwrap();
        let leased = s.lease_due("jobs", Utc::now(), 1).unwrap().remove(0);
        assert!(s.ack("jobs", &leased.attempt_id).unwrap());

        let t0 = Utc::now();
        assert_eq!(s.prune_terminal("jobs", t0).unwrap(), 0);
        // Before the retention ends, the id is still taken.
        assert_eq!(
            s.create_task("jobs", test_task("a", 0, 3)),
            Err(SchedulerError::TaskExists("a".to_string()))
        );

        assert_eq!(
            s.prune_terminal("jobs", t0 + Duration::milliseconds(1_000))
                .unwrap(),
            1
        );
        s.create_task("jobs", test_task("a", 0, 3)).unwrap();
        assert_eq!(s.status("jobs", "a").unwrap(), Some(TaskStatus::Scheduled));
    }

    #[test]
    fn prune_terminal_unknown_queue_errors() {
        let mut s = DeferScheduler::new();
        assert_eq!(
            s.prune_terminal("nope", Utc::now()),
            Err(SchedulerError::QueueMissing("nope".to_string()))
        );
    }

    #[test]
    fn old_snapshot_without_new_fields_still_loads() {
        let policy: QueuePolicy = serde_json::from_str(
            r#"{"max_in_flight":1,"max_dispatch_per_tick":1,"max_dispatches_per_second":1,"max_burst_size":1,"lease_ttl_ms":1,"retry_backoff_ms":1}"#,
        )
        .unwrap();
        assert_eq!(policy.terminal_retention_ms, None);

        let mut s = DeferScheduler::new();
        s.configure_queue("jobs", retention_policy(Some(1_000)));
        s.create_task("jobs", test_task("a", -1_000, 3)).unwrap();
        let leased = s.lease_due("jobs", Utc::now(), 1).unwrap().remove(0);
        assert!(s.ack("jobs", &leased.attempt_id).unwrap());
        assert_eq!(s.prune_terminal("jobs", Utc::now()).unwrap(), 0);

        // Simulate an old snapshot: the record has no terminal_at key.
        let mut json = serde_json::to_value(&s).unwrap();
        let removed = json["queues"]["jobs"]["tasks"]["a"]
            .as_object_mut()
            .unwrap()
            .remove("terminal_at");
        assert!(removed.is_some());
        let loaded: DeferScheduler = serde_json::from_value(json).unwrap();
        assert_eq!(
            loaded.status("jobs", "a").unwrap(),
            Some(TaskStatus::Succeeded)
        );
    }
}
// HANDWRITE-END
