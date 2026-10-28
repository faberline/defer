// HANDWRITE-BEGIN gap="missing-generator:logic:defer-core-scheduler" tracker="#766" reason="In-memory delayed push-queue scheduler core."
use chrono::{DateTime, Utc};
use raft_runtime::FencedAssignment;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::types::{
    AttemptId, CreateTask, QueueControlState, QueueName, QueuePolicy, QueueSnapshot,
    SchedulerError, SchedulerResult, TaskId, TaskStatus,
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
    use chrono::{Duration, Utc};

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
}
// HANDWRITE-END
