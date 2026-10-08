// SPEC-MANAGED: tech-design/logic/core-scheduler-priority-rate-dispatch.md#logic
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
// HANDWRITE-END
