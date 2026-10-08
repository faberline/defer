// SPEC-MANAGED: tech-design/logic/core-scheduler-priority-rate-dispatch.md#logic
// HANDWRITE-BEGIN gap="missing-generator:logic:defer-core-scheduler" tracker="#766" reason="In-memory delayed push-queue scheduler core."
//! Committed dispatch ownership: lease due work under a fence, settle
//! ack/nack outcomes, reclaim expired leases, and cancel.

use chrono::{DateTime, Duration, Utc};
use raft_runtime::FenceToken;

use super::{millis, pick_ready, DeferScheduler};
use crate::types::{
    AttemptSettlement, DispatchLease, NackOutcome, QueueControlState, SchedulerError,
    SchedulerResult, SettlementOutcome, TaskId, TaskStatus,
};

impl DeferScheduler {
    /// Commit a group of HTTP outcomes under one executor fence command.
    pub fn settle_batch(
        &mut self,
        queue: &str,
        executor_node: u64,
        attempts: Vec<AttemptSettlement>,
    ) -> SchedulerResult<Vec<SettlementOutcome>> {
        // Resolve the queue before applying any item so QueueMissing is atomic.
        if !self.queues.contains_key(queue) {
            return Err(SchedulerError::QueueMissing(queue.to_string()));
        }
        attempts
            .into_iter()
            .map(|attempt| {
                if attempt.success {
                    self.ack_on_node(
                        queue,
                        &attempt.attempt_id,
                        executor_node,
                        attempt.epoch,
                        attempt.completed_at,
                    )
                    .map(SettlementOutcome::Acked)
                } else {
                    self.nack_on_node(
                        queue,
                        &attempt.attempt_id,
                        executor_node,
                        attempt.epoch,
                        attempt.completed_at,
                    )
                    .map(SettlementOutcome::Nacked)
                }
            })
            .collect()
    }

    /// Return dispatch attempts for due tasks.
    ///
    /// Defer owns the consume rate: the caller can ask for many tasks, but this
    /// method caps the result by queue `max_dispatch_per_tick` and
    /// `max_in_flight`. ETA is evaluated before priority, then higher priority
    /// wins, and same-priority tasks use creation FIFO.
    pub fn lease_due(
        &mut self,
        queue: &str,
        now: DateTime<Utc>,
        requested: usize,
    ) -> SchedulerResult<Vec<DispatchLease>> {
        self.lease_due_on_node(queue, 0, now, requested)
    }

    /// Commit dispatch ownership for `executor_node` before any HTTP effect.
    pub fn lease_due_on_node(
        &mut self,
        queue: &str,
        executor_node: u64,
        now: DateTime<Utc>,
        requested: usize,
    ) -> SchedulerResult<Vec<DispatchLease>> {
        let state = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        if state.control_state != QueueControlState::Running {
            return Ok(Vec::new());
        }
        state.promote_due(now);
        state.refill_dispatch_tokens(now);
        let limit = requested
            .min(state.policy.max_dispatch_per_tick)
            .min(state.in_flight_capacity())
            .min(state.rate_capacity());
        let mut out = Vec::with_capacity(limit);
        for _ in 0..limit {
            let Some(task_id) = pick_ready(state) else {
                break;
            };
            let task = state
                .tasks
                .get_mut(&task_id)
                .expect("ready task must exist");
            task.attempts += 1;
            let attempt_id = format!("{}:{}:{}", task.queue, task_id, state.next_attempt_seq);
            state.next_attempt_seq += 1;
            let expires_at = now + Duration::milliseconds(state.policy.lease_ttl_ms as i64);
            let token = task
                .assignment
                .assign(executor_node, millis(now), millis(expires_at))
                .expect("scheduled task must not retain an active assignment");
            task.status = TaskStatus::Leased {
                attempt_id: attempt_id.clone(),
                executor_node,
                epoch: token.epoch,
                expires_at,
            };
            state.in_flight.insert(attempt_id.clone(), task_id.clone());
            out.push(DispatchLease {
                attempt_id,
                task_id: task_id.clone(),
                queue: task.queue.clone(),
                target: task.create.target.clone(),
                payload: task.create.payload.clone(),
                priority: task.create.priority,
                attempt: task.attempts,
                // Stable for the task's whole lifecycle, not only one lease.
                // If the target accepted a request but the executor died
                // before ack committed, the retry must carry the same key.
                idempotency_key: format!("{}/{}", task.queue, task_id),
                executor_node,
                epoch: token.epoch,
                leased_at: now,
                expires_at,
            });
        }
        state.consume_dispatch_tokens(out.len());
        Ok(out)
    }

    pub fn ack(&mut self, queue: &str, attempt_id: &str) -> SchedulerResult<bool> {
        let Some((executor_node, epoch, expires_at)) = self.lease_fence(queue, attempt_id)? else {
            return Ok(false);
        };
        self.ack_on_node(
            queue,
            attempt_id,
            executor_node,
            epoch,
            expires_at - Duration::milliseconds(1),
        )
    }

    pub fn ack_on_node(
        &mut self,
        queue: &str,
        attempt_id: &str,
        executor_node: u64,
        epoch: u64,
        now: DateTime<Utc>,
    ) -> SchedulerResult<bool> {
        let state = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        let Some(task_id) = state.in_flight.get(attempt_id).cloned() else {
            return Ok(false);
        };
        let Some(task) = state.tasks.get_mut(&task_id) else {
            return Err(SchedulerError::TaskMissing(task_id));
        };
        if matches!(&task.status, TaskStatus::Leased { attempt_id: live, .. } if live == attempt_id)
            && task
                .assignment
                .release(
                    FenceToken {
                        owner: executor_node,
                        epoch,
                    },
                    millis(now),
                )
                .is_ok()
        {
            state.in_flight.remove(attempt_id);
            task.status = TaskStatus::Succeeded;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn nack(
        &mut self,
        queue: &str,
        attempt_id: &str,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Option<NackOutcome>> {
        let Some((executor_node, epoch, _)) = self.lease_fence(queue, attempt_id)? else {
            return Ok(None);
        };
        self.nack_on_node(queue, attempt_id, executor_node, epoch, now)
    }

    pub fn nack_on_node(
        &mut self,
        queue: &str,
        attempt_id: &str,
        executor_node: u64,
        epoch: u64,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Option<NackOutcome>> {
        let state = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        let Some(task_id) = state.in_flight.get(attempt_id).cloned() else {
            return Ok(None);
        };
        let Some(task) = state.tasks.get_mut(&task_id) else {
            return Err(SchedulerError::TaskMissing(task_id));
        };
        if !matches!(&task.status, TaskStatus::Leased { attempt_id: live, .. } if live == attempt_id)
        {
            return Ok(None);
        }
        if task
            .assignment
            .release(
                FenceToken {
                    owner: executor_node,
                    epoch,
                },
                millis(now),
            )
            .is_err()
        {
            return Ok(None);
        }
        state.in_flight.remove(attempt_id);
        if task.attempts >= task.create.max_attempts {
            task.status = TaskStatus::DeadLettered;
            return Ok(Some(NackOutcome::DeadLettered));
        }
        let next_at = now + retry_backoff(state.policy.retry_backoff_ms, task.attempts);
        task.create.schedule_at = next_at;
        task.status = TaskStatus::Scheduled;
        state.push_due(&task_id);
        Ok(Some(NackOutcome::Retried { next_at }))
    }

    pub fn reclaim_expired(
        &mut self,
        queue: &str,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Vec<TaskId>> {
        let state = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        let expired: Vec<_> = state
            .tasks
            .iter()
            .filter_map(|(id, task)| match &task.status {
                TaskStatus::Leased {
                    attempt_id,
                    expires_at,
                    ..
                } if *expires_at <= now => Some((id.clone(), attempt_id.clone())),
                _ => None,
            })
            .collect();
        for (task_id, attempt_id) in &expired {
            let _ = state.in_flight.remove(attempt_id);
            let Some(task) = state.tasks.get_mut(task_id) else {
                continue;
            };
            if task.assignment.expire(millis(now)).is_err() {
                continue;
            }
            if task.attempts >= task.create.max_attempts {
                task.status = TaskStatus::DeadLettered;
            } else {
                task.create.schedule_at =
                    now + retry_backoff(state.policy.retry_backoff_ms, task.attempts);
                task.status = TaskStatus::Scheduled;
                state.push_due(task_id);
            }
        }
        Ok(expired.into_iter().map(|(task_id, _)| task_id).collect())
    }

    pub fn cancel(&mut self, queue: &str, task_id: &str) -> SchedulerResult<bool> {
        let state = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        let Some(task) = state.tasks.get_mut(task_id) else {
            return Ok(false);
        };
        match &task.status {
            TaskStatus::Succeeded | TaskStatus::DeadLettered | TaskStatus::Canceled => Ok(false),
            TaskStatus::Leased {
                attempt_id,
                executor_node,
                epoch,
                expires_at,
            } => {
                let _ = task.assignment.release(
                    FenceToken {
                        owner: *executor_node,
                        epoch: *epoch,
                    },
                    millis(*expires_at - Duration::milliseconds(1)),
                );
                state.in_flight.remove(attempt_id);
                task.status = TaskStatus::Canceled;
                Ok(true)
            }
            TaskStatus::Scheduled => {
                task.status = TaskStatus::Canceled;
                Ok(true)
            }
        }
    }

    fn lease_fence(
        &self,
        queue: &str,
        attempt_id: &str,
    ) -> SchedulerResult<Option<(u64, u64, DateTime<Utc>)>> {
        let state = self
            .queues
            .get(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        let Some(task_id) = state.in_flight.get(attempt_id) else {
            return Ok(None);
        };
        Ok(state.tasks.get(task_id).and_then(|task| match task.status {
            TaskStatus::Leased {
                executor_node,
                epoch,
                expires_at,
                ..
            } => Some((executor_node, epoch, expires_at)),
            _ => None,
        }))
    }
}

fn retry_backoff(base_ms: u64, delivered_attempt: u32) -> Duration {
    let shift = delivered_attempt.saturating_sub(1).min(16);
    Duration::milliseconds(base_ms.saturating_mul(1u64 << shift) as i64)
}
// HANDWRITE-END
