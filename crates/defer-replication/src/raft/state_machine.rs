// SPEC-MANAGED: tech-design/logic/core-scheduler-priority-rate-dispatch.md#logic
// HANDWRITE-BEGIN gap="missing-generator:logic:defer-raft-scheduler" tracker="#766" reason="Raft-backed delayed-task state machine with durable snapshots and fenced dispatch ownership."
//! The scheduler state machine every replica applies committed commands to.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use defer_shared_kernel::{DeferScheduler, QueueControlState};
use raft_runtime::{Index, OutcomeWindow, ProposalCache, RaftStateMachine};

use super::command::{DeferCommand, DeferEnvelope, DeferOutcome, ProposalId, SchedulerSnapshot};

pub struct DeferStateMachine {
    scheduler: Arc<Mutex<DeferScheduler>>,
    applied: AtomicU64,
    /// Skip entries at or below the applied floor. A hosted replica sets
    /// this so log replay after a snapshot restore is not applied twice.
    skip_replayed: bool,
    outcomes: Mutex<OutcomeWindow<DeferOutcome>>,
    completed: Mutex<ProposalCache<ProposalId, DeferOutcome>>,
}

impl DeferStateMachine {
    pub fn new(scheduler: Arc<Mutex<DeferScheduler>>, skip_replayed: bool) -> Arc<Self> {
        Arc::new(Self {
            scheduler,
            applied: AtomicU64::new(0),
            skip_replayed,
            outcomes: Mutex::new(OutcomeWindow::default()),
            completed: Mutex::new(ProposalCache::default()),
        })
    }

    pub fn scheduler(&self) -> Arc<Mutex<DeferScheduler>> {
        Arc::clone(&self.scheduler)
    }

    pub fn claim_outcome(&self, index: Index) -> Option<DeferOutcome> {
        self.outcomes.lock().expect("outcome window").claim(index)
    }

    fn apply_command(&self, command: DeferCommand) -> DeferOutcome {
        let mut scheduler = self.scheduler.lock().expect("scheduler mutex poisoned");
        match command {
            DeferCommand::ConfigureQueue { queue, policy } => {
                scheduler.configure_queue(&queue, policy);
                DeferOutcome::Queue(scheduler.queue_snapshot(&queue))
            }
            DeferCommand::UpdateQueuePolicy { queue, policy } => DeferOutcome::Queue(
                scheduler
                    .update_queue_policy(&queue, policy)
                    .and_then(|()| scheduler.queue_snapshot(&queue)),
            ),
            DeferCommand::SetQueueControl { queue, control } => {
                let changed = match control {
                    QueueControlState::Running => scheduler.resume_queue(&queue),
                    QueueControlState::Paused => scheduler.pause_queue(&queue),
                    QueueControlState::Disabled => scheduler.disable_queue(&queue),
                };
                DeferOutcome::Queue(changed.and_then(|()| scheduler.queue_snapshot(&queue)))
            }
            DeferCommand::CreateTask { queue, task } => {
                DeferOutcome::Created(scheduler.create_task(queue, task))
            }
            DeferCommand::CreateTasks { queue, tasks } => {
                let count = tasks.len();
                DeferOutcome::CreatedBatch(scheduler.create_tasks(queue, tasks).map(|()| count))
            }
            DeferCommand::LeaseDue {
                queue,
                executor_node,
                now,
                requested,
            } => DeferOutcome::Leased(scheduler.lease_due_on_node(
                &queue,
                executor_node,
                now,
                requested,
            )),
            DeferCommand::Ack {
                queue,
                attempt_id,
                executor_node,
                epoch,
                now,
            } => DeferOutcome::Acked(scheduler.ack_on_node(
                &queue,
                &attempt_id,
                executor_node,
                epoch,
                now,
            )),
            DeferCommand::Nack {
                queue,
                attempt_id,
                executor_node,
                epoch,
                now,
            } => DeferOutcome::Nacked(scheduler.nack_on_node(
                &queue,
                &attempt_id,
                executor_node,
                epoch,
                now,
            )),
            DeferCommand::SettleBatch {
                queue,
                executor_node,
                attempts,
            } => DeferOutcome::Settled(scheduler.settle_batch(&queue, executor_node, attempts)),
            DeferCommand::ReclaimExpired { queue, now } => {
                DeferOutcome::Reclaimed(scheduler.reclaim_expired(&queue, now))
            }
            DeferCommand::Cancel { queue, task_id } => {
                DeferOutcome::Canceled(scheduler.cancel(&queue, &task_id))
            }
        }
    }
}

impl RaftStateMachine for DeferStateMachine {
    fn apply(&self, index: Index, command: &[u8]) -> Result<()> {
        if index <= self.applied.load(Ordering::Acquire) && self.skip_replayed {
            return Ok(());
        }
        let envelope: DeferEnvelope = serde_json::from_slice(command)?;
        let cached = self
            .completed
            .lock()
            .expect("completed proposals")
            .get(&envelope.proposal_id);
        let outcome = cached.unwrap_or_else(|| self.apply_command(envelope.command));
        self.completed
            .lock()
            .expect("completed proposals")
            .insert(envelope.proposal_id, outcome.clone());
        let mut outcomes = self.outcomes.lock().expect("outcome window");
        outcomes.insert(index, outcome);
        outcomes.advance(index);
        drop(outcomes);
        self.applied.store(index, Ordering::Release);
        Ok(())
    }

    fn snapshot(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        let bytes = serde_json::to_vec(&SchedulerSnapshot {
            up_to: self.applied_index(),
            scheduler: self
                .scheduler
                .lock()
                .expect("scheduler mutex poisoned")
                .clone(),
            completed_proposals: self
                .completed
                .lock()
                .expect("completed proposals")
                .snapshot(),
        })?;
        writer.write_all(&bytes)?;
        Ok(())
    }

    fn restore(&self, reader: &mut dyn std::io::Read) -> Result<()> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        let snapshot: SchedulerSnapshot = serde_json::from_slice(&bytes)?;
        *self.scheduler.lock().expect("scheduler mutex poisoned") = snapshot.scheduler;
        self.completed
            .lock()
            .expect("completed proposals")
            .restore(snapshot.completed_proposals);
        self.applied.store(snapshot.up_to, Ordering::Release);
        Ok(())
    }

    fn applied_index(&self) -> Index {
        self.applied.load(Ordering::Acquire)
    }
}
// HANDWRITE-END
