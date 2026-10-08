// SPEC-MANAGED: tech-design/logic/core-scheduler-priority-rate-dispatch.md#logic
// HANDWRITE-BEGIN gap="missing-generator:logic:defer-raft-scheduler" tracker="#766" reason="Raft-backed delayed-task state machine with durable snapshots and fenced dispatch ownership."
//! The replicated command log: commands, their outcomes, the proposal
//! envelope, and the state-machine snapshot body.

use chrono::{DateTime, Utc};
use defer_shared_kernel::{
    AttemptSettlement, CreateTask, DeferScheduler, DispatchLease, NackOutcome, QueueControlState,
    QueuePolicy, QueueSnapshot, SchedulerError, SettlementOutcome,
};
use raft_runtime::{Index, NodeId};
use serde::{Deserialize, Serialize};

/// Every authoritative scheduler transition. All clocks and executor ids are
/// resolved by the proposer so replicas apply identical bytes and state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DeferCommand {
    ConfigureQueue {
        queue: String,
        policy: QueuePolicy,
    },
    UpdateQueuePolicy {
        queue: String,
        policy: QueuePolicy,
    },
    SetQueueControl {
        queue: String,
        control: QueueControlState,
    },
    CreateTask {
        queue: String,
        task: CreateTask,
    },
    CreateTasks {
        queue: String,
        tasks: Vec<CreateTask>,
    },
    LeaseDue {
        queue: String,
        executor_node: NodeId,
        now: DateTime<Utc>,
        requested: usize,
    },
    Ack {
        queue: String,
        attempt_id: String,
        executor_node: NodeId,
        epoch: u64,
        now: DateTime<Utc>,
    },
    Nack {
        queue: String,
        attempt_id: String,
        executor_node: NodeId,
        epoch: u64,
        now: DateTime<Utc>,
    },
    SettleBatch {
        queue: String,
        executor_node: NodeId,
        attempts: Vec<AttemptSettlement>,
    },
    ReclaimExpired {
        queue: String,
        now: DateTime<Utc>,
    },
    Cancel {
        queue: String,
        task_id: String,
    },
}

/// Read-your-write outcome retained by proposal id across snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeferOutcome {
    Queue(Result<QueueSnapshot, SchedulerError>),
    Created(Result<(), SchedulerError>),
    CreatedBatch(Result<usize, SchedulerError>),
    Leased(Result<Vec<DispatchLease>, SchedulerError>),
    Acked(Result<bool, SchedulerError>),
    Nacked(Result<Option<NackOutcome>, SchedulerError>),
    Settled(Result<Vec<SettlementOutcome>, SchedulerError>),
    Reclaimed(Result<Vec<String>, SchedulerError>),
    Canceled(Result<bool, SchedulerError>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(super) struct ProposalId {
    pub(super) node: NodeId,
    pub(super) session: u64,
    pub(super) sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DeferEnvelope {
    pub(super) proposal_id: ProposalId,
    pub(super) command: DeferCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SchedulerSnapshot {
    pub(super) up_to: Index,
    pub(super) scheduler: DeferScheduler,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) completed_proposals: Vec<(ProposalId, DeferOutcome)>,
}
// HANDWRITE-END
