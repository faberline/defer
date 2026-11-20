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
    PurgeQueue {
        queue: String,
    },
    PruneTerminal {
        queue: String,
        now: DateTime<Utc>,
    },
    DeleteQueue {
        queue: String,
    },
}

/// Read-your-write outcome retained by proposal id across snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeferOutcome {
    Queue(Result<QueueSnapshot, SchedulerError>),
    QueueDeleted(Result<(), SchedulerError>),
    Created(Result<(), SchedulerError>),
    CreatedBatch(Result<usize, SchedulerError>),
    Leased(Result<Vec<DispatchLease>, SchedulerError>),
    Acked(Result<bool, SchedulerError>),
    Nacked(Result<Option<NackOutcome>, SchedulerError>),
    Settled(Result<Vec<SettlementOutcome>, SchedulerError>),
    Reclaimed(Result<Vec<String>, SchedulerError>),
    Canceled(Result<bool, SchedulerError>),
    QueuePurged(Result<usize, SchedulerError>),
    Pruned(Result<usize, SchedulerError>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(super) struct ProposalId {
    pub(super) node: NodeId,
    pub(super) session: u64,
    pub(super) sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DeferEnvelope {
    #[serde(default)]
    pub(super) format_version: u32,
    pub(super) proposal_id: ProposalId,
    pub(super) command: DeferCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SchedulerSnapshot {
    #[serde(default)]
    pub(super) format_version: u32,
    pub(super) up_to: Index,
    pub(super) scheduler: DeferScheduler,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) completed_proposals: Vec<(ProposalId, DeferOutcome)>,
}

/// Version of the JSON shape of `DeferEnvelope` (raft log entries) and
/// `SchedulerSnapshot`. 0 means data written before the field existed; its
/// shape equals version 1. Raise it only when a new binary can no longer read
/// what an older one wrote, or the reverse, and say so in
/// docs/operations/compatibility.md.
pub(super) const FORMAT_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub(super) enum FormatError {
    #[error("malformed {what}: {source}")]
    Malformed {
        what: &'static str,
        source: serde_json::Error,
    },
    #[error(
        "{what} has format_version {found}, but this binary reads up to {}; run a newer defer",
        FORMAT_VERSION
    )]
    Newer { what: &'static str, found: u32 },
}

fn check_format_version(what: &'static str, found: u32) -> Result<(), FormatError> {
    if found > FORMAT_VERSION {
        Err(FormatError::Newer { what, found })
    } else {
        Ok(())
    }
}

impl SchedulerSnapshot {
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let snapshot: Self =
            serde_json::from_slice(bytes).map_err(|source| FormatError::Malformed {
                what: "snapshot",
                source,
            })?;
        check_format_version("snapshot", snapshot.format_version)?;
        Ok(snapshot)
    }
}

impl DeferEnvelope {
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let envelope: Self =
            serde_json::from_slice(bytes).map_err(|source| FormatError::Malformed {
                what: "log entry",
                source,
            })?;
        check_format_version("log entry", envelope.format_version)?;
        Ok(envelope)
    }
}
// HANDWRITE-END
