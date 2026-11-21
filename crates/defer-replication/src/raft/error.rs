//! [`ProposeError`]: why a proposed command did not produce its outcome.

use defer_shared_kernel::{QueueImportError, SchedulerError};

/// Why `DeferRaft::import_queue` did not import.
#[derive(Debug, thiserror::Error)]
pub enum ImportQueueError {
    /// The command committed and the target already has a queue of that name.
    #[error(transparent)]
    Refused(#[from] QueueImportError),
    /// Proposal, commit, or outcome retrieval failed.
    #[error(transparent)]
    Infra(#[from] anyhow::Error),
}

/// A proposal either committed and the scheduler refused it, or it never
/// produced a committed outcome. Callers map the first to the domain's typed
/// status and the second to an internal failure.
#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    /// The command committed and the scheduler rejected it.
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    /// Proposal, commit, or outcome retrieval failed.
    #[error(transparent)]
    Infra(#[from] anyhow::Error),
}

pub type ProposeResult<T> = std::result::Result<T, ProposeError>;
