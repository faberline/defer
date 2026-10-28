// HANDWRITE-BEGIN gap="missing-generator:logic:defer-assembly" tracker="#766" reason="Assembly facade: re-exports the defer crates under the stable `defer::` paths the CLI, tests, and docs use."
//! Defer: Raft-backed delayed HTTP push queue.
//!
//! This crate only assembles the defer crates. The scheduler model lives in
//! `defer-shared-kernel`, Raft replication in `defer-replication`, the data
//! directory on disk in `defer-storage`, HTTP
//! delivery in `defer-dispatch`, access policy in `defer-access`, the HTTP API
//! in `defer-queue`, and the Kubernetes operator in `defer-operator`.

pub mod declared;

pub use defer_access as auth;
pub use defer_dispatch as dispatch;
#[cfg(feature = "operator")]
pub use defer_operator as operator;
pub use defer_queue::{metrics, openapi, server};
pub use defer_replication::{peer_tls, raft};
pub use defer_shared_kernel::{scheduler, types};
pub use defer_storage as storage;

pub use auth::AuthConfig;
pub use declared::{
    ensure_declared_queues, parse_declared_queues, DeclaredQueue, DeclaredQueueDecision,
    DeclaredQueueOutcome,
};
pub use dispatch::{DispatchDisposition, DispatchReport, HttpDispatcher, TargetSigningKey};
pub use raft::{DeferCommand, DeferOutcome, DeferRaft, DeferStateMachine};
pub use scheduler::DeferScheduler;
pub use types::{
    AttemptSettlement, CreateTask, DispatchLease, NackOutcome, QueueControlState, QueuePolicy,
    QueueSnapshot, SchedulerError, SchedulerResult, SettlementOutcome, Target, TaskStatus,
    DEFAULT_PRIORITY,
};
// HANDWRITE-END
