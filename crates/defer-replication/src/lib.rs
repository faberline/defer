// HANDWRITE-BEGIN gap="missing-generator:logic:defer-replication" tracker="#766" reason="Raft replication of the defer scheduler and the DEFER_PEER peer-TLS adapter."
//! Raft replication for Defer: every scheduler mutation is a committed
//! [`DeferCommand`] applied by [`DeferStateMachine`] on every replica.

pub mod peer_tls;
pub mod raft;

pub use raft::{DeferCommand, DeferOutcome, DeferRaft, DeferStateMachine};
// HANDWRITE-END
