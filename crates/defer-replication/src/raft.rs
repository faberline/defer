// SPEC-MANAGED: tech-design/logic/core-scheduler-priority-rate-dispatch.md#logic
// HANDWRITE-BEGIN gap="missing-generator:logic:defer-raft-scheduler" tracker="#766" reason="Raft-backed delayed-task state machine with durable snapshots and fenced dispatch ownership."
//! Raft-backed authoritative state for Defer.
//!
//! Every scheduler mutation is a committed command. In particular, an
//! executor may call an HTTP target only after [`DeferCommand::LeaseDue`] has
//! committed the executor node and fence epoch. Ack/nack therefore reject a
//! stale replica or an expired attempt. External HTTP effects remain
//! at-least-once: a crash after the target accepts but before ack commits can
//! cause a retry, while the stable attempt idempotency key lets a cooperating
//! target collapse that ambiguity.

mod bootstrap;
mod command;
mod host;
mod state_machine;
#[cfg(test)]
mod tests;

pub use bootstrap::prepare_bootstrap_seed;
pub use command::{DeferCommand, DeferOutcome};
pub use host::DeferRaft;
pub use state_machine::DeferStateMachine;

pub const SNAPSHOT_EVERY: u64 = 1024;
// HANDWRITE-END
