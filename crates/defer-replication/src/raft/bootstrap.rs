// HANDWRITE-BEGIN gap="missing-generator:logic:defer-raft-scheduler" tracker="#766" reason="Raft-backed delayed-task state machine with durable snapshots and fenced dispatch ownership."
//! Cold-start seeding of an empty data directory from an admin backup.

use std::path::Path;

use anyhow::{Context, Result};
use raft_runtime::NodeId;

use super::command::SchedulerSnapshot;

/// Seed an empty PVC with the exact state-machine snapshot served by the
/// admin backup endpoint. This is cold-start only and refuses replacement of
/// any existing state.
pub fn prepare_bootstrap_seed(data_dir: &Path, node_id: NodeId, bytes: &[u8]) -> Result<()> {
    let snapshot: SchedulerSnapshot =
        serde_json::from_slice(bytes).context("decode Defer scheduler snapshot")?;
    defer_storage::ensure_empty_data_dir(data_dir)?;
    let store = defer_storage::open_raft_store(&defer_storage::raft_dir(data_dir), node_id)?;
    store.seed_snapshot(snapshot.up_to, 0, bytes.to_vec())?;
    Ok(())
}
// HANDWRITE-END
