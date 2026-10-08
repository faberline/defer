// SPEC-MANAGED: tech-design/logic/core-scheduler-priority-rate-dispatch.md#logic
// HANDWRITE-BEGIN gap="missing-generator:logic:defer-raft-scheduler" tracker="#766" reason="Raft-backed delayed-task state machine with durable snapshots and fenced dispatch ownership."
//! Cold-start seeding of an empty data directory from an admin backup.

use std::path::Path;

use anyhow::{Context, Result};
use raft_runtime::{FsyncPolicy as RaftFsyncPolicy, NodeId, RaftStore};

use super::command::SchedulerSnapshot;

/// Seed an empty PVC with the exact state-machine snapshot served by the
/// admin backup endpoint. This is cold-start only and refuses replacement of
/// any existing state.
pub fn prepare_bootstrap_seed(data_dir: &Path, node_id: NodeId, bytes: &[u8]) -> Result<()> {
    let snapshot: SchedulerSnapshot =
        serde_json::from_slice(bytes).context("decode Defer scheduler snapshot")?;
    if data_dir.exists() {
        let mut entries = std::fs::read_dir(data_dir)
            .with_context(|| format!("read bootstrap data dir {}", data_dir.display()))?;
        anyhow::ensure!(
            entries.next().transpose()?.is_none(),
            "bootstrap seed requires an empty data directory {}",
            data_dir.display()
        );
    } else {
        std::fs::create_dir_all(data_dir)?;
    }
    let raft_dir = data_dir.join("raft");
    let store = RaftStore::open(
        raft_dir
            .to_str()
            .context("raft data dir is not valid UTF-8")?,
        node_id,
        RaftFsyncPolicy::Always,
    )?;
    store.seed_snapshot(snapshot.up_to, 0, bytes.to_vec())?;
    Ok(())
}
// HANDWRITE-END
