// HANDWRITE-BEGIN gap="missing-generator:logic:defer-storage" tracker="#766" reason="The defer data directory on disk: the Raft log and the durable scheduler snapshots."
//! The data directory: where each piece of state lives and how it is opened.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use raft_runtime::{FsyncPolicy, NodeId, RaftStore};

/// The folder inside the data directory that holds the Raft log and snapshots.
pub const RAFT_DIR: &str = "raft";

/// The Raft store folder of `data_dir`.
pub fn raft_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(RAFT_DIR)
}

/// Open the Raft store in `raft_dir`, creating the folder when it is missing.
/// Every write is fsynced.
pub fn open_raft_store(raft_dir: &Path, node_id: NodeId) -> Result<RaftStore> {
    std::fs::create_dir_all(raft_dir)
        .with_context(|| format!("create raft data dir {}", raft_dir.display()))?;
    let path = raft_dir
        .to_str()
        .context("raft data dir is not valid UTF-8")?;
    Ok(RaftStore::open(path, node_id, FsyncPolicy::Always)?)
}

/// Make sure `data_dir` holds no state, creating it when it is missing. A
/// cold-start seed calls this first, so it never replaces existing state.
pub fn ensure_empty_data_dir(data_dir: &Path) -> Result<()> {
    if !data_dir.exists() {
        return std::fs::create_dir_all(data_dir)
            .with_context(|| format!("create data dir {}", data_dir.display()));
    }
    let mut entries = std::fs::read_dir(data_dir)
        .with_context(|| format!("read bootstrap data dir {}", data_dir.display()))?;
    anyhow::ensure!(
        entries.next().transpose()?.is_none(),
        "bootstrap seed requires an empty data directory {}",
        data_dir.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_dir_is_inside_the_data_dir() {
        assert_eq!(raft_dir(Path::new("/data")), PathBuf::from("/data/raft"));
    }

    #[test]
    fn empty_data_dir_is_created_and_accepted() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("new");
        ensure_empty_data_dir(&data).unwrap();
        assert!(data.is_dir());
        ensure_empty_data_dir(&data).unwrap();
    }

    #[test]
    fn data_dir_with_state_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(raft_dir(tmp.path())).unwrap();
        let err = ensure_empty_data_dir(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("empty data directory"), "{err}");
    }

    #[test]
    fn open_creates_the_raft_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = raft_dir(tmp.path());
        open_raft_store(&dir, 0).unwrap();
        assert!(dir.is_dir());
    }
}
// HANDWRITE-END
