// HANDWRITE-BEGIN gap="missing-generator:logic:defer-storage" tracker="#766" reason="The defer data directory on disk: the Raft log and the durable scheduler snapshots."
//! Defer storage: the data directory on disk.
//!
//! A replica keeps all of its state in one data directory (the PVC). The Raft
//! log and the durable scheduler snapshots are in `<data_dir>/raft`, and every
//! write is fsynced before it is acknowledged. Replication opens the store
//! through this crate; nothing else touches the files.

mod data_dir;

pub use data_dir::{ensure_empty_data_dir, open_raft_store, raft_dir, RAFT_DIR};
// HANDWRITE-END
