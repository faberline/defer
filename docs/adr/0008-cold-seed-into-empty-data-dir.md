# ADR 0008: A bootstrap seed restores only into an empty data directory

- **Status:** accepted
- **Date:** 2026-10-11

## Context

`GET /admin/backup` streams the state-machine snapshot, and `defer backup`
ships it to a destination. Restoring it has to be safe to run from a
StatefulSet init path on every start, which means it must be impossible for
a seed to overwrite a node that already has a log.

## Decisions

- **The seed is a serve flag.** `--bootstrap-seed-uri` names the snapshot;
  `prepare_bootstrap_seed` runs before the Raft host starts.
- **A non-empty data directory refuses the seed.** The function checks the
  directory and fails the start rather than merging or replacing. A node
  that has ever applied a log is never seeded.
- **The seed is written as a Raft store with fsync-always.** The snapshot
  is installed into a fresh `RaftStore` under `FsyncPolicy::Always`, so the
  node starts from a durable floor and serves the snapshot's task state
  without re-applying any log.
- **The seed is read once.** Nothing re-reads the URI after start; a changed
  seed has no effect on a running node.

## Consequences

- Recovering a group from backup is: empty the directories, set the seed
  URI, start. The `http_api` gate proves the restored node serves the
  snapshot's tasks.
- A seed cannot repair a corrupt member in place; that member is emptied and
  rejoins by snapshot from the group, or is seeded.
- The applied-marker files ADR 0001 removed are not needed: the seed is the
  store, not a side file next to it.
