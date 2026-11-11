# Upgrades and version compatibility

This page states what defer promises across versions, and what it does not.
Each statement names the code or test behind it. A statement with no backing
is written as "not promised".

## HTTP API

- The HTTP contract is the OpenAPI document `clients/openapi.json`, a committed
  snapshot of `defer spec --format openapi` and of `GET /openapi.json`. See
  [clients/README.md](../../clients/README.md). Backed by
  `committed_snapshot_matches_live_document` in
  `crates/defer-queue/src/openapi.rs`, which fails on any route or schema
  drift, and by `offline_spec_and_typed_client_generation_use_one_contract` in
  `crates/defer/tests/it/cli_contract.rs`.
- Not promised: that a change keeps old clients working. The snapshot test
  catches a change; it does not classify it. `info.version` is the fixed
  string `snapshot`, so the file carries no API version. defer has no API
  version and no deprecation policy; read the diff of `clients/openapi.json`
  between releases.

## Snapshot and Raft log format

- Every committed command is a JSON `DeferEnvelope { proposal_id, command }`,
  defined in `crates/defer-replication/src/raft/command.rs`. The
  `RaftStateMachine` implementation for `DeferStateMachine` applies it in
  `apply`, in `crates/defer-replication/src/raft/state_machine.rs`.
- A snapshot is the JSON struct `SchedulerSnapshot { up_to, scheduler,
  completed_proposals }`, defined in `crates/defer-replication/src/raft/command.rs`.
  `snapshot` writes it and `restore` reads it, both in
  `crates/defer-replication/src/raft/state_machine.rs`.
- Neither format has a version field. The one compatibility device in code is
  `#[serde(default)]` on newer fields, such as `completed_proposals` in
  `SchedulerSnapshot`.
- An unknown command variant (`DeferCommand` uses `#[serde(tag = "op")]`) or a
  malformed snapshot makes `apply` or `restore` return an error. So an older
  node can fail on a log entry that a newer leader committed.
- Not promised:
  - a newer binary reading an older log or snapshot;
  - an older binary reading a log or snapshot written by a newer one;
  - two versions in one group at the same time.

  No test in the repository proves any of these. The rolling upgrade test is
  issue #110 and is not merged.

## Rolling upgrade order

- The operator renders the StatefulSet with `RollingUpdate` and a
  PodDisruptionBudget with `maxUnavailable: 1`, in
  `crates/defer-operator/src/render.rs`. The PDB is asserted in
  `production_render_composes_shared_stateful_primitives` in
  `crates/defer/tests/operator.rs`. The readiness probe is `/readyz`, every
  5 seconds, asserted in `crates/defer/tests/it/e2e/operator_recovery.rs`.
- With `RollingUpdate`, Kubernetes replaces pods one at a time and waits for
  each readiness probe. This is Kubernetes behavior. defer has no test of the
  order, so the order is not promised.
- Steps for a version change:
  1. Read the `clients/openapi.json` diff and the release notes.
  2. Take a backup with `defer backup <dest>` or `GET /admin/backup` (see
     [install.md](install.md)).
  3. Change `spec.image` on the `Defer` resource.
  4. Watch `kubectl get defer` until `status.phase` is `Ready` and
     `readyReplicas` equals `desiredReplicas` (`crates/defer-operator/src/reconcile.rs`).
     Start no other change before that.
  5. Check `GET /healthz` and one queue read.
- Not promised: that the group stays writable while two versions run. A
  mixed-version group exists for the length of the rollout; see the snapshot
  and log section.
- The direct-install base (`k8s/base`) is a singleton with `maxUnavailable: 0`,
  asserted in `crates/defer/tests/it/direct_k8s_assets.rs`. An upgrade there
  restarts the one pod and is a short outage.

## Downgrade

- Not promised: a downgrade. Before one, take a backup with `defer backup`.
- To restore, start an empty data directory with `DEFER_BOOTSTRAP_SEED_URI`
  (listed in [README.md](README.md)). The seed is read once, and only into an
  empty directory ([ADR 0008](../adr/0008-cold-seed-into-empty-data-dir.md)).
- The snapshot format has no version. A snapshot written by a newer binary may
  fail to restore on an older one. Restore it only into a binary you have
  tested.
