# Upgrades and version compatibility

This page states what defer promises across versions, and what it does not.
Each statement names the code or test behind it. A statement with no backing
is written as "not promised".
The policy for 0.x is in [compatibility.md](compatibility.md).

## HTTP API

- The HTTP contract is the OpenAPI document `clients/openapi.json`, a committed
  snapshot of `defer spec --format openapi` and of `GET /openapi.json`. See
  [clients/README.md](../../clients/README.md). Backed by
  `committed_snapshot_matches_live_document` in
  `crates/defer-queue/src/openapi.rs`, which fails on any route or schema
  drift, and by `offline_spec_and_typed_client_generation_use_one_contract` in
  `crates/defer/tests/it/cli_contract.rs`.
- The snapshot test catches a change; it does not classify it. `info.version` is the fixed string `snapshot`. What stays compatible inside 0.x, and how a break is announced, is in [compatibility.md](compatibility.md).

## Snapshot and Raft log format

- Every committed command is a JSON `DeferEnvelope { proposal_id, command }`,
  defined in `crates/defer-replication/src/raft/command.rs`. The
  `RaftStateMachine` implementation for `DeferStateMachine` applies it in
  `apply`, in `crates/defer-replication/src/raft/state_machine.rs`.
- A snapshot is the JSON struct `SchedulerSnapshot { up_to, scheduler,
  completed_proposals }`, defined in `crates/defer-replication/src/raft/command.rs`.
  `snapshot` writes it and `restore` reads it, both in
  `crates/defer-replication/src/raft/state_machine.rs`.
- Both formats carry `format_version` (current 1; absent reads as 0). A binary refuses a higher version; see `snapshot_from_a_newer_version_is_refused` and `log_entry_from_a_newer_version_is_refused` in `crates/defer-replication/src/raft/tests.rs`. `#[serde(default)]` on newer fields, such as `completed_proposals`, stays the device for adding fields.
- An unknown command variant (`DeferCommand` uses `#[serde(tag = "op")]`) or a
  malformed snapshot makes `apply` or `restore` return an error. So an older
  node can fail on a log entry that a newer leader committed.
- Not promised:
  - an older binary reading a log or snapshot written by a newer one;
  - two versions in one group at the same time.

  No test proves these two. The rolling-upgrade suite exists (crates/defer/tests/it/e2e/rolling_upgrade.rs) and no paid run has happened.

## Rolling upgrade order

- The operator renders the StatefulSet with `RollingUpdate` and a
  PodDisruptionBudget with `maxUnavailable: 1`, in
  `crates/defer-operator/src/render.rs`. The PDB is asserted in
  `production_render_composes_shared_stateful_primitives` in
  `crates/defer/tests/it/operator.rs`. The readiness probe is `/readyz`, every
  5 seconds, asserted in `crates/defer/tests/it/e2e/operator_recovery.rs`.
- With `RollingUpdate`, Kubernetes replaces pods one at a time and waits for
  each readiness probe. This is Kubernetes behavior. defer has no test of the
  order, so the order is not promised.
- Steps for a version change:
  1. Read the `clients/openapi.json` diff and the release notes.
  2. Take a backup with `defer backup --dest <uri>` or `GET /admin/backup` (see
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

## Rolling upgrade drill

- What it does: starts a 3-replica group on the previous image, writes one queue and 200 tasks, changes `spec.image` to the image under test, and checks that at most one replica is unavailable and that the queue policy and every task are unchanged after the roll. It checks nothing about writes during the roll; that is not promised (see above).
- How to dispatch (manual, paid; a person or the desk approves the run): `gh workflow run gke-test.yml -f suite=rolling_upgrade -f previous_image=ghcr.io/faberline/defer@sha256:<64 hex>`. The digest is the root digest of the previous release image, read with `docker buildx imagetools inspect ghcr.io/faberline/defer:<version>` (field `Digest`).
- The image under test is built by `defer-test-image.yml` from the dispatched `main` commit (tag `sha-<git12>`, pinned by its root digest). The suite is not part of `suite=all`.
- The code behind it: `crates/defer/tests/it/e2e/rolling_upgrade.rs`, `k8s/gke-test-job/rolling_upgrade/`, `.github/workflows/gke-test.yml`.

## Downgrade

- Not promised: a downgrade. Before one, take a backup with `defer backup`.
- To restore, start an empty data directory with `DEFER_BOOTSTRAP_SEED_URI`
  (listed in [README.md](README.md)). The seed is read once, and only into an
  empty directory ([ADR 0008](../adr/0008-cold-seed-into-empty-data-dir.md)).
- A snapshot written by a newer binary carries a `format_version`. An older binary that reads a higher version refuses to restore it, with an error that names both versions. Restore it only into a binary that you have tested.
