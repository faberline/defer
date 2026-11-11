# Replication and availability

Every scheduler mutation is a committed Raft command, and every replica
applies the same scheduler bytes. This area is the README capabilities
`replicated-availability` and `backup-and-seed`, and it owns the three
outcomes that need state the shared Raft runtime and the custom resource do
not carry yet.

## Replicated group with peer mTLS

- Problem: none open as shipped.
- Who: operators running more than one replica; targets that must not see
  two owners of one attempt.
- Promise: queue configuration, control, task creation, leases, settlements,
  reclaims, and cancels are replicated commands; a three-voter group
  converges on all of them, survives a leader loss, and restarts a member
  from its snapshot without re-applying the log; every lease records the
  executor node and fence epoch, and a settlement from another owner or a
  stale epoch is refused; with `DEFER_PEER_*` material the Raft listener runs
  on `--raft-port` under mutual TLS, an untrusted peer is rejected, and the
  public router refuses the Raft routes.
- Limits today: membership is static from `DEFER_PEERS`, so a voter change
  is a restart; certificate material is file-based without automatic
  rotation; without material the Raft routes share the plaintext public
  port.
- Non-goals: live membership change; that is Live replica membership
  below.
- Neighbours: none; first section of the area. The fencing half is consumed
  by [dispatch-and-delivery.md](dispatch-and-delivery.md) § Committed HTTP
  dispatch.
- Status rows: `raft-replication`, `raft-peer-mtls`.

## Snapshot backup and cold seed

- Problem: none open as shipped.
- Who: operators restoring an instance; the backup CronJob.
- Promise: `GET /admin/backup` streams the consistent state-machine snapshot
  and needs the admin role when auth is required; `defer backup --dest <uri>`
  ships it to `file://`, `s3://` (feature `backup`), or `gs://` with
  retention pruning; `--bootstrap-seed-uri` restores a snapshot into an
  empty data directory before the Raft host starts, and the restored node
  serves the snapshot's task state.
- Limits today: a non-empty data directory refuses the seed; the seed is read
  once at start; no gate in this repository writes to S3 or GCS.
- Non-goals: incremental or point-in-time backup.
- Neighbours: extends Replicated group with peer mTLS; the CronJob is
  rendered in [operations.md](operations.md) § Kubernetes operator and direct
  install.
- Status rows: `admin-backup`, `cold-seed-bootstrap`.

## Live replica membership

- Problem: a replica-count change on the custom resource restarts members
  instead of adding a voter live; no run proves the change under load.
- Who: operators scaling an instance without a maintenance window.
- Promise: a replica-count change adds a learner and promotes it once caught
  up, or transfers leadership and removes a voter, without restarting the
  surviving members, while clients keep creating and settling tasks, with no
  committed task lost and no lease settled twice.
- Non-goals: shard count; a membership-change engine of defer's own. Core's
  `membership-controller` drives the change, and defer adopts it when it
  moves onto core's engine (core ADR 0002, phase 6).
- Decided: until defer moves onto core's engine, the replica-change driver runs in the operator (reconcile step), one whole replica layer per generation. The pure plan is `defer_operator::plan_replica_change`. Wiring it to a live membership actuator is [#84](https://github.com/faberline/defer/issues/84) and follows the answer in faberline/core#181.
- Neighbours: extends Replicated group with peer mTLS; the evidence is a
  cluster run from [operations.md](operations.md) § Kubernetes operator and
  direct install.
- Outcome: `live-replica-membership`. Tracking: not assigned.

## Live topology transitions

- Problem: the operator pins one shard, so one group carries every queue;
  no run proves a shard split under load.
- Who: operators running more queues or task throughput than one group
  holds.
- Promise: a `Defer` instance runs more than one shard and splits a shard
  while clients keep creating and settling tasks, with no committed task
  lost and no lease settled twice; each queue lives on one shard with its
  permits; task identity and idempotency keys do not change shape when a
  queue moves between shards. Core decides when a shard splits and forwards
  each request; defer's hooks classify requests by queue and export and
  import a bucket's tasks. Shards never appear in the public API.
- Non-goals: merging shards back into fewer; a split or routing mechanism of
  defer's own; a queue-to-shard map in the custom resource.
- Open: whether a queue that outgrows one shard needs a placement key finer
  than the queue, which would split its permits across shards.
- Decided: shard split is [#83](https://github.com/faberline/defer/issues/83). The `shardCount: 1` pin stays until it lands.
- Neighbours: extends Live replica membership; the evidence is a cluster run
  from [operations.md](operations.md) § Kubernetes operator and direct
  install.
- Outcome: `live-topology-transitions`. Tracking: not assigned.

## Capacity policy

- Problem: the custom resource carries raw `resources` and `storage` values;
  nothing derives sizing from a stated policy.
- Who: operators sizing replicas from a workload description.
- Promise: a `Defer` custom resource states CPU, memory, and disk capacity
  policy per replica, and the operator sizes requests, limits, and the
  volume claim from it; volume growth follows the storage class's expansion
  support and is never destructive. Resources and storage are core's spec
  blocks, so core renders the sizing and grows the volume.
- Non-goals: scheduling across shards or moving queues; sizing code of
  defer's own.
- Open: the policy vocabulary (task rate and payload size, or explicit
  tiers), settled in core's resources and storage blocks rather than in a
  defer field.
- Neighbours: depends on Live topology transitions for the multi-shard case.
- Outcome: `capacity-policy`. Tracking: not assigned.

## Non-goals in this area

- `cloud-tasks-wire-compatibility`: defer replicates its own command set
  behind its own API; nothing here serves the Google wire shape.
