# Replication and availability

Every scheduler mutation is a committed Raft command, and every replica
applies the same scheduler bytes. This area is the README capabilities
`replicated-availability` and `backup-and-seed`, and it owns the two
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
- Non-goals: live membership change; that is Live topology transitions
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

## Live topology transitions

- Problem: the operator pins one shard, and a replica-count change restarts
  members; no run proves a transition under load.
- Who: operators scaling an instance without a maintenance window.
- Promise: a `Defer` instance moves from one shard with one replica to three
  shards with three replicas and back while clients keep creating and
  settling tasks, with no committed task lost and no lease settled twice;
  task identity and idempotency keys do not change shape when a queue moves
  between shards.
- Non-goals: a membership-change engine of defer's own; the renderer lifts
  the `shardCount: 1` pin only when the shared Raft runtime offers a live
  membership actuator defer can call.
- Open: whether the transition is driven by the operator alone or needs a
  queue-to-shard placement map in the custom resource.
- Neighbours: extends Replicated group with peer mTLS; the evidence is a
  cluster run from [operations.md](operations.md) § Kubernetes operator and
  direct install.
- Outcome: `live-topology-transitions`. Tracking: not assigned.

## Capacity policy

- Problem: the custom resource carries raw `resources` and `storage` values;
  nothing derives sizing from a stated policy.
- Who: operators sizing replicas from a workload description.
- Promise: a `Defer` custom resource states CPU, memory, and disk capacity
  policy per replica, and the operator sizes requests, limits, and the
  volume claim from it; volume growth follows the storage class's expansion
  support and is never destructive.
- Non-goals: scheduling across shards or moving queues.
- Open: the policy vocabulary (task rate and payload size, or explicit
  tiers).
- Neighbours: depends on Live topology transitions for the multi-shard case.
- Outcome: `capacity-policy`. Tracking: not assigned.

## Non-goals in this area

- `cloud-tasks-wire-compatibility`: defer replicates its own command set
  behind its own API; nothing here serves the Google wire shape.
