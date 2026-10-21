# Domain

One section per part of defer's model. Crate boundaries and allowed
dependencies are in [architecture.md](../architecture.md); terms are in
[glossary.md](../glossary.md); the promises each area makes are in
[product/](../product/README.md).

## Scheduler (shared kernel)

`defer-shared-kernel` holds `DeferScheduler`, an in-memory, deterministic
model of queues and tasks. It does no I/O and reads no clock: every call takes
the time from the caller, so every replica that applies the same commands
reaches the same state.

- A task is eligible once `schedule_at` has passed. Among eligible tasks,
  higher priority goes first, then creation order.
- A queue's policy and control state decide how many leases it may hand out:
  `max_in_flight`, `max_dispatch_per_tick`, and a token bucket
  (`max_dispatches_per_second`, `max_burst_size`). The bucket and the
  in-flight count are scheduler state, so the limits hold across replicas.
- A lease names its executor and fence epoch (`scheduler/lease.rs`). A
  settlement for another executor or a stale epoch is refused.
- A failed attempt retries after `retry_backoff_ms` until `max_attempts`, then
  the task is dead-lettered.

## Replication

`defer-replication` makes every scheduler mutation a `DeferCommand` committed
through `raft-runtime`. `DeferStateMachine` applies committed commands on every
replica; `DeferRaft` hosts the node. A bootstrap seed restores a snapshot only
into an empty data directory. `peer_tls` reads the `DEFER_PEER_TLS_*`
material for the peer listener.

## Dispatch

`defer-dispatch` holds `HttpDispatcher`. It sends a task's target request only
after the lease is committed, with a stable idempotency key across retries,
and optionally signs it with HMAC-SHA256 (`TargetSigningKey`). It returns a
`DispatchReport`; the caller commits the outcome as a settlement.

## Access

`defer-access` adapts `service-auth`. `AuthConfig` selects the token mode;
`authorize` checks a bearer token's grant on one queue: write for queue and
task writes, read for reads, admin for dispatch and backup.

## Queue (HTTP API)

`defer-queue` serves the API under `/v1/queues/{queue}`: queue get and put,
`control`, task create, `tasks:batch`, task read and cancel, and `dispatch`;
plus `/admin/backup`. `openapi` builds the published document from the same
handlers. `metrics` holds the metric names.

## Operator

`defer-operator` defines the `Defer` custom resource (`crd`), renders its
children (`render`), and reconciles them (`reconcile`). It is linked only with
the `operator` feature.
