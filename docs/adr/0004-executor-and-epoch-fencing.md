# ADR 0004: Every settlement carries the executor and the fence epoch

- **Status:** accepted
- **Date:** 2026-10-11

## Context

ADR 0002 makes the lease a committed record, but a committed lease still
expires. The node that held it may be slow rather than dead: it can finish
its HTTP request after the group reclaimed the lease and handed the task to
another node. Without a fence, its late ack would mark a task `Succeeded`
that the new owner is about to send again, or its late nack would reschedule
a task the new owner already completed.

## Decisions

- **A lease names its owner.** The applied `LeaseDue` records the executor
  node id and a fence epoch on the task's `Leased` status, and the dispatcher
  carries both as a `FencedAssignment`.
- **Ack and nack present the fence.** `ack` and `nack` take a `FenceToken`
  of owner and epoch. A token whose owner or epoch does not match the
  current lease is refused: the scheduler reports no change rather than an
  error, and the dispatcher maps that to `DispatchDisposition::LostOwnership`.
- **A lost fence is counted, not retried by the loser.** The losing node
  increments `defer_dispatch_lost_ownership_total` and stops; the task is
  already with the owner that holds the new epoch, and that owner's retry
  carries the same idempotency key.
- **The fence travels to the target.** Every request carries
  `x-defer-fence-epoch`, so a receiver that wants to can refuse a request
  from an epoch it has already seen superseded.

## Consequences

- A `LostOwnership` result means the target may have received the request.
  It is the signal to watch in production; the observability component ships
  an alert on it.
- The fence types come from the shared `raft-runtime` crate so that every
  replicated service in the family presents the same token to its state
  machine. ADR 0001 records that as a shared-kernel exception.
