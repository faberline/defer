# ADR 0002: Commit the lease before the side effect

- **Status:** accepted
- **Date:** 2026-10-11

## Context

A push queue has one irreversible step: the HTTP request to the target. If a
node sends that request and then crashes before anything records that it
did, the next node has no way to tell whether the target already acted. If a
node records the attempt only in its own memory, a failover produces two
nodes that each believe they own the attempt.

Defer replicates its scheduler through Raft (`defer-replication`), so the
cheapest durable record is a committed command. The question was whether the
lease is such a command or a local decision the dispatcher makes before
calling the target.

## Decisions

- **A lease is a committed command.** `HttpDispatcher::dispatch_batch`
  proposes `LeaseDue` and waits for it to apply on the group before it sends
  a single request. The applied lease carries the attempt id, the executor
  node, the fence epoch, and the expiry.
- **The outcome is a committed command too.** Every attempt in a batch ends
  in an ack or a nack, and the dispatcher proposes them together as one
  `SettleBatch`. A crash between the request and the settlement leaves a
  lease that expires; `ReclaimExpired` then reschedules or dead-letters the
  task.
- **The idempotency key is stable across attempts.** The target sees
  `idempotency-key: {queue}/{task_id}` on every attempt and a fresh
  `x-defer-attempt-id` per attempt, so a request whose ack never committed is
  recognisable as a repeat.
- **Dispatch concurrency is local; permits are committed.** One process
  sends at most `--dispatch-concurrency` requests at a time through the shared
  bounded executor, but the queue's in-flight count, per-tick cap, and token
  bucket are scheduler state every replica applies.

## Consequences

- Delivery is at least once. A target that must act exactly once
  deduplicates on the idempotency key.
- Dispatch latency includes one Raft round trip before the request and one
  after the batch. The `relay_performance_ceiling` gate bounds that overhead
  against the sibling pull queue.
- A lost fence (ADR 0004) can only mean the request may have been sent; it
  never means the lease was never recorded.
