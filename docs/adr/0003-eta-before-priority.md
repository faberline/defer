# ADR 0003: ETA and permits first, priority among the rest

- **Status:** accepted
- **Date:** 2026-10-11

## Context

Defer shares its `priority` field shape with relay: an unsigned byte, `0`
lowest, `255` highest, default `10`. In a pull queue priority is the whole
ordering. In a push queue a task also has an ETA, and its queue has rate,
burst, per-tick, and in-flight permits that exist to protect the target. The
question was what priority is allowed to override.

## Decisions

- **A task is a candidate only once it is due.** `DeferScheduler` keeps
  future tasks in a schedule keyed by `schedule_at`; a dispatch tick first
  promotes everything whose time has passed into the ready set. Priority
  never pulls a task forward in time.
- **The queue's committed permits filter the candidates.** `lease_due_on_node`
  returns nothing unless the queue is `Running`, then caps the batch by
  `max_dispatch_per_tick`, by `max_in_flight` minus the current leases, and
  by the token bucket fed from `max_dispatches_per_second` and
  `max_burst_size`.
- **Priority orders only what is left.** The ready set is 256 bands scanned
  from high to low; within a band tasks lease in creation order.
- **The permits are scheduler state.** The in-flight count, the bucket
  level, and the last refill instant are part of the replicated scheduler,
  so every replica computes the same lease set and the limits hold across
  failover.

## Consequences

- A high-priority task behind a paused queue, an empty bucket, or a full
  in-flight set waits like any other. There is no force run; the roadmap
  records that as the `force-run-bypass` non-goal.
- The bucket refills on lease proposals, so an idle queue's first tick after
  a long pause can dispatch one full burst. STATUS states that limit on
  `queue-rate-and-concurrency`.
- ETA resolution is the dispatch tick (`--dispatch-tick-ms`, default 100 ms).
