# ADR 0006: Defer owns scheduled push; relay owns worker-leased pull

- **Status:** accepted
- **Date:** 2026-10-11

## Context

The queue family has two shapes. In a pull queue a worker asks for a job,
holds a lease, does the work, and acks. In a push queue the service calls
the target itself at a time of the producer's choosing. Both need leases,
retries, and a terminal state, which invited a single implementation with
two front ends.

## Decisions

- **Two services, one priority byte.** Relay is the pull queue and defer is
  the push queue. They share the `priority` field shape (`u8`, `0` lowest,
  `255` highest, default `10`) so a producer can carry one value through
  both, and nothing else: ETA, queue rate and burst limits, HTTP target
  dispatch, target signing, and the dead-letter state are defer's alone, and
  worker leases over HTTP are relay's alone.
- **Defer never hands a task to relay.** A task's attempt is defer's own
  outbound HTTP request, settled against defer's own replicated lease.
- **Relay is the performance baseline, not a dependency.** The
  `relay_performance_ceiling` gate runs the same durable enqueue, lease, and
  ack workload through both and bounds defer's extra ETA, permit, retry, and
  terminal-state machinery at 20% of relay's throughput.
- **Workflows are loom's.** Loom may use defer for timers and callbacks;
  workflow state stays in loom. Defer's batch create is one atomic insert,
  not a chain.

## Consequences

- A worker framework that wants to lease jobs is pointed at relay. The
  competitor matrix excludes Celery and Sidekiq by category for the same
  reason.
- The roadmap records `pull-workers` and `workflow-orchestration` as
  non-goals.
- Relay is a dev-dependency of defer's assembly crate, used only by the
  ceiling benchmark; no runtime path links it.
