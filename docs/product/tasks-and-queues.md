# Tasks and queues

A queue is a named policy plus a control state; a task is one future HTTP
request with a terminal outcome. This area is the README capabilities
`scheduled-task-lifecycle` and `queue-controls-and-limits`, and it owns one
H2 outcome.

## Scheduled tasks

- Problem: none open as shipped; the listing limit below is what
  § Resource listing and retention closes.
- Who: producers creating work for later; operators reading task state.
- Promise: `POST /v1/queues/{queue}/tasks` creates one task and
  `POST .../tasks:batch` creates an atomic batch, each with `task_id`,
  `schedule_at`, optional `priority` (u8, default 10), `target`, `payload`,
  and optional `max_attempts` (default 3); a duplicate id or a disabled queue
  refuses the whole request; a task is eligible once `schedule_at` has
  passed, higher priority first and creation order within a priority;
  `GET .../tasks/{task_id}` reads `Scheduled`, `Leased` with executor and
  epoch, `Succeeded`, `DeadLettered`, or `Canceled`; `DELETE` cancels a
  scheduled or leased task; a dead-lettered task stays readable; a refusal
  answers the shared error envelope with 404 for a missing queue or task and
  409 for a duplicate id or a disabled queue.
- Limits today: there is no listing, purge, or retention, so terminal
  records stay in every snapshot; ETA resolution is the dispatch tick.
- Non-goals: nothing beyond the single-task model is promised by this
  section.
- Neighbours: none; first section of the area.
- Status rows: `scheduled-eta`, `task-create-and-batch`,
  `task-status-and-cancel`, `dead-letter-terminal-state`.

## Queue policy and control

- Problem: none open as shipped.
- Who: operators sizing a queue; producers that must not overrun a target.
- Promise: `PUT /v1/queues/{queue}` sets `max_in_flight`,
  `max_dispatch_per_tick`, `max_dispatches_per_second`, `max_burst_size`,
  `lease_ttl_ms`, and `retry_backoff_ms`, and `GET` reads them back;
  `POST .../control` switches the queue between `Running`, `Paused`, and
  `Disabled`; a paused or disabled queue leases nothing and a disabled queue
  also refuses creates; the token bucket and in-flight count are committed
  state, so the limits hold across replicas and failover.
- Limits today: the bucket refills on lease proposals, so the first tick
  after a long idle can dispatch one full burst; control is per queue, with
  no instance-wide pause.
- Non-goals: queue declarations on the custom resource are
  [operations.md](operations.md) § Declarative queues.
- Neighbours: Scheduled tasks consumes the permits this section defines.
- Status rows: `queue-rate-and-concurrency`, `queue-pause-resume-disable`.

## Resource listing and retention

- Problem: a client must already know every queue name and task id, and a
  terminal record never leaves the replicated state.
- Who: operators inspecting a backlog; producers reusing an id after
  completion.
- Promise: list queues, list a queue's tasks by state, purge a queue, delete
  a queue, and prune terminal records after a configurable retention through
  a committed command so every replica forgets the same records; a pruned id
  becomes creatable again only after retention.
- Non-goals: full-text or payload search; cross-queue queries.
- Open: whether listing is served from the leader only or from any applied
  replica; the default retention.
- Neighbours: extends Scheduled tasks and Queue policy and control; the
  OpenAPI document and clients in
  [api-and-clients.md](api-and-clients.md) carry the new operations.
- Outcome: `resource-listing-and-retention`. Tracking: not assigned.

## Non-goals in this area

- `force-run-bypass`: nothing runs a task ahead of its ETA or past the
  queue's committed permits; cancel and recreate is the path.
- `periodic-schedules`: a task runs once; recurrence is composed outside
  defer.
- `workflow-orchestration`: batch create is one atomic insert, not a
  workflow; chaining belongs to loom.
