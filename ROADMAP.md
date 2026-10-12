# Defer roadmap

## Purpose

This document records future product outcomes and explicit non-goals. It does
not describe current support. [STATUS.md](STATUS.md) owns that contract.

The issue tracker owns assignees, work state, schedules, and delivery history.
This file keeps stable outcome IDs so current limits can point to one future
destination without copying tracker state.

Defer's peer for feature parity is Google Cloud Tasks. Each near-term outcome
below closes one group of STATUS rows; the later outcomes follow once the
custom resource and the shared Raft runtime carry the state they need.

## Near-term outcomes

### Write-path error envelope

- ID: `write-path-error-envelope`
- Outcome: A scheduler refusal on task create, batch create, cancel, or queue
  control answers with the shared error envelope and the status the OpenAPI
  document already declares: 404 for a missing queue or task, 409 for a
  duplicate task id or a disabled queue, never 500.
- Boundary: The refusal set stays the current `SchedulerError` variants; this
  outcome changes only how the HTTP layer maps a committed refusal. Read
  routes already map correctly and do not change. No new error kinds are
  added.
- Completion evidence: The live HTTP API case proves each refusal status over
  HTTP/1.1 and h2c, the served OpenAPI document declares every status the
  handlers return, and the generated clients type the envelope.
- Tracking: Not assigned.

### Declarative queues and request limits

- ID: `declarative-queues-and-request-limits`
- Outcome: A `Defer` custom resource declares the queues an instance must
  serve, each with its policy and control state, and the request body limit,
  so an operator never has to `PUT` a queue after a fresh install or a cold
  seed.
- Boundary: Queue declarations are proposed through Raft at start, not
  written into the data directory. The CRD exposes `--body-limit-bytes`
  as a spec field that the renderer forwards as `DEFER_BODY_LIMIT_BYTES`.
  Queue creation over HTTP stays, and a declared queue is reconciled on
  every operator pass instead of only once.
- Completion evidence: The operator render case proves the forwarded
  environment, the kind script proves a declared queue exists after the pod
  is replaced without any `PUT`, and a live case proves the configured body
  limit answers 413.
- Tracking: Not assigned.

### Live topology transitions

- ID: `live-topology-transitions`
- Outcome: A `Defer` instance moves from one shard with one replica to three
  shards with three replicas and back while clients keep creating and
  settling tasks, with no committed task lost and no lease settled twice.
- Boundary: Defer carries no membership-change engine of its own; the
  renderer lifts the `shardCount: 1` pin only when the shared Raft runtime
  offers a live membership actuator defer can call. Task identity and
  idempotency keys do not change shape when a queue moves between shards.
- Completion evidence: A cluster acceptance run drives the 1x1 to 3x3 to 1x1
  path under load and proves every task created before the transition
  reaches exactly one terminal state afterwards.
- Tracking: Not assigned.

### GKE acceptance in repo

- ID: `gke-acceptance-in-repo`
- Outcome: This repository carries the script and the expected readback for
  a paid acceptance run of the operator path on a zonal GKE Standard
  cluster, so a release can cite a run that lives next to the code it
  proves.
- Boundary: The run is manual and paid; it never joins the default test
  set. The organisation-level acceptance workflow that exists today is
  outside this repository and is not a STATUS gate until it is mirrored
  here.
- Completion evidence: The script provisions, deploys, runs the queue and
  task smoke sequence, replaces a pod, and tears the cluster down, and the
  release notes cite one passing run with its date.
- Tracking: Not assigned.

### Reliable soak gate

- ID: `reliable-soak-gate`
- Outcome: The bounded soak proves a fixed task keyspace holds RSS, file
  descriptors, task count, and p99 flat across two steady windows after the
  proposal cache fills and at least one snapshot is taken, and it runs the
  same way on every host.
- Boundary: The soak keeps its fixed two-task workload. The snapshot cadence
  it must cross comes from the serve configuration instead of a hard-coded
  1,024, and the RSS assertion tolerates allocator noise without hiding a
  real leak.
- Completion evidence: The script reads the snapshot cadence from the node,
  three consecutive runs on one idle host pass with the default thresholds,
  and a deliberately leaking build fails the RSS check.
- Tracking: Not assigned.

## Later outcomes

### Retry config and dispatch deadline

- ID: `retry-config-and-dispatch-deadline`
- Outcome: A queue carries a retry configuration with minimum and maximum
  backoff, maximum doublings, and a maximum retry duration, and a task can
  carry its own dispatch deadline, matching the Cloud Tasks `RetryConfig`
  and `dispatchDeadline` shapes.
- Boundary: The per-task `max_attempts` and the stable idempotency key stay.
  The process-wide `--target-timeout-secs` becomes the default a task can
  lower, not raise beyond. Dead-lettering stays the terminal state after
  exhaustion.
- Completion evidence: Scheduler cases prove the backoff schedule and the
  retry-duration cut-off, and a dispatch case proves a task deadline aborts
  the target request and commits a nack.
- Tracking: Not assigned.

### Resource listing and retention

- ID: `resource-listing-and-retention`
- Outcome: Clients can list queues, list a queue's tasks by state, purge a
  queue, and delete a queue, and terminal task records leave the replicated
  state after a configurable retention instead of staying forever.
- Boundary: Listing is paged and read from the applied state. Retention
  pruning is a committed command so every replica forgets the same records.
  A pruned task id becomes creatable again only after retention, which is
  the dedupe window Cloud Tasks also describes.
- Completion evidence: HTTP cases prove the list and purge routes, a Raft
  case proves pruning converges on every replica and survives restart, and
  the OpenAPI document and generated clients carry the new operations.
- Tracking: Not assigned.

### Capacity policy

- ID: `capacity-policy`
- Outcome: A `Defer` custom resource states CPU, memory, and disk capacity
  policy per replica, and the operator sizes requests, limits, and the
  volume claim from it instead of from fixed defaults.
- Boundary: Capacity policy describes one replica; it does not schedule
  across shards or move queues. Volume growth follows the storage class's
  expansion support and is never destructive.
- Completion evidence: The operator render case proves the derived
  resources and claim size, and the kind script proves a resized claim
  stays bound through pod replacement.
- Tracking: Not assigned.

### Sift observability integration

- ID: `sift-observability-integration`
- Outcome: Defer's request, dispatch, and Raft metrics and traces land in
  Sift with a shipped dashboard and alert pack, so an operator sees lease
  loss, dead-lettering, and leader changes without building their own
  views.
- Boundary: Defer keeps exporting Prometheus text and OTLP; the integration
  is the dashboard, alert, and resource-attribute contract, not a new
  exporter.
- Completion evidence: A documented import of the pack renders the panels
  against a live instance, and the alert rules fire in a kind run that
  forces lease loss.
- Tracking: Not assigned.

## Non-goals

### Cloud Tasks wire compatibility

- ID: `cloud-tasks-wire-compatibility`
- Reason: Defer offers Cloud Tasks' feature contract through its own
  HTTP/1.1 + h2c API described by `/openapi.json`. It does not serve the
  `cloudtasks.googleapis.com` REST or gRPC surface, so a Google client
  library cannot point at it.

### Cloud identity target tokens

- ID: `cloud-identity-target-tokens`
- Reason: Target authentication is HMAC signing with a key id, which works
  against any HTTP receiver. Minting Google OIDC or OAuth tokens for a
  target ties the product to one cloud's identity plane, which defer stays
  neutral to.

### App Engine targets

- ID: `app-engine-targets`
- Reason: Every target is a URL. There is no App Engine routing target
  because defer runs outside Google's platform.

### Force-run bypass

- ID: `force-run-bypass`
- Reason: Running a task ahead of its ETA or past the queue's committed
  rate, burst, and in-flight permits weakens the service contract those
  permits exist to hold. Cancel and recreate is the supported path.

### Periodic schedules

- ID: `periodic-schedules`
- Reason: A task runs once at its ETA. Cron-style recurrence is a scheduler
  composed outside defer, as Cloud Scheduler is outside Cloud Tasks.

### Pull workers

- ID: `pull-workers`
- Reason: Defer is the push side of the queue family. Workers that lease
  jobs over HTTP belong to relay, which owns the pull contract.

### Workflow orchestration

- ID: `workflow-orchestration`
- Reason: Chaining tasks, fan-out, and batch orchestration belong to loom.
  Defer's batch create is one atomic insert, not a workflow.

### Cloud Tasks performance claims

- ID: `cloud-tasks-performance-claims`
- Reason: Defer's only performance gate measures its overhead against its
  sibling relay on one host. No figure against Google Cloud Tasks is
  published until a real queue and a public target run under declared
  conditions.
