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

Operations move into `core`, as core
[ADR 0002](https://github.com/faberline/core/blob/main/docs/adr/0002-core-owned-service-operations.md)
records. Defer keeps its product: queues, tasks, dispatch, retries, and
target signing. Core decides how defer runs: replica membership, shards and
their split, routing between pods, resources and storage, rollout, and
monitoring. The outcomes that change replicas, shards, or sizing adopt the
core outcome that does that work, and keep defer's own proof.

## Near-term outcomes

### Declarative queues

- ID: `declarative-queues`
- Outcome: A `Defer` custom resource declares the queues an instance must
  serve, each with its policy and control state, so an operator never has to
  `PUT` a queue after a fresh install or a cold seed.
- Boundary: Queue declarations are proposed through Raft at start, not
  written into the data directory. Queue creation over HTTP stays, and a
  declared queue is reconciled on every operator pass instead of only once.
- Completion evidence: The operator render case proves the forwarded
  declarations, and the GKE e2e suite proves a declared queue exists after the
  pod is replaced without any `PUT`.
- Tracking: Not assigned.

### Live replica membership

- ID: `live-replica-membership`
- Outcome: A replica-count change on a `Defer` instance adds or removes a
  voter without restarting the surviving members, while clients keep
  creating and settling tasks, with no committed task lost and no lease
  settled twice.
- Boundary: Defer adopts core's
  [`membership-controller`](https://github.com/faberline/core/blob/main/crates/service-k8s/ROADMAP.md#membership-controller)
  instead of carrying a membership-change engine of its own. Defer delivers
  this outcome when it moves onto core's engine and deletes `defer-operator`,
  in phase 6 of ADR 0002. Until then the operator drives replica changes: the pure step plan is `defer_operator::plan_replica_change`, and wiring it to a live membership actuator is [#84](https://github.com/faberline/defer/issues/84), which follows the answer in faberline/core#181. Every defer replica is a voter: scaling out adds a
  learner, waits until it catches up, and promotes it; scaling in transfers
  leadership first, then demotes and removes the member. Shard count is not
  part of this outcome.
- Completion evidence: A cluster acceptance run drives one replica to three
  and back to one under load and proves every task created before the
  transition reaches exactly one terminal state afterwards.
- Tracking: Not assigned.

### GKE acceptance in repo

- ID: `gke-acceptance-in-repo`
- Outcome: This repository carries the script and the expected readback for
  a paid acceptance run of the operator path on a zonal GKE Standard
  cluster, so a release can cite a run that lives next to the code it
  proves.
- Boundary: The run is manual and paid; it never joins the default test
  set. The `gke-test` workflow already runs the `operator_recovery` suite
  on the shared test cluster as the STATUS gate `gke-operator-acceptance`;
  it provisions no cluster of its own.
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
- Boundary: Defer adopts core's
  [`service-profile-engine`](https://github.com/faberline/core/blob/main/crates/service-k8s/ROADMAP.md#service-profile-engine):
  resources and storage are spec blocks core owns, renders, and grows, so the
  policy vocabulary belongs to those blocks and defer keeps no sizing code of
  its own. Capacity policy describes one replica; it does not schedule
  across shards or move queues. Volume growth follows the storage class's
  expansion support and is never destructive.
- Completion evidence: The operator render case proves the derived
  resources and claim size, and the GKE e2e suite proves a resized claim
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
  exporter. The alert rules are data that defer's service profile returns
  from `alerts()`, and core renders them beside its standard alerts and the
  ServiceMonitor, as core's
  [`service-profile-engine`](https://github.com/faberline/core/blob/main/crates/service-k8s/ROADMAP.md#service-profile-engine)
  records.
- Completion evidence: A documented import of the pack renders the panels
  against a live instance, and the alert rules fire in a GKE test-cluster
  run that forces lease loss.
- Tracking: Not assigned.

### Live topology transitions

- ID: `live-topology-transitions`
- Outcome: A `Defer` instance runs more than one shard, and a shard splits
  while clients keep creating and settling tasks, with no committed task
  lost and no lease settled twice.
- Boundary: Core owns shards and their split. Core's
  [`bucketed-resharding`](https://github.com/faberline/core/blob/main/crates/service-k8s/ROADMAP.md#bucketed-resharding)
  decides when a shard splits and moves its data, and
  [`shard-routing`](https://github.com/faberline/core/blob/main/crates/service-k8s/ROADMAP.md#shard-routing)
  forwards each request to its shard. Defer switches to
  `Partitioning::Bucketed` with the queue as the placement key, so a queue's
  tasks and its rate, burst, and in-flight permits stay on one shard. Defer
  supplies the data-plane hooks: they classify each request by its queue,
  merge the results of a request that spans shards, and export and import
  the tasks of a bucket. Task identity and idempotency keys do not change
  shape when a queue moves between shards. Merging shards back into fewer and
  a queue-to-shard map in the custom resource are outside this outcome, and
  shards never appear in the public API. The renderer keeps the `shardCount: 1` pin until live shard split lands ([#83](https://github.com/faberline/defer/issues/83)).
- Completion evidence: A cluster acceptance run splits one shard into two
  while clients keep creating and settling tasks, and proves every task
  created before the split reaches exactly one terminal state afterwards.
- Tracking: Not assigned.

### Public API certificate rotation

- ID: `public-api-certificate-rotation`
- Outcome: The public API's TLS certificate and key can change while
  `defer serve` runs, without a restart, and a client certificate can be
  required on the public port.
- Boundary: Today the files given by `--tls-cert-file` and `--tls-key-file`
  are read once at start, and the public port checks no client certificate.
  Peer (Raft) TLS is a separate listener and is not part of this outcome.
- Completion evidence: A test replaces the certificate on disk, triggers a
  reload, and proves new handshakes use the new certificate while existing
  connections keep the old one.
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
