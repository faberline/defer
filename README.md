# Defer

## Brief

Defer is Axiom's self-hosted stand-in for Google Cloud Tasks: a
Raft-replicated push queue where a client schedules an HTTP request for later,
and defer delivers it to the target at or after that time, under the queue's
rate, burst, and in-flight limits, retrying until the target answers 2xx or
the attempts run out.

Cloud Tasks is the feature-contract peer. [STATUS.md](STATUS.md) is the
checklist: one row per Cloud Tasks surface and per operational surface,
stating whether defer supports it today, supports it with a named limit, or
has not built it yet. [ROADMAP.md](ROADMAP.md) names the outcome that closes
each gap.

Today defer ships scheduled tasks with ETA and priority, committed leases
before any side effect, HTTP target dispatch with a stable idempotency key and
optional HMAC signing, bounded retry into a dead-letter state, per-queue
pause, rate, burst, and concurrency limits, Raft replication with executor
and epoch fencing and peer mTLS, snapshot backup and cold seed, a Kubernetes
operator, and generated TypeScript, Python, and Rust clients. Queue and task
listing, retention pruning, a Cloud Tasks-shaped retry configuration, and
declarative queues on the custom resource are ROADMAP outcomes, not shipped
behaviour.

**Boundaries.**

- [`relay`](https://github.com/faberline/relay/blob/main/README.md) is the
  pull side of the queue family: a worker leases a job, does the work, and
  acks. Defer is the push side: defer calls the target. Both carry the same
  `priority` byte (`0` lowest, `255` highest, default `10`); ETA, rate
  limiting, and HTTP target dispatch are defer's alone.
- [`tape`](https://github.com/faberline/tape/blob/main/README.md) is the
  Pub/Sub stand-in. Tape's push delivery is tape's own outbound HTTPS, not a
  hand-off to defer, and defer never fans one task out to many targets.
- `loom` may use defer for timers and callbacks; workflow state stays in loom.
- Defer does not speak the `cloudtasks.googleapis.com` wire protocol, serves
  no gRPC, mints no Google identity tokens for targets, and has no App Engine
  routing. It publishes no performance claim against Cloud Tasks; its only
  performance gate is defer against its sibling relay on one host.

## Primary workflow

1. Run one node. `--data-dir` holds the Raft log and snapshots. Auth is off by
   default, so the calls below need no token.

   ```bash
   cargo run -q -p defer --bin defer -- serve --bind 127.0.0.1:7141 --data-dir /tmp/defer/data
   ```

2. Create a queue. The policy names all six fields; this example sets 10 in
   flight and keeps the rest at the built-in defaults: 100 dispatches per tick
   and per second, a 100-token burst, a 30-second lease, and a 1-second retry
   base.

   ```bash
   curl -sS -X PUT http://127.0.0.1:7141/v1/queues/jobs \
     -H 'content-type: application/json' \
     -d '{"max_in_flight":10,"max_dispatch_per_tick":100,"max_dispatches_per_second":100,"max_burst_size":100,"lease_ttl_ms":30000,"retry_backoff_ms":1000}'
   ```

3. Schedule a task. `task_id` is unique within the queue, `schedule_at` is an
   RFC 3339 timestamp, `target.url` receives `payload` as JSON, and
   `max_attempts` (default 3) bounds delivery.

   ```bash
   curl -sS -X POST http://127.0.0.1:7141/v1/queues/jobs/tasks \
     -H 'content-type: application/json' \
     -d '{"task_id":"invoice-42","schedule_at":"2026-10-11T12:00:00Z","target":{"url":"https://example.invalid/hooks/invoice"},"payload":{"invoice":42}}'
   ```

4. Read the task. The status moves from `Scheduled` to `Leased` while an
   attempt is in flight, then to `Succeeded`, `DeadLettered`, or `Canceled`.

   ```bash
   curl -sS http://127.0.0.1:7141/v1/queues/jobs/tasks/invoice-42
   ```

5. Cancel it if the work is no longer wanted. A scheduled or leased task
   becomes `Canceled` and the call answers 204; a task that is already
   terminal answers 404.

   ```bash
   curl -sS -X DELETE http://127.0.0.1:7141/v1/queues/jobs/tasks/invoice-42
   ```

The same verbs exist on the CLI against a running node: `defer queue get|put|control`,
`defer task create|status|cancel`, `defer dispatch`, and `defer backup`, each
taking `--url` (`DEFER_URL`) and, when auth is required, `--token`
(`DEFER_TOKEN`).

## Queues and tasks

A queue is a named policy plus a control state. A task is one future HTTP
request with a terminal outcome. That is the whole model today, and it maps
onto Cloud Tasks as follows.

| Cloud Tasks concept | Defer today | Gap owner in ROADMAP |
|---|---|---|
| Queue | `PUT /v1/queues/{queue}` with `max_in_flight`, `max_dispatch_per_tick`, `max_dispatches_per_second`, `max_burst_size`, `lease_ttl_ms`, `retry_backoff_ms`; `GET` reads it back | `resource-listing-and-retention` adds list, purge, and delete; `declarative-queues` declares queues on the custom resource |
| Queue state | `POST /v1/queues/{queue}/control` with `Running`, `Paused`, or `Disabled` | None |
| Task | `{task_id, schedule_at, priority?, target, payload, max_attempts?}`; `POST .../tasks` for one, `POST .../tasks:batch` for an atomic batch | None |
| HTTP target | `target.url`, `target.method` (default `POST`), `target.headers`; the payload is the body | `cloud-identity-target-tokens` and `app-engine-targets` are non-goals |
| Target authentication | `--target-signing-key-id` and `--target-signing-secret-file` add an HMAC-SHA256 signature header | `cloud-identity-target-tokens` is a non-goal |
| Schedule time | `schedule_at`; the task is eligible once due, higher `priority` first, then creation order | None |
| Retry config | `max_attempts` per task, `retry_backoff_ms` per queue doubling per attempt, `--target-timeout-secs` per process | `retry-config-and-dispatch-deadline` |
| Rate limits | `max_dispatches_per_second`, `max_burst_size`, `max_in_flight` are committed state shared by every replica | None |
| Dead letter | The task becomes `DeadLettered` and stays readable | `resource-listing-and-retention` prunes terminal records |
| Get and delete task | `GET` and `DELETE /v1/queues/{queue}/tasks/{task_id}` | `resource-listing-and-retention` adds listing |
| Run task now | Not built | `force-run-bypass` is a non-goal |
| Deduplication | `task_id` is unique within its queue; a duplicate refuses the create | `resource-listing-and-retention` adds a retention window after which an id is creatable again |

Two defer-only routes exist for operators: `POST /v1/queues/{queue}/dispatch`
runs one dispatch tick by hand, and `GET /admin/backup` streams the state
snapshot. Both require the admin role when auth is on.

## Contract discovery

| Need | Source of truth |
|---|---|
| Route inventory | `defer spec --format routes`, or `GET /openapi.json` on a running node. The offline document and the served one come from one source; a gate refuses drift. |
| Request and response schemas | `defer spec --format openapi`, or the `components` section of the OpenAPI document. |
| CLI surface | `defer --help`; `defer llm` prints the agent-facing topic index. |
| Support state per Cloud Tasks surface | [STATUS.md](STATUS.md) |
| Future outcomes and non-goals | [ROADMAP.md](ROADMAP.md) |
| Typed clients | `defer spec gen --lang ts|py|rust --out <dir>`; clients are generated, not committed. |
| Serve flags, environment, ports | `defer serve --help`; `DEFER_BIND`, `DEFER_DATA_DIR`, `DEFER_PEERS`, `DEFER_AUTH`, `DEFER_BODY_LIMIT_BYTES`, and the `DEFER_PEER_*` and `DEFER_ADMISSION_*` families. |
| Kubernetes custom resource | `defer k8s crd render`; the operator and instance layers come from `defer k8s operator render` and `defer k8s instance render --profile <dev\|staging\|prod\|template>`. |
| Direct-install manifests | [`k8s/`](k8s/): `base`, `overlays/`, and the `network-policy` and `observability` components. |
| Backup destinations | `defer backup --dest <uri>` accepts `file://`, `s3://` (feature `backup`), and `gs://`; `defer llm` renders the scheme table. |
| Architecture decisions | [`docs/adr/`](docs/adr/) |

## Capabilities

Every entry below is a defer product capability. The list has no primary and
secondary classes.

A capability can have several sources. `crates/<package>` supplies
defer-specific behaviour from the named workspace member. `core/<name>`
supplies a reusable mechanism from
[faberline/core](https://github.com/faberline/core). `external:<name>`
supplies an outside runtime or provisioned contract. Each source below states
its direct contribution.

### Capability index

| Capability | ID | User promise | Sources |
|---|---|---|---|
| Scheduled task lifecycle | `scheduled-task-lifecycle` | Schedule a task by ETA and priority, bound its attempts, and read every state it passes through until a terminal outcome. | `crates/defer-shared-kernel`, `crates/defer-queue` |
| Committed HTTP dispatch | `committed-http-dispatch` | Deliver each due task to its target only after the lease is committed, with a stable idempotency key, bounded retry, and optional HMAC signing. | `crates/defer-dispatch`, `crates/defer-shared-kernel`, `core/service-executor` |
| Queue controls and limits | `queue-controls-and-limits` | Pause, resume, and disable a queue, and hold its rate, burst, and in-flight limits as committed state across every replica. | `crates/defer-shared-kernel`, `crates/defer-queue` |
| Replicated availability | `replicated-availability` | Run a Raft group whose members replicate every scheduler mutation, fence stale executors, fail over, and rejoin over mutually authenticated peer links. | `crates/defer-replication`, `core/raft-runtime`, `core/peer-tls` |
| Backup and seed | `backup-and-seed` | Export the state snapshot to a sink and restore it into an empty node. | `crates/defer-replication`, `crates/defer-queue`, `crates/defer`, `core/service-backup` |
| Security hardening | `security-hardening` | Gate data-plane routes by per-queue grants, bound request admission and body size, and keep the Raft routes off the public port. | `crates/defer-access`, `crates/defer-queue`, `core/service-auth`, `core/service-http` |
| Kubernetes-native deployment | `kubernetes-native-deployment` | Reconcile a `Defer` custom resource, or apply the direct-install base, into stable Kubernetes workloads. | `crates/defer-operator`, `crates/defer`, `core/service-k8s`, `external:kubernetes` |
| Operations and observability | `operations-observability` | Expose health, readiness, metrics, traces, and graceful drain on one port, and hold a fixed task keyspace flat under sustained retry load. | `crates/defer-queue`, `crates/defer`, `core/service-http`, `core/server-lifecycle`, `core/metrics-prometheus` |
| API, CLI, and clients | `api-cli-clients` | Publish one discoverable HTTP contract and generate typed clients from it. | `crates/defer-queue`, `crates/defer`, `core/openapi-codegen`, `core/cli-std` |
| Competitor contract and ceiling | `competitor-contract-and-ceiling` | Keep an explicit Cloud Tasks feature matrix and bound defer's scheduler overhead against its sibling relay. | `crates/defer`, `external:relay` |

### Scheduled task lifecycle

- ID: `scheduled-task-lifecycle`
- Promise: Create one task or an atomic batch under a queue; refuse a duplicate
  `task_id` or a disabled queue; lease a task only once `schedule_at` has
  passed, higher `priority` first and creation order within a priority; count
  every attempt against `max_attempts`; land in `Succeeded`, `DeadLettered`,
  or `Canceled` and keep that record readable; cancel a scheduled or leased
  task.
- Sources:
  - [`crates/defer-shared-kernel`](crates/defer-shared-kernel) defines the
    scheduler, the task and queue types, the due and priority ordering, the
    lease table, and the settlement rules.
  - [`crates/defer-queue`](crates/defer-queue) defines the queue and task
    routes and their JSON shapes.
- Gate: `cargo test -p defer --test it -- task_lifecycle::`
- Gate: `cargo test -p defer --test it -- http_api::`

### Committed HTTP dispatch

- ID: `committed-http-dispatch`
- Promise: Commit the lease, then send one HTTP request per attempt with the
  task's method, headers, and JSON payload plus the `idempotency-key`,
  `x-defer-attempt-id`, `x-defer-attempt`, and `x-defer-fence-epoch` headers;
  treat 2xx as success and everything else as a failed attempt; reschedule a
  failed attempt with doubling backoff until `max_attempts`, then dead-letter;
  when a signing key is configured, add a timestamp, key id, and
  `v1=` HMAC-SHA256 signature over the key, attempt id, URL, timestamp, and
  body; report an attempt whose fence was lost as `LostOwnership` and retry it
  under the same idempotency key.
- Sources:
  - [`crates/defer-dispatch`](crates/defer-dispatch) defines the dispatch
    batch, the attempt headers, the disposition mapping, and the signing
    scheme.
  - [`crates/defer-shared-kernel`](crates/defer-shared-kernel) defines the
    lease, ack, nack, and fence semantics the dispatcher settles against.
  - [`core/service-executor`](https://github.com/faberline/core/blob/v0.4.14/crates/service-executor/README.md)
    provides the bounded concurrent HTTP execution with per-request timeout.
- Gate: `cargo test -p defer --test it -- http_dispatch::`
- Gate: `cargo test -p defer --test it -- http_dispatch_signing::`

### Queue controls and limits

- ID: `queue-controls-and-limits`
- Promise: Set and read a queue's `max_in_flight`, `max_dispatch_per_tick`,
  `max_dispatches_per_second`, `max_burst_size`, `lease_ttl_ms`, and
  `retry_backoff_ms`; switch it between `Running`, `Paused`, and `Disabled`;
  lease nothing from a paused or disabled queue and refuse creates on a
  disabled one; keep the token bucket and in-flight count as committed state
  so the limits hold across replicas and failover.
- Sources:
  - [`crates/defer-shared-kernel`](crates/defer-shared-kernel) defines the
    queue policy, the control state, the token bucket, and the per-tick lease
    caps.
  - [`crates/defer-queue`](crates/defer-queue) defines the queue policy and
    control routes.
- Gate: `cargo test -p defer --test it -- rate_limits::`
- Gate: `cargo test -p defer --test it -- raft_scheduler::`

### Replicated availability

- ID: `replicated-availability`
- Promise: Apply every queue, task, lease, and settlement command on every
  member of a three-voter group; survive a leader loss and a member restart
  from snapshot without committed loss; record the executor node and fence
  epoch on every lease and refuse an ack or nack from any other owner; run the
  Raft listener on its own port under mutual TLS when peer material is set,
  and keep the Raft routes off the public router in that mode.
- Sources:
  - [`crates/defer-replication`](crates/defer-replication) defines the
    replicated command set, the state machine and its snapshot, the host, and
    the `DEFER_PEER` TLS adapter.
  - [`core/raft-runtime`](https://github.com/faberline/core/blob/v0.4.14/crates/raft-runtime/README.md)
    hosts the group, the log, snapshots, and the fence value types.
  - [`core/peer-tls`](https://github.com/faberline/core/blob/v0.4.14/crates/peer-tls/README.md)
    provides the mutual TLS material and verification for peer links.
- Gate: `cargo test -p defer --test it -- raft_scheduler::`
- Gate: `cargo test -p defer --test it -- raft_peer_mtls::`

### Backup and seed

- ID: `backup-and-seed`
- Promise: Stream the consistent state-machine snapshot from `GET
  /admin/backup`, ship it with `defer backup` to a `file://`, `s3://`, or
  `gs://` destination with retention pruning, and restore it with
  `--bootstrap-seed-uri` into an empty data directory only.
- Sources:
  - [`crates/defer-replication`](crates/defer-replication) defines the
    snapshot shape and the seed-only-into-empty bootstrap.
  - [`crates/defer-queue`](crates/defer-queue) defines the admin backup route
    and its admin-role requirement.
  - [`crates/defer`](crates/defer) defines the `backup` verb and the serve
    flag that reads the seed.
  - [`core/service-backup`](https://github.com/faberline/core/blob/v0.4.14/crates/service-backup/README.md)
    provides the destination sinks and retention pruning.
- Gate: `cargo test -p defer --test it -- http_api::`
- Gate: `cargo test -p defer --test it -- cli_contract::`

### Security hardening

- ID: `security-hardening`
- Promise: Require per-queue read and write grants and the admin role for
  dispatch and backup when `--auth required`; keep probes tokenless; reload
  the token registry without restart and keep the last good registry on a
  malformed rotation; classify mutations, reads, and admin calls for bounded
  admission; refuse an oversized body with 413 before it reaches the
  scheduler.
- Sources:
  - [`crates/defer-access`](crates/defer-access) defines the token modes and
    the per-queue authorization rules.
  - [`crates/defer-queue`](crates/defer-queue) defines the admission classes,
    the auth middleware placement, and the body limit layer.
  - [`core/service-auth`](https://github.com/faberline/core/blob/v0.4.14/crates/service-auth/README.md)
    provides the bearer-token registry, grant evaluation, and rotation.
  - [`core/service-http`](https://github.com/faberline/core/blob/v0.4.14/crates/service-http/README.md)
    provides the shared router shell, error envelope, and admission hooks.
- Gate: `cargo test -p defer --test it -- service_auth::`
- Gate: `cargo test -p defer --test it -- service_admission::`
- Gate: `cargo test -p defer --test it -- http_api::`

### Kubernetes-native deployment

- ID: `kubernetes-native-deployment`
- Promise: Render and reconcile a `Defer` custom resource into a StatefulSet,
  headless and client Services, PodDisruptionBudget, and backup CronJob with
  projected token, signing, and peer TLS secrets, and report phase and ready
  replicas in status; ship a direct-install base for a restricted durable
  singleton with opt-in NetworkPolicy and observability components; prove pod
  replacement keeps the claim and the task state on the shared GKE test
  cluster.
- Sources:
  - [`crates/defer-operator`](crates/defer-operator) defines the CRD,
    defaults, renderer, and reconcile loop.
  - [`crates/defer`](crates/defer) defines the `k8s` render verbs and the
    direct-install manifests under `k8s/`.
  - [`core/service-k8s`](https://github.com/faberline/core/blob/v0.4.14/crates/service-k8s/README.md)
    provides the cluster spec, reconciliation, and workload mechanisms.
  - `external:kubernetes` stores desired state and runs the workload,
    network, RBAC, and Secret contracts.
- Gate: `cargo test -p defer --features operator --test it -- direct_k8s_assets::`
- Gate: `cargo test -p defer --features operator --test operator`
- Gate: `gh workflow run gke-test.yml -f suite=operator_recovery`

### Operations and observability

- ID: `operations-observability`
- Promise: Serve `/healthz`, `/readyz`, `/metrics`, `/openapi.json`, and
  `/docs` on the data-plane port over HTTP/1.1 and h2c; carry `Server-Timing`
  on every response; flip readiness to draining the moment shutdown starts and
  finish the in-flight tick, its acks, and the Raft shutdown within one grace
  budget; expose request and dispatch counters; export OTLP traces when an
  endpoint is set; hold RSS, descriptors, task count, and p99 flat under a
  fixed retrying workload.
- Sources:
  - [`crates/defer-queue`](crates/defer-queue) defines the defer metric
    families and the router composition.
  - [`crates/defer`](crates/defer) defines the serve lifecycle, the drain
    order, and the bounded soak script.
  - [`core/service-http`](https://github.com/faberline/core/blob/v0.4.14/crates/service-http/README.md)
    provides the probe routes, `Server-Timing`, structured logs, and OTLP
    wiring.
  - [`core/server-lifecycle`](https://github.com/faberline/core/blob/v0.4.14/crates/server-lifecycle/README.md)
    provides the one shutdown deadline the drain and the Raft handoff share.
  - [`core/metrics-prometheus`](https://github.com/faberline/core/blob/v0.4.14/crates/metrics-prometheus/README.md)
    provides the Prometheus text exposition.
- Gate: `cargo test -p defer --test it -- http_api::`
- Gate: `DEFER_SOAK_AUTOSTART=1 bash scripts/soak.sh`

### API, CLI, and clients

- ID: `api-cli-clients`
- Promise: Serve exactly the routes the spec lists, over HTTP/1.1 and h2c on
  one port; print the same OpenAPI document offline; generate TypeScript,
  Python, and Rust clients scoped to those routes; ship the standard CLI
  surface with `llm`, `upgrade`, and `issue` plus the queue, task, dispatch,
  backup, k8s, and dockerfile verbs.
- Sources:
  - [`crates/defer-queue`](crates/defer-queue) defines the route inventory
    and the OpenAPI document.
  - [`crates/defer`](crates/defer) defines the CLI verbs and the offline spec
    and client generation.
  - [`core/openapi-codegen`](https://github.com/faberline/core/blob/v0.4.14/crates/openapi-codegen/README.md)
    provides the in-binary client generator.
  - [`core/cli-std`](https://github.com/faberline/core/blob/v0.4.14/crates/cli-std/README.md)
    provides the standard command set and output conventions.
- Gate: `cargo test -p defer --test it -- cli_contract::`
- Gate: `cargo test -p defer --test it -- http_api::`

### Competitor contract and ceiling

- ID: `competitor-contract-and-ceiling`
- Promise: Keep the committed Cloud Tasks feature matrix in step with the
  product decisions STATUS records, and keep defer's release-mode enqueue,
  committed lease, and committed ack throughput within 20% of relay's under
  an identical single-voter, fsync-always workload, never claiming a figure
  against Cloud Tasks.
- Sources:
  - [`crates/defer`](crates/defer) defines the matrix gate and the
    relay-ceiling benchmark.
  - `external:relay` supplies the sibling pull queue the ceiling is measured
    against.
- Gate: `cargo test -p defer --test it -- competitor_feature_matrix::`
- Gate: `cargo test --release -p defer --test it -- relay_performance_ceiling:: --ignored`

## Supporting documents

| Document | Use it for |
|---|---|
| [Product requirements](docs/product/README.md) | What defer promises per capability area, one section per shipped capability or ROADMAP outcome |
| [STATUS.md](STATUS.md) | Current support per Cloud Tasks surface and operational surface, with limits and gates |
| [ROADMAP.md](ROADMAP.md) | Future outcomes and explicit non-goals |
| [Docs index](docs/README.md) | Every document under `docs/` and what it answers |
| [Architecture decisions](docs/adr/) | The decisions behind the workspace cut, the commit-before-side-effect rule, fencing, signing, and the relay boundary |
| [Competitor feature matrix](benchmarks/competitor-feature-matrix.md) | The Cloud Tasks checklist and each row's product decision |
| [Relay performance ceiling](benchmarks/relay-performance-ceiling.md) | The declared workload and the latest recorded ceiling run |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Defer edit rules and required verification commands |

Historical evidence and implementation planning do not define the current
product contract. Use the capability gates and the published runtime
contracts.
