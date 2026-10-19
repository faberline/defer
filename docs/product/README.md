# Defer product requirements

Defer is Axiom's self-hosted stand-in for Google Cloud Tasks. This directory
is the product requirements document: what defer promises to task producers,
HTTP targets, and operators, written down before the work items that deliver
it. Release milestones are carved from these sections, not the other way
round.

## How this directory is organised

- One file per capability area, named for the area and never for a work item.
  Each `## <title>` section is one promise.
- A shipped promise names the [STATUS](../../STATUS.md) rows that measure it.
  A future promise names the [ROADMAP](../../ROADMAP.md) outcome that owns it
  and ends with `Tracking: not assigned` until its release milestone exists.
- Every section carries the five product-planning parts — Problem, Who,
  Promise, Non-goals, Neighbours — plus `Open:` lines for decisions the
  release plan still has to settle. An `Open:` line is a question, not a
  default; the release plan answers it or the human does.
- A new capability area is a change to this index first and to the README
  `### Capability index` when the area ships.

## Positioning

Defer offers Cloud Tasks' feature contract through its own HTTP/1.1 + h2c API
described by `/openapi.json`. The comparison baseline is the Cloud Tasks
feature checklist, one row per surface in STATUS. Delivery is at least once;
targets deduplicate on the `idempotency-key` every attempt carries.

Boundaries that every section inherits:

- `relay` is the pull side of the queue family: a worker leases a job and
  acks it. Defer is the push side: defer calls the target at or after the
  task's ETA under the queue's permits.
- `tape` is the Pub/Sub stand-in. Its push delivery is its own outbound
  HTTPS, never a hand-off to defer, and defer never fans one task out to many
  targets.
- `loom` owns workflow state; defer is at most its timer and callback.
- No `cloudtasks.googleapis.com` wire compatibility, no gRPC, no Google
  identity tokens on targets, no App Engine routing, no force run, no cron,
  no performance claim against Cloud Tasks.

## Who defer is for

| Reader | What they hold defer to |
|---|---|
| Producer | One committed record per task, a refusal before the write when the id is taken or the queue is disabled, and a readable state until the task is terminal. |
| Target | At most one request per attempt, never before the ETA, with a stable idempotency key, a fresh attempt id, and, when configured, a verifiable signature. |
| Operator | A replicated group on Kubernetes with backup, restore, probes, metrics, traces, grants, and queue limits that hold across replicas and failover. |

## Horizons

| Horizon | Outcome | Section |
|---|---|---|
| H1 | `declarative-queues` | [operations.md](operations.md) § Declarative queues |
| H1 | `live-topology-transitions` | [replication-and-availability.md](replication-and-availability.md) § Live topology transitions |
| H1 | `gke-acceptance-in-repo` | [operations.md](operations.md) § GKE acceptance in repo |
| H1 | `reliable-soak-gate` | [operations.md](operations.md) § Reliable soak gate |
| H2 | `retry-config-and-dispatch-deadline` | [dispatch-and-delivery.md](dispatch-and-delivery.md) § Retry config and dispatch deadline |
| H2 | `resource-listing-and-retention` | [tasks-and-queues.md](tasks-and-queues.md) § Resource listing and retention |
| H2 | `capacity-policy` | [replication-and-availability.md](replication-and-availability.md) § Capacity policy |
| H2 | `sift-observability-integration` | [operations.md](operations.md) § Sift observability integration |

H1 is ordered by dependency, not by value: declarative queues first, which
the GKE runs need so a fresh instance serves something; then the
topology and soak gates, which are the evidence the H2 capacity work builds
on.

## Section index

| Section | File | Kind | Owner |
|---|---|---|---|
| Scheduled tasks | tasks-and-queues.md | shipped, limited | STATUS `scheduled-eta`, `task-create-and-batch`, `task-status-and-cancel`, `dead-letter-terminal-state` |
| Queue policy and control | tasks-and-queues.md | shipped | STATUS `queue-rate-and-concurrency`, `queue-pause-resume-disable` |
| Resource listing and retention | tasks-and-queues.md | outcome | ROADMAP `resource-listing-and-retention` |
| Committed HTTP dispatch | dispatch-and-delivery.md | shipped, limited | STATUS `http-target-dispatch`, `target-method-and-headers`, `completion-on-2xx`, `at-least-once-retry`, `retry-policy`, `dispatch-fencing` |
| Signed targets | dispatch-and-delivery.md | shipped | STATUS `target-signing` |
| Retry config and dispatch deadline | dispatch-and-delivery.md | outcome | ROADMAP `retry-config-and-dispatch-deadline` |
| Replicated group with peer mTLS | replication-and-availability.md | shipped | STATUS `raft-replication`, `raft-peer-mtls` |
| Snapshot backup and cold seed | replication-and-availability.md | shipped | STATUS `admin-backup`, `cold-seed-bootstrap` |
| Live topology transitions | replication-and-availability.md | outcome | ROADMAP `live-topology-transitions` |
| Capacity policy | replication-and-availability.md | outcome | ROADMAP `capacity-policy` |
| Grants, admission, and body limit | operations.md | shipped, limited | STATUS `per-queue-authorization`, `request-admission`, `request-body-limit` |
| Kubernetes operator and direct install | operations.md | shipped, limited | STATUS `k8s-deployment-assets`, `k8s-operator`, `gke-operator-acceptance` |
| Health, metrics, traces, and drain | operations.md | shipped | STATUS `standard-operational-endpoints`, `dispatch-metrics`, `bounded-soak` |
| Relay performance ceiling | operations.md | shipped | STATUS `relay-performance-ceiling`, `competitor-feature-matrix` |
| Declarative queues | operations.md | outcome | ROADMAP `declarative-queues` |
| GKE acceptance in repo | operations.md | outcome | ROADMAP `gke-acceptance-in-repo` |
| Reliable soak gate | operations.md | outcome | ROADMAP `reliable-soak-gate` |
| Sift observability integration | operations.md | outcome | ROADMAP `sift-observability-integration` |
| One discoverable HTTP contract | api-and-clients.md | shipped | STATUS `openapi-and-generated-clients`, `cli-surface` |

Non-goals are not sections. Each file ends with the non-goals that a reader of
that area would otherwise assume, pointing at the ROADMAP entry that gives the
reason: `cloud-tasks-wire-compatibility`, `cloud-identity-target-tokens`,
`app-engine-targets`, `force-run-bypass`, `periodic-schedules`,
`pull-workers`, `workflow-orchestration`, `cloud-tasks-performance-claims`.
