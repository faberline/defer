# Operations

Operators get one port, standard probes, grants, a custom resource, a direct
install base, and the gates that prove them. This area is the README
capabilities `security-hardening`, `kubernetes-native-deployment`,
`operations-observability`, and `competitor-contract-and-ceiling`, and it
owns four outcomes.

## Grants, admission, and body limit

- Problem: none open as shipped.
- Who: operators exposing defer to more than one producer.
- Promise: with `--auth required`, queue writes and task create or cancel
  need a write grant on the queue, reads need a read grant, dispatch and
  backup need the admin role, and probes stay tokenless; the token registry
  reloads without restart and a malformed rotation keeps the last good
  registry; `DEFER_ADMISSION_*` enables bounded admission classified
  `defer.write`, `defer.read`, and `defer.admin` per bearer token;
  `--body-limit-bytes` (default 8 MiB), or `spec.bodyLimitBytes` on the
  custom resource, refuses oversized data-plane bodies with 413 before the
  scheduler sees them.
- Limits today: grants are per queue name with no wildcard; `--auth off` is
  the default; admission is not exposed on the custom resource; the body
  limit is one value per instance.
- Non-goals: per-task or per-target grants.
- Neighbours: none; first section of the area.
- Status rows: `per-queue-authorization`, `request-admission`,
  `request-body-limit`.

## Kubernetes operator and direct install

- Problem: none open as shipped; queues on the resource and the topology
  pin are the outcomes below.
- Who: operators deploying on Kubernetes.
- Promise: a `Defer` custom resource renders and reconciles a StatefulSet,
  headless and client Services, a PodDisruptionBudget with
  `maxUnavailable: 1`, and a backup CronJob, projecting the token registry,
  target signing, and peer TLS secrets, and reports phase and ready
  replicas; the direct-install base is a restricted durable singleton with
  opt-in NetworkPolicy and observability components; `defer k8s crd`,
  `operator`, and `instance render` emit every layer offline; the GKE
  `operator_recovery` suite proves pod replacement keeps the claim and the
  task state.
- Limits today: `shardCount` is pinned to 1; the resource declares no
  queues; status carries no conditions; the GKE run is
  single-voter on a shared cluster.
- Non-goals: an HPA; replicas are a replication decision, not a load
  decision.
- Neighbours: projects the secrets from Grants, admission, and body limit
  and [dispatch-and-delivery.md](dispatch-and-delivery.md) § Signed
  targets.
- Status rows: `k8s-deployment-assets`, `k8s-operator`,
  `gke-operator-acceptance`.

## Health, metrics, traces, and drain

- Problem: none open as shipped; the soak's reliability is the outcome
  below.
- Who: operators wiring probes, scrapes, and shutdown.
- Promise: `/healthz`, `/readyz`, `/metrics`, `/openapi.json`, and `/docs`
  answer on the serve port over HTTP/1.1 and h2c; every response carries
  `Server-Timing`; readiness reports draining the moment shutdown starts and
  the in-flight tick, its acks, and the Raft shutdown finish within one
  grace budget; `/metrics` exposes the request counter and the acked,
  retried, dead-lettered, and lost-ownership dispatch counters;
  `--otlp-endpoint` exports traces; the bounded soak holds RSS, descriptors,
  task count, and p99 flat under a fixed retrying workload.
- Limits today: no per-queue metric label or depth gauge; no gate exercises
  an OTLP exporter or sends SIGTERM to a live node; the soak is manual and
  assumes the 1,024-entry snapshot cadence.
- Non-goals: a dashboard or alert pack; that is Sift observability
  integration below.
- Neighbours: extends Kubernetes operator and direct install with the
  observability component.
- Status rows: `standard-operational-endpoints`, `dispatch-metrics`,
  `bounded-soak`.

## Relay performance ceiling

- Problem: none open as shipped.
- Who: maintainers changing the scheduler; readers of the matrix.
- Promise: in release mode, under an identical single-voter fsync-always
  enqueue, committed lease, and committed ack workload, defer's throughput
  stays within 20% of relay's; the committed Cloud Tasks feature matrix
  records each checklist row's product decision and excludes worker
  frameworks by category.
- Limits today: single idle host; the gate bounds scheduler overhead only
  and publishes no figure against Cloud Tasks.
- Non-goals: a benchmark against Cloud Tasks or any hosted queue.
- Neighbours: none within the area.
- Status rows: `relay-performance-ceiling`, `competitor-feature-matrix`.

## Declarative queues

- Problem: a fresh install or a cold seed serves nothing until a client
  `PUT`s a queue.
- Who: operators provisioning an instance from a manifest.
- Promise: a `Defer` custom resource declares the queues an instance must
  serve, each with policy and control state; declarations are proposed
  through Raft at start and reconciled on every operator pass; queue creation
  over HTTP stays.
- Non-goals: writing queues into the data directory; deleting a queue the
  resource no longer lists.
- Open: whether a declared policy overrides a later HTTP `PUT` on every
  reconcile or only on creation.
- Neighbours: extends Kubernetes operator and direct install and
  [tasks-and-queues.md](tasks-and-queues.md) § Queue policy and control.
- Outcome: `declarative-queues`. Tracking: not assigned.

## GKE acceptance in repo

- Problem: the in-repo GKE suite runs on a shared test cluster that this
  repository does not provision, so a release cannot cite a dedicated-cluster
  run next to the code it proves.
- Who: release owners.
- Promise: this repository carries the script and expected readback for a
  manual, paid run of the operator path on a zonal GKE Standard cluster that
  provisions, deploys, runs the queue and task smoke sequence, replaces a
  pod, and tears the cluster down; the run never joins the default test set.
- Non-goals: an unpaid or automatic GKE gate.
- Open: whether the script is copied from the organisation workflow or
  built on the `gke-test` workflow.
- Neighbours: extends Kubernetes operator and direct install; depends on
  Declarative queues so the instance serves a queue
  without a client `PUT`.
- Outcome: `gke-acceptance-in-repo`. Tracking: not assigned.

## Reliable soak gate

- Problem: the soak hard-codes the snapshot cadence and its RSS check is
  sensitive to allocator noise, so a pass on one host is not a pass on
  another.
- Who: maintainers running the soak before a release.
- Promise: the bounded soak reads the snapshot cadence from the node, keeps
  its fixed two-task workload, tolerates allocator noise without hiding a
  real leak, and passes three consecutive runs on one idle host with the
  default thresholds while a deliberately leaking build fails.
- Non-goals: a longer or multi-node soak.
- Open: the noise tolerance (percentage, or absolute kilobytes).
- Neighbours: extends Health, metrics, traces, and drain.
- Outcome: `reliable-soak-gate`. Tracking: not assigned.

## Sift observability integration

- Problem: an operator builds every dashboard and alert from the raw
  Prometheus text.
- Who: operators watching lease loss, dead-lettering, and leader changes.
- Promise: defer's request, dispatch, and Raft metrics and traces land in
  Sift with a shipped dashboard and alert pack; the pack imports against a
  live instance and its alerts fire in a GKE test-cluster run that forces
  lease loss.
- Non-goals: a new exporter; defer keeps Prometheus text and OTLP.
- Open: the resource-attribute contract Sift expects from defer.
- Neighbours: extends Health, metrics, traces, and drain.
- Outcome: `sift-observability-integration`. Tracking: not assigned.

## Non-goals in this area

- `cloud-tasks-performance-claims`: the only performance gate is defer
  against relay on one host.
- `cloud-tasks-wire-compatibility`: no Google client library can point at
  defer.
