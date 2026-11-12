# Operations

How to run, configure, and test defer. What each operational feature promises
is in [product/operations.md](../product/operations.md).

Start with [install.md](install.md): build, run one node, quickstart, and the 3-replica Kubernetes install.

Upgrades and version compatibility are in [upgrades.md](upgrades.md).

## Serve

`defer serve` runs one node. Its main settings (flag, or environment
variable):

| Variable | Default | Meaning |
|----------|---------|---------|
| `DEFER_BIND` | `0.0.0.0:7141` | HTTP API, probes, metrics, and OpenAPI |
| `DEFER_DATA_DIR` | `.defer/data` | raft log and snapshots |
| `DEFER_RAFT_PORT` | `7142` | raft peer listener when peer TLS is configured |
| `DEFER_GRACE_SECS` | `10` | shutdown deadline for drain and raft shutdown |
| `DEFER_DISPATCH_TICK_MS` | `100` | dispatch worker tick |
| `DEFER_AUTH` | off | bearer auth mode; `DEFER_TOKEN_REGISTRY_FILE` names the registry |
| `DEFER_BODY_LIMIT_BYTES` | 8 MiB | largest accepted data-plane body |
| `DEFER_PEER_TLS_CA`, `_CERT`, `_KEY` | — | peer mTLS material |
| `DEFER_TARGET_SIGNING_KEY_ID`, `DEFER_TARGET_SIGNING_SECRET_FILE` | — | HMAC target signing |
| `DEFER_BOOTSTRAP_SEED_URI` | — | snapshot restored into an empty data directory |
| `DEFER_DECLARED_QUEUES` | — | JSON array of declared queues to provision at start-up on the leader |

`defer serve --help` lists every setting.

### Declarative queues

`defer serve --declared-queues <json>` (or `DEFER_DECLARED_QUEUES`) provisions
declared queues at start-up once the node becomes the Raft leader. The input is a
JSON array of declared queue objects:

```json
[
  {
    "name": "jobs",
    "policy": {
      "max_in_flight": 100,
      "max_dispatch_per_tick": 100,
      "max_dispatches_per_second": 100,
      "max_burst_size": 100,
      "lease_ttl_ms": 30000,
      "retry_backoff_ms": 1000
    },
    "control": "Running"
  }
]
```

Each field of `policy` is optional and defaults to `QueuePolicy::default()`.
`control` is optional and defaults to `Running`.

#### No declarative deletion boundary

Declarative queue provisioning is an upsert: it creates missing declared queues
and reconciles existing declared queues to match the declaration. Queues that
are not declared are never touched, disabled, or deleted; there is no
declarative deletion boundary. Runtime `PUT /v1/queues/{queue}` continues to
function as before.

## Endpoints

`/healthz`, `/readyz`, `/metrics`, `/openapi.json` and `/docs` answer on the
serve port. `/readyz` turns false the moment shutdown starts.

## Kubernetes

- `k8s/base/`: the direct-install base (StatefulSet, Services, PDB,
  ConfigMap).
- `k8s/components/`: opt-in `network-policy` and `observability`.
- `k8s/operator/`: the CRD, the operator Deployment, and its RBAC.
- `k8s/overlays/`: `dev`, `staging`, `prod`, `template`, and `gke-test`.
- `defer k8s crd render`, `defer k8s operator render` and
  `defer k8s instance render` print the same manifests offline.
  `defer k8s operator run` runs the operator (feature `operator`).

## Tests

| Command | What runs |
|---------|-----------|
| `cargo test --workspace` | unit tests and the `it` integration binary |
| `cargo test -p defer --test it -- declarative_queues::` | declarative queue provisioning unit and lifecycle integration |
| `cargo test -p defer --features operator --test operator` | operator render conformance |
| `gh workflow run gke-test.yml -f suite=operator_recovery` | the cluster suite `e2e::operator_recovery::` on the shared GKE test cluster |

The cluster suites in `crates/defer/tests/it/e2e/` are `#[ignore]`. Each one
has a Job under `k8s/gke-test-job/<suite>/`.

## Runbook

A 3-replica group keeps quorum with 2 nodes: one node down is survivable,
two are not. `<ns>` is the namespace, `<svc>` the Service and `<queue>` the
queue name. Probes and metrics are served on the serve port (`7141`).

### One node down

- **Symptom**: `kubectl -n <ns> get statefulset defer` reads `2/3`;
  `kubectl -n <ns> get defer` shows `readyReplicas` 2.
- **Read**: `/readyz` of the other pods; `kubectl -n <ns> describe pod defer-<n>`.
- **Command**:
  ```bash
  kubectl -n <ns> get pods -l app.kubernetes.io/name=defer
  kubectl -n <ns> logs defer-<n> --previous
  ```
- **Next**: the StatefulSet replaces the pod and keeps its PVC (`operator_recovery`
  suite). If the pod stays Pending, fix the node pool. If two nodes are down
  the group has no quorum and writes fail with 5xx: restore a node first and
  do not delete PVCs.

### A leader that flaps

- **Symptom**: repeated 409 `not_leader` responses whose `x-defer-leader`
  header changes, or pods that restart.
- **Read**: the RESTARTS column of `kubectl -n <ns> get pods`; `kubectl -n <ns> logs defer-<n>`.
- **Command**:
  ```bash
  curl -si -X POST http://<svc>:7141/v1/queues/<queue>/tasks | grep -i x-defer-leader
  ```
- **Next**: check the peer TLS secret (`DEFER_PEER_TLS_CERT`,
  `DEFER_PEER_TLS_KEY`) and the network between the pods. defer exports no
  leader metric, so the header and the logs are the only leader signals.

### A queue that does not drain

- **Symptom**: `defer_queue_tasks{state="scheduled"}` grows while
  `defer_dispatch_acked_total` stays flat.
- **Read**: `GET /v1/queues/<queue>` (`control_state`, `task_count`,
  `terminal_count`); `defer_dispatch_retried_total` and
  `defer_dispatch_dead_lettered_total` on `/metrics`.
- **Command**:
  ```bash
  curl -sS http://<svc>:7141/v1/queues/<queue>
  curl -sS http://<svc>:7141/metrics | grep defer_dispatch_retried_total
  ```
- **Next**: `control_state` `Paused` is deliberate. To resume, send
  `POST /v1/queues/<queue>/control` with body `{"state":"Running"}`. A rising
  retried count means the target fails: read the task with
  `GET /v1/queues/<queue>/tasks/<task_id>`. Raise `max_dispatches_per_second`
  only after the target is healthy.

### A lost-ownership alert

- **Symptom**: `defer_dispatch_lost_ownership_total` rises.
- **Meaning**: a settlement came from a stale owner or epoch and was refused
  (ADR `docs/adr/0004-executor-and-epoch-fencing.md`).
- **Read**: that counter and `defer_dispatch_retried_total` over time.
- **Command**:
  ```bash
  curl -sS http://<svc>:7141/metrics | grep defer_dispatch_lost_ownership_total
  ```
- **Next**: one refusal after a leader change is expected, and the task is
  retried. A counter that keeps rising with no restart means a node still
  dispatches with an old lease: restart that pod.

### A full disk

- **Symptom**: a pod restarts, or `/readyz` fails on one pod.
- **Read**: `kubectl -n <ns> describe pod defer-<n>`; `kubectl -n <ns> get pvc`.
- **Command**:
  ```bash
  kubectl -n <ns> get pvc
  kubectl -n <ns> exec defer-<n> -- df -h /data
  ```
- **Next**: take a backup first (see "Restore from a backup"). Then grow the
  PVC if its storage class allows it (`allowVolumeExpansion`). Otherwise
  restore into a bigger PVC. defer has no disk metric or disk check, so this
  entry relies on Kubernetes only.

### Restore from a backup

- **Symptom**: data is lost or a replica must be rebuilt from a copy.
- **Read**: `GET /v1/queues/<queue>` after the group starts.
- **Command**:
  ```bash
  defer backup --url http://<svc>:7141 --token <token> --dest <uri>
  ```
  To restore: stop the group, give every replica an empty PVC, set
  `DEFER_BOOTSTRAP_SEED_URI=<uri>` on each instance (or
  `--bootstrap-seed-uri <uri>`), then start the group.
- **Next**: unset the variable after the first start. The seed is read once,
  and only into an empty data directory. A restore drill on GKE is not done
  (its own issue).

### Rolling upgrade

- **Symptom**: a new image must be rolled out.
- **Steps**:
  1. Take a backup (see "Restore from a backup").
  2. Change `spec.image` of the `Defer` resource (`ghcr.io/faberline/defer:<tag>`).
  3. Watch `kubectl -n <ns> get defer` until it is `Ready` and `readyReplicas`
     equals `desiredReplicas`.
  4. Check `curl -sS http://<svc>:7141/healthz` on each pod.
- **Next**: the upgrade policy is not written here.

### Not covered

- Live membership change (`POST /v1/admin/membership`); it is not shipped (#84).
- Shard changes.
- A leader metric and a disk metric: defer has neither.
