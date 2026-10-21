# Operations

How to run, configure, and test defer. What each operational feature promises
is in [product/operations.md](../product/operations.md).

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

`defer serve --help` lists every setting.

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
| `cargo test -p defer --features operator --test operator` | operator render conformance |
| `gh workflow run gke-test.yml -f suite=operator_recovery` | the cluster suite `e2e::operator_recovery::` on the shared GKE test cluster |

The cluster suites in `crates/defer/tests/it/e2e/` are `#[ignore]`. Each one
has a Job under `k8s/gke-test-job/<suite>/`.
