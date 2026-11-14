# Defer configuration

Every `defer serve` flag has an environment variable of the same name in upper case with the prefix `DEFER_`. A flag given on the command line wins over the variable.

## defer serve

| Flag | Environment variable | Default | Effect |
|------|----------------------|---------|--------|
| `--bind` | `DEFER_BIND` | `0.0.0.0:7141` | HTTP API, probes, metrics, and OpenAPI listen address. |
| `--tls-cert-file` | `DEFER_TLS_CERT_FILE` | none | PEM certificate chain for the public API; set it with `--tls-key-file`, both or neither. See [Grants, admission, and body limit](../product/operations.md). |
| `--tls-key-file` | `DEFER_TLS_KEY_FILE` | none | PEM private key for the public API; set it with `--tls-cert-file`, both or neither. |
| `--data-dir` | `DEFER_DATA_DIR` | `.defer/data` | Directory for the Raft log and snapshots. |
| `--peer-service` | `DEFER_PEER_SERVICE` | `defer` | Peer service name. No help text; see `crates/defer/src/bin/defer/serve.rs`. |
| `--raft-port` | `DEFER_RAFT_PORT` | `7142` | Raft peer listener port, used when peer TLS is configured. |
| `--grace-secs` | `DEFER_GRACE_SECS` | `10` | Shutdown deadline, in seconds, for drain and Raft shutdown. |
| `--log-format` | `DEFER_LOG_FORMAT` | `pretty` | Log output format: `pretty` or `json`. Kubernetes uses `json` for the shared `axiom.service.log.v1` collector contract. |
| `--dispatch-tick-ms` | `DEFER_DISPATCH_TICK_MS` | `100` | Dispatch worker tick, in milliseconds. |
| `--dispatch-max-per-queue` | `DEFER_DISPATCH_MAX_PER_QUEUE` | `100` | Maximum tasks dispatched per queue in one tick. No help text; see `crates/defer/src/bin/defer/serve.rs`. |
| `--dispatch-concurrency` | `DEFER_DISPATCH_CONCURRENCY` | `32` | Maximum target HTTP requests in flight per process. The committed queue permits remain global across replicas. See [Limits](limits.md). |
| `--target-timeout-secs` | `DEFER_TARGET_TIMEOUT_SECS` | `30` | Timeout, in seconds, for one target HTTP request. No help text; see `crates/defer/src/bin/defer/serve.rs`. |
| `--target-signing-key-id` | `DEFER_TARGET_SIGNING_KEY_ID` | none | Key ID for HMAC target signing; set it with `--target-signing-secret-file`. See [Grants, admission, and body limit](../product/operations.md). |
| `--target-signing-secret-file` | `DEFER_TARGET_SIGNING_SECRET_FILE` | none | File holding the HMAC secret for target signing. |
| `--auth` | `DEFER_AUTH` | `off` | Bearer auth mode. |
| `--token-registry-file` | `DEFER_TOKEN_REGISTRY_FILE` | none | Token registry file that the bearer auth mode reads. |
| `--otlp-endpoint` | `DEFER_OTLP_ENDPOINT` | none | OTLP endpoint that exports traces. |
| `--bootstrap-seed-uri` | `DEFER_BOOTSTRAP_SEED_URI` | none | Snapshot restored into an empty data directory on the first start. |
| `--body-limit-bytes` | `DEFER_BODY_LIMIT_BYTES` | `8388608` (8 MiB) | Largest accepted data-plane request body in bytes. Larger requests get 413. See [Limits](limits.md). |
| `--declared-queues` | `DEFER_DECLARED_QUEUES` | none | JSON array of declared queues, provisioned at start-up on the Raft leader. |

## Kubernetes

The `Defer` custom resource sets these values through the operator. See [the operator](../../k8s/operator).

## Generated page

This table lists every flag that `defer serve --help` prints. A test (`configuration_page_names_every_serve_flag`) keeps it so.
