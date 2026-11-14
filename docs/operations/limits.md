# Defer limits

The numeric limits of a running node. Each row names where the value is set and the evidence for it: a row ID in [STATUS.md](../../STATUS.md), or a source file.

| Limit | Value | Where it is set | Evidence |
|-------|-------|-----------------|----------|
| Request body | 8 MiB by default; larger bodies get 413 | `--body-limit-bytes` / `DEFER_BODY_LIMIT_BYTES`; `spec.bodyLimitBytes` on the `Defer` resource | STATUS.md row `request-body-limit` |
| In-flight tasks per queue | 100 | `max_in_flight` in `QueuePolicy::default()` (`crates/defer-shared-kernel/src/types.rs`) | STATUS.md row `queue-rate-and-concurrency` |
| Dispatch per tick per queue | 100 | `max_dispatch_per_tick` in `QueuePolicy::default()`; `--dispatch-max-per-queue` | STATUS.md row `queue-rate-and-concurrency`; `crates/defer/src/bin/defer/serve.rs` |
| Dispatches per second per queue | 100 | `max_dispatches_per_second` in `QueuePolicy::default()` | STATUS.md row `queue-rate-and-concurrency` |
| Burst size per queue | 100 | `max_burst_size` in `QueuePolicy::default()` | STATUS.md row `queue-rate-and-concurrency` |
| Lease time | 30000 ms | `lease_ttl_ms` in `QueuePolicy::default()` | STATUS.md row `queue-rate-and-concurrency` |
| Retry back-off | 1000 ms | `retry_backoff_ms` in `QueuePolicy::default()` | STATUS.md row `queue-rate-and-concurrency` |
| Dispatch concurrency per process | 32 target requests | `--dispatch-concurrency` / `DEFER_DISPATCH_CONCURRENCY` | STATUS.md row `http-target-dispatch` |
| Target request timeout | 30 s | `--target-timeout-secs` / `DEFER_TARGET_TIMEOUT_SECS` | `crates/defer/src/bin/defer/serve.rs` |
| Terminal record retention | none: nothing is pruned | `terminal_retention_ms` in the queue policy | STATUS.md row `terminal-record-retention` |
| Backup retention | set by the `defer backup` command; value not measured here | `defer backup` retention pruning | STATUS.md row `admin-backup` |
| Group size | 3 replicas (voters); quorum is 2 | the StatefulSet in `k8s/base/` | STATUS.md row `k8s-operator`; the `operator_recovery` suite |
| Request admission | off by default | `DEFER_ADMISSION_*` | STATUS.md row `request-admission` |

## Not limited

- Queue count: not measured.
- Payload size below the body limit: not measured.
