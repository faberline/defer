# Defer alerts

The alerts are in `k8s/components/observability/prometheusrule.yaml`. Each shipped alert has one `###` entry below, headed by its exact alert name.

### DeferDispatchLosingOwnership

- **Fires when**: `increase(defer_dispatch_lost_ownership_total[5m]) > 0`, for 5m.
- **Meaning**: a settlement came from a stale owner or epoch and was refused (ADR [0004, executor and epoch fencing](../adr/0004-executor-and-epoch-fencing.md)).
- **Read**: that counter and `defer_dispatch_retried_total` over time.
- **Command**:
  ```bash
  curl -sS http://<svc>:7141/metrics | grep defer_dispatch_lost_ownership_total
  ```
- **Next**: one refusal after a leader change is expected, and the task is retried. A counter that keeps rising with no restart means a node still dispatches with an old lease: restart that pod.

## What is not alerted

- defer exports **no leader metric**. The `x-defer-leader` header and the logs are the only leader signals; see the README entry "A leader that flaps".
- defer exports **no disk metric**. A full disk shows as a pod restart or a failing `/readyz`; see the README entry "A full disk".
