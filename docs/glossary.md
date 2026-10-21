# Glossary

Terms defer's code, API and docs use. Architecture terms follow core's
[glossary](https://github.com/faberline/core/blob/main/docs/glossary.md).

## Tasks and queues

| Term | Meaning |
|------|---------|
| **queue** | A named policy plus a control state. Tasks are created in a queue. |
| **queue policy** | A queue's limits: `max_in_flight`, `max_dispatch_per_tick`, `max_dispatches_per_second`, `max_burst_size`, `lease_ttl_ms`, `retry_backoff_ms` (`QueuePolicy`). |
| **control state** | `Running`, `Paused` or `Disabled` (`QueueControlState`). A paused or disabled queue leases nothing; a disabled queue also refuses creates. |
| **task** | One future HTTP request with a terminal outcome (`CreateTask`). |
| **target** | The HTTP request a task sends when it runs (`Target`). |
| **ETA / `schedule_at`** | The time after which a task is eligible. Due time filters first; priority only orders due tasks ([ADR 0003](adr/0003-eta-before-priority.md)). |
| **priority** | A `u8`, default 10. Higher runs first among due tasks. |
| **task status** | `Scheduled`, `Leased`, `Succeeded`, `DeadLettered` or `Canceled` (`TaskStatus`). |
| **dead letter** | The terminal state of a task that used all its attempts. It stays readable. |

## Dispatch

| Term | Meaning |
|------|---------|
| **dispatch tick** | One pass of the dispatch worker, every `DEFER_DISPATCH_TICK_MS`. |
| **lease** | The committed right of one executor to send one attempt of a task (`DispatchLease`). |
| **executor** | The node that holds a lease. |
| **fence epoch** | The ownership epoch a lease carries. A settlement from a stale epoch is refused ([ADR 0004](adr/0004-executor-and-epoch-fencing.md)). |
| **settlement** | The committed HTTP result of one attempt against its fenced lease (`AttemptSettlement`). It becomes an ack or a nack (`SettlementOutcome`). |
| **ack** | A successful settlement. The task becomes `Succeeded`. |
| **nack** | A failed settlement. The task retries after `retry_backoff_ms` or is dead-lettered (`NackOutcome`). |
| **target signing** | An HMAC-SHA256 signature with a key id over the attempt's identity and body ([ADR 0005](adr/0005-hmac-target-signing.md)). |

## Replication and deployment

| Term | Meaning |
|------|---------|
| **command** | One replicated write (`DeferCommand`). Every scheduler mutation is one. |
| **state machine** | `DeferStateMachine`: applies committed commands to the scheduler on every replica. |
| **voter** | A raft member that counts toward quorum. |
| **bootstrap seed** | A snapshot restored once into an empty data directory at start ([ADR 0008](adr/0008-cold-seed-into-empty-data-dir.md)). |
| **peer TLS** | The mutual TLS between raft peers, on its own listener (`DEFER_PEER_TLS_*`). |
| **drain** | Shutdown: readiness turns false, the in-flight tick and its acks finish, then raft shuts down. |
| **`Defer` resource** | The custom resource the operator reconciles into a StatefulSet, Services, a PodDisruptionBudget and a backup CronJob. |
