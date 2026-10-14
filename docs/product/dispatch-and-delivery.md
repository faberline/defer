# Dispatch and delivery

Dispatch is the committed lease, the HTTP request to the target, and the
committed settlement, in that order. This area is the README capability
`committed-http-dispatch` and the half of `replicated-availability` that
fences stale executors.

## Committed HTTP dispatch

- Problem: none open as shipped; the retry shape is the H2 section below.
- Who: targets receiving requests; operators reading dispositions.
- Promise: a due task is leased by a committed command, then sent to
  `target.url` as one request with `target.method` (default `POST`),
  `target.headers`, the JSON payload, and the `idempotency-key`
  (`queue/task_id`), `x-defer-attempt-id`, `x-defer-attempt`, and
  `x-defer-fence-epoch` headers; 2xx commits an ack and `Succeeded`; any
  other status or transport error commits a nack that reschedules at the
  queue's `retry_backoff_ms` doubled per attempt, capped at 2^16 times the
  base, until `max_attempts`, then `DeadLettered`; an expired lease is
  reclaimed the same way; a settlement from another executor or a stale
  epoch is refused and reported as `LostOwnership`, and the retry carries the
  same idempotency key.
- Limits today: there is no per-task dispatch deadline, no minimum or
  maximum backoff, and no retry-duration cut-off; the target timeout is
  process-wide; a 3xx is a failure; the dispatch concurrency per process is a
  flag, while the queue permits are committed.
- Non-goals: exactly-once delivery; targets deduplicate on the idempotency
  key.
- Neighbours: consumes the permits from
  [tasks-and-queues.md](tasks-and-queues.md) § Queue policy and control; the
  fence epoch comes from
  [replication-and-availability.md](replication-and-availability.md) §
  Replicated group with peer mTLS.
- Status rows: `http-target-dispatch`, `target-method-and-headers`,
  `completion-on-2xx`, `at-least-once-retry`, `retry-policy`,
  `dispatch-fencing`.

## Signed targets

- Problem: none open as shipped.
- Who: targets that must refuse requests not sent by defer.
- Promise: with `--target-signing-key-id` and `--target-signing-secret-file`
  (at least 32 bytes) every target request carries `x-defer-timestamp-ms`,
  `x-defer-key-id`, and `x-defer-signature: v1=<base64 HMAC-SHA256>` over
  the length-delimited idempotency key, attempt id, target URL, timestamp,
  and exact body bytes, so a receiver with the secret can verify the request
  and bind it to one attempt.
- Limits today: one active key per process, so rotation is a restart; without
  the flags requests are unsigned.
- Non-goals: Google-signed OIDC or OAuth tokens on the target request.
- Neighbours: extends Committed HTTP dispatch; the secret is projected by the
  operator in [operations.md](operations.md) § Kubernetes operator and direct
  install.
- Status rows: `target-signing`.

## Retry config and dispatch deadline

- Problem: a queue cannot bound how long a failing task keeps retrying, and a
  task cannot ask for a shorter request timeout than the process default.
- Who: operators tuning a queue against a slow target; producers with a
  time-boxed request.
- Promise: a queue carries a retry configuration with minimum and maximum
  backoff, maximum doublings, and a maximum retry duration, and a task can
  carry its own dispatch deadline, matching the Cloud Tasks `RetryConfig` and
  `dispatchDeadline` shapes; `max_attempts` and the stable idempotency key
  stay; dead-lettering stays the terminal state after exhaustion.
- Non-goals: raising a task's deadline above the process timeout.
- Open: whether the retry configuration lives on the queue policy document
  or a sibling resource; the default maximum retry duration.
- Neighbours: extends Committed HTTP dispatch.
- Outcome: `retry-config-and-dispatch-deadline`. Tracking: not assigned.

## Non-goals in this area

- `cloud-identity-target-tokens`: HMAC signing with a key id works against
  any HTTP receiver; minting Google identity tokens ties defer to one cloud.
- `app-engine-targets`: every target is a URL.
- `pull-workers`: a worker that leases jobs belongs to relay.
