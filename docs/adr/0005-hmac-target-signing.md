# ADR 0005: Target authentication is an HMAC signature with a key id

- **Status:** accepted
- **Date:** 2026-10-11

## Context

A target must be able to refuse requests that did not come from defer. Cloud
Tasks solves this by minting Google OIDC or OAuth tokens for the target.
Defer runs outside Google's platform and against arbitrary HTTP receivers, so
it needs a scheme that any receiver can verify with a shared secret and no
identity provider.

## Decisions

- **One key per process.** `--target-signing-key-id` names the key and
  `--target-signing-secret-file` holds the secret, which must be at least 32
  bytes. Without both flags requests are unsigned.
- **Three headers per request.** A signed request carries
  `x-defer-timestamp-ms`, `x-defer-key-id`, and
  `x-defer-signature: v1=<base64 HMAC-SHA256>`.
- **The signed string binds the attempt.** The MAC covers, each
  length-prefixed and in order, the idempotency key, the attempt id, the
  target URL, the timestamp, and the exact body bytes. A receiver can
  therefore reject a replay with a different attempt id, URL, or body even
  when the signature itself is genuine.
- **The scheme is versioned.** The `v1=` prefix lets a later scheme coexist
  with this one on the same key id.

## Consequences

- Rotation is a restart with the new key id; there is no overlap window
  inside one process. The operator projects the secret from a Kubernetes
  Secret, so a rolling restart achieves the overlap.
- Google identity tokens on targets are a roadmap non-goal
  (`cloud-identity-target-tokens`).
- `http_dispatch_signing` is the oracle: it recomputes the signature from the
  received request and the known secret and refuses any drift in the signed
  string.
