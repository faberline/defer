# Defer threat model

This page covers the current source of defer. A control is claimed only when the test named in its row proves it, and the commands are copied from [STATUS.md](../../STATUS.md).

## Assets

| Asset | Where it lives | Why it matters |
|-------|----------------|----------------|
| task payloads | The Raft state machine and its snapshots | They carry the request data that defer delivers to a target. |
| target URLs | Each queue's task definition | A caller that controls a URL controls where defer sends requests. |
| tokens | The bearer-token registry, `DEFER_TOKEN_REGISTRY_FILE` | A token grants read, write or admin rights on queues. |
| signing secret | The file at `DEFER_TARGET_SIGNING_SECRET_FILE` | It signs each target request, so a target can check that defer sent it. |
| peer certificates | `DEFER_PEER_TLS_CA`, `DEFER_PEER_TLS_CERT`, `DEFER_PEER_TLS_KEY` | They decide which nodes may join the Raft group. |

## Trust boundaries

| Boundary | Who can reach it | Control |
|----------|------------------|---------|
| public API (port 7141) | Callers with a network path to `--bind`. | Bearer tokens with per-queue grants, admission limits, body limit, and optional HTTPS. |
| Raft port (port 7142) | Only the other Raft nodes. | Mutual TLS on its own listener; the public router refuses Raft routes. |
| target requests | The customer's target URL, which defer calls. | HMAC signature on each request and an idempotency key. |
| backup destination | The `file://`, `s3://` or `gs://` URI given to `defer backup`. | Admin role on `GET /admin/backup`; the destination's own credentials come from the process environment. |

## Threats and controls

| ID | Threat | Control | Evidence |
|----|--------|---------|----------|
| T1 | A caller without a grant reads or writes a queue. | Per-queue authorization with read, write and admin grants. | `cargo test -p defer --test it -- service_auth:: http_api::` |
| T2 | Oversized or flooding requests exhaust the node. | Request body limit (413 above the limit) and bounded request admission. | `cargo test -p defer --test it -- service_admission::` and `cargo test -p defer --test it -- http_api::` |
| T3 | A forged or replayed request reaches a target. | HMAC-SHA256 target signing over the idempotency key, attempt, URL, timestamp and body. | `cargo test -p defer --test it -- http_dispatch_signing::` |
| T4 | An untrusted peer joins the Raft group, or reads Raft routes from the public port. | Peer mTLS on the Raft listener; the public router refuses Raft routes. | `cargo test -p defer --test it -- raft_peer_mtls::` |
| T5 | A network observer reads the public API. | HTTPS on the public API. | `cargo test -p defer --test it -- tls_public_api::` |
| T6 | A backup is read by a caller without the admin role. | `GET /admin/backup` requires the admin role when auth is required. | `cargo test -p defer --test it -- http_api:: cli_contract::` |
| T7 | A seed snapshot is restored into a live data directory. | Cold seed refuses a non-empty data directory. | `cargo test -p defer --test it -- http_api::` |
| T8 | A stale executor settles a lease. | Fence epoch on each lease; an ack or nack from another node or a stale epoch is refused. | `cargo test -p defer --test it -- raft_scheduler:: http_dispatch::` |
| T9 | A vulnerable or unlicensed dependency enters the build. | `cargo deny check` and `cargo audit` in the `test: security` CI job, which the `gate` job needs. | CI job `test: security`; `crates/defer/tests/it/security_gates.rs` |

## Open risks

- Public API certificates are read once at start and do not rotate; see the `public-api-tls` row in [STATUS.md](../../STATUS.md).
- Peer certificate material is file-based and has no automatic rotation; see the `raft-peer-mtls` row in [STATUS.md](../../STATUS.md).
- A target URL is called as given. The dispatch code does not filter private, loopback or link-local addresses, so a queue owner can point requests at internal services.
- Backup destination credentials come from the environment of the process; defer does not manage them.
- There is no independent security review yet.
- The reproducible image build is not measured.
