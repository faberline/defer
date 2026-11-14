# Defer threat model

This page names what defer protects, where its trust boundaries are, which control answers each threat, and which risks stay open. It describes the current source; a claim is backed only by the test named in its row.

## Assets

| Asset | Where it lives | Why it matters |
|-------|----------------|----------------|
| **task payloads** | Queue state in the Raft state machine and the data directory | They carry the caller's work and are sent to the target. |
| **target URLs** | Each task's target, stored with the task | A request is sent to this URL with the task's payload. |
| **tokens** | The bearer-token registry, `DEFER_TOKEN_REGISTRY_FILE` | A token grants read, write or admin rights per queue. |
| **signing secret** | `DEFER_TARGET_SIGNING_SECRET_FILE` | It signs every target request; a leak lets anyone forge one. |
| **peer certificates** | `DEFER_PEER_TLS_CA`, `DEFER_PEER_TLS_CERT`, `DEFER_PEER_TLS_KEY` | They admit a node into the Raft group. |

## Trust boundaries

| Boundary | Who can reach it | Control |
|----------|------------------|---------|
| **public API** (port 7141) | Any client that can reach `--bind` | Bearer tokens with `--auth required`; optional HTTPS with `--tls-cert-file` and `--tls-key-file`. |
| **Raft port** (port 7142) | Peer nodes only | Mutual TLS on its own listener when peer material is set; the public router refuses the Raft routes. |
| **target requests** | Defer sends them to the customer's target URL | HMAC-SHA256 signature with a key id when target signing is on. |
| **backup destination** | The operator's configured destination: `file://`, `s3://` or `gs://` | Admin role on `GET /admin/backup`; the destination's own credentials come from the process environment. |

## Threats and controls

| ID | Threat | Control | Evidence |
|----|--------|---------|----------|
| T1 | A caller without a grant reads or writes a queue. | Per-queue authorization: write and read grants per queue, admin role for dispatch and backup. | `cargo test -p defer --test it -- service_auth:: http_api::` |
| T2 | An oversized or flooding request exhausts the service. | Request body limit (413 above the limit) and bounded request admission. | `cargo test -p defer --test it -- http_api::` and `cargo test -p defer --test it -- service_admission::` |
| T3 | A forged or replayed target request is accepted by the target. | HMAC-SHA256 target signing over the idempotency key, attempt id, URL, timestamp and body; the same idempotency key on retry. | `cargo test -p defer --test it -- http_dispatch_signing::` |
| T4 | An untrusted peer joins the Raft group or reads Raft routes from the public port. | Peer mTLS on the Raft port; the public router refuses the Raft routes. | `cargo test -p defer --test it -- raft_peer_mtls::` |
| T5 | A network observer reads the public API. | Public API TLS when the certificate files are set. | `cargo test -p defer --test it -- tls_public_api::` |
| T6 | A backup is read by a caller without the admin role. | `GET /admin/backup` requires the admin role when auth is required. | `cargo test -p defer --test it -- http_api:: cli_contract::` |
| T7 | A seed is restored over a live data directory. | Cold seed bootstrap refuses a non-empty data directory. | `cargo test -p defer --test it -- http_api::` |
| T8 | A stale executor settles a lease it no longer owns. | Fencing: a lease names its executor and fence epoch; a settlement from another owner or a stale epoch is refused. | `cargo test -p defer --test it -- raft_scheduler:: http_dispatch::` |
| T9 | A vulnerable or unlicensed dependency enters the build. | `cargo deny check` and `cargo audit` in the `test: security` CI job; the job fails the `gate` job on a finding. | `crates/defer/tests/it/security_gates.rs` |

## Open risks

- Public API certificates are read once at start; there is no rotation (`STATUS.md`, row `public-api-tls`).
- Peer certificate material is file-based and has no automatic rotation (`STATUS.md`, row `raft-peer-mtls`).
- A target URL is called as given: the dispatcher in `crates/defer-dispatch/src/dispatcher.rs` has no filter for private or link-local addresses, so a task can reach hosts inside the network that defer runs in.
- Target signing has one active key per process; rotation is a restart with the new key id (`STATUS.md`, row `target-signing`).
- The backup destinations `s3://` and `gs://` are not written to by any gate in this repository (`STATUS.md`, row `admin-backup`).
- There has been no independent security review.
- A reproducible image build has not been measured.
