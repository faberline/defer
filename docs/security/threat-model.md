# Defer threat model

This model covers the current source of defer. A claim here is backed only by the test named in its row, and a row without a test says so.

## Assets

| Asset | Where it lives | Why it matters |
|-------|----------------|----------------|
| task payloads | the Raft log and state machine in the data directory | They are sent to the target and may hold customer data. |
| target URLs | each task's `target` in the state machine | A wrong or hostile URL receives every request defer sends. |
| tokens | the bearer-token registry, `DEFER_TOKEN_REGISTRY_FILE` | A token grants read, write or admin access to queues. |
| signing secret | `DEFER_TARGET_SIGNING_SECRET_FILE` | It signs every target request; a leak lets anyone forge one. |
| peer certificates | `DEFER_PEER_TLS_CA`, `DEFER_PEER_TLS_CERT`, `DEFER_PEER_TLS_KEY` | They admit a node into the Raft group. |

## Trust boundaries

| Boundary | Who can reach it | Control |
|----------|------------------|---------|
| public API (port 7141) | clients that can reach `--bind` | Bearer tokens with per-queue grants; optional HTTPS. |
| Raft port (port 7142) | other defer nodes | Mutual TLS on its own listener when peer material is set. |
| target requests | the customer's target URL, on the open network | HMAC-SHA256 signature with a key id, when signing is configured. |
| backup destination | the `file://`, `s3://` or `gs://` URI of a backup | Credentials come from the environment of the process that writes the backup. |

## Threats and controls

| ID | Threat | Control | Evidence |
|----|--------|---------|----------|
| T1 | A caller without a grant reads or writes a queue. | Per-queue authorization with read, write and admin grants. | `cargo test -p defer --test it -- service_auth:: http_api::` |
| T2 | Oversized or flooding requests exhaust the node. | Request body limit and request admission. | `cargo test -p defer --test it -- service_admission::` |
| T3 | A forged or replayed target request is accepted by the target. | HMAC target signing with a timestamp; the target deduplicates on `idempotency-key`. | `cargo test -p defer --test it -- http_dispatch_signing::` |
| T4 | An untrusted peer joins the Raft group or reads Raft routes from the public port. | Peer mTLS on its own listener; the public router refuses Raft routes. | `cargo test -p defer --test it -- raft_peer_mtls::` |
| T5 | A network observer reads the public API. | Public API served over HTTPS. | `cargo test -p defer --test it -- tls_public_api::` |
| T6 | A backup is read by a non-admin. | `GET /admin/backup` requires the admin role. | `cargo test -p defer --test it -- http_api:: cli_contract::` |
| T7 | A seed is restored into a live data directory. | The cold seed refuses a non-empty data directory. | `cargo test -p defer --test it -- http_api::` |
| T8 | A stale executor settles a lease. | Fencing epoch on every lease; a stale settlement is refused. | `cargo test -p defer --test it -- raft_scheduler:: http_dispatch::` |
| T9 | A vulnerable or unlicensed dependency enters the build. | `cargo deny check` and `cargo audit` in the `test: security` CI job. | `crates/defer/tests/it/security_gates.rs` |

## Open risks

- The public API certificates are read once at start and are not rotated (`public-api-tls` in `STATUS.md`).
- Peer certificate material has no automatic rotation (`raft-peer-mtls` in `STATUS.md`).
- A target URL is called as given. The dispatcher does not filter private or link-local addresses, so a task can reach internal hosts.
- Backup destination credentials come from the environment of the process, not from a separate secret store.
- No independent security review has been done.
- The reproducible image build has not been measured.
