# ADR 0007: Peer mTLS runs the Raft routes on their own listener

- **Status:** accepted
- **Date:** 2026-10-11

## Context

A single-node or trusted-network deployment is simplest when the Raft routes
share the public port. A replicated production deployment must not let a
client, or a compromised neighbour, reach the Raft routes at all. The
question was whether to gate the Raft routes behind bearer auth on the
shared port or move them.

## Decisions

- **Peer material selects the mode.** With the `DEFER_PEER_*` TLS material
  present, `defer serve` opens a second listener on `--raft-port` (default
  7142) over mutual TLS through the shared `peer-tls` and `raft-runtime`
  transport, and an untrusted peer or server certificate is rejected.
- **The public router drops the Raft routes in that mode.** The node builds
  `router_without_raft_routes_with_admission`; a request for a Raft route on
  the public port is not found rather than refused by auth.
- **Without material the routes stay on the public port.** The plaintext
  single-cluster mode keeps `router_with_admission`, which is the development
  and single-voter shape.
- **The peer listener closes last.** On shutdown the Raft listener closes in
  the final flush, after the dispatch tick's acks have committed, because
  those acks still need it (ADR 0001).

## Consequences

- Certificate material is file-based; rotation is a restart. STATUS states
  that limit on `raft-peer-mtls`.
- The direct-install base exposes both 7141 and 7142 so a later move to the
  secure mode is a Secret away.
- Adopting core's `ReplicaHostBuilder`, which only supports the mTLS mode,
  would remove the plaintext shape; ADR 0001 defers that.
