# defer docs

Project-level documentation for defer. Usage starts at the root
[`README.md`](../README.md); the support contract is [`STATUS.md`](../STATUS.md),
future outcomes are [`ROADMAP.md`](../ROADMAP.md), and contributor rules live in
[`CONTRIBUTING.md`](../CONTRIBUTING.md).

| Document | What it answers |
|----------|-----------------|
| [product/](product/README.md) | What defer promises per capability area, one section per shipped capability or ROADMAP outcome. |
| [adr/](adr/) | Architecture decisions and the reasons behind them. |

## Architecture decisions

| ADR | Decision |
|-----|----------|
| [0001](adr/0001-multi-crate-workspace.md) | Multi-crate workspace on faberline/core: one crate per context, one assembly crate, shutdown on server-lifecycle. |
| [0002](adr/0002-commit-lease-before-side-effect.md) | A target request is sent only after its lease is a committed Raft command, and its outcome is committed as a settlement. |
| [0003](adr/0003-eta-before-priority.md) | Due time and the queue's committed permits filter first; priority orders only the dispatchable candidates. |
| [0004](adr/0004-executor-and-epoch-fencing.md) | Every lease names its executor and fence epoch; a settlement from another owner or a stale epoch is refused. |
| [0005](adr/0005-hmac-target-signing.md) | Target authentication is an HMAC-SHA256 signature with a key id over the attempt's identity and body. |
| [0006](adr/0006-push-not-pull.md) | Defer owns scheduled push dispatch; relay owns worker-leased pull. They share the priority byte and nothing else. |
| [0007](adr/0007-peer-mtls-on-its-own-listener.md) | With peer TLS material the Raft routes move to their own mutually authenticated listener and leave the public router. |
| [0008](adr/0008-cold-seed-into-empty-data-dir.md) | A bootstrap seed restores only into an empty data directory and is read once at start. |
