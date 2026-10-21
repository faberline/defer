# Architecture

defer is a Cargo virtual workspace of seven crates under `crates/`. Five hold
defer's bounded contexts, one is the shared kernel, and one is the assembly
crate that wires them into the `defer` library and binary. This page says how
the crates are cut and what may depend on what. The machine-checked form is
[`ddd.toml`](../ddd.toml); the decisions behind it are in
[ADR 0001](adr/0001-multi-crate-workspace.md).

Everything that is not specific to defer — the HTTP service kit, bearer auth,
the raft runtime, peer TLS, service lifecycle, backups, the Kubernetes
operator kit, the standard CLI commands — comes from
[faberline/core](https://github.com/faberline/core) crates pinned by git tag.

## Crates

| Crate | Role | Internal dependencies | What it owns |
|-------|------|-----------------------|--------------|
| `defer-shared-kernel` | shared kernel (pure) | — | `DeferScheduler` and the task, queue, lease and settlement types |
| `defer-replication` | context `replication` (infrastructure) | shared kernel | the raft state machine (`DeferStateMachine`), its host (`DeferRaft`), bootstrap seeding, and the `DEFER_PEER` TLS adapter |
| `defer-dispatch` | context `dispatch` (infrastructure) | shared kernel, replication | `HttpDispatcher`: the committed-lease HTTP target executor and HMAC target signing |
| `defer-access` | context `access` (application) | — | the token modes and per-queue authorization over `service-auth` |
| `defer-queue` | context `queue` (interfaces) | shared kernel, access, dispatch, replication | the HTTP API (`server`), its OpenAPI document, and the metric names |
| `defer-operator` | context `operator` (infrastructure) | — | the `Defer` custom resource, the renderer, and the reconcile loop |
| `defer` | assembly | all of the above; `defer-operator` behind feature `operator` | the `defer` library re-exports, the `defer` binary, and the integration tests |

Dependencies point one way: `shared-kernel ← replication ← dispatch ← queue`.
`access` and `operator` are independent leaves. `server` and `openapi`
reference each other, so they share the `defer-queue` crate.

## Source layout

```text
Cargo.toml                      virtual workspace: [workspace.package],
                                [workspace.dependencies], [workspace.lints]
crates/
  defer-shared-kernel/src/      scheduler.rs + scheduler/, types.rs
  defer-replication/src/        raft.rs + raft/, peer_tls.rs
  defer-dispatch/src/           dispatcher.rs
  defer-access/src/             policy.rs
  defer-queue/src/              server.rs + server/, openapi.rs, metrics.rs
  defer-operator/src/           crd.rs, render.rs, reconcile.rs
  defer/
    build.rs                    build stamp
    src/lib.rs                  re-exports only (`defer::scheduler`, ...)
    src/bin/defer/main.rs       `defer`: entry point only
    src/bin/defer/*.rs          one module per verb group: serve, spec,
                                remote, offline, backup, k8s, dockerfile
    tests/it/main.rs            the integration-test binary; one module per file
    tests/it/e2e/               the cluster suites (all #[ignore])
    tests/operator.rs           standalone: needs --features operator
```

Every crate's `lib.rs` holds only `mod`, `pub mod` and `pub use` lines.
Modules use the `foo.rs` + `foo/` layout; `mod.rs` files are denied by
`[workspace.lints.clippy] mod_module_files = "deny"`.

## Layers

Inside one context a layer may use only the layers listed for it:

| Layer | May use |
|-------|---------|
| domain | — |
| application | domain |
| infrastructure | domain |
| interfaces | application |

Every layer may use the shared kernel. The shared kernel is held to the
domain purity rules: no I/O, no clock, no randomness. The assembly crate
belongs to no context and is unrestricted.

## Allowed exceptions

`ddd.toml` lists two B2 exceptions for the shared kernel. Both are plain data
with no I/O:

- `utoipa`: `ToSchema` derives on the kernel types, which the OpenAPI
  document serves as-is.
- `raft_runtime`: the `FenceToken` and `FencedAssignment` value types that
  fence dispatch ownership.

## Runtime shape

`defer serve` builds one `TaskSupervisor` from `server-lifecycle`. It owns
readiness, listener drain, and the shutdown deadline (`DEFER_GRACE_SECS`).
The dispatch worker ticks every `DEFER_DISPATCH_TICK_MS`, proposes leases
through raft, sends the target request only after the lease commits, and
commits the outcome as a settlement
([ADR 0002](adr/0002-commit-lease-before-side-effect.md)). With peer TLS
material the raft routes move to their own mTLS listener
([ADR 0007](adr/0007-peer-mtls-on-its-own-listener.md)).
