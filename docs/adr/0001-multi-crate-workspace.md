# ADR 0001: Multi-crate workspace on faberline/core

- **Status:** accepted
- **Date:** 2026-10-08

## Context

defer left the axiom monorepo as one package: `src/` held the scheduler, the
Raft state machine, the HTTP dispatcher, the HTTP API, the operator and a
1,062-line `src/bin/defer.rs`, which is over the organisation's 1,000-line
limit. Every module could name every other, so the context boundaries existed
only by convention.

defer already used most faberline/core crates, but it still hand-rolled
service lifecycle: an `AtomicBool` drain flag, a bare `tokio::spawn` dispatch
worker that polled that flag, a hand-wired oneshot for the Raft peer listener,
and a `DeferRaft::shutdown()` that nothing called. faberline/core,
faberline/tape and the organisation's other Rust repositories are virtual
workspaces with one crate per context.

## Decisions

- **Virtual workspace.** The root `Cargo.toml` holds only `[workspace]`,
  `[workspace.package]`, `[workspace.dependencies]`, `[workspace.lints]` and
  the profiles. Members live under `crates/<package>` and inherit everything
  they can. The release version stays a single column-0 `version = "x.y.z"`
  line in the root manifest, which `build.sh` and `scripts/release/` read.
- **The crates only split defer's own code.** Generic infrastructure — peer
  TLS, lifecycle, backup, HTTP serving, auth, Raft hosting — comes from
  faberline/core and gets no defer crate.
- **One crate per context, named `defer-<context>`.** Dependencies point one
  way: `shared-kernel ← replication ← dispatch ← queue`, with `access` and
  `operator` as independent leaves.
  - `defer-shared-kernel`: `DeferScheduler` and the task, queue, lease and
    settlement types. Replication applies it, dispatch settles against it and
    the HTTP API serves its types, so it is the shared kernel. It names
    `utoipa` and the `raft_runtime` fence value types; both are B2 exceptions
    in `ddd.toml`.
  - `defer-replication`: the Raft state machine, its host, bootstrap seeding,
    and the `DEFER_PEER` TLS adapter.
  - `defer-dispatch`: the committed-lease HTTP target executor and signing.
  - `defer-access`: the token modes and per-queue authorization over
    service-auth.
  - `defer-queue`: the HTTP API, its OpenAPI document and the metrics.
    `server` and `openapi` reference each other, so they share a crate.
  - `defer-operator`: the CRD, the renderer and the reconcile loop.
- **One assembly crate, `defer`.** The package and library keep the name
  `defer`, so `-p defer`, `CARGO_BIN_EXE_defer` and the existing
  `defer::DeferRaft`-style imports keep working. Its `lib.rs` only re-exports.
  The binary is `src/bin/defer/main.rs` plus one module per verb group
  (`serve`, `spec`, `remote`, `offline`, `backup`, `k8s`, `dockerfile`).
- **Feature names do not change.** `operator` now enables the optional
  `defer-operator` dependency, so the default build does not link kube.
- **Integration tests stay flat** in `crates/defer/tests/`, so every
  `cargo test -p defer --test <name>` command in `aw.toml`, CI and the
  external contracts keeps its target name.
- **Shutdown runs on server-lifecycle.** `defer serve` builds one
  `TaskSupervisor` whose lifecycle owns readiness, listener drain and the
  shutdown deadline (`DEFER_GRACE_SECS`):
  - `AppState` holds a `DrainController` bound to that lifecycle, so
    `/readyz` reports draining the moment shutdown starts.
  - The dispatch worker stops on the `DrainSignal`; a tick already in flight
    finishes and commits its acks. `BackgroundStop` waits for it.
  - The Raft peer listener closes in `FinalFlush`, not `TransportDrain`,
    because those last acks still need it to commit. `DeferRaft::shutdown()`
    then runs as the last `FinalFlush` hook.
  - Any hook that fails or times out makes `defer serve` exit non-zero.
- **The applied-marker reads are gone.** Nothing wrote `applied-<node>.idx`
  or `snapshot-<node>.json`; bootstrap seeds the Raft store directly. The state
  machine keeps the replay-skip behaviour as an explicit flag.

## Consequences

- The compiler enforces the context map: a crate can use only what its
  manifest lists.
- `cargo test --workspace` is the full unit and integration run; `-p defer`
  alone runs only the assembly crate's tests.
- The shutdown sequence no longer holds the listener open for a grace sleep
  after the signal: the lifecycle stops accepting as soon as draining starts,
  and the grace window is now the deadline for the hooks instead.
- `ddd.toml` is not enforced yet (`[migration] enforce = false`). The checker
  still reports the flat integration tests (A1) and the missing
  `docs/README.md`, `docs/architecture.md`, `docs/glossary.md`,
  `docs/domain/` and `docs/operations/` (A5).

## Deferred

- Move the Raft host onto core's `ReplicaHostBuilder`. It only supports mTLS
  today, so adopting it would change the plaintext single-cluster mode.
- Render the operator's YAML through the service-k8s helpers and publish
  `status.conditions`.
- Upgrade faberline/core past v0.4.14.
- Consolidate the integration tests under `crates/defer/tests/it/`, add the
  missing docs, and turn on `ddd.toml` enforcement.
- The served OpenAPI document references `#/components/schemas/crate.DispatchReport`
  for the dispatch response, which no schema defines. The split keeps it
  byte-identical; fixing it changes the published contract.
