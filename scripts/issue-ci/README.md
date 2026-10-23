# CI lifecycle handoff

`issue-ci.yml` uses the shared immutable action:
`faberline/aw/.github/actions/issue-ci@693e45bded9a4ec8167573e82b423c823c193ec0`
(pinned from `faberline/aw#73` merged via `faberline/aw#74`).

Shared qualification contract:
https://github.com/faberline/aw/blob/693e45bded9a4ec8167573e82b423c823c193ec0/plugins/faberline/skills/issue-ci/QUALIFICATION.md

The privileged default-branch workflow checks out only the default branch with no
persisted credentials. It reads the trusted manifest and GitHub metadata, never PR code
or artifacts. One repository concurrency group (`issue-ci-handoff`) serializes all
reconciliations. AGY owns only todo → doing; after submission CI alone may set done/todo.
A fresh review independently checks the same current head after qualification; no automatic merge.

## Staged pilot qualification contract

Defer owns affected-test mapping and thin pinned CI integration. Defer does not fork or
duplicate the shared controller/evaluator into a new local implementation.

### Defer affected-test mapping by change category

1. **Documentation-only** (`docs/**`, `*.md`, `STATUS.md`, `ROADMAP.md`):
   - Local validation: zero local unit, CLI-contract, integration or e2e test binaries.
     Run `python3 -B scripts/meta/project_docs_contract.py check .` and `git diff --check`.
   - Cloud CI gate: `defer-ci` / `fmt, clippy, tests, release contract` (runs doc contract).
     GKE is path-filtered and does not run. GKE exemption requires reviewed trusted policy;
     missing GKE never silently passes.
2. **Workflow / controller** (`.github/workflows/**`, `.github/issue-ci-required-gates.json`, `scripts/issue-ci/**`):
   - Local validation: `python3 -B -m unittest discover -s scripts/issue-ci -p 'test_*.py'`,
     `actionlint .github/workflows/issue-ci.yml`.
   - Cloud CI gate: `defer-ci` / `fmt, clippy, tests, release contract`.
3. **Runtime** (`crates/defer-queue/**`, `crates/defer-replication/**`, `crates/defer-storage/**`, `crates/defer/src/**`, `Cargo.toml`, `Cargo.lock`):
   - Local validation: affected isolated unit tests only (`cargo test --lib -p <crate>`).
     No local server/network/Raft/cluster execution.
   - Cloud CI gate: `defer-ci` (fmt, clippy, workspace tests, release contract).
4. **Deployment / storage / topology** (`k8s/**`, `Dockerfile*`, `crates/defer-operator/**`, `crates/defer/tests/operator.rs`):
   - Local validation: affected isolated operator unit tests only.
   - Cloud CI gate: `defer-ci` (operator test feature) and `gke-test`
     (`operator_recovery` suite on shared GKE cluster).
5. **Performance-relevant** (benchmarks, hot paths, storage engine):
   - Current status: blocked by qualification blocker.
   - Cloud CI gate: requires an automated measured PR performance regression gate.

### Failure-only baseline and qualification blockers preserved

The manifest inventories every existing ordinary PR CI job and all four exact GKE
job identities:
- `defer-ci`: `fmt, clippy, tests, release contract`
- `gke-test`: `build server image / build + publish ghcr test image`,
  `build test-client image`, `resolve image identity`,
  `GKE e2e (operator_recovery) / deploy + test on shared GKE`

The explicit `lifecycle_mode: failure-only` is preserved. A genuine failure of any
configured current-head gate returns an open `doing` issue to `todo` with exact run/job
links. Success, missing, pending, cancelled or skipped evidence produces no lifecycle mutation.
All-green evidence cannot produce `done`.

The nonempty `qualification_blockers` are preserved:
1. "No automatically enforced affected performance PR qualification gate exists."
2. "The existing path-filtered GKE suite is partial e2e coverage; affected functional/e2e selection and docs-only applicability require a reviewed policy. Missing or skipped GKE runs cannot qualify done."

Clearing blockers and activating live full mode requires a separate reviewed change backed
by real current-head pilot execution evidence, trusted source binding, and negative cases.

## Validation

Run only affected offline tests locally:

```sh
python3 -B -m unittest discover -s scripts/issue-ci -p 'test_*.py'
actionlint .github/workflows/issue-ci.yml
```
