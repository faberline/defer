# CI lifecycle handoff

`issue-ci.yml` uses the shared immutable action:
`faberline/aw/.github/actions/issue-ci@693e45bded9a4ec8167573e82b423c823c193ec0`
(pinned from `faberline/aw#73` merged via `faberline/aw#74`).

Shared qualification contract:
https://github.com/faberline/aw/blob/693e45bded9a4ec8167573e82b423c823c193ec0/plugins/faberline/skills/issue-ci/QUALIFICATION.md

The privileged default-branch workflow checks out no consumer code. The shared action
reads policy directly from a frozen default-branch revision through GitHub API metadata.
One repository concurrency group (`issue-ci-handoff`) serializes all reconciliations.
AGY owns only `todo -> doing`; CI alone may update lifecycle after submission.
A fresh review independently checks the same current head after qualification; no automatic merge.

## Deployed shared action interface

```yaml
- uses: faberline/aw/.github/actions/issue-ci@693e45bded9a4ec8167573e82b423c823c193ec0
  with:
    operation: handoff
    run-id: ${{ github.event.workflow_run.id }}
    policy: .github/issue-ci-required-gates.json
```

- `operation: handoff`: reconciles all managed PRs upon each `workflow_run` completed event.
- `run-id`: passes `github.event.workflow_run.id` in the `workflow_run` event context.
- `policy`: specifies `.github/issue-ci-required-gates.json` on the consumer default branch.

## Staged pilot qualification contract (v1)

Defer owns affected-test mapping and thin pinned CI integration. Defer does not fork or
duplicate the shared controller/evaluator into a local implementation.

### Trusted workflow bindings

Allowlisted workflows and their immutable definitions:
- `.github/workflows/defer-ci.yml`: ID `381777738`, blob SHA `35065bbbf26d435098490a835ef9393c8847b309`
- `.github/workflows/gke-test.yml`: ID `389390520`, blob SHA `b8cb109f019b6e360c7e4ad1de7c961784507cd3`

### Check mappings

- `policy`: `defer-ci` / `fmt, clippy, tests, release contract` (kind: `policy`, required in `always`)
- `unit`: `defer-ci` / `fmt, clippy, tests, release contract` (kind: `unit`)
- `gke_server_image`: `gke-test` / `build server image / build + publish ghcr test image` (kind: `functional_e2e`)
- `gke_client_image`: `gke-test` / `build test-client image` (kind: `functional_e2e`)
- `gke_resolve`: `gke-test` / `resolve image identity` (kind: `functional_e2e`)
- `gke_e2e`: `gke-test` / `GKE e2e (operator_recovery) / deploy + test on shared GKE` (kind: `functional_e2e`)

### Applicability rules by change category

1. **Runtime & clients** (`crates/**`, `clients/**`, `Cargo.toml`, `Cargo.lock`, `build.sh`, `ddd.toml`):
   - Require: `unit`
   - Exempt: `functional_e2e` (covered by unit/workspace integration tests; cluster e2e applies to deployment/operator paths), `performance` (blocked by explicit qualification blocker)
2. **Deployment & operator** (`k8s/**`, `crates/defer-operator/**`, `crates/defer/tests/operator.rs`, `Dockerfile*`):
   - Require: `unit`, `gke_server_image`, `gke_client_image`, `gke_resolve`, `gke_e2e`
   - Exempt: `performance` (blocked by explicit qualification blocker)
3. **Documentation & non-executable** (`docs/**`, `*.md`, `aw.toml`, `.claude/**`, `benchmarks/**`):
   - Require: none (always-required `policy` check runs)
   - Exempt: `unit`, `functional_e2e`, `performance` (reviewed non-executable paths)
4. **Tooling & workflow maintenance** (`.github/**`, `scripts/**`):
   - Require: `unit`
   - Exempt: `functional_e2e`, `performance`

### Protected paths

Gate infrastructure paths whose modification requires reviewed policy updates:
`.github/issue-ci-required-gates.json`, `scripts/issue-ci/**`, `scripts/meta/**`, `scripts/release/**`.

## Baseline preservation & remaining activation requirements

- `lifecycle_mode: failure-only` is preserved. Genuine failures return an open `doing` issue
  to `todo` with exact links; all-green evidence cannot produce `done`.
- Nonempty `qualification_blockers` are preserved:
  1. "No automatically enforced affected performance PR qualification gate exists."
  2. "The existing path-filtered GKE suite is partial e2e coverage; affected functional/e2e selection and docs-only applicability require a reviewed policy. Missing or skipped GKE runs cannot qualify done."
- **Remaining requirements before full pilot activation**:
  - Genuine current-head / latest-attempt execution and source binding proof on PR head.
  - Automated measured performance PR qualification gate.
  - Separate reviewed controller activation change backed by verified pilot evidence. Normal CI green is NOT full qualification and will not mark done.

## Local validation

Run only affected offline tests locally:

```sh
python3 -B -m unittest discover -s scripts/issue-ci -p 'test_*.py'
actionlint .github/workflows/issue-ci.yml
```
