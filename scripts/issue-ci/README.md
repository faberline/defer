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

## Live failure-only policy preservation

`.github/issue-ci-required-gates.json` is the LIVE policy consumed by the shared action.
It preserves the complete original gate inventory:
- `defer-ci`: `fmt, clippy, tests, release contract`
- `gke-test`: `build server image / build + publish ghcr test image`,
  `build test-client image`, `resolve image identity`,
  `GKE e2e (operator_recovery) / deploy + test on shared GKE`

Crucially, the live policy omits the live `qualification` key to prevent the shared evaluator
from narrowing the workflow inventory during unfinished qualification. A failure in either
`defer-ci` or `gke-test` (including operator recovery tests) correctly transitions an open
`doing` issue to `todo` with exact failure links. All-green evidence in `failure-only` mode
evaluates to `pending` and never produces `done`.

## Non-live candidate qualification policy (v1)

Incomplete v1 qualification mapping is staged in `.github/issue-ci-qualification-candidate.json`
as an explicitly NON-LIVE candidate document. It defines the candidate v1 structure:
- Trusted workflow bindings (`defer-ci` ID `381777738`, `gke-test` ID `389390520`).
- Check allowlists and `always: ["policy"]`.
- Protected paths including gate infrastructure and selection job manifests.
- Trigger-path coverage across all GKE and unit paths.

## Explicit deferred full-activation blockers

Activating full qualification mode (`lifecycle_mode: full`) and removing blockers requires
a separately reviewed change backed by real execution evidence. The following real blockers
remain unresolved:

1. **Real performance applicability**: Stating "no gate exists" is NOT an exemption under qualification v1; an actual automated measured PR performance regression gate must be implemented and verified.
2. **GKE reusable workflow transitive pin**: In `.github/workflows/gke-test.yml`, the reusable workflow is referenced via mutable `uses: faberline/aw/.github/workflows/gke-test.yml@main`. It must be pinned to an immutable 40-hex commit hash.
3. **Protected roots**: Gate infrastructure must declare all local test selection roots as protected paths, including `k8s/gke-test-job/operator_recovery/job.yaml` (which selects the test binary and `--operator_recovery` filter).
4. **GKE trigger path alignment**: All repository paths that trigger `gke-test` (`crates/defer/tests/it/**`, `.github/workflows/defer-test-image.yml`, `Dockerfile*`, `k8s/**`, `crates/defer-operator/**`) must be aligned in qualification rules so GKE is never dropped.
5. **Real current-head provenance & negative cases**: The platform must prove actual tested source and execution head/base binding for current PR and latest attempt, with tested negative cases for missing, skipped, cancelled, stale, rerun, and untrusted evidence.
6. **Failure-only baseline**: Live policy remains strictly `failure-only` with nonempty `qualification_blockers`. General CI green is NOT qualification and must not mark done.

## Staged PR review boundary

PR #44 is an intermediate staging draft PR for controller review. It is NOT a managed
lifecycle-success pilot and references #42 with `References #42` (without `Closes/Fixes/Resolves`).
General CI success does not authorize marking done or activating full mode.

## Local validation & pinned evaluator regression

Run only affected offline tests locally (network-free):

```sh
# Run all issue-ci unit tests (includes consumer policy and pinned evaluator suite):
python3 -B -m unittest discover -s scripts/issue-ci -p 'test_*.py'

# Static workflow validation:
actionlint .github/workflows/issue-ci.yml
```

### Test suite structure

1. **Pinned Evaluator Regression (`scripts/issue-ci/test_pinned_evaluator.py`)**:
   - Executes against the ACTUAL pinned shared action modules from `faberline/aw@693e45bded9a4ec8167573e82b423c823c193ec0`.
   - Verified fixture sources in `scripts/issue-ci/fixtures/pinned_aw/`:
     - `handoff.py`: SHA256 `8313b8c5bf88a42c4cd8b3131fc576360fb0d2b6d120aa30d1a820f4e5ada773`
     - `qualification.py`: SHA256 `acc235139940b4e5d8ea1e75b4101ac442ee14a1e77ea01207838a55fdc7eaed`
     - `action.py`: SHA256 `4a6894fee5a1f56f2b469335c9e061ce39c51d7cb2220ee06fe680e6c3d51ab6`
   - Dynamically loads and verifies modules from isolated temporary directories during tests.
   - Proves that live policy (`.github/issue-ci-required-gates.json`) without `qualification` cannot narrow inventory away from GKE, GKE failure yields `todo` with exact failed job URL, missing/in-progress/skipped GKE yields `pending`, and all-green failure-only mode yields `pending`.
   - Exercises the `action.py` boundary with bounded mocks, verifying `trusted_policy` and `main` dispatch.

2. **Consumer Policy Contracts (`scripts/issue-ci/test_policy.py`)**:
   - Verifies consumer gate inventory, deployed action interface inputs, pilot commit pin, and candidate qualification schema.
   - Distinct from pinned evaluator execution to prevent local vendored files from being misrepresented as proof of the shared evaluator.

3. **Consumer Handoff Suite (`scripts/issue-ci/test_handoff.py`)**:
   - Preserves unit coverage of consumer-local helper functions.
