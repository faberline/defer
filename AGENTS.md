# defer repository instructions

defer is a self-hosted, Raft-replicated push queue that stands in for Google Cloud Tasks. The product contract is README.md, STATUS.md and ROADMAP.md.

## Output style

Write in the user's language. Keep paths, commands and exact output intact. Use short sentences, one idea in each. Report what changed, what passed and what is unknown.

## Scope

Keep these true: the README, STATUS.md and ROADMAP.md say the same about what is shipped (`aw metadoc check .` checks it). A change of HTTP route follows "Adding an HTTP route" in CONTRIBUTING.md. `clients/openapi.json` is regenerated when the OpenAPI document changes. The repository is public: no secret, token, private host name, e-mail address or personal path in any file, issue or pull request.

## Issue work

Work on a claimed issue only through the `aw` CLI (`aw claim`, the work, `aw report`). `aw` does the branch, push, pull request, merge and signal comment. Edit labels only in the plan step of the `aw-worker` skill. Stages are described in CONTRIBUTING.md.

## Verify

Run these commands from the repository root. They are the commands that `.github/workflows/ci.yml` runs for format, lint and tests:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --locked --workspace
cargo test --locked -p defer --features operator --test it -- operator::
aw metadoc check .
```

CI also runs `cargo deny check`, `cargo audit`, the performance gate and coverage. CI is the test evidence, so keep local runs to the affected unit and static checks.

Do not delete, skip or weaken a test, a threshold or the coverage `line` in `aw.toml`.
