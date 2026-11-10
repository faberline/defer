# Contributing to defer

## Brief

How to change `defer`. What it promises and the work roots it owns live in
[README.md](README.md); repository-wide authoring and verification rules live
in [AW standards](https://github.com/faberline/aw/tree/main/docs/standards).

Use the shared Faberline `aw-worker` skill and the `aw` CLI from
[AW](https://github.com/faberline/aw/tree/main/plugins/faberline/skills).
One run plans, specs, implements or reviews one issue. `aw` owns the branch,
the pull request, the review verdict, the squash merge and the signal comments.

An issue moves through the labels `stage:spec`, `stage:tdd`, `stage:review`
and `stage:done`. Spec writes the bounded issue contract. Tdd writes tests
first, then code, and `aw` opens the pull request. Review reads the issue, the
diff and current CI; it runs no tests. `aw` squash-merges a passed review.
Local work is limited to affected unit/static checks; CI is the test evidence.

Use the repository's existing complete gates for product claims. Keep local
work limited to affected unit/static checks; functional, integration and
performance execution belongs in authorized CI. Preserve all feature matrices,
test selection checks, numeric limits and exact-head/base evidence.

## Verification

| Gate | Command |
|---|---|
| unit + colocated tests | `cargo test --workspace` |
| format | `cargo fmt --all -- --check` |
| lint | `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
| operator | `cargo test -p defer --features operator --test operator` |
| product documents | `python3 -B scripts/meta/project_docs_contract.py check .` |
