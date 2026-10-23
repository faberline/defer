# Defer generated clients

## Contract

[`openapi.json`](openapi.json) is a committed snapshot of the OpenAPI document
that `defer spec --format openapi` prints and a running node serves at
`GET /openapi.json`. One field differs: `info.version` is fixed to
`snapshot`, so a release version bump does not change the file.

A unit test in `defer-queue` refuses any other drift between the snapshot and
the live document. Regenerate the snapshot in the same change that alters a
route or a schema field:

```bash
DEFER_WRITE_OPENAPI=1 cargo test -p defer-queue --lib openapi::
```

## Generate

Clients are generated in-binary by `defer spec gen` through the shared
`core/openapi-codegen` crate from faberline/core. A `cargo` toolchain is the
only requirement.

| Language | Command |
|---|---|
| TypeScript | `cargo run -q -p defer --bin defer -- spec gen --lang ts --out clients/ts` |
| Python | `cargo run -q -p defer --bin defer -- spec gen --lang py --out clients/py` |
| Rust | `cargo run -q -p defer --bin defer -- spec gen --lang rust --out clients/rust` |

[`codegen.toml`](codegen.toml) pins the default target for each language.
Every generated client includes `.openapi-codegen.json` naming the exact
target contract. Generated language directories are not committed.

## Language matrix

| Language | Generated form | Transport | Auth input | Current limits |
|---|---|---|---|---|
| TypeScript | Typed client from the `typescript-5.0` target | HTTP/1.1 | Fixed `Authorization: Bearer` header | No published package. No defer test runs it. |
| Python | Typed client from the `python-3.14` target | HTTP/1.1 | Fixed `Authorization: Bearer` header | No published package. No defer test runs it. |
| Rust | Serde models and a client from the `rust-2024` target | HTTP/1.1 | Fixed `Authorization: Bearer` header | No published package. No defer test runs it. |

## Connect

In a cluster, clients use the `defer` Service on port `7141`, for example
`http://defer.defer.svc.cluster.local:7141`. Port `7142` carries Raft traffic
between nodes and is not a client port.

With `DEFER_AUTH=required`, the data-plane routes need a bearer token that the
token registry (`DEFER_TOKEN_REGISTRY_FILE`) grants for the queue.

## Current boundaries

- The snapshot covers the routes and schemas only. The
  [API and clients](../docs/product/api-and-clients.md) area owns behavior and
  limits.
- The dispatch response schema is published under the `crate.DispatchReport`
  name.
- No client package is published to npm, PyPI, or crates.io.

## Verification

The snapshot test refuses drift between `openapi.json` and the live document:

```bash
cargo test -p defer-queue --lib openapi::
```

## Supporting documents

- [API and clients](../docs/product/api-and-clients.md) for the contract and
  its limits.
- [README.md](../README.md) for the product workflow and capabilities.
