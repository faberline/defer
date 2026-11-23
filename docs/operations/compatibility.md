# Compatibility policy

This page is the policy for defer 0.x. It is the maintainers' rule, not a
guarantee to every user. Each rule names how it is enforced, or says
`review only` when a person must check it.

## Versions

- defer uses `0.Y.Z`.
- A patch release (Z) changes no route, field or format in a way that breaks
  an existing client or stored data.
- A minor release (Y) may break a route, a field or a format. It must follow
  [How a break is announced](#how-a-break-is-announced).
- A 1.0 release is not planned here.

## HTTP API

- Within a patch release the API only grows: new routes and new optional
  fields. Nothing is removed or renamed.
- A minor release may remove or change a route or a field.
- Enforcement: the committed snapshot `clients/openapi.json` is checked by
  `committed_snapshot_matches_live_document` in
  `crates/defer-queue/src/openapi.rs`. That test fails on any drift but does
  not say whether the drift breaks clients. The reviewer classifies the diff:
  `review only`.

## Snapshot and log formats

- Both the snapshot (`SchedulerSnapshot`) and the raft log entry
  (`DeferEnvelope`) carry `format_version`. The current version is `1`. A
  value of `0`, or a missing field, means the data was written before the
  field existed, and it reads as version 1.
- A binary reads every version up to its own. It refuses a higher version
  with an error that names both versions.
- A format version changes only in a minor release.
- Reading older data with a newer binary is promised for versions 0 and 1
  only.
- Enforcement: the tests `snapshot_v1_fixture_restores`,
  `envelope_v1_fixture_applies`, `snapshot_without_format_version_loads_as_version_0`,
  `snapshot_from_a_newer_version_is_refused` and
  `log_entry_from_a_newer_version_is_refused` in
  `crates/defer-replication/src/raft/tests.rs`. The fixtures are in
  `crates/defer-replication/src/raft/fixtures/`.
- Not promised: a downgrade. Also not promised: two versions in one group at
  the same time. The rolling-upgrade suite exists (crates/defer/tests/it/e2e/rolling_upgrade.rs) and no paid run has happened. Until
  it has run, a mixed-version group is not promised.

## How a break is announced

- Every minor release that changes a route or a field incompatibly, or that
  raises `format_version`, has a `Breaking changes` section in the release
  notes of its tag. The section names each route or field, or the new format
  version, and links the issue.
- A pull request that changes `clients/openapi.json` incompatibly, or that
  raises `FORMAT_VERSION`, says so in its body.
- Enforcement: `review only`.

## Changing a format

1. Raise `FORMAT_VERSION` in `crates/defer-replication/src/raft/command.rs`.
2. Keep the old fixture. Add a new one with
   `cargo test -p defer-replication write_format_fixtures -- --ignored`,
   after renaming the generated files so the old ones stay (for example
   `snapshot_v2.json` and `envelope_v2.json` for the new version).
3. Add a test that the new binary reads the old fixture.
4. Update this page and [upgrades.md](upgrades.md).

See also [upgrades.md](upgrades.md) and [clients/README.md](../../clients/README.md).
