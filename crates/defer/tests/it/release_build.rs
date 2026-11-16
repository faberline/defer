// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-release-build" tracker="#230" reason="Keep the release candidate binary built with the operator, so the GKE acceptance deploy gets a working operator."
use std::path::Path;

/// The release candidate binary must be built with the operator feature.
/// Without it `defer k8s operator run` exits at start, no StatefulSet is
/// rendered, and the GKE acceptance deploy waits 900 s for `defer-0` (#230).
#[test]
fn candidate_binary_is_built_with_the_operator() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workflow =
        std::fs::read_to_string(root.join(".github/workflows/defer-release-candidate.yml"))
            .expect("read .github/workflows/defer-release-candidate.yml");
    let builds: Vec<&str> = workflow
        .lines()
        .filter(|line| line.contains("cargo build --release"))
        .collect();
    assert_eq!(
        builds.len(),
        1,
        "exactly one `cargo build --release` line in the candidate workflow, found {builds:?}"
    );
    let line = builds[0];
    assert!(
        line.contains("-p defer --bin defer")
            && line.contains("--features operator,backup")
            && line.contains("--target"),
        "the release candidate binary must be built with --features operator,backup: without it the operator image exits at start (issue #230); line: {line}"
    );
}
// HANDWRITE-END
