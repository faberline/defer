// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-product-docs-gate" tracker="#198" reason="Keep the product-document gate on aw metadoc check in both workflows."
use std::path::Path;

/// The product-document gate runs `aw metadoc check .` in both workflows,
/// after an `Install aw` step, and the Python check and its helpers are gone
/// (#198). The old Python path is built from parts so that this file does not
/// contain it as one literal.
#[test]
fn product_docs_gate_runs_aw_metadoc_check() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let old_path = ["scripts", "meta"].join("/");
    let workflows = [
        ".github/workflows/ci.yml",
        ".github/workflows/defer-release-candidate.yml",
    ];
    for workflow in workflows {
        let text = std::fs::read_to_string(root.join(workflow))
            .unwrap_or_else(|error| panic!("read {workflow}: {error}"));
        let gates = text
            .lines()
            .filter(|line| line.trim() == "aw metadoc check .")
            .count();
        assert_eq!(
            gates, 1,
            "{workflow} must run `aw metadoc check .` on exactly one line, found {gates}"
        );
        assert!(
            !text.contains(&old_path),
            "{workflow} must not call the Python product-document check any more (#198)"
        );
    }
    assert!(
        !root.join("scripts").join("meta").exists(),
        "the Python product-document check and its helper must be deleted (#198)"
    );
}
// HANDWRITE-END
