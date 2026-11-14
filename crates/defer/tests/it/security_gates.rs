// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-security-gates" tracker="#180" reason="Keep the threat model, the CI security job and the advisory ignore rule from drifting."

const THREAT_MODEL: &str = include_str!("../../../../docs/security/threat-model.md");
const CI_YML: &str = include_str!("../../../../.github/workflows/ci.yml");
const DENY_TOML: &str = include_str!("../../../../deny.toml");
const AUDIT_TOML: &str = include_str!("../../../../.cargo/audit.toml");

const ASSETS: [&str; 5] = [
    "task payloads",
    "target URLs",
    "tokens",
    "signing secret",
    "peer certificates",
];
const BOUNDARIES: [&str; 4] = [
    "public API",
    "Raft port",
    "target requests",
    "backup destination",
];

/// The rows of the `## Threats and controls` table whose ID is `T<digits>`.
/// Each row is its cells, trimmed, in order: ID, Threat, Control, Evidence.
fn threat_rows(doc: &str) -> Vec<Vec<String>> {
    let section = doc.split("## Threats and controls").nth(1).unwrap_or("");
    let section = section.split("\n## ").next().unwrap_or("");
    section
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with('|'))
        .map(|line| {
            line.trim_matches('|')
                .split('|')
                .map(|cell| cell.trim().to_string())
                .collect::<Vec<String>>()
        })
        .filter(|cells| {
            cells.first().is_some_and(|id| {
                id.len() >= 2 && id.starts_with('T') && id[1..].chars().all(|c| c.is_ascii_digit())
            })
        })
        .collect()
}

fn has_open_risks_heading(doc: &str) -> bool {
    doc.lines().any(|line| line.trim() == "## Open risks")
}

/// True when the `needs:` line of the `gate` job names `job`.
fn gate_needs_job(ci: &str, job: &str) -> bool {
    let Some(gate) = ci.split("\n  gate:\n").nth(1) else {
        return false;
    };
    gate.lines()
        .find(|line| line.trim_start().starts_with("needs:"))
        .is_some_and(|line| line.contains(job))
}

fn is_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 10
        && b[..4].iter().all(u8::is_ascii_digit)
        && b[4] == b'-'
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[7] == b'-'
        && b[8..10].iter().all(u8::is_ascii_digit)
}

fn explains_advisory(line: &str) -> bool {
    line.contains("reason:")
        && line
            .split("expires:")
            .nth(1)
            .is_some_and(|rest| is_date(rest.trim_start()))
}

/// 1-based line numbers of `RUSTSEC-` lines that have no reason and expiry
/// on the same line or on the line directly above.
fn unexplained_advisory_ignores(config: &str) -> Vec<usize> {
    let lines: Vec<&str> = config.lines().collect();
    lines
        .iter()
        .enumerate()
        .filter(|(i, line)| {
            line.contains("RUSTSEC-")
                && !explains_advisory(line)
                && !(*i > 0 && explains_advisory(lines[*i - 1]))
        })
        .map(|(i, _)| i + 1)
        .collect()
}

#[test]
fn threat_model_names_every_asset_and_boundary() {
    for name in ASSETS.iter().chain(BOUNDARIES.iter()) {
        assert!(
            THREAT_MODEL.contains(name),
            "threat-model.md names `{name}`"
        );
    }
    assert!(
        has_open_risks_heading(THREAT_MODEL),
        "threat-model.md has the `## Open risks` section"
    );
    // Negative case: a document without the open risks section is refused.
    assert!(!has_open_risks_heading("# Defer threat model\n"));
}

#[test]
fn threat_model_cites_a_named_test_for_each_control_row() {
    let rows = threat_rows(THREAT_MODEL);
    assert!(rows.len() >= 9, "at least nine T rows, got {}", rows.len());
    for row in &rows {
        let evidence = row.get(3).map(|cell| cell.trim_matches('`')).unwrap_or("");
        assert!(
            evidence.contains("cargo test")
                || evidence.contains("no test named")
                || evidence.ends_with(".rs"),
            "row {} cites a test in its Evidence cell: {evidence}",
            row.first().map(String::as_str).unwrap_or("?")
        );
    }
}

#[test]
fn ci_runs_cargo_deny_and_cargo_audit_and_the_gate_needs_the_job() {
    assert!(CI_YML.contains("test-security:"), "ci.yml has the job");
    assert!(
        CI_YML.contains("cargo deny check"),
        "ci.yml runs cargo deny"
    );
    assert!(CI_YML.contains("cargo audit"), "ci.yml runs cargo audit");
    assert!(
        gate_needs_job(CI_YML, "test-security"),
        "the gate job needs test-security"
    );
    // Negative case: a gate whose needs list drops the job is refused.
    assert!(!gate_needs_job(
        "jobs:\n  gate:\n    needs: [lint, test-workspace]\n    runs-on: ubuntu-latest\n",
        "test-security"
    ));
}

#[test]
fn cargo_audit_uses_its_own_database_directory() {
    // cargo deny fills ~/.cargo/advisory-db (deny.toml db-path); cargo audit
    // refuses a non-empty directory, so it must read a separate one.
    assert!(
        CI_YML.contains("cargo audit --db "),
        "ci.yml gives cargo audit its own --db directory"
    );
    assert!(
        !CI_YML.lines().any(|line| line.trim() == "cargo audit"),
        "ci.yml has no bare cargo audit line"
    );
}

#[test]
fn advisory_ignores_carry_a_reason_and_an_expiry() {
    assert!(unexplained_advisory_ignores(DENY_TOML).is_empty());
    assert!(unexplained_advisory_ignores(AUDIT_TOML).is_empty());
    // Negative case: an ignore without a reason is found.
    assert_eq!(
        unexplained_advisory_ignores("ignore = [\n    \"RUSTSEC-2024-0001\",\n]\n"),
        vec![2]
    );
}

#[test]
fn required_check_job_is_still_named_gate() {
    assert!(CI_YML.lines().any(|line| line == "    name: gate"));
    let gate = CI_YML.split("\n  gate:\n").nth(1).unwrap_or("");
    assert!(
        !gate.contains("name: \"test: security\""),
        "the gate job keeps its own name"
    );
}

// HANDWRITE-END
