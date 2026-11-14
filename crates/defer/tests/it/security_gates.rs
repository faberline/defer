// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-security-gates" tracker="#180" reason="Keep the threat model, the CI security job and the advisory ignore rule from drifting."
//! Keeps the threat model, the CI security job and the advisory ignore rule
//! from drifting apart. Each test reads committed text; none starts a server.

const THREAT_MODEL: &str = include_str!("../../../../docs/security/threat-model.md");
const CI: &str = include_str!("../../../../.github/workflows/ci.yml");
const DENY: &str = include_str!("../../../../deny.toml");
const AUDIT: &str = include_str!("../../../../.cargo/audit.toml");

/// The text of one `## ` section, from its heading to the next `## ` heading.
fn section<'a>(doc: &'a str, heading: &str) -> &'a str {
    let Some(start) = doc.find(heading) else {
        return "";
    };
    let rest = &doc[start + heading.len()..];
    match rest.find("\n## ") {
        Some(end) => &rest[..end],
        None => rest,
    }
}

/// The text of the `gate` job: from its `  gate:` key to the end of the file.
fn gate_section(ci: &str) -> &str {
    match ci.find("\n  gate:\n") {
        Some(start) => &ci[start..],
        None => "",
    }
}

/// True when the threat model has the `## Open risks` heading.
fn has_open_risks(doc: &str) -> bool {
    doc.lines().any(|line| line.trim_end() == "## Open risks")
}

/// True when an ignore note has `reason:` and `expires: YYYY-MM-DD`.
fn valid_ignore_note(note: &str) -> bool {
    let Some(at) = note.find("expires:") else {
        return false;
    };
    let date = note[at + "expires:".len()..].trim_start();
    let bytes = date.as_bytes();
    let shape_ok = bytes.len() >= 10
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit);
    note.contains("reason:") && shape_ok
}

/// Every `RUSTSEC-` line in a config has a reason and an expiry on it or the line above.
fn ignores_carry_reason_and_expiry(config: &str) -> bool {
    let lines: Vec<&str> = config.lines().collect();
    lines.iter().enumerate().all(|(i, line)| {
        if !line.contains("RUSTSEC-") {
            return true;
        }
        let same = valid_ignore_note(line);
        let above = i > 0 && valid_ignore_note(lines[i - 1]);
        same || above
    })
}

#[test]
fn threat_model_names_every_asset_and_boundary() {
    for name in [
        "task payloads",
        "target URLs",
        "tokens",
        "signing secret",
        "peer certificates",
        "public API",
        "Raft port",
        "target requests",
        "backup destination",
    ] {
        assert!(THREAT_MODEL.contains(name), "threat model misses `{name}`");
    }
    assert!(
        has_open_risks(THREAT_MODEL),
        "threat model has no `## Open risks` heading"
    );
    // Negative: a document without the heading fails the same check.
    assert!(!has_open_risks("# Defer threat model\n\n## Assets\n"));
}

#[test]
fn threat_model_cites_a_named_test_for_each_control_row() {
    let rows: Vec<&str> = section(THREAT_MODEL, "## Threats and controls")
        .lines()
        .filter(|line| line.starts_with("| T"))
        .filter(|line| {
            let id = line.split('|').nth(1).unwrap_or("").trim();
            id.len() == 2 && id.starts_with('T') && id.as_bytes()[1].is_ascii_digit()
        })
        .collect();
    assert!(
        rows.len() >= 9,
        "expected at least 9 control rows, found {}",
        rows.len()
    );
    for row in &rows {
        let evidence = row.split('|').nth(4).unwrap_or("").trim();
        let cites = evidence.contains("cargo test")
            || evidence.contains("no test named")
            || evidence.trim_matches('`').ends_with(".rs");
        assert!(cites, "control row has no test citation: {row}");
    }
}

#[test]
fn ci_runs_cargo_deny_and_cargo_audit_and_the_gate_needs_the_job() {
    assert!(CI.lines().any(|line| line.trim() == "cargo deny check"));
    assert!(CI.lines().any(|line| line.trim() == "cargo audit"));
    assert!(
        CI.contains("  test-security:"),
        "ci.yml has no `test-security` job"
    );
    let gate = gate_section(CI);
    assert!(
        gate.contains("test-security"),
        "the gate job does not need `test-security`"
    );
    // Negative: a gate `needs` list without the job fails the same check.
    let without = gate.replace(", test-security,", ",");
    assert!(!without.contains("test-security"));
}

#[test]
fn advisory_ignores_carry_a_reason_and_an_expiry() {
    assert!(
        ignores_carry_reason_and_expiry(DENY),
        "deny.toml ignore lacks reason or expiry"
    );
    assert!(
        ignores_carry_reason_and_expiry(AUDIT),
        "audit.toml ignore lacks reason or expiry"
    );
    // Negative: an ignore without a note fails.
    assert!(!ignores_carry_reason_and_expiry(
        "ignore = [\n\"RUSTSEC-2026-0001\",\n]\n"
    ));
}

#[test]
fn required_check_job_is_still_named_gate() {
    assert!(
        CI.lines().any(|line| line == "    name: gate"),
        "the gate job is not named `gate`"
    );
    assert!(!gate_section(CI).contains("name: \"test: security\""));
}
// HANDWRITE-END
