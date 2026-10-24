// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-competitor-feature-matrix" tracker="#2216" reason="Machine-check the deliberately scoped Google Cloud Tasks comparison and explicit non-goals without treating prose as runtime behavior proof."
use std::collections::{BTreeMap, BTreeSet};

const MATRIX: &str = include_str!("../../../../benchmarks/competitor-feature-matrix.md");
const RELAY_CEILING_DOC: &str = include_str!("../../../../benchmarks/relay-performance-ceiling.md");

const TASK_LIFECYCLE_SRC: &str = include_str!("task_lifecycle.rs");
const HTTP_DISPATCH_SRC: &str = include_str!("http_dispatch.rs");
const HTTP_API_SRC: &str = include_str!("http_api.rs");
const RATE_LIMITS_SRC: &str = include_str!("rate_limits.rs");
const RAFT_SCHEDULER_SRC: &str = include_str!("raft_scheduler.rs");

fn scan_test_functions(sources: &[&str]) -> BTreeSet<String> {
    let mut fns = BTreeSet::new();
    for source in sources {
        let mut in_test = false;
        for line in source.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("#[test") || trimmed.starts_with("#[tokio::test") {
                in_test = true;
                continue;
            }
            if in_test {
                if let Some(rest) = trimmed
                    .strip_prefix("async fn ")
                    .or_else(|| trimmed.strip_prefix("fn "))
                {
                    if let Some(name) = rest.split('(').next() {
                        fns.insert(name.trim().to_string());
                        in_test = false;
                    }
                } else if !trimmed.starts_with('#') {
                    in_test = false;
                }
            }
        }
    }
    fns
}

fn rows() -> BTreeMap<&'static str, Vec<&'static str>> {
    let mut rows = BTreeMap::new();
    for line in MATRIX.lines().filter(|line| line.starts_with('|')).skip(2) {
        let cells: Vec<_> = line.trim_matches('|').split('|').map(str::trim).collect();
        assert_eq!(cells.len(), 4, "matrix rows have exactly four fields");
        let prior = rows.insert(cells[0], cells);
        assert!(prior.is_none(), "duplicate matrix row `{line}`");
    }
    rows
}

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2216" reason="Own the parsed, duplicate-free 15-row Google Cloud Tasks-shaped comparison, explicit category exclusions, accurate retry-exhaustion semantics, and bounded performance-claim boundary.">
#[test]
fn cloud_tasks_shaped_contract_and_exclusions_are_explicit() {
    let normalized = MATRIX.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(normalized.contains(
        "Google Cloud Tasks is the semantic competitor; language worker frameworks such as Celery and Sidekiq are explicitly outside the comparison because they do not own the same HTTP push service contract."
    ));
    assert_eq!(MATRIX.matches("Celery").count(), 1);
    assert_eq!(MATRIX.matches("Sidekiq").count(), 1);

    let contract_yaml = MATRIX
        .split_once("```yaml\n")
        .and_then(|(_, rest)| rest.split_once("\n```").map(|(yaml, _)| yaml))
        .expect("machine-readable comparison contract exists");
    let contract: serde_yaml::Value = serde_yaml::from_str(contract_yaml).unwrap();
    let contract = &contract["comparison_contract"];
    assert_eq!(contract["semantic_competitor"], "google-cloud-tasks");
    assert_eq!(contract["category"], "managed-http-push-queue");
    assert_eq!(
        contract["worker_frameworks"]["celery"],
        "excluded-category-mismatch"
    );
    assert_eq!(
        contract["worker_frameworks"]["sidekiq"],
        "excluded-category-mismatch"
    );
    assert_eq!(contract["cloud_tasks_performance"], "unproven");
    assert_eq!(contract["vat_role"], "protocol-emulator-only");
    assert_eq!(
        contract["relay_role"],
        "local-implementation-overhead-ceiling-only"
    );

    let rows = rows();
    assert_eq!(rows.len(), 15, "matrix capability inventory is exact");

    for (capability, cloud_tasks, defer, decision) in [
        ("Future schedule / ETA", "yes", "yes", "core"),
        ("HTTP target dispatch", "native", "native", "core"),
        ("Success deletes/completes task", "2xx", "2xx", "core"),
        (
            "At-least-once retry",
            "yes",
            "yes",
            "core; stable idempotency key is sent on every attempt",
        ),
        (
            "Queue rate / burst / max in-flight",
            "yes",
            "yes",
            "committed globally across replicas",
        ),
        ("Pause / resume / disable", "yes", "yes", "core operations"),
        ("Per-task target headers and method", "yes", "yes", "core"),
        (
            "Target authentication",
            "OAuth/OIDC",
            "HMAC signing",
            "intentionally cloud-neutral",
        ),
        (
            "Durable HA scheduler state",
            "managed",
            "Raft + per-replica durable state",
            "core",
        ),
        (
            "DLQ terminal state",
            "no; exhausted tasks are deleted",
            "explicit replicated terminal state",
            "intentional Defer extension; Cloud Tasks is not a DLQ oracle",
        ),
        ("Task cancellation / inspection", "yes", "yes", "core"),
    ] {
        assert_eq!(
            rows[capability],
            [capability, cloud_tasks, defer, decision],
            "matrix drifted for {capability}"
        );
    }

    assert_eq!(
        rows["Force-run bypass"],
        [
            "Force-run bypass",
            "yes",
            "no",
            "excluded: bypassing committed rate/permit policy weakens the service contract",
        ]
    );
    assert_eq!(
        rows["Arbitrary language worker execution"],
        [
            "Arbitrary language worker execution",
            "no",
            "no",
            "excluded: Relay owns pull workers; Defer owns HTTP push",
        ]
    );
    assert_eq!(
        rows["Periodic/cron schedules"],
        [
            "Periodic/cron schedules",
            "separate Scheduler product",
            "no",
            "excluded: scheduler composition belongs outside Defer",
        ]
    );
    assert_eq!(
        rows["Workflow/batches"],
        [
            "Workflow/batches",
            "no",
            "no",
            "excluded: Loom owns orchestration",
        ]
    );

    assert!(normalized.contains(
        "Until a real Cloud Tasks queue and publicly reachable target are available under the same declared region/hardware/network conditions, that claim remains unproven."
    ));
    assert!(normalized.contains(
        "The local vat emulator may verify protocol behavior, but must not be reported as Google Cloud Tasks performance."
    ));
    assert!(normalized.contains(
        "Defer's local implementation-efficiency ceiling is its sibling Relay, not a substitute competitor."
    ));
    assert!(normalized.contains("Defer may trail Relay by at most 20%"));
    assert!(normalized.contains(
        "Passing this gate proves bounded scheduler overhead only; it does not make a Cloud Tasks performance claim."
    ));
    assert!(
        normalized.contains("Google Cloud Tasks RetryConfig deletes tasks after retry exhaustion:")
    );
    assert!(normalized
        .contains("Google Cloud Tasks versus Pub/Sub assigns dead-letter topics to Pub/Sub:"));
}
// </HANDWRITE>

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2318" reason="Verify that claimed Cloud Tasks matrix rows map to existing tests, exclusions stay unmapped, and Relay ceiling ratio matches the benchmark contract.">
#[test]
fn claimed_matrix_rows_map_to_existing_tests_and_relay_ratio_matches_contract() {
    let mapping: BTreeMap<&'static str, &'static str> = BTreeMap::from([
        ("Future schedule / ETA", "eta_is_checked_before_priority"),
        (
            "HTTP target dispatch",
            "dispatches_real_http_and_retries_with_stable_task_idempotency",
        ),
        (
            "Success deletes/completes task",
            "dispatches_real_http_and_retries_with_stable_task_idempotency",
        ),
        (
            "At-least-once retry",
            "nack_reschedules_then_dead_letters_after_max_attempts",
        ),
        (
            "Queue rate / burst / max in-flight",
            "per_queue_rate_bucket_limits_dispatch_over_time",
        ),
        (
            "Pause / resume / disable",
            "queue_pause_resume_is_per_queue_control",
        ),
        (
            "Per-task target headers and method",
            "dispatches_real_http_and_retries_with_stable_task_idempotency",
        ),
        (
            "DLQ terminal state",
            "dead_letter_terminal_state_converges_and_survives_restart",
        ),
        ("Task cancellation / inspection", "cancel_prevents_dispatch"),
    ]);

    let test_fns = scan_test_functions(&[
        TASK_LIFECYCLE_SRC,
        HTTP_DISPATCH_SRC,
        HTTP_API_SRC,
        RATE_LIMITS_SRC,
        RAFT_SCHEDULER_SRC,
    ]);

    for (capability, test_fn) in &mapping {
        assert!(
            test_fns.contains(*test_fn),
            "mapped test fn `{test_fn}` for `{capability}` not found in scanned module sources"
        );
    }

    let all_rows = rows();
    let mut claimed_count = 0;
    for (capability, cells) in &all_rows {
        let defer_val = cells[2];
        let decision = cells[3];
        let is_claimed = matches!(
            defer_val,
            "yes" | "native" | "2xx" | "explicit replicated terminal state"
        );
        if is_claimed {
            claimed_count += 1;
            assert!(
                mapping.contains_key(*capability),
                "row `{capability}` added to benchmarks/competitor-feature-matrix.md without a mapping"
            );
        }
        if decision.contains("excluded:") {
            assert!(
                !mapping.contains_key(*capability),
                "excluded row `{capability}` must not have a mapped test"
            );
        }
    }

    assert_eq!(
        claimed_count,
        mapping.len(),
        "every claimed capability must have exactly one mapping"
    );

    for excluded in [
        "Force-run bypass",
        "Arbitrary language worker execution",
        "Periodic/cron schedules",
        "Workflow/batches",
    ] {
        assert!(
            !mapping.contains_key(excluded),
            "excluded feature `{excluded}` must stay absent from mapping"
        );
    }

    let contract_yaml = RELAY_CEILING_DOC
        .split_once("```yaml\n")
        .and_then(|(_, rest)| rest.split_once("\n```").map(|(yaml, _)| yaml))
        .expect("machine-readable benchmark contract exists");
    let contract: serde_yaml::Value = serde_yaml::from_str(contract_yaml).unwrap();
    let min_ratio = contract["benchmark_contract"]["minimum_ratio"]
        .as_f64()
        .expect("minimum_ratio is a float");
    assert_eq!(
        min_ratio, 0.8,
        "benchmarks/relay-performance-ceiling.md states minimum_ratio: 0.8"
    );
    assert_eq!(
        min_ratio,
        crate::relay_performance_ceiling::MIN_DEFER_TO_RELAY_RATIO,
        "doc minimum_ratio must equal MIN_DEFER_TO_RELAY_RATIO constant in relay_performance_ceiling.rs"
    );
}
// </HANDWRITE>
// HANDWRITE-END
