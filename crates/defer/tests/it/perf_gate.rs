// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-perf-gate" tracker="#181" reason="Compare the defer scheduler numbers with the committed baseline and fail on a regression of more than 20 percent."
const BASELINE: &str = include_str!("../../../../benchmarks/baseline.json");
const README: &str = include_str!("../../../../benchmarks/README.md");
pub(crate) const MAX_REGRESSION: f64 = 0.20;
pub(crate) const RUNS: usize = 9;

pub(crate) struct Ratios {
    pub throughput_vs_relay: f64,
    pub p50_vs_relay: f64,
    pub p99_vs_relay: f64,
}

pub(crate) fn ratios(
    defer_ops: f64,
    relay_ops: f64,
    defer_p50_us: u64,
    relay_p50_us: u64,
    defer_p99_us: u64,
    relay_p99_us: u64,
) -> Ratios {
    Ratios {
        throughput_vs_relay: defer_ops / relay_ops,
        p50_vs_relay: defer_p50_us as f64 / relay_p50_us as f64,
        p99_vs_relay: defer_p99_us as f64 / relay_p99_us as f64,
    }
}

pub(crate) fn compare(baseline: &Ratios, measured: &Ratios) -> Result<(), String> {
    let mut failures = Vec::new();
    if measured.throughput_vs_relay < baseline.throughput_vs_relay * (1.0 - MAX_REGRESSION) {
        failures.push(format!(
            "throughput regressed: baseline ratio {:.4}, measured {:.4}",
            baseline.throughput_vs_relay, measured.throughput_vs_relay
        ));
    }
    if measured.p50_vs_relay > baseline.p50_vs_relay * (1.0 + MAX_REGRESSION) {
        failures.push(format!(
            "p50 regressed: baseline ratio {:.4}, measured {:.4}",
            baseline.p50_vs_relay, measured.p50_vs_relay
        ));
    }
    if measured.p99_vs_relay > baseline.p99_vs_relay * (1.0 + MAX_REGRESSION) {
        failures.push(format!(
            "p99 regressed: baseline ratio {:.4}, measured {:.4}",
            baseline.p99_vs_relay, measured.p99_vs_relay
        ));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn median(values: &mut [f64]) -> f64 {
    assert!(!values.is_empty(), "median of no values");
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values[values.len() / 2]
}

/// The median of each ratio over the runs. `RUNS` is odd, so the median is one run's value.
pub(crate) fn median_ratios(runs: &[Ratios]) -> Ratios {
    let mut throughput: Vec<f64> = runs.iter().map(|r| r.throughput_vs_relay).collect();
    let mut p50: Vec<f64> = runs.iter().map(|r| r.p50_vs_relay).collect();
    let mut p99: Vec<f64> = runs.iter().map(|r| r.p99_vs_relay).collect();
    Ratios {
        throughput_vs_relay: median(&mut throughput),
        p50_vs_relay: median(&mut p50),
        p99_vs_relay: median(&mut p99),
    }
}

fn baseline_ratios() -> Ratios {
    let value: serde_json::Value = serde_json::from_str(BASELINE).unwrap();
    let read = |key: &str| value["ratios"][key].as_f64().unwrap();
    Ratios {
        throughput_vs_relay: read("throughput_vs_relay"),
        p50_vs_relay: read("p50_vs_relay"),
        p99_vs_relay: read("p99_vs_relay"),
    }
}

/// The defer / relay ratio of one metric key in the stored baseline.
fn ratio(defer: &serde_json::Value, relay: &serde_json::Value, key: &str) -> f64 {
    defer[key].as_f64().unwrap() / relay[key].as_f64().unwrap()
}

#[test]
fn compare_passes_for_equal_numbers() {
    let base = Ratios {
        throughput_vs_relay: 1.0,
        p50_vs_relay: 1.0,
        p99_vs_relay: 1.0,
    };
    let same = Ratios {
        throughput_vs_relay: 1.0,
        p50_vs_relay: 1.0,
        p99_vs_relay: 1.0,
    };
    assert!(compare(&base, &same).is_ok());
}

#[test]
fn compare_fails_when_latency_is_doubled() {
    let base = Ratios {
        throughput_vs_relay: 1.0,
        p50_vs_relay: 1.0,
        p99_vs_relay: 1.0,
    };
    let p50_doubled = Ratios {
        throughput_vs_relay: 1.0,
        p50_vs_relay: 2.0,
        p99_vs_relay: 1.0,
    };
    let err = compare(&base, &p50_doubled).unwrap_err();
    assert!(err.contains("p50"), "error names p50: {err}");

    let p99_doubled = Ratios {
        throughput_vs_relay: 1.0,
        p50_vs_relay: 1.0,
        p99_vs_relay: 2.0,
    };
    let err = compare(&base, &p99_doubled).unwrap_err();
    assert!(err.contains("p99"), "error names p99: {err}");
}

#[test]
fn compare_fails_when_throughput_is_halved() {
    let base = Ratios {
        throughput_vs_relay: 1.0,
        p50_vs_relay: 1.0,
        p99_vs_relay: 1.0,
    };
    let halved = Ratios {
        throughput_vs_relay: 0.5,
        p50_vs_relay: 1.0,
        p99_vs_relay: 1.0,
    };
    let err = compare(&base, &halved).unwrap_err();
    assert!(err.contains("throughput"), "error names throughput: {err}");
}

#[test]
fn compare_edge_is_twenty_percent() {
    let base = Ratios {
        throughput_vs_relay: 1.0,
        p50_vs_relay: 1.0,
        p99_vs_relay: 1.0,
    };
    let p50 = |v: f64| Ratios {
        throughput_vs_relay: 1.0,
        p50_vs_relay: v,
        p99_vs_relay: 1.0,
    };
    assert!(compare(&base, &p50(1.19)).is_ok());
    assert!(compare(&base, &p50(1.21)).is_err());

    let throughput = |v: f64| Ratios {
        throughput_vs_relay: v,
        p50_vs_relay: 1.0,
        p99_vs_relay: 1.0,
    };
    assert!(compare(&base, &throughput(0.81)).is_ok());
    assert!(compare(&base, &throughput(0.79)).is_err());
}

#[test]
fn baseline_file_is_complete() {
    let value: serde_json::Value = serde_json::from_str(BASELINE).unwrap();
    assert_eq!(value["workload"]["messages"].as_u64(), Some(1000));
    for section in ["defer", "relay", "ratios"] {
        let object = value[section].as_object().unwrap();
        assert!(!object.is_empty(), "{section} has numbers");
        for (key, number) in object {
            let number = number.as_f64().unwrap();
            assert!(
                number.is_finite() && number > 0.0,
                "{section}.{key} is finite and positive"
            );
        }
    }
    assert!(!value["machine"].as_str().unwrap_or_default().is_empty());
    assert!(!value["recorded"].as_str().unwrap_or_default().is_empty());

    let stored = baseline_ratios();
    let close = |a: f64, b: f64| (a - b).abs() <= 0.01 * b.abs();
    let recomputed_throughput = ratio(&value["defer"], &value["relay"], "throughput_ops_s");
    let recomputed_p50 = ratio(&value["defer"], &value["relay"], "p50_us");
    let recomputed_p99 = ratio(&value["defer"], &value["relay"], "p99_us");
    assert!(close(stored.throughput_vs_relay, recomputed_throughput));
    assert!(close(stored.p50_vs_relay, recomputed_p50));
    assert!(close(stored.p99_vs_relay, recomputed_p99));
}

#[test]
fn readme_publishes_the_baseline() {
    let value: serde_json::Value = serde_json::from_str(BASELINE).unwrap();
    let machine = value["machine"].as_str().unwrap();
    assert!(README.contains(machine), "README names the machine");
    for label in [
        "Throughput (ops/s)",
        "p50 batch lifecycle (us)",
        "p99 batch lifecycle (us)",
    ] {
        assert!(README.contains(label), "README has the row {label}");
    }
}

fn run(throughput: f64, p50: f64, p99: f64) -> Ratios {
    Ratios {
        throughput_vs_relay: throughput,
        p50_vs_relay: p50,
        p99_vs_relay: p99,
    }
}

#[test]
fn median_of_odd_count_is_the_middle_value() {
    assert_eq!(median(&mut [3.0, 1.0, 2.0, 5.0, 4.0]), 3.0);
}

#[test]
#[allow(clippy::assertions_on_constants)] // RUNS is a const: the test pins its value on purpose.
fn runs_is_odd_and_at_least_nine() {
    assert_eq!(RUNS % 2, 1, "an odd RUNS makes the median one run's value");
    assert!(
        RUNS >= 9,
        "RUNS is {RUNS}: fewer runs let one noisy runner fail the p99 gate"
    );
}

#[test]
fn median_ratios_ignores_one_slow_run() {
    let runs = vec![
        run(5.0, 0.15, 0.22),
        run(5.0, 0.15, 0.22),
        run(1.0, 0.90, 0.40),
        run(5.0, 0.15, 0.22),
        run(5.0, 0.15, 0.22),
    ];
    let m = median_ratios(&runs);
    assert_eq!(m.throughput_vs_relay, 5.0);
    assert_eq!(m.p50_vs_relay, 0.15);
    assert_eq!(m.p99_vs_relay, 0.22);
}

#[test]
fn median_ratios_passes_gate_when_only_one_run_is_slow() {
    let baseline = run(5.0, 0.15, 0.2187);
    let runs = vec![
        run(5.0, 0.15, 0.22),
        run(5.0, 0.15, 0.22),
        run(5.0, 0.15, 0.34),
        run(5.0, 0.15, 0.22),
        run(5.0, 0.15, 0.22),
    ];
    assert!(compare(&baseline, &median_ratios(&runs)).is_ok());
    // Negative proof: the single slow run alone fails the gate.
    let err = compare(&baseline, &runs[2]).unwrap_err();
    assert!(err.contains("p99"), "one slow run fails on its own: {err}");
}

#[test]
fn median_ratios_still_fails_for_a_real_regression() {
    let baseline = run(5.0, 0.15, 0.2187);
    let runs: Vec<Ratios> = (0..RUNS).map(|_| run(5.0, 0.15, 0.34)).collect();
    let err = compare(&baseline, &median_ratios(&runs)).unwrap_err();
    assert!(err.contains("p99"), "error names p99: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn measured_numbers_stay_within_twenty_percent_of_baseline() {
    if cfg!(debug_assertions) {
        // Debug builds are not comparable with the release baseline.
        return;
    }
    // Runs one after another: parallel runs would share the CPU and disturb each other.
    let mut all = Vec::new();
    let mut reports = Vec::new();
    for _ in 0..RUNS {
        let measured = crate::relay_performance_ceiling::run_workload().await;
        all.push(ratios(
            measured.defer_ops,
            measured.relay_ops,
            measured.defer_p50_us,
            measured.relay_p50_us,
            measured.defer_p99_us,
            measured.relay_p99_us,
        ));
        reports.push(measured.report);
    }
    if let Err(text) = compare(&baseline_ratios(), &median_ratios(&all)) {
        println!("{}", serde_json::to_string_pretty(&reports).unwrap());
        panic!("{text}");
    }
}
// HANDWRITE-END
