// HANDWRITE-BEGIN gap="missing-generator:logic:defer-capacity-sample" tracker="#13" reason="Pure PodMetrics to capacity Sample conversion for the Defer operator."
//! Pure conversion of `metrics.k8s.io` `PodMetrics` JSON into one population
//! `Sample` for the capacity policy. No I/O, no clock.

use crate::capacity_policy::Sample;

/// Parse a CPU quantity to cores: `n` (nano), `u` (micro), `m` (milli) or plain cores.
pub fn parse_cpu_quantity(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, div) = if let Some(n) = s.strip_suffix('n') {
        (n, 1e9)
    } else if let Some(u) = s.strip_suffix('u') {
        (u, 1e6)
    } else if let Some(m) = s.strip_suffix('m') {
        (m, 1000.0)
    } else {
        (s, 1.0)
    };
    let v = num.trim().parse::<f64>().ok()?;
    if v.is_finite() && v >= 0.0 {
        Some(v / div)
    } else {
        None
    }
}

/// Parse a memory quantity to bytes: `Ki`, `Mi`, `Gi` or plain bytes.
pub fn parse_memory_quantity(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, mul): (&str, u64) = if let Some(n) = s.strip_suffix("Gi") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("Mi") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("Ki") {
        (n, 1024)
    } else {
        (s, 1)
    };
    let v = num.trim().parse::<u64>().ok()?;
    v.checked_mul(mul).map(|b| b as f64)
}

/// Sum the CPU cores and memory bytes of one pod's containers.
/// `None` when the pod has no containers or any container value does not parse.
fn pod_usage(pod: &serde_json::Value) -> Option<(f64, f64)> {
    let containers = pod.get("containers")?.as_array()?;
    if containers.is_empty() {
        return None;
    }
    let mut cpu = 0.0;
    let mut mem = 0.0;
    for c in containers {
        let usage = c.get("usage")?;
        cpu += parse_cpu_quantity(usage.get("cpu")?.as_str()?)?;
        mem += parse_memory_quantity(usage.get("memory")?.as_str()?)?;
    }
    Some((cpu, mem))
}

/// Mean over the parsable pods of usage / limit. `None` without limits or pods.
pub fn population_sample(
    at_ms: u64,
    pods: &[serde_json::Value],
    cpu_limit_cores: Option<f64>,
    mem_limit_bytes: Option<f64>,
) -> Option<Sample> {
    let cpu_limit = cpu_limit_cores.filter(|v| *v > 0.0)?;
    let mem_limit = mem_limit_bytes.filter(|v| *v > 0.0)?;
    let usages: Vec<(f64, f64)> = pods.iter().filter_map(pod_usage).collect();
    if usages.is_empty() {
        return None;
    }
    let n = usages.len() as f64;
    let cpu = usages.iter().map(|(c, _)| c / cpu_limit).sum::<f64>() / n;
    let memory = usages.iter().map(|(_, m)| m / mem_limit).sum::<f64>() / n;
    Some(Sample { at_ms, cpu, memory })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod(cpu: &str, mem: &str) -> serde_json::Value {
        json!({"metadata": {"name": "p"}, "containers": [{"name": "defer", "usage": {"cpu": cpu, "memory": mem}}]})
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn cpu_quantity_forms() {
        assert!(close(parse_cpu_quantity("250m").unwrap(), 0.25));
        assert!(close(parse_cpu_quantity("2").unwrap(), 2.0));
        assert!(close(parse_cpu_quantity("1500000n").unwrap(), 0.0015));
        assert!(close(parse_cpu_quantity("1500u").unwrap(), 0.0015));
        assert_eq!(parse_cpu_quantity(""), None);
        assert_eq!(parse_cpu_quantity("abc"), None);
        assert_eq!(parse_cpu_quantity("-1"), None);
    }

    #[test]
    fn memory_quantity_forms() {
        assert!(close(parse_memory_quantity("1Ki").unwrap(), 1024.0));
        assert!(close(parse_memory_quantity("512Mi").unwrap(), 536870912.0));
        assert!(close(parse_memory_quantity("2Gi").unwrap(), 2147483648.0));
        assert!(close(parse_memory_quantity("1000").unwrap(), 1000.0));
        assert_eq!(parse_memory_quantity(""), None);
        assert_eq!(parse_memory_quantity("x"), None);
    }

    #[test]
    fn sample_is_mean_of_pods() {
        let pods = vec![pod("400m", "256Mi"), pod("600m", "768Mi")];
        let s = population_sample(7, &pods, Some(1.0), Some(1073741824.0)).unwrap();
        assert_eq!(s.at_ms, 7);
        assert!(close(s.cpu, 0.5));
        assert!(close(s.memory, 0.5));
    }

    #[test]
    fn sample_sums_containers_of_one_pod() {
        let p = json!({"containers": [
            {"name": "a", "usage": {"cpu": "200m", "memory": "1Mi"}},
            {"name": "b", "usage": {"cpu": "300m", "memory": "1Mi"}}
        ]});
        let s = population_sample(1, &[p], Some(1.0), Some(1073741824.0)).unwrap();
        assert!(close(s.cpu, 0.5));
    }

    #[test]
    fn sample_none_without_limit() {
        let pods = vec![pod("500m", "1Gi")];
        assert!(population_sample(1, &pods, None, Some(1.0)).is_none());
        assert!(population_sample(1, &pods, Some(0.0), Some(1.0)).is_none());
        assert!(population_sample(1, &pods, Some(1.0), None).is_none());
    }

    #[test]
    fn sample_none_without_pods() {
        assert!(population_sample(1, &[], Some(1.0), Some(1.0)).is_none());
    }

    #[test]
    fn sample_skips_bad_pod() {
        let bad = json!({"containers": "nope"});
        let good = pod("500m", "1Gi");
        let s = population_sample(1, &[bad.clone(), good], Some(1.0), Some(2147483648.0)).unwrap();
        assert!(close(s.cpu, 0.5));
        assert!(close(s.memory, 0.5));
        assert!(population_sample(1, &[bad], Some(1.0), Some(2147483648.0)).is_none());
    }
}
// HANDWRITE-END
