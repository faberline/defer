// HANDWRITE-BEGIN gap="missing-generator:logic:defer-capacity-policy" tracker="#13" reason="Pure capacity policy for the Defer operator."
//! Pure capacity policy: CPU and memory samples in, at most one replica
//! change request out.
//!
//! The policy does no I/O and reads no clock. Time enters only as `now_ms`.
//! A `None` result means hold.

/// One population average of CPU and memory, as fractions of the limit.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    pub at_ms: u64,
    pub cpu: f64,
    pub memory: f64,
}

/// High and low watermarks for CPU and memory, as fractions of the limit.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Watermarks {
    pub cpu_high: f64,
    pub cpu_low: f64,
    pub mem_high: f64,
    pub mem_low: f64,
}

impl Default for Watermarks {
    fn default() -> Self {
        Self {
            cpu_high: 0.80,
            cpu_low: 0.30,
            mem_high: 0.80,
            mem_low: 0.30,
        }
    }
}

/// Lowest and highest replica count the policy may request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    pub floor: u32,
    pub ceiling: u32,
}

/// State of the policy between two decisions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PolicyState {
    pub generation: i64,
    pub converged_at_ms: Option<u64>,
    pub pending_generation: bool,
}

/// Direction of a replica change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}

/// One replica change request. `reason` is `"cpu"` or `"memory"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaDecision {
    pub direction: Direction,
    pub from: u32,
    pub to: u32,
    pub reason: &'static str,
}

/// Window that must be above a high watermark before a scale-out.
pub const SCALE_OUT_MS: u64 = 120_000;
/// Window that must be below both low watermarks before a scale-in.
pub const SCALE_IN_MS: u64 = 600_000;
/// Time after a converged generation during which no decision is made.
pub const COOLDOWN_MS: u64 = 300_000;

/// Largest allowed gap between neighbouring samples, and the largest age of
/// the newest sample.
const MAX_GAP_MS: u64 = 30_000;

/// Decide at most one replica change for the given samples.
///
/// Returns `None` to hold.
pub fn decide_replicas(
    samples: &[Sample],
    now_ms: u64,
    current: u32,
    marks: &Watermarks,
    bounds: &Bounds,
    state: &PolicyState,
) -> Option<ReplicaDecision> {
    if state.pending_generation {
        return None;
    }
    let cooldown_end = state.converged_at_ms.map(|c| c.saturating_add(COOLDOWN_MS));
    if let Some(end) = cooldown_end {
        if now_ms < end {
            return None;
        }
    }

    // Samples before the cooldown end never count.
    let mut counted: Vec<Sample> = samples
        .iter()
        .copied()
        .filter(|s| s.at_ms <= now_ms)
        .filter(|s| match cooldown_end {
            Some(end) => s.at_ms >= end,
            None => true,
        })
        .collect();
    counted.sort_by_key(|s| s.at_ms);

    let up_reason = if window_ok(&counted, now_ms, SCALE_OUT_MS) {
        let window = in_window(&counted, now_ms, SCALE_OUT_MS);
        if window.clone().all(|s| s.cpu > marks.cpu_high) {
            Some("cpu")
        } else if window.clone().all(|s| s.memory > marks.mem_high) {
            Some("memory")
        } else {
            None
        }
    } else {
        None
    };

    if let Some(reason) = up_reason {
        let to = current.checked_add(1)?;
        if to > bounds.ceiling {
            return None;
        }
        return Some(ReplicaDecision {
            direction: Direction::Up,
            from: current,
            to,
            reason,
        });
    }

    let down = window_ok(&counted, now_ms, SCALE_IN_MS)
        && in_window(&counted, now_ms, SCALE_IN_MS)
            .all(|s| s.cpu < marks.cpu_low && s.memory < marks.mem_low);
    if down {
        let to = current.checked_sub(1)?;
        if to < bounds.floor {
            return None;
        }
        return Some(ReplicaDecision {
            direction: Direction::Down,
            from: current,
            to,
            reason: "cpu",
        });
    }

    None
}

/// Samples whose age is within `window_ms` of `now_ms`.
fn in_window(
    counted: &[Sample],
    now_ms: u64,
    window_ms: u64,
) -> impl Iterator<Item = &Sample> + Clone {
    let start = now_ms.saturating_sub(window_ms);
    counted.iter().filter(move |s| s.at_ms >= start)
}

/// True when the samples cover the whole window ending at `now_ms`.
///
/// The anchor is the newest sample at or before the window start; it must
/// exist, and from the anchor on every gap and the age of the newest sample
/// must be at most `MAX_GAP_MS`.
fn window_ok(counted: &[Sample], now_ms: u64, window_ms: u64) -> bool {
    let Some(start) = now_ms.checked_sub(window_ms) else {
        return false;
    };
    let Some(anchor) = counted.iter().rposition(|s| s.at_ms <= start) else {
        return false;
    };
    let relevant = &counted[anchor..];
    let Some(newest) = relevant.last() else {
        return false;
    };
    if newest.at_ms > now_ms || now_ms - newest.at_ms > MAX_GAP_MS {
        return false;
    }
    relevant
        .windows(2)
        .all(|pair| pair[1].at_ms - pair[0].at_ms <= MAX_GAP_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Samples every `step` ms from `start` to `end` (inclusive when on a step).
    fn samples(start: u64, end: u64, step: u64, cpu: f64, mem: f64) -> Vec<Sample> {
        let mut out = Vec::new();
        let mut at = start;
        while at <= end {
            out.push(Sample {
                at_ms: at,
                cpu,
                memory: mem,
            });
            at += step;
        }
        out
    }

    fn bounds() -> Bounds {
        Bounds {
            floor: 1,
            ceiling: 5,
        }
    }

    fn run(s: &[Sample], now: u64, current: u32) -> Option<ReplicaDecision> {
        decide_replicas(
            s,
            now,
            current,
            &Watermarks::default(),
            &bounds(),
            &PolicyState::default(),
        )
    }

    fn up(from: u32, reason: &'static str) -> Option<ReplicaDecision> {
        Some(ReplicaDecision {
            direction: Direction::Up,
            from,
            to: from + 1,
            reason,
        })
    }

    fn down(from: u32, reason: &'static str) -> Option<ReplicaDecision> {
        Some(ReplicaDecision {
            direction: Direction::Down,
            from,
            to: from - 1,
            reason,
        })
    }

    #[test]
    fn up_after_exactly_120s_above_high() {
        let s = samples(0, 120_000, 10_000, 0.90, 0.50);
        assert_eq!(run(&s, 120_000, 1), up(1, "cpu"));
    }

    #[test]
    fn up_holds_when_window_not_covered() {
        let s = samples(10_000, 120_000, 10_000, 0.90, 0.50);
        assert_eq!(run(&s, 120_000, 1), None);
    }

    #[test]
    fn up_holds_when_now_is_before_window_length() {
        let s = samples(0, 110_000, 10_000, 0.90, 0.50);
        assert_eq!(run(&s, 110_000, 1), None);
    }

    #[test]
    fn down_after_exactly_600s_below_low() {
        let s = samples(0, 600_000, 10_000, 0.10, 0.10);
        assert_eq!(run(&s, 600_000, 3), down(3, "cpu"));
    }

    #[test]
    fn down_holds_before_600s() {
        let s = samples(0, 600_000, 10_000, 0.10, 0.10);
        assert_eq!(run(&s, 599_999, 3), None);
    }

    #[test]
    fn down_holds_when_window_not_covered() {
        let s = samples(10_000, 600_000, 10_000, 0.10, 0.10);
        assert_eq!(run(&s, 600_000, 3), None);
    }

    #[test]
    fn deadband_holds() {
        let s = samples(0, 600_000, 10_000, 0.50, 0.50);
        assert_eq!(run(&s, 600_000, 3), None);
    }

    #[test]
    fn exact_watermark_is_not_above() {
        let s = samples(0, 120_000, 10_000, 0.80, 0.50);
        assert_eq!(run(&s, 120_000, 1), None);
    }

    #[test]
    fn one_in_band_sample_resets_window() {
        let mut s = samples(0, 100_000, 10_000, 0.90, 0.50);
        s.push(Sample {
            at_ms: 110_000,
            cpu: 0.50,
            memory: 0.50,
        });
        s.extend(samples(120_000, 120_000, 10_000, 0.90, 0.50));
        assert_eq!(run(&s, 120_000, 1), None);
    }

    #[test]
    fn memory_alone_scales_up() {
        let s = samples(0, 120_000, 10_000, 0.50, 0.95);
        assert_eq!(run(&s, 120_000, 1), up(1, "memory"));
    }

    #[test]
    fn down_needs_both_low() {
        let s = samples(0, 600_000, 10_000, 0.10, 0.50);
        assert_eq!(run(&s, 600_000, 3), None);
    }

    #[test]
    fn up_beats_down() {
        let s = samples(0, 600_000, 10_000, 0.10, 0.95);
        assert_eq!(run(&s, 600_000, 3), up(3, "memory"));
    }

    #[test]
    fn ceiling_holds() {
        let s = samples(0, 120_000, 10_000, 0.90, 0.50);
        assert_eq!(run(&s, 120_000, 5), None);
    }

    #[test]
    fn floor_holds() {
        let s = samples(0, 600_000, 10_000, 0.10, 0.10);
        assert_eq!(run(&s, 600_000, 1), None);
    }

    #[test]
    fn stale_gap_holds() {
        let mut s = samples(0, 50_000, 10_000, 0.90, 0.50);
        s.extend(samples(110_000, 120_000, 10_000, 0.90, 0.50));
        assert_eq!(run(&s, 120_000, 1), None);
    }

    #[test]
    fn stale_newest_sample_holds() {
        let s = samples(0, 100_000, 10_000, 0.90, 0.50);
        assert_eq!(run(&s, 140_000, 1), None);
    }

    #[test]
    fn pending_generation_holds() {
        let s = samples(0, 120_000, 10_000, 0.90, 0.50);
        let state = PolicyState {
            pending_generation: true,
            ..Default::default()
        };
        let got = decide_replicas(&s, 120_000, 1, &Watermarks::default(), &bounds(), &state);
        assert_eq!(got, None);
    }

    #[test]
    fn cooldown_holds_until_300s() {
        let s = samples(1_000_000, 1_299_999, 10_000, 0.90, 0.50);
        let state = PolicyState {
            converged_at_ms: Some(1_000_000),
            ..Default::default()
        };
        let got = decide_replicas(&s, 1_299_999, 1, &Watermarks::default(), &bounds(), &state);
        assert_eq!(got, None);
    }

    #[test]
    fn pre_cooldown_samples_never_count() {
        let state = PolicyState {
            converged_at_ms: Some(1_000_000),
            ..Default::default()
        };
        let s = samples(1_000_000, 1_400_000, 10_000, 0.90, 0.50);
        let got = decide_replicas(&s, 1_400_000, 1, &Watermarks::default(), &bounds(), &state);
        assert_eq!(got, None);

        let s = samples(1_000_000, 1_420_000, 10_000, 0.90, 0.50);
        let got = decide_replicas(&s, 1_420_000, 1, &Watermarks::default(), &bounds(), &state);
        assert_eq!(got, up(1, "cpu"));
    }

    #[test]
    fn empty_samples_hold() {
        assert_eq!(run(&[], 120_000, 1), None);
    }
}
// HANDWRITE-END
