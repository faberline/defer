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
    let times: Vec<u64> = counted.iter().map(|s| s.at_ms).collect();

    let up_reason = if covered(&times, now_ms, SCALE_OUT_MS) {
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

    let down = covered(&times, now_ms, SCALE_IN_MS)
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

/// Samples of a disk series whose age is within `window_ms` of `now_ms`.
fn disk_window(
    counted: &[DiskSample],
    now_ms: u64,
    window_ms: u64,
) -> impl Iterator<Item = &DiskSample> + Clone {
    let start = now_ms.saturating_sub(window_ms);
    counted.iter().filter(move |s| s.at_ms >= start)
}

/// True when the sample times cover the whole window ending at `now_ms`.
///
/// `times` is sorted ascending. The anchor is the newest time at or before the
/// window start; it must exist, and from the anchor on every gap and the age of
/// the newest time must be at most `MAX_GAP_MS`.
fn covered(times: &[u64], now_ms: u64, window_ms: u64) -> bool {
    let Some(start) = now_ms.checked_sub(window_ms) else {
        return false;
    };
    let Some(anchor) = times.iter().rposition(|&t| t <= start) else {
        return false;
    };
    let relevant = &times[anchor..];
    let Some(&newest) = relevant.last() else {
        return false;
    };
    if newest > now_ms || now_ms - newest > MAX_GAP_MS {
        return false;
    }
    relevant
        .windows(2)
        .all(|pair| pair[1] - pair[0] <= MAX_GAP_MS)
}

/// Decide at most one shard split or merge for the given disk samples.
///
/// Returns `None` to hold. Split is checked first; merge only when split did
/// not fire.
pub fn decide_disk(
    samples: &[DiskSample],
    now_ms: u64,
    shard_count: u32,
    marks: &DiskMarks,
    max_shards: u32,
    state: &PolicyState,
) -> Option<Change> {
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
    let mut counted: Vec<DiskSample> = samples
        .iter()
        .copied()
        .filter(|s| s.at_ms <= now_ms)
        .filter(|s| match cooldown_end {
            Some(end) => s.at_ms >= end,
            None => true,
        })
        .collect();
    counted.sort_by_key(|s| s.at_ms);
    let times: Vec<u64> = counted.iter().map(|s| s.at_ms).collect();
    let n = shard_count;

    let split = covered(&times, now_ms, DISK_SPLIT_MS)
        && disk_window(&counted, now_ms, DISK_SPLIT_MS)
            .all(|s| s.bytes_per_shard > marks.split_bytes);
    if split {
        let to = n.checked_add(1)?;
        if to > max_shards {
            return None;
        }
        return Some(Change::ShardSplit { from: n, to });
    }

    if n <= 1 {
        return None;
    }
    let merge = covered(&times, now_ms, DISK_MERGE_MS)
        && disk_window(&counted, now_ms, DISK_MERGE_MS).all(|s| {
            let projected = (s.bytes_per_shard as u128 * n as u128) / (n as u128 - 1);
            s.bytes_per_shard < marks.merge_bytes && projected < marks.split_bytes as u128
        });
    if merge {
        return Some(Change::ShardMerge { from: n, to: n - 1 });
    }

    None
}

/// One average of used bytes per shard, with the shard count at that time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskSample {
    pub at_ms: u64,
    pub bytes_per_shard: u64,
    pub shard_count: u32,
}

/// Disk watermarks in bytes per shard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskMarks {
    pub split_bytes: u64,
    pub merge_bytes: u64,
}

impl Default for DiskMarks {
    fn default() -> Self {
        Self {
            split_bytes: 1_073_741_825,
            merge_bytes: 268_435_456,
        }
    }
}

/// Window that must be above the split mark before a shard split.
pub const DISK_SPLIT_MS: u64 = 30_000;
/// Window that must be below the merge mark before a shard merge.
pub const DISK_MERGE_MS: u64 = 1_800_000;

/// One topology change request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    Replicas(ReplicaDecision),
    ShardSplit { from: u32, to: u32 },
    ShardMerge { from: u32, to: u32 },
}

/// Pick the one change to make from a replica decision and a disk decision.
///
/// Any expansion beats any contraction: a `ShardSplit` beats a replica change
/// in either direction, and a replica change beats a `ShardMerge`. Returns
/// exactly one change, or `None` when neither axis asks for one.
pub fn arbitrate(replica: Option<ReplicaDecision>, disk: Option<Change>) -> Option<Change> {
    match (replica, disk) {
        (_, Some(split @ Change::ShardSplit { .. })) => Some(split),
        (Some(d), _) => Some(Change::Replicas(d)),
        (None, other) => other,
    }
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

    /// Disk samples every `step` ms from `start` to `end` (inclusive on a step).
    fn disk(start: u64, end: u64, step: u64, bytes: u64, shards: u32) -> Vec<DiskSample> {
        let mut out = Vec::new();
        let mut at = start;
        while at <= end {
            out.push(DiskSample {
                at_ms: at,
                bytes_per_shard: bytes,
                shard_count: shards,
            });
            at += step;
        }
        out
    }

    fn run_disk(
        s: &[DiskSample],
        now: u64,
        shards: u32,
        marks: &DiskMarks,
        max: u32,
    ) -> Option<Change> {
        decide_disk(s, now, shards, marks, max, &PolicyState::default())
    }

    const GIB: u64 = 1_073_741_824;
    const MIB256: u64 = 268_435_456;

    #[test]
    fn split_when_above_split_watermark() {
        let s = disk(0, 30_000, 10_000, 2_000_000_000, 1);
        assert_eq!(
            run_disk(&s, 30_000, 1, &DiskMarks::default(), 4),
            Some(Change::ShardSplit { from: 1, to: 2 })
        );
    }

    #[test]
    fn split_holds_at_exactly_1_gib() {
        let marks = DiskMarks {
            split_bytes: GIB,
            merge_bytes: MIB256,
        };
        let s = disk(0, 30_000, 10_000, GIB, 1);
        assert_eq!(run_disk(&s, 30_000, 1, &marks, 4), None);
        assert_eq!(DiskMarks::default().split_bytes, 1_073_741_825);
    }

    #[test]
    fn split_holds_at_max_shards() {
        let s = disk(0, 30_000, 10_000, 2_000_000_000, 4);
        assert_eq!(run_disk(&s, 30_000, 4, &DiskMarks::default(), 4), None);
    }

    #[test]
    fn split_holds_when_window_not_covered() {
        let s = disk(10_000, 30_000, 10_000, 2_000_000_000, 1);
        assert_eq!(run_disk(&s, 30_000, 1, &DiskMarks::default(), 4), None);
    }

    #[test]
    fn merge_after_exactly_1800s_with_headroom() {
        let s = disk(0, 1_800_000, 10_000, 100_000_000, 2);
        assert_eq!(
            run_disk(&s, 1_800_000, 2, &DiskMarks::default(), 4),
            Some(Change::ShardMerge { from: 2, to: 1 })
        );
    }

    #[test]
    fn merge_holds_when_window_not_covered() {
        let s = disk(10_000, 1_800_000, 10_000, 100_000_000, 2);
        assert_eq!(run_disk(&s, 1_800_000, 2, &DiskMarks::default(), 4), None);
    }

    #[test]
    fn merge_holds_at_1799s() {
        let s = disk(0, 1_799_000, 1_000, 100_000_000, 2);
        assert_eq!(run_disk(&s, 1_799_000, 2, &DiskMarks::default(), 4), None);
    }

    #[test]
    fn merge_holds_without_post_merge_headroom() {
        let marks = DiskMarks {
            split_bytes: 400_000_000,
            merge_bytes: MIB256,
        };
        let s = disk(0, 1_800_000, 10_000, 250_000_000, 2);
        assert_eq!(run_disk(&s, 1_800_000, 2, &marks, 4), None);
    }

    #[test]
    fn merge_holds_for_single_shard() {
        let s = disk(0, 1_800_000, 10_000, 100_000_000, 1);
        assert_eq!(run_disk(&s, 1_800_000, 1, &DiskMarks::default(), 4), None);
    }

    #[test]
    fn hysteresis_prevents_reversal() {
        let s = disk(0, 1_800_000, 10_000, 600_000_000, 2);
        assert_eq!(run_disk(&s, 1_800_000, 2, &DiskMarks::default(), 4), None);
        let s = disk(0, 30_000, 10_000, 600_000_000, 2);
        assert_eq!(run_disk(&s, 30_000, 2, &DiskMarks::default(), 4), None);
    }

    #[test]
    fn disk_cooldown_and_pending_hold() {
        let s = disk(1_000_000, 1_299_999, 10_000, 2_000_000_000, 1);
        let state = PolicyState {
            converged_at_ms: Some(1_000_000),
            ..Default::default()
        };
        let got = decide_disk(&s, 1_299_999, 1, &DiskMarks::default(), 4, &state);
        assert_eq!(got, None);

        let s = disk(0, 30_000, 10_000, 2_000_000_000, 1);
        let state = PolicyState {
            pending_generation: true,
            ..Default::default()
        };
        let got = decide_disk(&s, 30_000, 1, &DiskMarks::default(), 4, &state);
        assert_eq!(got, None);
    }

    #[test]
    fn arbitrate_orders() {
        let up = ReplicaDecision {
            direction: Direction::Up,
            from: 1,
            to: 2,
            reason: "cpu",
        };
        let down = ReplicaDecision {
            direction: Direction::Down,
            from: 3,
            to: 2,
            reason: "cpu",
        };
        let split = Change::ShardSplit { from: 1, to: 2 };
        let merge = Change::ShardMerge { from: 2, to: 1 };
        assert_eq!(arbitrate(Some(up), Some(split)), Some(split));
        assert_eq!(arbitrate(Some(up), Some(merge)), Some(Change::Replicas(up)));
        assert_eq!(arbitrate(Some(down), Some(split)), Some(split));
        assert_eq!(
            arbitrate(Some(down), Some(merge)),
            Some(Change::Replicas(down))
        );
        assert_eq!(arbitrate(Some(up), None), Some(Change::Replicas(up)));
        assert_eq!(arbitrate(None, Some(merge)), Some(merge));
        assert_eq!(arbitrate(None, None), None);
    }

    #[test]
    fn one_change_per_generation() {
        let up = ReplicaDecision {
            direction: Direction::Up,
            from: 1,
            to: 2,
            reason: "cpu",
        };
        let down = ReplicaDecision {
            direction: Direction::Down,
            from: 3,
            to: 2,
            reason: "cpu",
        };
        let split = Change::ShardSplit { from: 1, to: 2 };
        let merge = Change::ShardMerge { from: 2, to: 1 };
        let cases = [
            (up, split, split),
            (up, merge, Change::Replicas(up)),
            (down, split, split),
            (down, merge, Change::Replicas(down)),
        ];
        for (r, d, expected) in cases {
            assert_eq!(arbitrate(Some(r), Some(d)), Some(expected));
        }
    }
}
// HANDWRITE-END
