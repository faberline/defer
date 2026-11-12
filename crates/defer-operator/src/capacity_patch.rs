// HANDWRITE-BEGIN gap="missing-generator:logic:defer-capacity-patch" tracker="#13" reason="Generation-bound Defer CR patch for the capacity policy."
//! Generation-bound CR patch for a capacity change.
//!
//! This module turns one [`Change`] into a merge patch for the Defer custom
//! resource, guarded by the generation the decision was made against, plus the
//! evidence that explains it. It does no I/O, reads no clock and calls no
//! actuator or kube client.

use crate::capacity_policy::Change;
use serde::Serialize;

/// Why a capacity change was decided. Serialized as the evidence record.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Evidence {
    pub direction: &'static str,
    pub reason: &'static str,
    pub population: u32,
    pub window_ms: u64,
    pub watermark: f64,
    pub observed: f64,
    pub generation: i64,
}

/// A merge patch for the Defer CR and the generation it must be applied on.
#[derive(Clone, Debug, PartialEq)]
pub struct CapacityPatch {
    pub merge_patch: serde_json::Value,
    pub resource_version_guard: i64,
    pub evidence: Evidence,
}

/// Build the CR patch for one change, or `None` when no patch is produced.
///
/// A `Change::Replicas` becomes `{"spec": {"replicasPerShard": to}}`, guarded
/// by `generation`. Shard-count patches are not produced while `shardCount` is
/// pinned to 1 in `render.rs`: `ShardSplit` and `ShardMerge` return `None`, and
/// the decision stays in the policy result. #83 wires them.
///
/// The evidence's `generation` is always set to `generation`, so the two cannot
/// disagree.
pub fn build_patch(change: &Change, generation: i64, evidence: Evidence) -> Option<CapacityPatch> {
    match change {
        Change::Replicas(decision) => Some(CapacityPatch {
            merge_patch: serde_json::json!({"spec": {"replicasPerShard": decision.to}}),
            resource_version_guard: generation,
            evidence: Evidence {
                generation,
                ..evidence
            },
        }),
        Change::ShardSplit { .. } | Change::ShardMerge { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity_policy::{Direction, ReplicaDecision};

    fn ev(generation: i64) -> Evidence {
        Evidence {
            direction: "up",
            reason: "cpu",
            population: 3,
            window_ms: 120_000,
            watermark: 0.8,
            observed: 0.9,
            generation,
        }
    }

    #[test]
    fn replica_up_patch_shape() {
        let change = Change::Replicas(ReplicaDecision {
            direction: Direction::Up,
            from: 1,
            to: 2,
            reason: "cpu",
        });
        let patch = build_patch(&change, 7, ev(7)).expect("replica change gives a patch");
        assert_eq!(
            patch.merge_patch,
            serde_json::json!({"spec": {"replicasPerShard": 2}})
        );
        assert_eq!(patch.resource_version_guard, 7);
    }

    #[test]
    fn replica_down_patch_shape() {
        let change = Change::Replicas(ReplicaDecision {
            direction: Direction::Down,
            from: 3,
            to: 2,
            reason: "memory",
        });
        let patch = build_patch(&change, 9, ev(9)).expect("replica change gives a patch");
        assert_eq!(
            patch.merge_patch,
            serde_json::json!({"spec": {"replicasPerShard": 2}})
        );
        assert_eq!(patch.resource_version_guard, 9);
    }

    #[test]
    fn shard_change_has_no_patch() {
        let split = Change::ShardSplit { from: 1, to: 2 };
        let merge = Change::ShardMerge { from: 2, to: 1 };
        assert!(build_patch(&split, 7, ev(7)).is_none());
        assert!(build_patch(&merge, 7, ev(7)).is_none());
    }

    #[test]
    fn evidence_serializes_all_fields() {
        let value = serde_json::to_value(ev(7)).expect("evidence serializes");
        let object = value.as_object().expect("evidence is an object");
        let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "direction",
                "generation",
                "observed",
                "population",
                "reason",
                "watermark",
                "window_ms",
            ]
        );
        assert_eq!(object["generation"], serde_json::json!(7));
    }

    #[test]
    fn evidence_generation_follows_argument() {
        let change = Change::Replicas(ReplicaDecision {
            direction: Direction::Up,
            from: 1,
            to: 2,
            reason: "cpu",
        });
        let patch = build_patch(&change, 7, ev(99)).expect("replica change gives a patch");
        assert_eq!(patch.evidence.generation, 7);
    }

    #[test]
    fn patch_is_idempotent() {
        let change = Change::Replicas(ReplicaDecision {
            direction: Direction::Up,
            from: 1,
            to: 2,
            reason: "cpu",
        });
        let first = build_patch(&change, 7, ev(7));
        let second = build_patch(&change, 7, ev(7));
        assert_eq!(first, second);
    }

    #[test]
    fn patch_independent_of_actuator() {
        let change = Change::Replicas(ReplicaDecision {
            direction: Direction::Up,
            from: 1,
            to: 2,
            reason: "cpu",
        });
        assert!(build_patch(&change, 7, ev(7)).is_some());
    }
}
// HANDWRITE-END
