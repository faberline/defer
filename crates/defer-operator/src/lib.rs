// HANDWRITE-BEGIN gap="missing-generator:logic:defer-operator" tracker="#766" reason="Feature-gated Defer operator adapter over service-k8s."
pub mod capacity_patch;
pub mod capacity_policy;
pub mod capacity_sample;
pub mod crd;
pub mod membership_driver;
pub mod membership_http;
pub mod reconcile;
pub mod render;
pub mod replica_controller;
pub mod topology_plan;

pub use capacity_patch::{build_patch, CapacityPatch, Evidence};
pub use capacity_policy::{
    arbitrate, decide_disk, decide_replicas, Bounds, Change, Direction, DiskMarks, DiskSample,
    PolicyState, ReplicaDecision, Sample, Watermarks, COOLDOWN_MS, DISK_MERGE_MS, DISK_SPLIT_MS,
    SCALE_IN_MS, SCALE_OUT_MS,
};
pub use capacity_sample::{parse_cpu_quantity, parse_memory_quantity, population_sample};
pub use crd::{
    crd_yaml, DeclaredQueueControl, DeclaredQueuePolicySpec, DeclaredQueueSpec, Defer,
    DeferBackupSpec, DeferPlacement, DeferResourceLimits, DeferSpec, DeferStatus,
    LocalObjectReference, Toleration,
};
pub use membership_driver::{run_steps, ActuatorError, MembershipActuator, RetryPolicy};
pub use membership_http::HttpMembershipActuator;
pub use reconcile::run;
pub use replica_controller::{
    leader_ordinal_from_hint, next_replica_layer, pod_http_url, pod_peer_url, LayerOutcome,
    ReplicaError,
};
pub use topology_plan::{plan_replica_change, MemberSet, PlanError, Step};
// HANDWRITE-END
