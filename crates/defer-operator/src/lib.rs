// HANDWRITE-BEGIN gap="missing-generator:logic:defer-operator" tracker="#766" reason="Feature-gated Defer operator adapter over service-k8s."
pub mod crd;
pub mod reconcile;
pub mod render;
pub mod topology_plan;

pub use crd::{
    crd_yaml, DeclaredQueueControl, DeclaredQueuePolicySpec, DeclaredQueueSpec, Defer,
    DeferBackupSpec, DeferPlacement, DeferResourceLimits, DeferSpec, DeferStatus,
    LocalObjectReference, Toleration,
};
pub use reconcile::run;
pub use topology_plan::{plan_replica_change, MemberSet, PlanError, Step};
// HANDWRITE-END
