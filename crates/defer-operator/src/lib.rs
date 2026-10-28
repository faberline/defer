// HANDWRITE-BEGIN gap="missing-generator:logic:defer-operator" tracker="#766" reason="Feature-gated Defer operator adapter over service-k8s."
pub mod crd;
pub mod reconcile;
pub mod render;

pub use crd::{
    crd_yaml, DeclaredQueueControl, DeclaredQueuePolicySpec, DeclaredQueueSpec, Defer,
    DeferBackupSpec, DeferPlacement, DeferSpec, DeferStatus, LocalObjectReference, Toleration,
};
pub use reconcile::run;
// HANDWRITE-END
