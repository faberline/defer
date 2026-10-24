// HANDWRITE-BEGIN gap="missing-generator:logic:defer-crd" tracker="#766" reason="Defer Kubernetes custom resource over shared cluster and backup contracts."
use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "defer.dev",
    version = "v1alpha1",
    kind = "Defer",
    plural = "defers",
    shortname = "dfr",
    namespaced,
    status = "DeferStatus",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.readyReplicas"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct DeferSpec {
    #[serde(flatten)]
    pub cluster: service_k8s::ClusterSpec,
    #[serde(default = "default_storage")]
    pub storage: String,
    #[serde(default)]
    pub storage_class: Option<String>,
    #[serde(default = "default_grace_secs")]
    pub grace_secs: u64,
    #[serde(default)]
    pub log_level: Option<String>,
    /// Data-plane request body size limit (bytes). Requests with
    /// `Content-Length` over it are rejected with 413; streamed bodies are
    /// bounded mid-read. Defaults to 8 MiB, the `defer serve` default
    /// (`DEFER_BODY_LIMIT_BYTES`, #18).
    #[serde(default = "default_body_limit_bytes")]
    pub body_limit_bytes: u64,
    #[serde(default = "default_auth")]
    pub auth: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens_secret_provider_class: Option<String>,
    /// CSI driver name for the `tokensSecretProviderClass` projection.
    /// Defaults to the community `secrets-store.csi.k8s.io`; GKE's managed
    /// Secrets Store add-on registers `secrets-store-gke.csi.k8s.io`, so GKE
    /// instances must set that value (#2456).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens_secret_csi_driver: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_signing_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_signing_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_tls_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_seed_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<DeferBackupSpec>,
    /// Secrets in the instance's namespace used to pull `image`, copied to
    /// `imagePullSecrets` of every pod the operator renders (the serving
    /// StatefulSet and the optional backup CronJob). Empty means the node's
    /// own registry credentials, the previous behavior.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub image_pull_secrets: Vec<LocalObjectReference>,
    /// Which nodes the operator-rendered pods may run on. Unset leaves the
    /// scheduler free to pick any node, the previous behavior.
    ///
    /// The StatefulSet is operator-rendered, so a `kubectl patch` adding a
    /// nodeSelector or toleration is reverted on the next reconcile; without
    /// this field a defer instance cannot be scheduled onto a tainted,
    /// dedicated node pool at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<DeferPlacement>,
    /// Container resource limits for the serving StatefulSet container.
    /// Unset leaves the container unbounded (requests only), the previous
    /// behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<DeferResourceLimits>,
}

/// Container resource limits for the serving StatefulSet container.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeferResourceLimits {
    /// Maximum CPU quantity (e.g. `500m`, `2`). Unset leaves CPU unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    /// Maximum memory quantity (e.g. `512Mi`, `4Gi`). Unset leaves memory unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

/// Node placement for the operator-rendered pods: the serving StatefulSet
/// and the optional backup CronJob. Layered on top of the operator-owned
/// one-pod-per-node anti-affinity, which it never replaces.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeferPlacement {
    /// Pod `nodeSelector`, e.g. `{ "faberline.dev/pool": "defer-server" }`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,
    /// Pod taint tolerations, so a dedicated node pool can carry a taint that
    /// keeps every other workload off it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,
}

/// One entry of `placement.tolerations`, mirroring the Kubernetes
/// `v1.Toleration` fields.
///
/// Declared here rather than reused from `k8s-openapi` because the CRD schema
/// is derived with `schemars`, which `k8s-openapi`'s types do not implement.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    /// The taint key this tolerates. Empty with `operator: Exists` tolerates
    /// every taint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// `Exists` or `Equal`. Unset means `Equal` (the Kubernetes default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    /// The taint value to match. Only meaningful with `operator: Equal`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// `NoSchedule`, `PreferNoSchedule`, or `NoExecute`. Unset tolerates every
    /// effect of the matching taint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,
    /// How long the pod stays bound after the node gains a matching taint.
    /// Only meaningful with `effect: NoExecute`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
}

/// A Secret named in the instance's namespace, mirroring the Kubernetes
/// `v1.LocalObjectReference`. Declared here because the CRD schema is
/// derived with `schemars`, which `k8s-openapi`'s types do not implement.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LocalObjectReference {
    /// Name of a Secret of type `kubernetes.io/dockerconfigjson` in the
    /// instance's namespace.
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeferBackupSpec {
    #[serde(flatten)]
    pub policy: service_backup::ScheduledBackupPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_token_secret: Option<String>,
}

impl std::ops::Deref for DeferBackupSpec {
    type Target = service_backup::ScheduledBackupPolicy;
    fn deref(&self) -> &Self::Target {
        &self.policy
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeferStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub observed_generation: i64,
    #[serde(default)]
    pub ready_replicas: i32,
    #[serde(default)]
    pub desired_replicas: i32,
    #[serde(default)]
    pub message: String,
}

fn default_storage() -> String {
    "10Gi".into()
}
fn default_grace_secs() -> u64 {
    10
}
fn default_body_limit_bytes() -> u64 {
    8 * 1024 * 1024
}
fn default_auth() -> String {
    "off".into()
}

/// The CRD manifest, normalized for structural-schema validation.
pub fn crd_yaml() -> String {
    use kube::CustomResourceExt;
    let mut crd = serde_json::to_value(Defer::crd()).expect("CRD JSON");
    service_k8s::crd::normalize_unsigned_integer_formats(&mut crd);
    let yaml = serde_yaml::to_string(&crd).expect("CRD YAML");
    service_k8s::crd::quote_yaml_1_1_boolean_like_strings(&yaml)
}
// HANDWRITE-END
