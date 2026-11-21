// HANDWRITE-BEGIN gap="missing-generator:logic:defer-capacity-loop" tracker="#13" reason="Capacity controller loop of the Defer operator."
//! Capacity controller loop: reads pod metrics per Defer, decides the replica
//! count, and patches `spec.replicasPerShard` behind a generation guard.

use std::collections::HashMap;
use std::future::Future;

use crate::capacity_patch::{build_patch, CapacityPatch, Evidence};
use crate::capacity_policy::{
    arbitrate, decide_disk, decide_replicas, Change, Direction, DiskMarks, DiskSample, PolicyState,
    Sample, DISK_MERGE_MS, DISK_SPLIT_MS, SCALE_IN_MS, SCALE_OUT_MS,
};
use crate::capacity_sample::{
    disk_sample, parse_cpu_quantity, parse_memory_quantity, population_sample,
};
use crate::crd::{Defer, MAX_SHARD_COUNT};

/// Error of one capacity patch.
#[derive(Debug)]
pub enum PatchError {
    Conflict,
    Other(String),
}

/// The I/O of the loop. The tests use a fake; the operator uses `KubeCapacityIo`.
pub trait CapacityIo {
    fn defers(&self) -> impl Future<Output = Vec<Defer>> + Send;
    fn pod_metrics(&self, defer: &Defer) -> impl Future<Output = Vec<serde_json::Value>> + Send;
    fn disk_used_bytes(&self, defer: &Defer) -> impl Future<Output = Vec<u64>> + Send;
    fn fresh_generation(&self, defer: &Defer) -> impl Future<Output = Option<i64>> + Send;
    fn patch(
        &self,
        defer: &Defer,
        patch: &CapacityPatch,
    ) -> impl Future<Output = Result<(), PatchError>> + Send;
}

#[derive(Default)]
struct PerDefer {
    samples: Vec<Sample>,
    disk_samples: Vec<DiskSample>,
    state: PolicyState,
    awaiting: Option<i64>,
}

/// Samples and policy state of every Defer, keyed by `(namespace, name)`.
#[derive(Default)]
pub struct CapacityLoop {
    per: HashMap<(String, String), PerDefer>,
}

fn key_of(defer: &Defer) -> (String, String) {
    (
        defer.metadata.namespace.clone().unwrap_or_default(),
        defer.metadata.name.clone().unwrap_or_default(),
    )
}

impl CapacityLoop {
    pub fn new() -> Self {
        Self::default()
    }

    /// One pass over every Defer. A problem with one Defer never stops the others.
    pub async fn tick(&mut self, io: &impl CapacityIo, now_ms: u64) {
        let defers = io.defers().await;
        let live: Vec<(String, String)> = defers.iter().map(key_of).collect();
        self.per.retain(|key, _| live.contains(key));

        for defer in &defers {
            let Some(policy) = defer.spec.capacity_policy.as_ref() else {
                continue;
            };
            let name = defer.metadata.name.clone().unwrap_or_default();
            let generation = defer.metadata.generation.unwrap_or(0);
            let entry = self.per.entry(key_of(defer)).or_default();

            // Convergence: the patched generation is observed and its replicas are ready.
            if let (true, Some(g), Some(status)) = (
                entry.state.pending_generation,
                entry.awaiting,
                defer.status.as_ref(),
            ) {
                if status.observed_generation >= g
                    && status.ready_replicas == status.desired_replicas
                {
                    entry.state.pending_generation = false;
                    entry.state.converged_at_ms = Some(now_ms);
                    entry.state.generation = g;
                    entry.awaiting = None;
                }
            }

            let cpu_limit = defer
                .spec
                .limits
                .as_ref()
                .and_then(|l| l.cpu.as_deref())
                .and_then(parse_cpu_quantity);
            let mem_limit = defer
                .spec
                .limits
                .as_ref()
                .and_then(|l| l.memory.as_deref())
                .and_then(parse_memory_quantity);

            let pods = io.pod_metrics(defer).await;
            if let Some(sample) = population_sample(now_ms, &pods, cpu_limit, mem_limit) {
                entry.samples.push(sample);
            }
            entry
                .samples
                .retain(|s| s.at_ms + SCALE_IN_MS + 60_000 >= now_ms);

            let current_shards = defer.spec.cluster.shard_count;
            let used = io.disk_used_bytes(defer).await;
            if let Some(sample) = disk_sample(now_ms, &used, current_shards) {
                entry.disk_samples.push(sample);
            }
            entry
                .disk_samples
                .retain(|s| s.at_ms + DISK_MERGE_MS + 60_000 >= now_ms);

            let current = defer.spec.cluster.replicas_per_shard;
            let marks = policy.watermarks();
            let bounds = policy.bounds(current);
            let replica = decide_replicas(
                &entry.samples,
                now_ms,
                current,
                &marks,
                &bounds,
                &entry.state,
            );
            let disk_marks = DiskMarks::default();
            let disk = decide_disk(
                &entry.disk_samples,
                now_ms,
                current_shards,
                &disk_marks,
                MAX_SHARD_COUNT,
                &entry.state,
            );
            let Some(change) = arbitrate(replica, disk) else {
                continue;
            };

            let population = pods.len() as u32;
            let evidence = match change {
                Change::Replicas(decision) => {
                    let Some(newest) = entry.samples.last().copied() else {
                        continue;
                    };
                    let (direction, window_ms, watermark) = match decision.direction {
                        Direction::Up => {
                            let mark = if decision.reason == "cpu" {
                                marks.cpu_high
                            } else {
                                marks.mem_high
                            };
                            ("up", SCALE_OUT_MS, mark)
                        }
                        Direction::Down => ("down", SCALE_IN_MS, marks.cpu_low),
                    };
                    let observed = if decision.reason == "cpu" {
                        newest.cpu
                    } else {
                        newest.memory
                    };
                    Evidence {
                        direction,
                        reason: decision.reason,
                        population,
                        window_ms,
                        watermark,
                        observed,
                        generation,
                    }
                }
                Change::ShardSplit { .. } | Change::ShardMerge { .. } => {
                    let Some(newest) = entry.disk_samples.last().copied() else {
                        continue;
                    };
                    let (direction, window_ms, watermark) =
                        if matches!(change, Change::ShardSplit { .. }) {
                            ("split", DISK_SPLIT_MS, disk_marks.split_bytes)
                        } else {
                            ("merge", DISK_MERGE_MS, disk_marks.merge_bytes)
                        };
                    Evidence {
                        direction,
                        reason: "disk",
                        population,
                        window_ms,
                        watermark: watermark as f64,
                        observed: newest.bytes_per_shard as f64,
                        generation,
                    }
                }
            };
            let Some(patch) = build_patch(&change, generation, evidence) else {
                continue;
            };

            // Guard: a stale generation never patches.
            if io.fresh_generation(defer).await != Some(generation) {
                continue;
            }
            match io.patch(defer, &patch).await {
                Ok(()) => {
                    entry.state.pending_generation = true;
                    entry.awaiting = Some(generation + 1);
                }
                Err(PatchError::Conflict) => {
                    tracing::debug!(%name, "capacity patch conflict; the next tick may retry");
                }
                Err(PatchError::Other(message)) => {
                    tracing::warn!(%name, %message, "capacity patch failed");
                }
            }
        }
    }
}

/// Runs the loop every 10 s with the wall clock. It never returns.
pub async fn run_capacity_loop(client: kube::Client) {
    let io = KubeCapacityIo { client };
    let mut state = CapacityLoop::new();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
    loop {
        interval.tick().await;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        state.tick(&io, now_ms).await;
    }
}

/// The real I/O over `kube::Client`.
struct KubeCapacityIo {
    client: kube::Client,
}

impl KubeCapacityIo {
    /// The kubelet `stats/summary` of one node, through the API server proxy.
    async fn node_summary(&self, node: &str) -> anyhow::Result<serde_json::Value> {
        let req = kube::core::Request::new("/api/v1/nodes")
            .get_subresource("proxy/stats/summary", node)?;
        let text = self.client.request_text(req).await?;
        Ok(serde_json::from_str::<serde_json::Value>(&text)?)
    }
}

impl CapacityIo for KubeCapacityIo {
    async fn defers(&self) -> Vec<Defer> {
        let api = kube::Api::<Defer>::all(self.client.clone());
        match api.list(&kube::api::ListParams::default()).await {
            Ok(list) => list.items,
            Err(e) => {
                tracing::warn!(error = %e, "capacity loop: listing Defer failed");
                Vec::new()
            }
        }
    }

    async fn pod_metrics(&self, defer: &Defer) -> Vec<serde_json::Value> {
        let ns = defer.metadata.namespace.clone().unwrap_or_default();
        let name = defer.metadata.name.clone().unwrap_or_default();
        let gvk = kube::api::GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics");
        let ar = kube::api::ApiResource::from_gvk_with_plural(&gvk, "pods");
        let api: kube::Api<kube::api::DynamicObject> =
            kube::Api::namespaced_with(self.client.clone(), &ns, &ar);
        let selector = format!("app.kubernetes.io/name=defer,app.kubernetes.io/instance={name}");
        let params = kube::api::ListParams::default().labels(&selector);
        match api.list(&params).await {
            Ok(list) => list
                .items
                .iter()
                .filter_map(|item| serde_json::to_value(item).ok())
                .collect(),
            Err(e) => {
                tracing::warn!(%name, error = %e, "capacity loop: pod metrics unavailable");
                Vec::new()
            }
        }
    }

    async fn disk_used_bytes(&self, defer: &Defer) -> Vec<u64> {
        let ns = defer.metadata.namespace.clone().unwrap_or_default();
        let name = defer.metadata.name.clone().unwrap_or_default();
        let selector = format!("app.kubernetes.io/name=defer,app.kubernetes.io/instance={name}");
        let pods =
            kube::Api::<k8s_openapi::api::core::v1::Pod>::namespaced(self.client.clone(), &ns)
                .list(&kube::api::ListParams::default().labels(&selector))
                .await;
        let pods = match pods {
            Ok(list) => list,
            Err(e) => {
                tracing::warn!(%name, error = %e, "capacity loop: disk usage unavailable");
                return Vec::new();
            }
        };
        let mut by_node: std::collections::BTreeMap<String, Vec<String>> =
            std::collections::BTreeMap::new();
        for pod in &pods.items {
            let node = pod.spec.as_ref().and_then(|s| s.node_name.clone());
            let pod_name = pod.metadata.name.clone();
            if let (Some(node), Some(pod_name)) = (node, pod_name) {
                by_node.entry(node).or_default().push(pod_name);
            }
        }
        let mut out = Vec::new();
        for (node, names) in &by_node {
            match self.node_summary(node).await {
                Ok(summary) => {
                    out.extend(crate::capacity_sample::volume_used_bytes(&summary, names));
                }
                Err(e) => {
                    tracing::warn!(%name, %node, error = %e, "capacity loop: disk usage unavailable");
                }
            }
        }
        out
    }

    async fn fresh_generation(&self, defer: &Defer) -> Option<i64> {
        let ns = defer.metadata.namespace.clone().unwrap_or_default();
        let name = defer.metadata.name.clone().unwrap_or_default();
        kube::Api::<Defer>::namespaced(self.client.clone(), &ns)
            .get(&name)
            .await
            .ok()
            .and_then(|d| d.metadata.generation)
    }

    async fn patch(&self, defer: &Defer, patch: &CapacityPatch) -> Result<(), PatchError> {
        let ns = defer.metadata.namespace.clone().unwrap_or_default();
        let name = defer.metadata.name.clone().unwrap_or_default();
        let api = kube::Api::<Defer>::namespaced(self.client.clone(), &ns);
        let params = kube::api::PatchParams::default();
        match api
            .patch(&name, &params, &kube::api::Patch::Merge(&patch.merge_patch))
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 409 => Err(PatchError::Conflict),
            Err(e) => Err(PatchError::Other(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{Defer, DeferStatus};
    use serde_json::json;
    use std::sync::Mutex;

    struct Fake {
        defers: Mutex<Vec<Defer>>,
        cpu_per_pod: Mutex<String>,
        fresh: Mutex<Option<i64>>,
        patch_result: Mutex<Result<(), ()>>,
        patches: Mutex<Vec<serde_json::Value>>,
        metrics_calls: Mutex<usize>,
        disk_bytes: Mutex<Vec<u64>>,
    }

    impl Default for Fake {
        fn default() -> Self {
            Fake {
                defers: Mutex::new(Vec::new()),
                cpu_per_pod: Mutex::new(String::new()),
                fresh: Mutex::new(None),
                patch_result: Mutex::new(Ok(())),
                patches: Mutex::new(Vec::new()),
                metrics_calls: Mutex::new(0),
                disk_bytes: Mutex::new(Vec::new()),
            }
        }
    }

    impl CapacityIo for Fake {
        async fn defers(&self) -> Vec<Defer> {
            self.defers.lock().unwrap().clone()
        }

        async fn pod_metrics(&self, _defer: &Defer) -> Vec<serde_json::Value> {
            *self.metrics_calls.lock().unwrap() += 1;
            let cpu = self.cpu_per_pod.lock().unwrap().clone();
            let pod = json!({"containers": [{"name": "defer", "usage": {"cpu": cpu, "memory": "100Mi"}}]});
            vec![pod.clone(), pod]
        }

        async fn disk_used_bytes(&self, _defer: &Defer) -> Vec<u64> {
            self.disk_bytes.lock().unwrap().clone()
        }

        async fn fresh_generation(&self, defer: &Defer) -> Option<i64> {
            match *self.fresh.lock().unwrap() {
                Some(g) => Some(g),
                None => defer.metadata.generation,
            }
        }

        async fn patch(&self, _defer: &Defer, patch: &CapacityPatch) -> Result<(), PatchError> {
            self.patches.lock().unwrap().push(patch.merge_patch.clone());
            match *self.patch_result.lock().unwrap() {
                Ok(()) => Ok(()),
                Err(()) => Err(PatchError::Conflict),
            }
        }
    }

    fn defer(replicas: u32, generation: i64, policy: bool) -> Defer {
        let mut spec = json!({
            "image": "registry/defer:1",
            "imagePullPolicy": "Always",
            "replicasPerShard": replicas,
            "voterCount": 3,
            "storage": "100Gi",
            "auth": "required",
            "tokensSecret": "defer-tokens",
            "targetSigningSecret": "target-signing",
            "targetSigningKeyId": "active",
            "peerTlsSecret": "peer-tls",
            "limits": {"cpu": "1", "memory": "1Gi"}
        });
        if policy {
            spec["capacityPolicy"] = json!({"ceiling": 4});
        }
        serde_json::from_value(json!({
            "apiVersion": "defer.dev/v1alpha1",
            "kind": "Defer",
            "metadata": {"name": "jobs", "namespace": "payments", "uid": "u-1", "generation": generation},
            "spec": spec
        }))
        .unwrap()
    }

    fn fake(defer: Defer, cpu: &str) -> Fake {
        let fake = Fake::default();
        *fake.defers.lock().unwrap() = vec![defer];
        *fake.cpu_per_pod.lock().unwrap() = cpu.to_string();
        fake
    }

    async fn run_window(loop_: &mut CapacityLoop, fake: &Fake) {
        for t in (0..=130_000u64).step_by(10_000) {
            loop_.tick(fake, t).await;
        }
    }

    fn patches(fake: &Fake) -> Vec<serde_json::Value> {
        fake.patches.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn high_cpu_patches_replicas_inside_ceiling() {
        let fake = fake(defer(2, 3, true), "900m");
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        let got = patches(&fake);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], json!({"spec": {"replicasPerShard": 3}}));
    }

    #[tokio::test]
    async fn ceiling_blocks_patch() {
        let fake = fake(defer(4, 3, true), "900m");
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        assert_eq!(patches(&fake).len(), 0);
    }

    #[tokio::test]
    async fn stale_generation_does_not_patch() {
        let fake = fake(defer(2, 3, true), "900m");
        *fake.fresh.lock().unwrap() = Some(4);
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        assert_eq!(patches(&fake).len(), 0);
    }

    #[tokio::test]
    async fn no_capacity_policy_no_call() {
        let fake = fake(defer(2, 3, false), "900m");
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        assert_eq!(patches(&fake).len(), 0);
        assert_eq!(*fake.metrics_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn pending_generation_holds() {
        let fake = fake(defer(2, 3, true), "900m");
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        lp.tick(&fake, 140_000).await;
        lp.tick(&fake, 150_000).await;
        assert_eq!(patches(&fake).len(), 1);
    }

    #[tokio::test]
    async fn conflict_is_not_an_error() {
        let fake = fake(defer(2, 3, true), "900m");
        *fake.patch_result.lock().unwrap() = Err(());
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        assert_eq!(patches(&fake).len(), 2);
        *fake.patch_result.lock().unwrap() = Ok(());
        lp.tick(&fake, 140_000).await;
        assert_eq!(patches(&fake).len(), 3);
        lp.tick(&fake, 150_000).await;
        assert_eq!(patches(&fake).len(), 3);
    }

    #[tokio::test]
    async fn converged_clears_pending() {
        let fake = fake(defer(2, 3, true), "900m");
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        assert_eq!(patches(&fake).len(), 1);

        let mut converged = defer(3, 4, true);
        converged.status = Some(
            serde_json::from_value::<DeferStatus>(
                json!({"observedGeneration": 4, "readyReplicas": 3, "desiredReplicas": 3}),
            )
            .unwrap(),
        );
        *fake.defers.lock().unwrap() = vec![converged];

        lp.tick(&fake, 140_000).await;
        for t in (150_000..=550_000u64).step_by(10_000) {
            lp.tick(&fake, t).await;
            assert_eq!(patches(&fake).len(), 1, "patch during cooldown at {t}");
        }
        lp.tick(&fake, 560_000).await;
        let got = patches(&fake);
        assert_eq!(got.len(), 2);
        assert_eq!(got[1], json!({"spec": {"replicasPerShard": 4}}));
    }

    /// A kube client whose API server is a fixed axum router.
    fn mock_kube(router: axum::Router) -> KubeCapacityIo {
        KubeCapacityIo {
            client: kube::Client::new(router, "payments"),
        }
    }

    fn pod_list(pods: &[(&str, Option<&str>)]) -> serde_json::Value {
        let items: Vec<serde_json::Value> = pods
            .iter()
            .map(|(name, node)| {
                let spec = match node {
                    Some(n) => json!({"nodeName": n, "containers": []}),
                    None => json!({"containers": []}),
                };
                json!({"metadata": {"name": name, "namespace": "payments"}, "spec": spec})
            })
            .collect();
        json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": items})
    }

    fn summary(pod: &str, used: u64) -> serde_json::Value {
        json!({"pods": [{"podRef": {"name": pod}, "volume": [{"name": "data", "usedBytes": used}]}]})
    }

    #[tokio::test]
    async fn disk_used_bytes_reads_data_volume_of_each_node() {
        use axum::routing::get;
        use axum::Json;
        let pods = pod_list(&[
            ("jobs-0", Some("n1")),
            ("jobs-1", Some("n2")),
            ("other", None),
        ]);
        let router = axum::Router::new()
            .route(
                "/api/v1/namespaces/payments/pods",
                get(move || {
                    let pods = pods.clone();
                    async move { Json(pods) }
                }),
            )
            .route(
                "/api/v1/nodes/n1/proxy/stats/summary",
                get(|| async { Json(summary("jobs-0", 500)) }),
            )
            .route(
                "/api/v1/nodes/n2/proxy/stats/summary",
                get(|| async { Json(summary("jobs-1", 700)) }),
            );
        let io = mock_kube(router);
        assert_eq!(io.disk_used_bytes(&defer(2, 3, true)).await, vec![500, 700]);
    }

    #[tokio::test]
    async fn disk_used_bytes_empty_when_pod_list_fails() {
        // No route for the pod list: the API answers 404 and the loop sees no volumes.
        let io = mock_kube(axum::Router::new());
        assert!(io.disk_used_bytes(&defer(2, 3, true)).await.is_empty());
    }

    #[tokio::test]
    async fn disk_used_bytes_skips_a_node_whose_summary_fails() {
        use axum::routing::get;
        use axum::Json;
        let pods = pod_list(&[("jobs-0", Some("bad")), ("jobs-1", Some("n2"))]);
        let router = axum::Router::new()
            .route(
                "/api/v1/namespaces/payments/pods",
                get(move || {
                    let pods = pods.clone();
                    async move { Json(pods) }
                }),
            )
            .route(
                "/api/v1/nodes/bad/proxy/stats/summary",
                get(|| async { "not json" }),
            )
            .route(
                "/api/v1/nodes/n2/proxy/stats/summary",
                get(|| async { Json(summary("jobs-1", 9)) }),
            );
        let io = mock_kube(router);
        assert_eq!(io.disk_used_bytes(&defer(2, 3, true)).await, vec![9]);
    }

    /// `defer(replicas, generation, true)` with `spec.cluster.shard_count` set.
    fn defer_shards(replicas: u32, shards: u32, generation: i64) -> Defer {
        let mut d = defer(replicas, generation, true);
        d.spec.cluster.shard_count = shards;
        d
    }

    #[tokio::test]
    async fn disk_above_split_mark_patches_shard_count_once() {
        let fake = fake(defer_shards(2, 1, 3), "500m");
        *fake.disk_bytes.lock().unwrap() = vec![2_000_000_000, 2_000_000_000];
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        let got = patches(&fake);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], json!({"spec": {"shardCount": 2}}));
        // While the generation is pending, later ticks add no patch.
        lp.tick(&fake, 140_000).await;
        lp.tick(&fake, 150_000).await;
        assert_eq!(patches(&fake).len(), 1);
    }

    #[tokio::test]
    async fn disk_split_waits_for_convergence_and_cooldown() {
        let fake = fake(defer_shards(2, 1, 3), "500m");
        *fake.disk_bytes.lock().unwrap() = vec![2_000_000_000, 2_000_000_000];
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        assert_eq!(patches(&fake).len(), 1);

        let mut converged = defer_shards(2, 2, 4);
        converged.status = Some(
            serde_json::from_value::<DeferStatus>(
                json!({"observedGeneration": 4, "readyReplicas": 4, "desiredReplicas": 4}),
            )
            .unwrap(),
        );
        *fake.defers.lock().unwrap() = vec![converged];

        // Convergence is recorded at 140 s; the cooldown ends at 440 s.
        lp.tick(&fake, 140_000).await;
        for t in (150_000..=430_000u64).step_by(10_000) {
            lp.tick(&fake, t).await;
            assert_eq!(patches(&fake).len(), 1, "patch during cooldown at {t}");
        }
        for t in (440_000..=480_000u64).step_by(10_000) {
            lp.tick(&fake, t).await;
        }
        let got = patches(&fake);
        assert_eq!(got.len(), 2);
        assert_eq!(got[1], json!({"spec": {"shardCount": 3}}));
    }

    #[tokio::test]
    async fn disk_merge_patches_only_when_below_merge_mark() {
        // Below the merge mark: one merge back to 1 shard.
        let below = fake(defer_shards(1, 2, 3), "500m");
        *below.disk_bytes.lock().unwrap() = vec![100_000_000, 100_000_000];
        let mut lp = CapacityLoop::new();
        for t in (0..=1_800_000u64).step_by(10_000) {
            lp.tick(&below, t).await;
        }
        let got = patches(&below);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], json!({"spec": {"shardCount": 1}}));

        // At or above the merge mark: no patch.
        let above = fake(defer_shards(1, 2, 3), "500m");
        *above.disk_bytes.lock().unwrap() = vec![300_000_000, 300_000_000];
        let mut lp = CapacityLoop::new();
        for t in (0..=1_800_000u64).step_by(10_000) {
            lp.tick(&above, t).await;
        }
        assert_eq!(patches(&above).len(), 0);
    }

    #[tokio::test]
    async fn split_holds_at_max_shard_count() {
        let fake = fake(defer_shards(2, 8, 3), "500m");
        *fake.disk_bytes.lock().unwrap() = vec![2_000_000_000, 2_000_000_000];
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        assert_eq!(patches(&fake).len(), 0);
    }

    #[tokio::test]
    async fn stale_generation_does_not_patch_shard_count() {
        let fake = fake(defer_shards(2, 1, 3), "500m");
        *fake.disk_bytes.lock().unwrap() = vec![2_000_000_000, 2_000_000_000];
        *fake.fresh.lock().unwrap() = Some(4);
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        assert_eq!(patches(&fake).len(), 0);
    }

    #[tokio::test]
    async fn split_beats_a_replica_change() {
        let fake = fake(defer_shards(2, 1, 3), "900m");
        *fake.disk_bytes.lock().unwrap() = vec![2_000_000_000, 2_000_000_000];
        let mut lp = CapacityLoop::new();
        run_window(&mut lp, &fake).await;
        let got = patches(&fake);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], json!({"spec": {"shardCount": 2}}));
    }
}
// HANDWRITE-END
