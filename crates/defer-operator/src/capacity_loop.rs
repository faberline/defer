// HANDWRITE-BEGIN gap="missing-generator:logic:defer-capacity-loop" tracker="#13" reason="Capacity controller loop of the Defer operator."
//! Capacity controller loop: reads pod metrics per Defer, decides the replica
//! count, and patches `spec.replicasPerShard` behind a generation guard.

use std::collections::HashMap;
use std::future::Future;

use crate::capacity_patch::{build_patch, CapacityPatch, Evidence};
use crate::capacity_policy::{
    decide_replicas, Change, Direction, PolicyState, Sample, SCALE_IN_MS, SCALE_OUT_MS,
};
use crate::capacity_sample::{parse_cpu_quantity, parse_memory_quantity, population_sample};
use crate::crd::Defer;

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

            let current = defer.spec.cluster.replicas_per_shard;
            let marks = policy.watermarks();
            let bounds = policy.bounds(current);
            let Some(decision) = decide_replicas(
                &entry.samples,
                now_ms,
                current,
                &marks,
                &bounds,
                &entry.state,
            ) else {
                continue;
            };
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
            let evidence = Evidence {
                direction,
                reason: decision.reason,
                population: pods.len() as u32,
                window_ms,
                watermark,
                observed,
                generation,
            };
            let Some(patch) = build_patch(&Change::Replicas(decision), generation, evidence) else {
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
}
// HANDWRITE-END
