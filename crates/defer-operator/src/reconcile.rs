// HANDWRITE-BEGIN gap="missing-generator:logic:defer-reconcile" tracker="#766" reason="Defer ManagedService implementation for shared operator host."
use k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Secret};
use kube::{Api, ResourceExt};
use serde_json::{json, Value};
use service_k8s::service::ReconcilePlan;
use service_k8s::{ManagedService, ReadinessTarget, ReadyFacts};

use super::crd::{Defer, QueueMoveStatus};
use crate::membership_driver::RetryPolicy;
use crate::membership_http::HttpMembershipActuator;
use crate::queue_move::{
    drive_queue_move, plan_queue_move_step, HttpQueueMoveActuator, QueueMoveDecision,
    QueueMoveInput, MOVE_DONE, MOVE_MOVING,
};
use crate::replica_controller::{pod_http_url, pod_peer_url};
use crate::replica_reconcile::{
    decide_replicas_for_reconcile, decide_shards_for_reconcile, ReconcileDecision, ReplicaInput,
    ShardsDecision, ShardsInput,
};

/// The shard's live facts for the replica planner. `None` when there is no
/// StatefulSet yet or no admin token is configured: the plain render stays.
fn replica_input(defer: &Defer, live: Option<(u32, u32)>) -> Option<ReplicaInput> {
    let (live_replicas, ready_replicas) = live?;
    defer.spec.admin_token_secret.as_ref()?;
    Some(ReplicaInput {
        live_replicas,
        ready_replicas,
        settled: defer
            .status
            .as_ref()
            .and_then(|s| s.settled_replicas)
            .and_then(|v| u32::try_from(v).ok()),
        desired: defer.spec.cluster.replicas_per_shard,
    })
}

/// True when the shard path plans this Defer: more than one shard is asked
/// for, or the status already records two or more shards (#270).
fn use_shard_path(defer: &Defer) -> bool {
    defer.spec.cluster.shard_count > 1
        || defer
            .status
            .as_ref()
            .and_then(|s| s.settled_shards.as_ref())
            .is_some_and(|shards| shards.len() >= 2)
}

/// The live facts of all shards: settled counts from the status (or the
/// one-shard `settledReplicas`), the desired shard count from the spec.
fn shards_input(defer: &Defer, input: &ReplicaInput) -> ShardsInput {
    let settled_shards = defer
        .status
        .as_ref()
        .and_then(|s| s.settled_shards.as_ref())
        .map(|shards| {
            shards
                .iter()
                .filter_map(|v| u32::try_from(*v).ok())
                .collect::<Vec<u32>>()
        })
        .or_else(|| input.settled.map(|v| vec![v]));
    ShardsInput {
        live_replicas: input.live_replicas,
        ready_replicas: input.ready_replicas,
        settled_shards,
        desired_shards: defer.spec.cluster.shard_count,
        replicas_per_shard: defer.spec.cluster.replicas_per_shard,
    }
}

/// The decision when the admin token cannot be read: no replica change.
fn shards_token_error(input: &ShardsInput, text: String) -> ShardsDecision {
    let r = input.replicas_per_shard.max(1);
    let shards = input.live_replicas / r;
    let settled = input
        .settled_shards
        .clone()
        .unwrap_or_else(|| vec![r; shards as usize]);
    ShardsDecision {
        statefulset_replicas: input.live_replicas,
        settled_shards: settled.into_iter().map(|s| s.min(r)).collect(),
        message: Some(text),
    }
}

/// The shard path of one reconcile: one StatefulSet holds every shard (#270).
/// `token` is the result of reading the admin token Secret (see `read_token`).
async fn plan_shards(
    defer: &Defer,
    ns: &str,
    name: &str,
    token: Result<String, String>,
    children: Vec<Value>,
    input: &ReplicaInput,
) -> ReconcilePlan {
    let shards = shards_input(defer, input);
    let token = match token {
        Ok(token) => token,
        Err(text) => {
            let decision = shards_token_error(&shards, text);
            return ReconcilePlan::new(
                apply_shards_decision(children, &decision),
                shards_context(&decision),
            );
        }
    };
    let token = membership_token(&defer.spec.auth, token);
    let peer_tls = defer.spec.peer_tls_secret.is_some();
    let replicas = shards.replicas_per_shard;
    let actuator_for_shard = |base: u32| {
        let (name_http, ns_http) = (name.to_string(), ns.to_string());
        let (name_peer, ns_peer) = (name.to_string(), ns.to_string());
        HttpMembershipActuator::with_leader_discovery(
            reqwest::Client::new(),
            move |ordinal| pod_http_url(&name_http, &ns_http, base + ordinal),
            replicas,
            token.clone(),
            move |ordinal| pod_peer_url(&name_peer, &ns_peer, base + ordinal, peer_tls),
        )
    };
    let desired_shards = shards.desired_shards;
    let decision =
        decide_shards_for_reconcile(actuator_for_shard, shards, RetryPolicy::default()).await;
    let mut context = shards_context(&decision);
    let step = plan_queue_move_step(&QueueMoveInput {
        recorded: defer.status.as_ref().and_then(|s| s.queue_move.as_ref()),
        live_shards: input.live_replicas / replicas.max(1),
        desired_shards,
        settled_shards: &decision.settled_shards,
        replicas_per_shard: replicas,
        live_replicas: input.live_replicas,
        ready_replicas: input.ready_replicas,
    });
    match step {
        QueueMoveDecision::Nothing => {}
        QueueMoveDecision::Record(status) => {
            context["queueMove"] = queue_move_context(&status);
        }
        QueueMoveDecision::Run {
            from_shards,
            to_shards,
        } => {
            let (name_http, ns_http) = (name.to_string(), ns.to_string());
            let mover = HttpQueueMoveActuator::new(
                reqwest::Client::new(),
                move |shard| pod_http_url(&name_http, &ns_http, shard * replicas),
                token.clone(),
            );
            let (outcome, message) = match drive_queue_move(&mover, from_shards, to_shards).await {
                Ok(report) if report.failed.is_empty() => (MOVE_DONE, None),
                Ok(report) => {
                    let first = report.failed[0].1.to_string();
                    (
                        MOVE_MOVING,
                        Some(format!(
                            "queue move to {to_shards} shards: {} failed, first: {first}",
                            report.failed.len()
                        )),
                    )
                }
                Err(error) => (
                    MOVE_MOVING,
                    Some(format!("queue move to {to_shards} shards: {error}")),
                ),
            };
            context["queueMove"] = queue_move_context(&QueueMoveStatus {
                from_shards: from_shards as i32,
                to_shards: to_shards as i32,
                step: outcome.to_string(),
            });
            if let Some(message) = message {
                context["message"] = json!(message);
            }
        }
    }
    ReconcilePlan::new(apply_shards_decision(children, &decision), context)
}

/// Sets the StatefulSet replicas to the shard decision; other children are unchanged.
fn apply_shards_decision(mut children: Vec<Value>, decision: &ShardsDecision) -> Vec<Value> {
    for child in children.iter_mut() {
        if child["kind"] == "StatefulSet" {
            child["spec"]["replicas"] = json!(decision.statefulset_replicas);
        }
    }
    children
}

fn shards_context(decision: &ShardsDecision) -> Value {
    json!({"settledShards": decision.settled_shards, "message": decision.message})
}

/// The queue move of the status, as the status patch reads it (#285).
fn queue_move_context(status: &QueueMoveStatus) -> Value {
    json!({
        "fromShards": status.from_shards,
        "toShards": status.to_shards,
        "step": status.step,
    })
}

/// Sets the replicas of the StatefulSet child; every other child is unchanged.
fn apply_replica_decision(mut children: Vec<Value>, decision: &ReconcileDecision) -> Vec<Value> {
    for child in children.iter_mut() {
        if child["kind"] == "StatefulSet" {
            child["spec"]["replicas"] = json!(decision.statefulset_replicas);
        }
    }
    children
}

fn replica_context(decision: &ReconcileDecision) -> Value {
    json!({"settledReplicas": decision.settled, "message": decision.message})
}

/// The decision when the admin token cannot be read: no replica change.
fn token_error_decision(input: &ReplicaInput, text: String) -> ReconcileDecision {
    let live = input.live_replicas;
    ReconcileDecision {
        statefulset_replicas: live,
        settled: input.settled.unwrap_or(live).min(live),
        message: Some(text),
    }
}

/// Adds `settledReplicas` and a non-empty `message` from the context to the
/// status patch. A `Null` context leaves the patch unchanged.
fn merge_context_into_status(mut patch: Value, context: &Value) -> Value {
    if context["settledReplicas"].is_number() {
        patch["status"]["settledReplicas"] = context["settledReplicas"].clone();
    }
    if context["settledShards"].is_array() {
        patch["status"]["settledShards"] = context["settledShards"].clone();
    }
    if context["queueMove"].is_object() {
        patch["status"]["queueMove"] = context["queueMove"].clone();
    }
    if let Some(message) = context["message"].as_str().filter(|m| !m.is_empty()) {
        patch["status"]["message"] = json!(message);
    }
    patch
}

/// The admin bearer token from the Secret's `token` key. Errors become the
/// status message, never an `Err` of the reconcile.
async fn read_token(client: &kube::Client, ns: &str, secret: &str) -> Result<String, String> {
    let found = Api::<Secret>::namespaced(client.clone(), ns)
        .get_opt(secret)
        .await
        .map_err(|e| e.to_string())?;
    let Some(found) = found else {
        return Err(format!("secret {secret} not found"));
    };
    let token = found
        .data
        .and_then(|mut data| data.remove("token"))
        .map(|bytes| String::from_utf8(bytes.0))
        .transpose()
        .map_err(|e| e.to_string())?
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    token.ok_or_else(|| format!("secret {secret} has no token key"))
}

/// The token that the membership calls send. Only `auth: "required"` checks
/// it; any other value sends no bearer token (#168).
fn membership_token(auth: &str, token: String) -> String {
    if auth == "required" {
        token
    } else {
        String::new()
    }
}

impl ManagedService for Defer {
    const MANAGER: &'static str = "defer-operator";

    fn render(&self) -> Vec<serde_json::Value> {
        super::render::render(self).expect("replicasPerShard must be at least 1")
    }

    async fn reconcile_plan(&self, client: kube::Client) -> anyhow::Result<ReconcilePlan> {
        self.spec.validate().map_err(anyhow::Error::msg)?;
        let children = super::render::render(self)?;
        let plain = |children: Vec<Value>| ReconcilePlan::new(children, Value::Null);
        let name = self.name_any();
        let Some(ns) = self.namespace() else {
            return Ok(plain(children));
        };
        let sts = Api::<StatefulSet>::namespaced(client.clone(), &ns)
            .get_opt(&name)
            .await?;
        let live = sts.as_ref().map(|s| {
            (
                s.spec.as_ref().and_then(|x| x.replicas).unwrap_or(0).max(0) as u32,
                s.status
                    .as_ref()
                    .and_then(|x| x.ready_replicas)
                    .unwrap_or(0)
                    .max(0) as u32,
            )
        });
        let Some(input) = replica_input(self, live) else {
            return Ok(plain(children));
        };
        if use_shard_path(self) {
            let secret = self.spec.admin_token_secret.clone().unwrap_or_default();
            let token = read_token(&client, &ns, &secret).await;
            return Ok(plan_shards(self, &ns, &name, token, children, &input).await);
        }
        let secret = self.spec.admin_token_secret.clone().unwrap_or_default();
        let token = match read_token(&client, &ns, &secret).await {
            Ok(token) => token,
            Err(text) => {
                let decision = token_error_decision(&input, text);
                return Ok(ReconcilePlan::new(
                    apply_replica_decision(children, &decision),
                    replica_context(&decision),
                ));
            }
        };
        let token = membership_token(&self.spec.auth, token);
        let (name_http, ns_http) = (name.clone(), ns.clone());
        let (name_peer, ns_peer) = (name.clone(), ns.clone());
        let peer_tls = self.spec.peer_tls_secret.is_some();
        let actuator = HttpMembershipActuator::with_leader_discovery(
            reqwest::Client::new(),
            move |ordinal| pod_http_url(&name_http, &ns_http, ordinal),
            input.live_replicas,
            token,
            move |ordinal| pod_peer_url(&name_peer, &ns_peer, ordinal, peer_tls),
        );
        let decision =
            decide_replicas_for_reconcile(&actuator, input, RetryPolicy::default()).await;
        Ok(ReconcilePlan::new(
            apply_replica_decision(children, &decision),
            replica_context(&decision),
        ))
    }

    fn readiness_targets(&self) -> Vec<ReadinessTarget> {
        vec![ReadinessTarget::new("StatefulSet", self.name_any())]
    }

    fn status_patch(&self, ready: &ReadyFacts) -> serde_json::Value {
        let name = self.name_any();
        let ready_replicas = ready.get(&name) as i32;
        let desired = self
            .spec
            .cluster
            .shard_count
            .saturating_mul(self.spec.cluster.replicas_per_shard) as i32;
        let phase = if desired > 0 && ready_replicas >= desired {
            "Ready"
        } else if ready_replicas > 0 {
            "Reconciling"
        } else {
            "Pending"
        };
        json!({"status": {
            "phase": phase,
            "observedGeneration": self.metadata.generation.unwrap_or(0),
            "readyReplicas": ready_replicas,
            "desiredReplicas": desired,
            "message": format!("{ready_replicas}/{desired} Defer pods ready")
        }})
    }

    fn status_patch_with_context(&self, ready: &ReadyFacts, context: &Value) -> Value {
        merge_context_into_status(self.status_patch(ready), context)
    }
}

pub async fn run() -> anyhow::Result<()> {
    let client = kube::Client::try_default().await?;
    tokio::spawn(crate::capacity_loop::run_capacity_loop(client));
    service_k8s::run::<Defer>().await
}

#[cfg(test)]
mod tests {
    use super::{
        apply_replica_decision, membership_token, replica_context, replica_input,
        token_error_decision,
    };
    use crate::crd::Defer;
    use crate::replica_reconcile::{ReconcileDecision, ReplicaInput};
    use serde_json::{json, Value};
    use service_k8s::{ManagedService, ReadyFacts};
    use std::collections::HashMap;

    fn defer(admin_token: bool) -> Defer {
        let mut spec = json!({
            "image": "registry/defer:1",
            "replicasPerShard": 3,
            "voterCount": 3
        });
        if admin_token {
            spec["adminTokenSecret"] = json!("defer-admin");
        }
        serde_json::from_value(json!({
            "apiVersion": "defer.dev/v1alpha1",
            "kind": "Defer",
            "metadata": {"name": "jobs", "namespace": "payments", "uid": "u-1"},
            "spec": spec
        }))
        .unwrap()
    }

    fn defer_with_replicas(n: u32) -> Defer {
        serde_json::from_value(json!({
            "apiVersion": "defer.dev/v1alpha1",
            "kind": "Defer",
            "metadata": {"name": "jobs", "namespace": "payments", "uid": "u-1"},
            "spec": {
                "image": "registry/defer:1",
                "replicasPerShard": n,
                "voterCount": 3,
                "adminTokenSecret": "defer-admin"
            }
        }))
        .unwrap()
    }

    fn with_status(mut defer: Defer, status: Value) -> Defer {
        defer.status = Some(serde_json::from_value(status).unwrap());
        defer
    }

    #[test]
    fn apply_replica_decision_sets_statefulset_replicas() {
        let defer = defer(true);
        let children = defer.render();
        let decision = ReconcileDecision {
            statefulset_replicas: 4,
            settled: 3,
            message: None,
        };
        let applied = apply_replica_decision(children.clone(), &decision);
        assert_eq!(applied.len(), children.len());
        let mut found = false;
        for (new, old) in applied.iter().zip(children.iter()) {
            if old["kind"] == "StatefulSet" {
                found = true;
                assert_eq!(new["spec"]["replicas"], json!(4));
            } else {
                assert_eq!(new, old);
            }
        }
        assert!(found, "render must contain a StatefulSet child");
    }

    #[test]
    fn status_patch_carries_settled_and_message() {
        let defer = defer(true);
        let facts = ReadyFacts::new(HashMap::from([("jobs".to_string(), 3i64)]));
        let plain = defer.status_patch(&facts);
        let patched = defer
            .status_patch_with_context(&facts, &json!({"settledReplicas": 3, "message": "boom"}));
        assert_eq!(patched["status"]["settledReplicas"], json!(3));
        assert_eq!(patched["status"]["message"], json!("boom"));
        assert_eq!(patched["status"]["phase"], plain["status"]["phase"]);

        let null_message = defer
            .status_patch_with_context(&facts, &json!({"settledReplicas": 3, "message": null}));
        assert_eq!(
            null_message["status"]["message"],
            json!("3/3 Defer pods ready")
        );
    }

    #[test]
    fn null_context_leaves_status_unchanged() {
        let defer = defer(true);
        let facts = ReadyFacts::new(HashMap::new());
        let plain = defer.status_patch(&facts);
        assert_eq!(defer.status_patch_with_context(&facts, &Value::Null), plain);
    }

    #[test]
    fn no_token_keeps_the_plain_render() {
        // Negative test: a controller that ignores a missing token would
        // return Some here and call the membership actuator.
        assert_eq!(replica_input(&defer(false), Some((3, 3))), None);
        assert_eq!(replica_input(&defer(true), None), None);
        assert_eq!(
            replica_input(&defer(true), Some((3, 3))),
            Some(ReplicaInput {
                live_replicas: 3,
                ready_replicas: 3,
                settled: None,
                desired: 3,
            })
        );
    }

    #[test]
    fn replica_input_reads_settled_from_status() {
        let defer = with_status(
            defer(true),
            json!({
                "settledReplicas": 2,
                "phase": "",
                "observedGeneration": 0,
                "readyReplicas": 0,
                "desiredReplicas": 0,
                "message": ""
            }),
        );
        let input = replica_input(&defer, Some((4, 4))).expect("token is set");
        assert_eq!(input.settled, Some(2));
        assert_eq!(input.live_replicas, 4);
    }

    #[test]
    fn token_error_keeps_the_live_statefulset_size() {
        let d = defer_with_replicas(5);
        let statefulset = d
            .render()
            .into_iter()
            .find(|c| c["kind"] == "StatefulSet")
            .expect("render must contain a StatefulSet child");
        assert_eq!(statefulset["spec"]["replicas"], json!(5));

        let input = ReplicaInput {
            live_replicas: 3,
            ready_replicas: 3,
            settled: Some(3),
            desired: 5,
        };
        let decision = token_error_decision(&input, "secret defer-admin not found".to_string());
        assert_eq!(decision.statefulset_replicas, 3);
        assert_eq!(decision.settled, 3);
        assert_eq!(
            decision.message.as_deref(),
            Some("secret defer-admin not found")
        );

        let applied = apply_replica_decision(d.render(), &decision);
        let applied_sts = applied
            .iter()
            .find(|c| c["kind"] == "StatefulSet")
            .expect("applied children must contain a StatefulSet");
        assert_eq!(applied_sts["spec"]["replicas"], json!(3));
    }

    #[test]
    fn token_error_message_reaches_the_status() {
        let d = defer(true);
        let decision = token_error_decision(
            &ReplicaInput {
                live_replicas: 3,
                ready_replicas: 3,
                settled: None,
                desired: 5,
            },
            "secret defer-admin has no token key".to_string(),
        );
        let facts = ReadyFacts::new(HashMap::from([("jobs".to_string(), 3i64)]));
        let patch = d.status_patch_with_context(&facts, &replica_context(&decision));
        assert_eq!(
            patch["status"]["message"],
            json!("secret defer-admin has no token key")
        );
        assert_eq!(patch["status"]["settledReplicas"], json!(3));
    }

    #[test]
    fn token_error_never_settles_above_live() {
        let decision = token_error_decision(
            &ReplicaInput {
                live_replicas: 2,
                ready_replicas: 2,
                settled: Some(4),
                desired: 3,
            },
            "secret defer-admin not found".to_string(),
        );
        assert_eq!(decision.settled, 2);
        assert_eq!(decision.statefulset_replicas, 2);
    }

    #[test]
    fn membership_token_is_kept_when_auth_is_required() {
        assert_eq!(
            membership_token("required", "t0k".to_string()),
            "t0k".to_string()
        );
    }

    #[test]
    fn membership_token_is_empty_when_auth_is_off() {
        assert_eq!(membership_token("off", "t0k".to_string()), String::new());
    }

    #[test]
    fn membership_token_is_empty_for_any_other_auth_value() {
        assert_eq!(membership_token("", "t0k".to_string()), String::new());
    }
}
#[cfg(test)]
mod shard_path_tests {
    use super::{
        apply_shards_decision, merge_context_into_status, shards_context, shards_input,
        shards_token_error, use_shard_path, Defer,
    };
    use crate::replica_reconcile::{ReplicaInput, ShardsDecision, ShardsInput};
    use serde_json::{json, Value};

    fn defer(shards: u32, status: Value) -> Defer {
        serde_json::from_value(json!({
            "apiVersion": "defer.dev/v1alpha1",
            "kind": "Defer",
            "metadata": {"name": "jobs", "namespace": "payments", "uid": "u-1"},
            "spec": {
                "image": "registry/defer:1",
                "shardCount": shards,
                "replicasPerShard": 3,
                "voterCount": 3
            },
            "status": status
        }))
        .unwrap()
    }

    #[test]
    fn shard_path_only_for_many_shards_or_recorded_shards() {
        // Negative test: without the new rule, a plain one-shard Defer takes the shard path.
        assert!(!use_shard_path(&defer(1, json!({}))));
        assert!(use_shard_path(&defer(2, json!({}))));
        assert!(use_shard_path(&defer(1, json!({"settledShards": [3, 1]}))));
        assert!(!use_shard_path(&defer(1, json!({"settledShards": [3]}))));
    }

    #[test]
    fn shards_input_reads_settled_shards_or_the_one_shard_count() {
        // A negative value cannot be a replica count and is dropped.
        let input = ReplicaInput {
            live_replicas: 6,
            ready_replicas: 6,
            settled: Some(3),
            desired: 3,
        };
        let shards = shards_input(&defer(2, json!({"settledShards": [3, -1, 2]})), &input);
        assert_eq!(shards.settled_shards, Some(vec![3, 2]));
        assert_eq!(shards.desired_shards, 2);
        assert_eq!(shards.replicas_per_shard, 3);

        let shards = shards_input(&defer(2, json!({})), &input);
        assert_eq!(shards.settled_shards, Some(vec![3]));
    }

    #[test]
    fn shards_token_error_keeps_the_live_size_and_clamps() {
        let input = ShardsInput {
            live_replicas: 6,
            ready_replicas: 6,
            settled_shards: Some(vec![3, 5]),
            desired_shards: 2,
            replicas_per_shard: 3,
        };
        let decision = shards_token_error(&input, "boom".into());
        assert_eq!(decision.statefulset_replicas, 6);
        assert_eq!(decision.settled_shards, vec![3, 3]);
        assert_eq!(decision.message.as_deref(), Some("boom"));

        let unknown = ShardsInput {
            settled_shards: None,
            ..input
        };
        assert_eq!(
            shards_token_error(&unknown, "boom".into()).settled_shards,
            vec![3, 3]
        );
    }

    #[test]
    fn shards_context_reaches_the_status_patch() {
        let decision = ShardsDecision {
            statefulset_replicas: 6,
            settled_shards: vec![3, 1],
            message: None,
        };
        let patch = merge_context_into_status(json!({"status": {}}), &shards_context(&decision));
        assert_eq!(patch["status"]["settledShards"], json!([3, 1]));
        assert!(patch["status"].get("message").is_none());

        let held = ShardsDecision {
            message: Some("lowering shardCount is not supported yet".into()),
            ..decision
        };
        let patch = merge_context_into_status(json!({"status": {}}), &shards_context(&held));
        assert_eq!(
            patch["status"]["message"],
            "lowering shardCount is not supported yet"
        );
    }

    #[test]
    fn apply_shards_decision_sets_only_the_statefulset() {
        let children = vec![
            json!({"kind": "Service", "spec": {}}),
            json!({"kind": "StatefulSet", "spec": {"replicas": 3}}),
        ];
        let decision = ShardsDecision {
            statefulset_replicas: 6,
            settled_shards: vec![3, 1],
            message: None,
        };
        let out = apply_shards_decision(children, &decision);
        assert_eq!(out[1]["spec"]["replicas"], 6);
        assert!(out[0]["spec"].get("replicas").is_none());
    }

    #[test]
    fn status_patch_records_the_queue_move() {
        use crate::crd::QueueMoveStatus;
        let moving = QueueMoveStatus {
            from_shards: 1,
            to_shards: 2,
            step: "moving".to_string(),
        };
        let context = json!({"queueMove": super::queue_move_context(&moving)});

        let patch = merge_context_into_status(json!({"status": {}}), &context);

        assert_eq!(patch["status"]["queueMove"]["step"], "moving");
        assert_eq!(patch["status"]["queueMove"]["fromShards"], 1);
        assert_eq!(patch["status"]["queueMove"]["toShards"], 2);

        // Negative test: a context without a queue move adds no queueMove key.
        let plain = merge_context_into_status(json!({"status": {}}), &json!({"message": "x"}));
        assert!(plain["status"].get("queueMove").is_none());
    }
}
#[cfg(test)]
mod shard_api_tests {
    use super::{plan_shards, Defer};
    use crate::replica_reconcile::ReplicaInput;
    use serde_json::{json, Value};

    fn defer_with_secret(shards: u32, status: Value) -> Defer {
        serde_json::from_value(json!({
            "apiVersion": "defer.dev/v1alpha1",
            "kind": "Defer",
            "metadata": {"name": "jobs", "namespace": "payments", "uid": "u-1"},
            "spec": {
                "image": "registry/defer:1",
                "shardCount": shards,
                "replicasPerShard": 3,
                "voterCount": 3,
                "adminTokenSecret": "defer-tokens"
            },
            "status": status
        }))
        .unwrap()
    }

    fn children(replicas: u32) -> Vec<Value> {
        vec![
            json!({"kind": "Service", "spec": {}}),
            json!({"kind": "StatefulSet", "spec": {"replicas": replicas}}),
        ]
    }

    #[tokio::test]
    async fn shard_plan_keeps_the_size_when_the_token_secret_is_missing() {
        let defer = defer_with_secret(2, json!({"settledShards": [3, 3]}));
        let input = ReplicaInput {
            live_replicas: 6,
            ready_replicas: 6,
            settled: None,
            desired: 3,
        };
        let token = Err("secret defer-tokens not found".to_string());
        let plan = plan_shards(&defer, "payments", "jobs", token, children(6), &input).await;
        let (children, context) = plan.into_parts();
        assert_eq!(children[1]["spec"]["replicas"], 6);
        assert_eq!(context["settledShards"], json!([3, 3]));
        assert!(context["message"]
            .as_str()
            .unwrap()
            .contains("secret defer-tokens not found"));
    }

    #[tokio::test]
    async fn shard_plan_with_the_token_keeps_settled_shards() {
        let defer = defer_with_secret(2, json!({"settledShards": [3, 3]}));
        let input = ReplicaInput {
            live_replicas: 6,
            ready_replicas: 6,
            settled: None,
            desired: 3,
        };
        let token = Ok("token".to_string());
        let plan = plan_shards(&defer, "payments", "jobs", token, children(6), &input).await;
        let (children, context) = plan.into_parts();
        assert_eq!(children[1]["spec"]["replicas"], 6);
        assert_eq!(context["settledShards"], json!([3, 3]));
        assert!(context["message"].is_null());
    }
}
// HANDWRITE-END
