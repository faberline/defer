// HANDWRITE-BEGIN gap="missing-generator:logic:defer-reconcile" tracker="#766" reason="Defer ManagedService implementation for shared operator host."
use k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Secret};
use kube::{Api, ResourceExt};
use serde_json::{json, Value};
use service_k8s::service::ReconcilePlan;
use service_k8s::{ManagedService, ReadinessTarget, ReadyFacts};

use super::crd::Defer;
use crate::membership_driver::RetryPolicy;
use crate::membership_http::HttpMembershipActuator;
use crate::replica_controller::{pod_http_url, pod_peer_url};
use crate::replica_reconcile::{decide_replicas_for_reconcile, ReconcileDecision, ReplicaInput};

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
        super::render::render(self)
    }

    async fn reconcile_plan(&self, client: kube::Client) -> anyhow::Result<ReconcilePlan> {
        let children = self.render();
        let plain = |children: Vec<Value>| ReconcilePlan {
            children,
            context: Value::Null,
        };
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
        let secret = self.spec.admin_token_secret.clone().unwrap_or_default();
        let token = match read_token(&client, &ns, &secret).await {
            Ok(token) => token,
            Err(text) => {
                let decision = token_error_decision(&input, text);
                return Ok(ReconcilePlan {
                    children: apply_replica_decision(children, &decision),
                    context: replica_context(&decision),
                });
            }
        };
        let token = membership_token(&self.spec.auth, token);
        let (name_http, ns_http) = (name.clone(), ns.clone());
        let (name_peer, ns_peer) = (name.clone(), ns.clone());
        let actuator = HttpMembershipActuator::with_leader_discovery(
            reqwest::Client::new(),
            move |ordinal| pod_http_url(&name_http, &ns_http, ordinal),
            input.live_replicas,
            token,
            move |ordinal| pod_peer_url(&name_peer, &ns_peer, ordinal),
        );
        let decision =
            decide_replicas_for_reconcile(&actuator, input, RetryPolicy::default()).await;
        Ok(ReconcilePlan {
            children: apply_replica_decision(children, &decision),
            context: replica_context(&decision),
        })
    }

    fn readiness_targets(&self) -> Vec<ReadinessTarget> {
        vec![ReadinessTarget {
            kind: "StatefulSet",
            name: self.name_any(),
        }]
    }

    fn status_patch(&self, ready: &ReadyFacts) -> serde_json::Value {
        let name = self.name_any();
        let ready_replicas = ready.get(&name) as i32;
        let desired = self.spec.cluster.replicas_per_shard as i32;
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
        let facts = ReadyFacts {
            ready: HashMap::from([("jobs".to_string(), 3i64)]),
        };
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
        let facts = ReadyFacts {
            ready: HashMap::new(),
        };
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
        let facts = ReadyFacts {
            ready: HashMap::from([("jobs".to_string(), 3i64)]),
        };
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
// HANDWRITE-END
