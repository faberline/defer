// HANDWRITE-BEGIN gap="missing-generator:logic:defer-declared" tracker="#766" reason="Declarative queue provisioning at service startup."
//! Declarative queue provisioning: parse declared queues and reconcile them
//! against the Raft-replicated state machine on the leader.

use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use defer_replication::raft::DeferRaft;
use defer_shared_kernel::types::{QueueControlState, QueuePolicy};
use serde::{Deserialize, Serialize};

/// A queue declaration provided via `--declared-queues` / `DEFER_DECLARED_QUEUES`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredQueue {
    pub name: String,
    #[serde(
        default = "default_declared_policy",
        deserialize_with = "deserialize_declared_policy"
    )]
    pub policy: QueuePolicy,
    #[serde(default)]
    pub control: QueueControlState,
}

fn default_declared_policy() -> QueuePolicy {
    QueuePolicy::default()
}

fn deserialize_declared_policy<'de, D>(deserializer: D) -> Result<QueuePolicy, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let val = serde_json::Value::deserialize(deserializer)?;
    if val.is_null() {
        return Ok(QueuePolicy::default());
    }
    let obj = val
        .as_object()
        .ok_or_else(|| serde::de::Error::custom("declared queue policy must be a JSON object"))?;
    let mut merged =
        match serde_json::to_value(QueuePolicy::default()).map_err(serde::de::Error::custom)? {
            serde_json::Value::Object(map) => map,
            _ => unreachable!(),
        };
    for (k, v) in obj {
        merged.insert(k.clone(), v.clone());
    }
    serde_json::from_value(serde_json::Value::Object(merged)).map_err(serde::de::Error::custom)
}

/// The structured reconciliation decision for one declared queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeclaredQueueDecision {
    Created,
    Updated,
    Unchanged,
}

/// The outcome of reconciling one declared queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredQueueOutcome {
    pub name: String,
    pub decision: DeclaredQueueDecision,
}

/// Parse and validate a JSON array of declared queues.
///
/// Stops with an error on:
/// - Malformed JSON
/// - Non-array JSON values
/// - Empty queue names
/// - Duplicate queue names
/// - Unknown control values
/// - Policy fields that `PUT /v1/queues/{queue}` would refuse
///
/// Empty input or `"[]"` returns an empty `Vec`.
pub fn parse_declared_queues(input: &str) -> Result<Vec<DeclaredQueue>> {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed == "[]" {
        return Ok(Vec::new());
    }
    let queues: Vec<DeclaredQueue> =
        serde_json::from_str(trimmed).context("failed to parse declared queues JSON")?;
    let mut seen = HashSet::new();
    for q in &queues {
        if q.name.trim().is_empty() {
            bail!("declared queue name cannot be empty");
        }
        if !seen.insert(&q.name) {
            bail!("duplicate declared queue name `{}`", q.name);
        }
    }
    Ok(queues)
}

/// Reconcile declared queues on the Raft leader.
///
/// Applies missing queues with `configure_queue` and `set_queue_control`.
/// Updates existing queues whose policy or control state differs.
/// Leaves undeclared queues untouched.
/// If the node is not the Raft leader, skips without error and returns an empty list.
pub async fn ensure_declared_queues(
    raft: &DeferRaft,
    declared: &[DeclaredQueue],
) -> Result<Vec<DeclaredQueueOutcome>> {
    if !raft.is_leader().await {
        tracing::debug!("skipping declared queue provisioning: node is not the raft leader");
        return Ok(Vec::new());
    }

    let mut outcomes = Vec::with_capacity(declared.len());
    for item in declared {
        let current = raft
            .scheduler()
            .lock()
            .unwrap()
            .queue_snapshot(&item.name)
            .ok();

        match current {
            None => {
                raft.configure_queue(item.name.clone(), item.policy.clone())
                    .await?;
                raft.set_queue_control(item.name.clone(), item.control)
                    .await?;
                tracing::info!(
                    queue = %item.name,
                    control = ?item.control,
                    "declared queue created"
                );
                outcomes.push(DeclaredQueueOutcome {
                    name: item.name.clone(),
                    decision: DeclaredQueueDecision::Created,
                });
            }
            Some(snapshot) => {
                let policy_differs = snapshot.policy != item.policy;
                let control_differs = snapshot.control_state != item.control;
                if policy_differs || control_differs {
                    if policy_differs {
                        raft.configure_queue(item.name.clone(), item.policy.clone())
                            .await?;
                    }
                    if control_differs {
                        raft.set_queue_control(item.name.clone(), item.control)
                            .await?;
                    }
                    tracing::info!(
                        queue = %item.name,
                        policy_updated = policy_differs,
                        control_updated = control_differs,
                        "declared queue updated"
                    );
                    outcomes.push(DeclaredQueueOutcome {
                        name: item.name.clone(),
                        decision: DeclaredQueueDecision::Updated,
                    });
                } else {
                    tracing::debug!(queue = %item.name, "declared queue unchanged");
                    outcomes.push(DeclaredQueueOutcome {
                        name: item.name.clone(),
                        decision: DeclaredQueueDecision::Unchanged,
                    });
                }
            }
        }
    }
    Ok(outcomes)
}
// HANDWRITE-END
