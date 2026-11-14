//! Suite `capacity_policy`: the Defer CR `defer` gets a capacity policy with a
//! low CPU high-watermark, the shipped operator patches `spec.replicasPerShard`
//! up by one, and the test restores the CR. The test never writes
//! `replicasPerShard` before the operator does.

use std::time::Duration;

use serde_json::{json, Value};

use super::{endpoint, evidence, kubectl, kubectl_json, try_kubectl, wait_for};

const SUITE: &str = "capacity_policy";
const DEFER: &str = "defer";
const TOKEN_SECRET: &str = "defer-admin-token";
const SCALE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const PATCH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// The CR patch that turns the policy on: a CPU high-watermark of 0.001, so
/// any load scales out, with the floor at `start` and the ceiling one above.
fn policy_patch(start: u64) -> Value {
    json!({
        "spec": {
            "limits": { "cpu": "250m", "memory": "512Mi" },
            "capacityPolicy": { "cpuHigh": 0.001, "floor": start, "ceiling": start + 1 }
        }
    })
}

/// The CR patch that clears the policy and puts `replicasPerShard` back.
fn restore_patch(start: u64) -> Value {
    json!({ "spec": { "capacityPolicy": null, "replicasPerShard": start } })
}

fn admin_token_patch() -> Value {
    json!({ "spec": { "adminTokenSecret": TOKEN_SECRET } })
}

/// Waits until the StatefulSet and the Defer CR status both show `want`
/// replicas, all of them ready.
fn wait_for_replicas(want: u64) {
    wait_for(
        &format!("Defer/{DEFER} settled at {want} replicas"),
        SCALE_TIMEOUT,
        || {
            let sts = kubectl_json(&["get", "statefulset", DEFER]);
            let cr = kubectl_json(&["get", "defer", DEFER])["status"].clone();
            let ok = sts["spec"]["replicas"] == want
                && sts["status"]["readyReplicas"] == want
                && cr["settledReplicas"] == want
                && cr["readyReplicas"] == want
                && cr["desiredReplicas"] == want;
            if ok {
                Ok(())
            } else {
                Err(format!(
                    "sts spec {} ready {}; cr {cr}",
                    sts["spec"]["replicas"], sts["status"]["readyReplicas"]
                ))
            }
        },
    )
}

/// Clears the policy and the token Secret. Cleanup never panics: the test's
/// own failure stays the reported one.
fn restore(start: u64) {
    let patch = restore_patch(start).to_string();
    let _ = try_kubectl(&[
        "patch",
        "defer",
        DEFER,
        "--type",
        "merge",
        "-p",
        patch.as_str(),
    ]);
    let _ = try_kubectl(&["delete", "secret", TOKEN_SECRET, "--ignore-not-found"]);
}

/// Runs [`restore`] when dropped, so a failed run still cleans up.
struct Restore(u64);

impl Drop for Restore {
    fn drop(&mut self) {
        restore(self.0);
    }
}

#[test]
#[ignore = "e2e: needs E2E_ENDPOINT"]
fn low_cpu_threshold_scales_out_and_restores() {
    endpoint();
    let start = kubectl_json(&["get", "defer", DEFER])["spec"]["replicasPerShard"]
        .as_u64()
        .expect("replicasPerShard");
    wait_for_replicas(start);
    let _restore = Restore(start);

    kubectl(&[
        "create",
        "secret",
        "generic",
        TOKEN_SECRET,
        "--from-literal=token=e2e-auth-is-off",
    ]);
    let token = admin_token_patch().to_string();
    kubectl(&[
        "patch",
        "defer",
        DEFER,
        "--type",
        "merge",
        "-p",
        token.as_str(),
    ]);
    wait_for_replicas(start);
    evidence(
        SUITE,
        "operator token ready",
        json!({ "secret": TOKEN_SECRET }),
    );

    let policy = policy_patch(start).to_string();
    kubectl(&[
        "patch",
        "defer",
        DEFER,
        "--type",
        "merge",
        "-p",
        policy.as_str(),
    ]);
    evidence(
        SUITE,
        "capacity policy set",
        json!({ "cpuHigh": 0.001, "floor": start, "ceiling": start + 1 }),
    );

    wait_for(
        "capacity controller patched replicasPerShard",
        PATCH_TIMEOUT,
        || {
            let value = kubectl_json(&["get", "defer", DEFER])["spec"]["replicasPerShard"].clone();
            if value == start + 1 {
                Ok(())
            } else {
                Err(format!("replicasPerShard {value}"))
            }
        },
    );
    wait_for_replicas(start + 1);
    evidence(
        SUITE,
        "scaled out by the operator",
        json!({ "from": start, "to": start + 1 }),
    );

    restore(start);
    wait_for_replicas(start);
    evidence(SUITE, "cleaned up", json!({ "replicasPerShard": start }));
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{admin_token_patch, policy_patch, restore_patch};

    #[test]
    fn policy_patch_has_a_low_cpu_threshold_and_a_ceiling() {
        let patch = policy_patch(2);
        assert_eq!(
            patch["spec"]["capacityPolicy"],
            json!({"cpuHigh": 0.001, "floor": 2, "ceiling": 3})
        );
        assert_eq!(
            patch["spec"]["limits"],
            json!({"cpu": "250m", "memory": "512Mi"})
        );
    }

    #[test]
    fn policy_patch_ceiling_is_above_floor() {
        for start in 1..=3u64 {
            let policy = &policy_patch(start)["spec"]["capacityPolicy"];
            let floor = policy["floor"].as_u64().unwrap();
            let ceiling = policy["ceiling"].as_u64().unwrap();
            assert!(
                ceiling > floor,
                "start {start}: ceiling {ceiling} <= floor {floor}"
            );
        }
    }

    #[test]
    fn restore_patch_clears_the_policy() {
        assert_eq!(
            restore_patch(2),
            json!({"spec": {"capacityPolicy": null, "replicasPerShard": 2}})
        );
    }

    #[test]
    fn admin_token_patch_names_the_secret() {
        assert_eq!(
            admin_token_patch(),
            json!({"spec": {"adminTokenSecret": "defer-admin-token"}})
        );
    }

    /// Reads a file relative to the repository root.
    fn repo_file(relative: &str) -> String {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        std::fs::read_to_string(root.join(relative))
            .unwrap_or_else(|err| panic!("read {relative}: {err}"))
    }

    #[test]
    fn kind_overlay_has_a_limit_on_both_axes() {
        let cr = repo_file("k8s/overlays/kind/defer-cr.yaml");
        assert!(cr.contains("limits:"), "Kind CR has no limits: block");
        assert!(cr.contains("cpu: 250m"), "Kind CR has no CPU limit");
        assert!(cr.contains("memory: 512Mi"), "Kind CR has no memory limit");
        assert!(!cr.contains("placement:"), "Kind CR must not set placement");
        assert!(
            !cr.contains("storageClass"),
            "Kind CR must not set storageClass"
        );
    }

    #[test]
    fn kind_workflow_is_manual_only_and_cleans_up() {
        let wf = repo_file(".github/workflows/kind-capacity.yml");
        assert!(wf.contains("workflow_dispatch:"), "no workflow_dispatch");
        assert!(
            wf.contains("delete cluster --name defer-capacity"),
            "cluster is not deleted"
        );
        assert!(
            wf.contains("--kubelet-insecure-tls"),
            "metrics-server patch is missing"
        );
        assert!(
            !wf.contains("pull_request:"),
            "workflow runs on pull_request"
        );
        assert!(!wf.contains("push:"), "workflow runs on push");
    }

    #[test]
    fn kind_workflow_applies_the_crd_before_the_overlay() {
        let wf = repo_file(".github/workflows/kind-capacity.yml");
        let established = wf
            .find("kubectl wait --for=condition=established crd/defers.defer.dev")
            .expect("workflow does not wait for the Defer CRD to be established");
        let overlay = wf
            .find("kubectl apply -k k8s/overlays/kind")
            .expect("workflow does not apply the Kind overlay");
        assert!(
            established < overlay,
            "the CRD must be established before the Defer CR is applied"
        );
    }
}
