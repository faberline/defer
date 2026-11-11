//! Suite `deploy_smoke`: dispatch journey and metrics diagnostics against the
//! operator-deployed Defer (k8s/overlays/gke-test).

use std::time::Duration;

use serde_json::{json, Value};

use super::{endpoint, evidence, try_kubectl, wait_for};

const SUITE: &str = "deploy_smoke";
const QUEUE: &str = "deploy-smoke";
const POD: &str = "defer-0";
const FIRST_POD_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const API_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Waits until defer-0 exists and is Ready.
fn wait_for_ready_pod(timeout: Duration) -> String {
    wait_for("pod defer-0 Ready", timeout, || {
        let out = try_kubectl(&["get", "pod", POD, "-o", "json"])?;
        let pod: Value = serde_json::from_str(&out).map_err(|err| err.to_string())?;
        let uid = pod["metadata"]["uid"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let ready = pod["status"]["conditions"].as_array().is_some_and(|all| {
            all.iter()
                .any(|c| c["type"] == "Ready" && c["status"] == "True")
        });
        if !uid.is_empty() && ready {
            Ok(uid)
        } else {
            Err(format!(
                "uid {uid:?} ready {ready} phase {}",
                pod["status"]["phase"]
            ))
        }
    })
}

/// Sends an HTTP request that must succeed and returns its JSON body.
async fn send(request: reqwest::RequestBuilder) -> Value {
    let response = request.send().await.expect("request");
    let status = response.status();
    let url = response.url().clone();
    let body = response.text().await.expect("body");
    assert!(status.is_success(), "{url}: {status}: {body}");
    serde_json::from_str(&body).unwrap_or_else(|err| panic!("{url}: not JSON: {err}: {body}"))
}

/// Waits until `GET /healthz` answers 2xx.
async fn wait_healthy(http: &reqwest::Client, base: &str) {
    let deadline = tokio::time::Instant::now() + FIRST_POD_TIMEOUT;
    loop {
        let last = match http.get(format!("{base}/healthz")).send().await {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => response.status().to_string(),
            Err(err) => err.to_string(),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "{base}/healthz: not healthy within {FIRST_POD_TIMEOUT:?}; last saw: {last}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Creates or updates queue `QUEUE` and checks that it runs.
async fn put_queue(http: &reqwest::Client, base: &str) {
    let queue = send(http.put(format!("{base}/v1/queues/{QUEUE}")).json(&json!({
        "max_in_flight": 100,
        "max_dispatch_per_tick": 100,
        "max_dispatches_per_second": 100,
        "max_burst_size": 100,
        "lease_ttl_ms": 30000,
        "retry_backoff_ms": 250
    })))
    .await;
    assert_eq!(queue["queue"], QUEUE, "{queue}");
    assert_eq!(queue["control_state"], "Running", "{queue}");
}

/// Creates a task in `QUEUE` whose target is `GET {base}{path}`.
async fn create_task(
    http: &reqwest::Client,
    base: &str,
    task_id: &str,
    path: &str,
    max_attempts: u32,
) {
    let resp = http
        .post(format!("{base}/v1/queues/{QUEUE}/tasks"))
        .json(&json!({
            "task_id": task_id,
            "target": {
                "url": format!("{base}{path}"),
                "method": "GET",
                "headers": {}
            },
            "payload": { "suite": SUITE },
            "schedule_at": "2020-01-01T00:00:00Z",
            "priority": 10,
            "max_attempts": max_attempts
        }))
        .send()
        .await
        .expect("create task request");
    assert_eq!(resp.status().as_u16(), 201, "create task {task_id} status");
}

/// Polls task status until it reads `expected_status`. Returns the task JSON,
/// or fails after `timeout`.
async fn wait_for_task_status(
    http: &reqwest::Client,
    base: &str,
    task_id: &str,
    expected_status: &str,
    timeout: Duration,
) -> Value {
    let url = format!("{base}/v1/queues/{QUEUE}/tasks/{task_id}");
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let last = match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => match resp.json::<Value>().await {
                Ok(body) if body["status"] == expected_status => return body,
                Ok(body) => format!("status: {}", body["status"]),
                Err(err) => err.to_string(),
            },
            Ok(resp) => format!("HTTP {}", resp.status()),
            Err(err) => err.to_string(),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "task {task_id} {expected_status}: not reached within {timeout:?}; last saw: {last}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn parse_counter(metrics: &str, name: &str) -> Option<u64> {
    for line in metrics.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix(name) {
            if rest.starts_with(' ') || rest.starts_with('\t') || rest.starts_with('{') {
                let rest = rest.trim();
                let val_str = if let Some((_, val)) = rest.split_once('}') {
                    val.trim()
                } else {
                    rest
                };
                if let Ok(val) = val_str.parse::<u64>() {
                    return Some(val);
                }
            }
        }
    }
    None
}

/// Reads `defer_dispatch_acked_total` and `defer_dispatch_dead_lettered_total`
/// from `GET /metrics`.
async fn read_dispatch_counters(http: &reqwest::Client, base: &str) -> Result<(u64, u64), String> {
    let text = http
        .get(format!("{base}/metrics"))
        .send()
        .await
        .map_err(|err| err.to_string())?
        .text()
        .await
        .map_err(|err| err.to_string())?;
    let acked = parse_counter(&text, "defer_dispatch_acked_total")
        .ok_or_else(|| "missing defer_dispatch_acked_total".to_string())?;
    let dead_lettered = parse_counter(&text, "defer_dispatch_dead_lettered_total")
        .ok_or_else(|| "missing defer_dispatch_dead_lettered_total".to_string())?;
    Ok((acked, dead_lettered))
}

/// Test 1: PUT /v1/queues/<q> then create a task whose target is
/// GET http://defer:7141/healthz (no receiver needed; a 2xx acks). Wait until the
/// task reads Succeeded; print an E2E_EVIDENCE line with the observed status.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn test_1_dispatch_acked_on_2xx() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    wait_for_ready_pod(FIRST_POD_TIMEOUT);
    wait_healthy(&http, &base).await;
    put_queue(&http, &base).await;

    let task_id = "deploy-smoke-ack";
    create_task(&http, &base, task_id, "/healthz", 3).await;

    let task = wait_for_task_status(&http, &base, task_id, "Succeeded", API_TIMEOUT).await;
    assert_eq!(task["task_id"], task_id);
    assert_eq!(task["status"], "Succeeded");

    evidence(
        SUITE,
        "task succeeded",
        json!({
            "task_id": task_id,
            "status": "Succeeded"
        }),
    );
}

/// Test 2: a task whose target is a path that answers non-2xx, with
/// max_attempts 2 and a small queue retry_backoff_ms. Wait until it reads
/// DeadLettered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn test_2_dispatch_dead_lettered_on_non_2xx() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    wait_healthy(&http, &base).await;
    put_queue(&http, &base).await;

    let task_id = "deploy-smoke-dead-letter";
    create_task(&http, &base, task_id, "/nonexistent", 2).await;

    let task = wait_for_task_status(&http, &base, task_id, "DeadLettered", API_TIMEOUT).await;
    assert_eq!(task["task_id"], task_id);
    assert_eq!(task["status"], "DeadLettered");

    evidence(
        SUITE,
        "task dead-lettered",
        json!({
            "task_id": task_id,
            "status": "DeadLettered"
        }),
    );
}

/// Test 3: GET /metrics shows defer_dispatch_acked_total and
/// defer_dispatch_dead_lettered_total at least 1, and /readyz answers 200.
/// The test creates its own acked and dead-lettered tasks, so it does not
/// depend on the other tests or their order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn test_3_metrics_and_readyz() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    wait_healthy(&http, &base).await;
    put_queue(&http, &base).await;

    let acked_task = "deploy-smoke-metrics-ack";
    let dead_task = "deploy-smoke-metrics-dead-letter";
    create_task(&http, &base, acked_task, "/healthz", 3).await;
    create_task(&http, &base, dead_task, "/nonexistent", 2).await;
    wait_for_task_status(&http, &base, acked_task, "Succeeded", API_TIMEOUT).await;
    wait_for_task_status(&http, &base, dead_task, "DeadLettered", API_TIMEOUT).await;

    let deadline = tokio::time::Instant::now() + API_TIMEOUT;
    let (acked, dead_lettered) = loop {
        let last = match read_dispatch_counters(&http, &base).await {
            Ok((acked, dead_lettered)) if acked >= 1 && dead_lettered >= 1 => {
                break (acked, dead_lettered);
            }
            Ok((acked, dead_lettered)) => {
                format!("acked={acked}, dead_lettered={dead_lettered}")
            }
            Err(err) => err,
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "metrics show defer_dispatch_acked_total and defer_dispatch_dead_lettered_total >= 1: not reached within {API_TIMEOUT:?}; last saw: {last}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    };

    assert!(
        acked >= 1,
        "defer_dispatch_acked_total must be >= 1, got {acked}"
    );
    assert!(
        dead_lettered >= 1,
        "defer_dispatch_dead_lettered_total must be >= 1, got {dead_lettered}"
    );

    let readyz_resp = http
        .get(format!("{base}/readyz"))
        .send()
        .await
        .expect("GET /readyz");
    assert_eq!(readyz_resp.status().as_u16(), 200, "readyz status");

    evidence(
        SUITE,
        "metrics and readiness verified",
        json!({
            "defer_dispatch_acked_total": acked,
            "defer_dispatch_dead_lettered_total": dead_lettered,
            "readyz": 200
        }),
    );
}
