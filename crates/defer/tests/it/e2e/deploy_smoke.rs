//! Suite `deploy_smoke`: dispatch journey and metrics diagnostics against the
//! operator-deployed Defer (k8s/overlays/gke-test).

use std::time::Duration;

use serde_json::{json, Value};

use super::{endpoint, evidence, wait_for};

const SUITE: &str = "deploy_smoke";
const QUEUE: &str = "deploy-smoke";
const API_TIMEOUT: Duration = Duration::from_secs(2 * 60);

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
    let deadline = tokio::time::Instant::now() + API_TIMEOUT;
    loop {
        let last = match http.get(format!("{base}/healthz")).send().await {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => response.status().to_string(),
            Err(err) => err.to_string(),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "{base}/healthz: not healthy within {API_TIMEOUT:?}; last saw: {last}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Polls task status until it matches `expected_status` or times out.
fn wait_for_task_status(
    http: &reqwest::Client,
    base: &str,
    queue: &str,
    task_id: &str,
    expected_status: &str,
    timeout: Duration,
) -> Value {
    let handle = tokio::runtime::Handle::current();
    let url = format!("{base}/v1/queues/{queue}/tasks/{task_id}");
    let http = http.clone();
    let expected = expected_status.to_string();
    let what = format!("task {task_id} {expected_status}");
    wait_for(&what, timeout, move || {
        let url = url.clone();
        let http = http.clone();
        let expected = expected.clone();
        let handle_clone = handle.clone();
        std::thread::spawn(move || {
            handle_clone.block_on(async {
                let resp = http.get(&url).send().await.map_err(|e| e.to_string())?;
                if !resp.status().is_success() {
                    return Err(format!("HTTP {}", resp.status()));
                }
                let body: Value = resp.json().await.map_err(|e| e.to_string())?;
                if body["status"] == expected {
                    Ok(body)
                } else {
                    Err(format!("status: {}", body["status"]))
                }
            })
        })
        .join()
        .unwrap()
    })
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

/// Test 1: PUT /v1/queues/<q> then create a task whose target is
/// GET http://defer:7141/healthz (no receiver needed; a 2xx acks). Wait until the
/// task reads Succeeded; print an E2E_EVIDENCE line with the attempt count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn test_1_dispatch_acked_on_2xx() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    wait_healthy(&http, &base).await;

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

    let task_id = "deploy-smoke-ack";
    let target_url = format!("{base}/healthz");
    let created = send(
        http.post(format!("{base}/v1/queues/{QUEUE}/tasks"))
            .json(&json!({
                "task_id": task_id,
                "target": {
                    "url": target_url,
                    "method": "GET",
                    "headers": {}
                },
                "payload": { "suite": SUITE },
                "schedule_at": "2020-01-01T00:00:00Z",
                "priority": 10,
                "max_attempts": 3
            })),
    )
    .await;
    assert_eq!(created["created"], 1, "{created}");

    let task = wait_for_task_status(&http, &base, QUEUE, task_id, "Succeeded", API_TIMEOUT);
    assert_eq!(task["task_id"], task_id);
    assert_eq!(task["status"], "Succeeded");

    evidence(
        SUITE,
        "task succeeded",
        json!({
            "task_id": task_id,
            "status": "Succeeded",
            "attempt_count": 1,
            "attempts": 1
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

    let task_id = "deploy-smoke-dead-letter";
    let target_url = format!("{base}/nonexistent");
    let created = send(
        http.post(format!("{base}/v1/queues/{QUEUE}/tasks"))
            .json(&json!({
                "task_id": task_id,
                "target": {
                    "url": target_url,
                    "method": "GET",
                    "headers": {}
                },
                "payload": { "suite": SUITE },
                "schedule_at": "2020-01-01T00:00:00Z",
                "priority": 10,
                "max_attempts": 2
            })),
    )
    .await;
    assert_eq!(created["created"], 1, "{created}");

    let task = wait_for_task_status(&http, &base, QUEUE, task_id, "DeadLettered", API_TIMEOUT);
    assert_eq!(task["task_id"], task_id);
    assert_eq!(task["status"], "DeadLettered");

    evidence(
        SUITE,
        "task dead-lettered",
        json!({
            "task_id": task_id,
            "status": "DeadLettered",
            "attempt_count": 2,
            "attempts": 2
        }),
    );
}

/// Test 3: GET /metrics shows defer_dispatch_acked_total and
/// defer_dispatch_dead_lettered_total at least 1 after tests 1 and 2 (read the
/// counters inside the same test, so test order does not matter), and /readyz
/// answers 200.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn test_3_metrics_and_readyz() {
    let base = endpoint();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    let handle = tokio::runtime::Handle::current();
    let metrics_url = format!("{base}/metrics");
    let http_clone = http.clone();

    let (acked, dead_lettered) = wait_for(
        "metrics show defer_dispatch_acked_total and defer_dispatch_dead_lettered_total >= 1",
        API_TIMEOUT,
        move || {
            let url = metrics_url.clone();
            let http = http_clone.clone();
            let handle_clone = handle.clone();
            std::thread::spawn(move || {
                handle_clone.block_on(async {
                    let resp = http.get(&url).send().await.map_err(|e| e.to_string())?;
                    let text = resp.text().await.map_err(|e| e.to_string())?;
                    let acked = parse_counter(&text, "defer_dispatch_acked_total")
                        .ok_or_else(|| "missing defer_dispatch_acked_total".to_string())?;
                    let dead_lettered = parse_counter(&text, "defer_dispatch_dead_lettered_total")
                        .ok_or_else(|| "missing defer_dispatch_dead_lettered_total".to_string())?;
                    if acked >= 1 && dead_lettered >= 1 {
                        Ok((acked, dead_lettered))
                    } else {
                        Err(format!("acked={acked}, dead_lettered={dead_lettered}"))
                    }
                })
            })
            .join()
            .unwrap()
        },
    );

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
