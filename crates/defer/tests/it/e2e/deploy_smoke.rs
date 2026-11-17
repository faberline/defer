//! Suite `deploy_smoke`: dispatch journey and metrics diagnostics against the
//! operator-deployed Defer (k8s/overlays/gke-test).

use std::time::Duration;

use serde_json::{json, Value};

use super::{endpoint, evidence, required_env, try_kubectl, wait_for};

const SUITE: &str = "deploy_smoke";
const QUEUE: &str = "deploy-smoke";
const POD: &str = "defer-0";
const FIRST_POD_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const API_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// replicasPerShard of k8s/overlays/gke-test/defer-cr.yaml; the pods are defer-0 .. defer-2.
const REPLICAS: u32 = 3;

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

/// Sends a request until it answers 2xx, and returns its JSON body. A 5xx, a
/// 3xx or a connection error is retried every `POLL_INTERVAL` until
/// `API_TIMEOUT`; a 4xx fails at once.
async fn send(request: reqwest::RequestBuilder) -> Value {
    let deadline = tokio::time::Instant::now() + API_TIMEOUT;
    loop {
        let attempt = request.try_clone().expect("cloneable request");
        let last = match attempt.send().await {
            Ok(response) if response.status().is_success() => {
                let url = response.url().clone();
                let body = response.text().await.expect("body");
                return serde_json::from_str(&body)
                    .unwrap_or_else(|err| panic!("{url}: not JSON: {err}: {body}"));
            }
            Ok(response) if response.status().is_client_error() => {
                let url = response.url().clone();
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                panic!("{url}: {status}: {body}");
            }
            Ok(response) => {
                let url = response.url().clone();
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                format!("{url}: {status}: {body}")
            }
            Err(err) => err.to_string(),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "no 2xx within {API_TIMEOUT:?}; last saw: {last}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
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

/// The base URL of each replica, read through the headless Service, which
/// publishes not-ready pods: `defer-<i>.defer-headless.<namespace>`.
fn replica_bases(namespace: &str) -> Vec<String> {
    (0..REPLICAS)
        .map(|i| format!("http://defer-{i}.defer-headless.{namespace}.svc.cluster.local:7141"))
        .collect()
}

/// Reads `defer_dispatch_acked_total` and `defer_dispatch_dead_lettered_total`
/// from `GET /metrics` of every base, and returns the sums. The counters are
/// per process. Every replica runs the dispatcher, and the replica that leases
/// a task (a follower forwards the lease to the leader) does the HTTP call and
/// counts it, so one replica can read 0 while another one dispatched. A failing
/// base fails the whole read.
async fn read_dispatch_counters(
    http: &reqwest::Client,
    bases: &[String],
) -> Result<(u64, u64), String> {
    let mut acked_total = 0;
    let mut dead_lettered_total = 0;
    for base in bases {
        let response = http
            .get(format!("{base}/metrics"))
            .send()
            .await
            .map_err(|err| format!("{base}: {err}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("{base}/metrics: {status}"));
        }
        let text = response
            .text()
            .await
            .map_err(|err| format!("{base}: {err}"))?;
        let acked = parse_counter(&text, "defer_dispatch_acked_total")
            .ok_or_else(|| format!("{base}: missing defer_dispatch_acked_total"))?;
        let dead_lettered = parse_counter(&text, "defer_dispatch_dead_lettered_total")
            .ok_or_else(|| format!("{base}: missing defer_dispatch_dead_lettered_total"))?;
        acked_total += acked;
        dead_lettered_total += dead_lettered;
    }
    Ok((acked_total, dead_lettered_total))
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

/// Test 3: GET /metrics of every replica shows defer_dispatch_acked_total and
/// defer_dispatch_dead_lettered_total at least 1 in sum, and /readyz answers 200.
/// Counters are per process and every replica dispatches the tasks it leases, so
/// the acked and the dead-lettered task can be counted on different replicas. The
/// test sums the counters of the three replicas instead of reading one through
/// the Service.
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
    let bases = replica_bases(&required_env("NAMESPACE"));

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
        let last = match read_dispatch_counters(&http, &bases).await {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::{read_dispatch_counters, replica_bases, send};

    /// One complete HTTP/1.1 response with a JSON body; the connection closes after it.
    fn http_response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// Starts a local server that answers one connection per entry of
    /// `responses`, in order. Returns the URL of its root path.
    async fn serve(responses: Vec<String>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = socket.read(&mut buf).await.expect("read");
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&buf[..n]);
                }
                socket
                    .write_all(response.as_bytes())
                    .await
                    .expect("write response");
                socket.shutdown().await.ok();
            }
        });
        format!("http://{addr}/v1/queues/q")
    }

    #[tokio::test]
    async fn send_retries_a_503_then_returns_the_json_body() {
        let url = serve(vec![
            http_response(
                "503 Service Unavailable",
                r#"{"error":"internal","message":"raft: no leader elected (cluster not ready)"}"#,
            ),
            http_response("200 OK", r#"{"queue":"q"}"#),
        ])
        .await;

        let body = send(reqwest::Client::new().get(url)).await;

        assert_eq!(body, json!({ "queue": "q" }));
    }

    /// `serve` returns `http://{addr}/v1/queues/q`; the base is the part before it.
    fn base_of(url: &str) -> String {
        url.trim_end_matches("/v1/queues/q").to_owned()
    }

    #[test]
    fn replica_bases_names_each_pod_through_the_headless_service() {
        assert_eq!(
            replica_bases("ns"),
            [
                "http://defer-0.defer-headless.ns.svc.cluster.local:7141",
                "http://defer-1.defer-headless.ns.svc.cluster.local:7141",
                "http://defer-2.defer-headless.ns.svc.cluster.local:7141",
            ]
        );
    }

    #[tokio::test]
    async fn read_dispatch_counters_sums_all_replicas() {
        let a = base_of(
            &serve(vec![http_response(
                "200 OK",
                "defer_dispatch_acked_total 1\ndefer_dispatch_dead_lettered_total 0\n",
            )])
            .await,
        );
        let b = base_of(
            &serve(vec![http_response(
                "200 OK",
                "defer_dispatch_acked_total 0\ndefer_dispatch_dead_lettered_total 1\n",
            )])
            .await,
        );
        let c = base_of(
            &serve(vec![http_response(
                "200 OK",
                "defer_dispatch_acked_total 0\ndefer_dispatch_dead_lettered_total 0\n",
            )])
            .await,
        );

        let counters = read_dispatch_counters(&reqwest::Client::new(), &[a, b, c]).await;

        assert_eq!(counters, Ok((1, 1)));
    }

    #[tokio::test]
    async fn read_dispatch_counters_reports_the_replica_that_fails() {
        let a = base_of(
            &serve(vec![http_response(
                "200 OK",
                "defer_dispatch_acked_total 1\ndefer_dispatch_dead_lettered_total 0\n",
            )])
            .await,
        );
        let b = base_of(&serve(vec![http_response("503 Service Unavailable", "no")]).await);

        let err = read_dispatch_counters(&reqwest::Client::new(), &[a, b.clone()])
            .await
            .expect_err("a 503 replica must fail the read");

        assert!(err.contains(&b), "{err}");
        assert!(err.contains("503"), "{err}");
    }

    #[tokio::test]
    async fn read_dispatch_counters_fails_when_a_counter_is_missing() {
        let a = base_of(&serve(vec![http_response("200 OK", "defer_requests_total 3\n")]).await);

        let err = read_dispatch_counters(&reqwest::Client::new(), &[a])
            .await
            .expect_err("a missing counter must fail the read");

        assert!(err.contains("missing defer_dispatch_acked_total"), "{err}");
    }

    #[tokio::test]
    #[should_panic(expected = "404")]
    async fn send_fails_at_once_on_a_404() {
        let url = serve(vec![http_response("404 Not Found", r#"{"error":"nf"}"#)]).await;

        // A 4xx must panic straight away; a retry would not return within 30 s.
        tokio::time::timeout(
            Duration::from_secs(30),
            send(reqwest::Client::new().get(url)),
        )
        .await
        .expect("send returned before API_TIMEOUT");
    }
}
