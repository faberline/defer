// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-request-audit" tracker="#261" reason="Read the defer.audit record of the shipped process for a write and a failed login."
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};

const REGISTRY: &str = r#"{"writer-secret-token":{"subject":"producer","roles":{"jobs":"write"}}}"#;
const WRITER_TOKEN: &str = "writer-secret-token";
const WRONG_TOKEN: &str = "wrong-secret-token";
const PAYLOAD_MARKER: &str = "payload-marker-9731";

/// Kills the defer child when the test ends, also when an assertion fails.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn tail_lines(path: &Path, count: usize) -> String {
    let Ok(text) = std::fs::read_to_string(path) else {
        return "<unreadable>".to_string();
    };
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(count);
    lines[start..].join("\n")
}

fn log_tail(stdout_path: &Path, stderr_path: &Path, count: usize) -> String {
    format!(
        "--- stdout ---\n{}\n--- stderr ---\n{}",
        tail_lines(stdout_path, count),
        tail_lines(stderr_path, count)
    )
}

async fn wait_ready(client: &reqwest::Client, url: &str) -> bool {
    for _ in 0..200 {
        if let Ok(response) = client.get(format!("{url}/readyz")).send().await {
            if response.status().is_success() {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// The `defer.audit` event whose operation, principal and result match.
fn audit<'a>(
    audits: &[&'a Value],
    operation: &str,
    principal: &str,
    result: &str,
) -> Option<&'a Value> {
    audits.iter().copied().find(|event| {
        let attributes = &event["attributes"];
        event["event"] == "request_audit"
            && attributes["operation"] == operation
            && attributes["principal"] == principal
            && attributes["result"] == result
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutating_requests_and_failed_logins_write_an_audit_record() {
    let dir = tempfile::tempdir().unwrap();
    let registry_path = dir.path().join("token-registry.json");
    std::fs::write(&registry_path, REGISTRY).unwrap();
    let data_dir = dir.path().join("data");
    let stdout_path = dir.path().join("defer.stdout.log");
    let stderr_path = dir.path().join("defer.stderr.log");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let url = format!("http://{address}");

    let child = Command::new(env!("CARGO_BIN_EXE_defer"))
        .args([
            "serve",
            "--bind",
            &address.to_string(),
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--auth",
            "required",
            "--token-registry-file",
            registry_path.to_str().unwrap(),
            "--log-format",
            "json",
            "--dispatch-tick-ms",
            "1000",
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::from(std::fs::File::create(&stdout_path).unwrap()))
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).unwrap()))
        .spawn()
        .unwrap();
    let server = Server(child);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        wait_ready(&client, &url).await,
        "defer not ready\n{}",
        log_tail(&stdout_path, &stderr_path, 40)
    );

    // 1. A mutating request with a valid token: a queue, then a task.
    let queue = client
        .put(format!("{url}/v1/queues/jobs"))
        .bearer_auth(WRITER_TOKEN)
        .json(&json!({
            "max_in_flight": 100,
            "max_dispatch_per_tick": 100,
            "max_dispatches_per_second": 100,
            "max_burst_size": 100,
            "lease_ttl_ms": 30000,
            "retry_backoff_ms": 250
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(queue.status().as_u16(), 200, "PUT queue");
    let task = client
        .post(format!("{url}/v1/queues/jobs/tasks"))
        .bearer_auth(WRITER_TOKEN)
        .json(&json!({
            "task_id": "audit-1",
            "target": { "url": "http://127.0.0.1:9/x", "method": "POST", "headers": {} },
            "payload": { "note": PAYLOAD_MARKER },
            "schedule_at": "2099-01-01T00:00:00Z",
            "priority": 10,
            "max_attempts": 3
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(task.status().as_u16(), 201, "POST task");

    // 2. Failed authentication: a wrong token on a write, and no token on a read.
    let wrong = client
        .put(format!("{url}/v1/queues/jobs"))
        .bearer_auth(WRONG_TOKEN)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status().as_u16(), 401, "PUT with a wrong token");
    let anonymous = client
        .get(format!("{url}/v1/queues/jobs"))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status().as_u16(), 401, "GET without a token");

    // 3. Stop the server so that its log files are complete.
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(server);
    let logs = format!(
        "{}\n{}",
        std::fs::read_to_string(&stdout_path).unwrap_or_default(),
        std::fs::read_to_string(&stderr_path).unwrap_or_default()
    );
    let events: Vec<Value> = logs
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let audits: Vec<&Value> = events
        .iter()
        .filter(|event| event["attributes"]["target"] == "defer.audit")
        .collect();
    let report = format!("audit events: {audits:#?}\nlogs:\n{logs}");

    let write = audit(&audits, "PUT /v1/queues/{queue}", "producer", "success")
        .unwrap_or_else(|| panic!("no PUT queue record\n{report}"));
    assert_eq!(write["attributes"]["queue"], "jobs", "{report}");
    let time = write["attributes"]["time"].as_str().unwrap_or_default();
    assert!(
        chrono::DateTime::parse_from_rfc3339(time).is_ok(),
        "time {time:?} is not RFC 3339\n{report}"
    );
    let posted = audit(
        &audits,
        "POST /v1/queues/{queue}/tasks",
        "producer",
        "success",
    )
    .unwrap_or_else(|| panic!("no POST task record\n{report}"));
    assert_eq!(posted["attributes"]["queue"], "jobs", "{report}");
    let refused = audit(
        &audits,
        "PUT /v1/queues/{queue}",
        "anonymous",
        "unauthenticated",
    )
    .unwrap_or_else(|| panic!("no record for the wrong token\n{report}"));
    assert_eq!(refused["attributes"]["queue"], "jobs", "{report}");
    assert!(
        audit(
            &audits,
            "GET /v1/queues/{queue}",
            "anonymous",
            "unauthenticated"
        )
        .is_some(),
        "no record for the GET without a token\n{report}"
    );
    let total = audits
        .iter()
        .filter(|event| event["event"] == "request_audit")
        .count();
    assert_eq!(total, 4, "one record per audited request\n{report}");
    for secret in [WRITER_TOKEN, WRONG_TOKEN, PAYLOAD_MARKER] {
        assert!(!logs.contains(secret), "the logs hold {secret}\n{report}");
    }
}
// HANDWRITE-END
