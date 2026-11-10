// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-process-security" tracker="#9" reason="Verify defer serve with --auth required enforces negative paths, tokenless probes, non-zero metrics, and redacted audit events."
use std::fs::File;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use axum::http::StatusCode;

const ADMIN_TOKEN: &str = "test-admin-secret-token";
const READ_TOKEN: &str = "test-read-secret-token";
const WRONG_TOKEN: &str = "test-wrong-secret-token";

struct ChildGuard {
    child: Option<Child>,
    stderr_path: PathBuf,
}

impl ChildGuard {
    fn spawn(
        bind_addr: &str,
        raft_port: u16,
        data_dir: &Path,
        registry_path: &Path,
        stdout_path: &Path,
        stderr_path: &Path,
    ) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_defer"))
            .args([
                "serve",
                "--bind",
                bind_addr,
                "--raft-port",
                &raft_port.to_string(),
                "--data-dir",
                data_dir.to_str().expect("valid data_dir utf8"),
                "--auth",
                "required",
                "--token-registry-file",
                registry_path.to_str().expect("valid registry_path utf8"),
                "--log-format",
                "json",
                "--dispatch-tick-ms",
                "1000",
            ])
            .env("RUST_LOG", "info,service_auth.audit=debug")
            .stdout(Stdio::from(
                File::create(stdout_path).expect("create stdout file"),
            ))
            .stderr(Stdio::from(
                File::create(stderr_path).expect("create stderr file"),
            ))
            .spawn()
            .expect("spawn defer serve");

        Self {
            child: Some(child),
            stderr_path: stderr_path.to_path_buf(),
        }
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn stderr_content(&self) -> String {
        std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.stop();
        if std::thread::panicking() {
            let stderr = self.stderr_content();
            if !stderr.is_empty() {
                eprintln!("defer serve child stderr on panic:\n{stderr}");
            }
        }
    }
}

fn allocate_free_port() -> (u16, String) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local addr");
    let port = addr.port();
    drop(listener);
    assert!(
        port != 7141 && port != 7142,
        "allocated port {port} must not be 7141 or 7142"
    );
    (port, addr.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn defer_serve_process_security_auth_and_observability() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let registry_path = dir.path().join("token-registry.json");
    let registry = serde_json::json!({
        ADMIN_TOKEN: {
            "subject": "admin-principal",
            "roles": {"*": "admin"}
        },
        READ_TOKEN: {
            "subject": "reader-principal",
            "roles": {"jobs": "read"}
        }
    });
    std::fs::write(
        &registry_path,
        serde_json::to_string(&registry).expect("serialize registry"),
    )
    .expect("write registry file");

    let data_dir = dir.path().join("data");
    let stdout_path = dir.path().join("defer.stdout.log");
    let stderr_path = dir.path().join("defer.stderr.log");

    let (bind_port, bind_addr) = allocate_free_port();
    let (raft_port, _) = allocate_free_port();
    assert!(
        bind_port != 7141 && bind_port != 7142 && raft_port != 7141 && raft_port != 7142,
        "free port allocation must not use default 7141 or 7142"
    );

    let mut guard = ChildGuard::spawn(
        &bind_addr,
        raft_port,
        &data_dir,
        &registry_path,
        &stdout_path,
        &stderr_path,
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("build reqwest client");
    let url = format!("http://{bind_addr}");

    let mut ready = false;
    for _ in 0..100 {
        if let Ok(res) = client.get(format!("{url}/healthz")).send().await {
            if res.status().is_success() {
                ready = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        ready,
        "defer serve failed to become ready at {url}/healthz within 10s; child stderr:\n{}",
        guard.stderr_content()
    );

    // GET /readyz answers 200 without a token
    let readyz_res = client
        .get(format!("{url}/readyz"))
        .send()
        .await
        .expect("GET /readyz request");
    assert_eq!(
        readyz_res.status(),
        StatusCode::OK,
        "GET /readyz should answer 200 without a token; stderr:\n{}",
        guard.stderr_content()
    );

    // PUT /v1/queues/jobs without a token answers 401
    let no_token_res = client
        .put(format!("{url}/v1/queues/jobs"))
        .json(&defer::QueuePolicy::default())
        .send()
        .await
        .expect("PUT /v1/queues/jobs without token");
    assert_eq!(
        no_token_res.status(),
        StatusCode::UNAUTHORIZED,
        "PUT /v1/queues/jobs without token should answer 401; stderr:\n{}",
        guard.stderr_content()
    );

    // PUT /v1/queues/jobs with a wrong token answers 401
    let wrong_token_res = client
        .put(format!("{url}/v1/queues/jobs"))
        .bearer_auth(WRONG_TOKEN)
        .json(&defer::QueuePolicy::default())
        .send()
        .await
        .expect("PUT /v1/queues/jobs with wrong token");
    assert_eq!(
        wrong_token_res.status(),
        StatusCode::UNAUTHORIZED,
        "PUT /v1/queues/jobs with wrong token should answer 401; stderr:\n{}",
        guard.stderr_content()
    );

    // PUT /v1/queues/jobs with the read token answers 403
    let read_token_res = client
        .put(format!("{url}/v1/queues/jobs"))
        .bearer_auth(READ_TOKEN)
        .json(&defer::QueuePolicy::default())
        .send()
        .await
        .expect("PUT /v1/queues/jobs with read token");
    assert_eq!(
        read_token_res.status(),
        StatusCode::FORBIDDEN,
        "PUT /v1/queues/jobs with read token should answer 403; stderr:\n{}",
        guard.stderr_content()
    );

    // PUT /v1/queues/jobs with the admin token answers 200
    let admin_token_res = client
        .put(format!("{url}/v1/queues/jobs"))
        .bearer_auth(ADMIN_TOKEN)
        .json(&defer::QueuePolicy::default())
        .send()
        .await
        .expect("PUT /v1/queues/jobs with admin token");
    assert_eq!(
        admin_token_res.status(),
        StatusCode::OK,
        "PUT /v1/queues/jobs with admin token should answer 200; stderr:\n{}",
        guard.stderr_content()
    );

    // GET /metrics answers 200 and body contains defer_requests_total > 0
    let metrics_res = client
        .get(format!("{url}/metrics"))
        .send()
        .await
        .expect("GET /metrics request");
    assert_eq!(
        metrics_res.status(),
        StatusCode::OK,
        "GET /metrics should answer 200; stderr:\n{}",
        guard.stderr_content()
    );
    let metrics_body = metrics_res.text().await.expect("read metrics body");
    let requests_line = metrics_body
        .lines()
        .find(|line| line.starts_with("defer_requests_total "))
        .unwrap_or_else(|| {
            panic!(
                "metrics body missing defer_requests_total metric line; body:\n{metrics_body}\nstderr:\n{}",
                guard.stderr_content()
            )
        });
    let count: u64 = requests_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_else(|| panic!("malformed defer_requests_total line: '{requests_line}'"))
        .parse()
        .unwrap_or_else(|e| panic!("failed to parse count from '{requests_line}': {e}"));
    assert!(
        count > 0,
        "defer_requests_total should be > 0, got {count}; stderr:\n{}",
        guard.stderr_content()
    );

    // Stop the child to flush output and inspect logs
    guard.stop();

    let stdout = std::fs::read_to_string(&stdout_path).unwrap_or_default();
    let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
    let logs = format!("{stdout}\n{stderr}");

    let audit_events: Vec<serde_json::Value> = logs
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .filter(|event: &serde_json::Value| {
            event["attributes"]["target"] == "service_auth.audit"
                || event["target"] == "service_auth.audit"
        })
        .collect();

    let has_deny = audit_events
        .iter()
        .any(|event| event["attributes"]["decision"] == "Deny" || event["decision"] == "Deny");
    assert!(
        has_deny,
        "missing JSON service_auth.audit event with decision Deny in logs:\n{logs}"
    );

    assert!(
        !stdout.contains(ADMIN_TOKEN),
        "stdout leaked admin token string"
    );
    assert!(
        !stdout.contains(READ_TOKEN),
        "stdout leaked read token string"
    );
    assert!(
        !stderr.contains(ADMIN_TOKEN),
        "stderr leaked admin token string"
    );
    assert!(
        !stderr.contains(READ_TOKEN),
        "stderr leaked read token string"
    );
}
