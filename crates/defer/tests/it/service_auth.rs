// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-service-auth" tracker="#766" reason="Defer integration proof for shared credential rotation and queue-scoped authorization."
use axum::http::{header, HeaderMap};
use defer::AuthConfig;
use service_auth::{Role, Verifier};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const REGISTRY: &str = r#"{
    "writer-token": {"subject": "producer", "roles": {"jobs": "write"}},
    "reader-token": {"subject": "worker", "roles": {"jobs": "read"}},
    "admin-token": {"subject": "root", "roles": {"*": "admin"}}
}"#;

fn required_auth() -> AuthConfig {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token-registry.json");
    std::fs::write(&path, REGISTRY).unwrap();
    AuthConfig::resolve("required", Some(path.to_str().unwrap()), None).unwrap()
}

#[test]
fn defer_auth_adapter_rotates_the_shared_registry_without_restart() {
    let verifier = required_auth().verifier();
    let mut before = HeaderMap::new();
    before.insert(
        header::AUTHORIZATION,
        "Bearer writer-token".parse().unwrap(),
    );
    let principal = verifier.authenticate(&before).unwrap();
    assert!(principal.ensure("jobs", Role::Write).is_ok());

    verifier
        .reload_json(r#"{"rotated":{"subject":"next","roles":{"jobs":"admin"}}}"#)
        .unwrap();
    assert!(verifier.authenticate(&before).is_err());

    let mut after = HeaderMap::new();
    after.insert(header::AUTHORIZATION, "Bearer rotated".parse().unwrap());
    let principal = verifier.authenticate(&after).unwrap();
    assert_eq!(principal.subject(), Some("next"));
    assert!(principal.ensure("jobs", Role::Admin).is_ok());
}

#[test]
fn malformed_rotation_keeps_the_last_known_good_registry() {
    let verifier = required_auth().verifier();
    assert!(verifier.reload_json("not-json").is_err());

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        "Bearer reader-token".parse().unwrap(),
    );
    let principal = verifier.authenticate(&headers).unwrap();
    assert!(principal.ensure("jobs", Role::Read).is_ok());
    assert!(principal.ensure("jobs", Role::Write).is_err());
}

// <HANDWRITE gap="missing-generator:e2e-test:defer-auth-runtime-wiring" tracker="#2215" reason="Exercise the shipped Defer process so removing its registry watcher or structured audit sink makes the security gate fail.">
fn timeout_diagnostic(elapsed: Duration, readyz: &str, log_tail: &str) -> String {
    format!(
        "aw-diag service_auth: first PUT /v1/queues/jobs failed after {} ms; readyz={}; defer log tail:\n{}",
        elapsed.as_millis(),
        readyz,
        log_tail
    )
}

fn tail_lines(path: &std::path::Path, count: usize) -> String {
    let Ok(text) = std::fs::read_to_string(path) else {
        return "<unreadable>".to_string();
    };
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(count);
    lines[start..].join("\n")
}

fn log_tail(stdout_path: &std::path::Path, stderr_path: &std::path::Path, count: usize) -> String {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn defer_serve_watches_registry_and_emits_redacted_audit_events() {
    let dir = tempfile::tempdir().unwrap();
    let registry_path = dir.path().join("token-registry.json");
    std::fs::write(
        &registry_path,
        r#"{"old-secret-token":{"subject":"old-admin","roles":{"*":"admin"}}}"#,
    )
    .unwrap();
    let data_dir = dir.path().join("data");
    let stdout_path = dir.path().join("defer.stdout.log");
    let stderr_path = dir.path().join("defer.stderr.log");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let mut child = Command::new(env!("CARGO_BIN_EXE_defer"))
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
        .env("RUST_LOG", "info,service_auth.audit=debug")
        .stdout(Stdio::from(std::fs::File::create(&stdout_path).unwrap()))
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).unwrap()))
        .spawn()
        .unwrap();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let url = format!("http://{address}");
    let ready = wait_ready(&client, &url).await;

    let old_status = if ready {
        let started = Instant::now();
        let response = client
            .put(format!("{url}/v1/queues/jobs"))
            .bearer_auth("old-secret-token")
            .json(&defer::QueuePolicy::default())
            .send()
            .await;
        match response {
            Ok(response) => response.status(),
            Err(error) => {
                let readyz = match client.get(format!("{url}/readyz")).send().await {
                    Ok(response) => format!("{}", response.status()),
                    Err(readyz_error) => format!("error: {readyz_error}"),
                };
                panic!(
                    "{}\nerror: {error}",
                    timeout_diagnostic(
                        started.elapsed(),
                        &readyz,
                        &log_tail(&stdout_path, &stderr_path, 40)
                    )
                );
            }
        }
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };

    let replacement_path = dir.path().join("token-registry.next.json");
    std::fs::write(
        &replacement_path,
        r#"{"new-secret-token":{"subject":"new-reader","roles":{"jobs":"read"}}}"#,
    )
    .unwrap();
    std::fs::rename(&replacement_path, &registry_path).unwrap();

    let mut new_status = axum::http::StatusCode::UNAUTHORIZED;
    // Production intentionally polls projected Secret files every 15 seconds;
    // wait through one full cadence instead of swapping in a test-only path.
    for _ in 0..400 {
        let response = client
            .get(format!("{url}/v1/queues/jobs"))
            .bearer_auth("new-secret-token")
            .send()
            .await;
        if let Ok(response) = response {
            new_status = response.status();
            if new_status == axum::http::StatusCode::OK {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let old_after_rotation = client
        .get(format!("{url}/v1/queues/jobs"))
        .bearer_auth("old-secret-token")
        .send()
        .await
        .unwrap()
        .status();
    let forbidden_status = client
        .put(format!("{url}/v1/queues/jobs"))
        .bearer_auth("new-secret-token")
        .json(&defer::QueuePolicy::default())
        .send()
        .await
        .unwrap()
        .status();

    tokio::time::sleep(Duration::from_millis(100)).await;
    child.kill().ok();
    child.wait().unwrap();
    let stdout = std::fs::read_to_string(&stdout_path).unwrap();
    let stderr = std::fs::read_to_string(&stderr_path).unwrap();
    let logs = format!("{stdout}\n{stderr}");
    let audit_events: Vec<serde_json::Value> = logs
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .filter(|event: &serde_json::Value| event["attributes"]["target"] == "service_auth.audit")
        .collect();

    assert!(ready, "Defer process did not become ready; logs:\n{logs}");
    assert_eq!(old_status, axum::http::StatusCode::OK);
    assert_eq!(new_status, axum::http::StatusCode::OK);
    assert_eq!(old_after_rotation, axum::http::StatusCode::UNAUTHORIZED);
    assert_eq!(forbidden_status, axum::http::StatusCode::FORBIDDEN);
    assert!(
        audit_events.iter().any(|event| {
            event["event"] == "credential_registry_reload"
                && event["attributes"]["applied"] == true
                && event["attributes"]["entries"] == 1
        }),
        "missing structured applied-reload audit event: {logs}"
    );
    assert!(
        audit_events.iter().any(|event| {
            event["event"] == "authorization_decision"
                && event["attributes"]["decision"] == "Deny"
                && event["attributes"]["reason"] == "InsufficientRole"
                && event["attributes"]["subject"] == "new-reader"
                && event["attributes"]["resource"] == "jobs"
                && event["attributes"]["needed"] == "Some(Write)"
        }),
        "missing structured queue-write denial audit event: {logs}"
    );
    assert!(
        !logs.contains("old-secret-token"),
        "audit leaked old bearer"
    );
    assert!(
        !logs.contains("new-secret-token"),
        "audit leaked new bearer"
    );
}

#[test]
fn timeout_diagnostic_names_elapsed_readyz_and_log_tail() {
    assert_eq!(
        timeout_diagnostic(
            Duration::from_millis(2001),
            "503 Service Unavailable",
            "line one\nline two"
        ),
        "aw-diag service_auth: first PUT /v1/queues/jobs failed after 2001 ms; readyz=503 Service Unavailable; defer log tail:\nline one\nline two"
    );
}

#[test]
fn log_tail_shows_the_end_of_stdout_and_stderr() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.log");
    let err = dir.path().join("err.log");
    std::fs::write(&out, "o1\no2\no3").unwrap();
    std::fs::write(&err, "e1\ne2\ne3").unwrap();
    assert_eq!(
        log_tail(&out, &err, 2),
        "--- stdout ---\no2\no3\n--- stderr ---\ne2\ne3"
    );
}

#[test]
fn log_tail_marks_an_unreadable_file() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.log");
    let err = dir.path().join("err.log");
    std::fs::write(&err, "e1").unwrap();
    assert!(
        log_tail(&missing, &err, 2).starts_with("--- stdout ---\n<unreadable>\n--- stderr ---\ne1")
    );
}
// </HANDWRITE>
// HANDWRITE-END
