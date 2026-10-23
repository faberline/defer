// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-process-journey" tracker="#6" reason="Prove the shipped defer serve binary keeps its domain state across a real process restart."
use std::fs::File;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use axum::http::StatusCode;
use chrono::Utc;

struct ChildGuard {
    child: Option<Child>,
    stderr_path: PathBuf,
}

impl ChildGuard {
    fn spawn(
        bind_addr: &str,
        raft_port: u16,
        data_dir: &Path,
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
                "--dispatch-tick-ms",
                "1000",
            ])
            .stdout(Stdio::from(File::create(stdout_path).expect("create stdout file")))
            .stderr(Stdio::from(File::create(stderr_path).expect("create stderr file")))
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
    (port, addr.to_string())
}

async fn wait_for_healthz(client: &reqwest::Client, url: &str, guard: &ChildGuard) {
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
}

fn task_body(task_id: &str) -> serde_json::Value {
    serde_json::json!({
        "task_id": task_id,
        "target": {"url": "http://127.0.0.1/", "method": "POST", "headers": {}},
        "payload": {},
        "schedule_at": Utc::now() + chrono::Duration::minutes(5),
        "priority": 10,
        "max_attempts": 1
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn defer_serve_domain_state_survives_process_restart() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let data_dir = dir.path().join("data");

    let (bind_port1, bind_addr1) = allocate_free_port();
    let (raft_port1, _) = allocate_free_port();
    assert!(
        bind_port1 != 7141 && bind_port1 != 7142 && raft_port1 != 7141 && raft_port1 != 7142,
        "free port allocation must not use default 7141 or 7142"
    );

    let stdout_path1 = dir.path().join("defer.stdout.1.log");
    let stderr_path1 = dir.path().join("defer.stderr.1.log");
    let mut guard1 = ChildGuard::spawn(
        &bind_addr1,
        raft_port1,
        &data_dir,
        &stdout_path1,
        &stderr_path1,
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("build reqwest client");

    let url1 = format!("http://{bind_addr1}");
    wait_for_healthz(&client, &url1, &guard1).await;

    let put_queue_res = client
        .put(format!("{url1}/v1/queues/jobs"))
        .json(&defer::QueuePolicy::default())
        .send()
        .await
        .expect("send PUT /v1/queues/jobs");
    assert_eq!(
        put_queue_res.status(),
        StatusCode::OK,
        "PUT /v1/queues/jobs failed; stderr:\n{}",
        guard1.stderr_content()
    );

    let post_kept = client
        .post(format!("{url1}/v1/queues/jobs/tasks"))
        .json(&task_body("kept"))
        .send()
        .await
        .expect("send POST task kept");
    assert_eq!(
        post_kept.status(),
        StatusCode::CREATED,
        "POST task kept failed; stderr:\n{}",
        guard1.stderr_content()
    );

    let post_dropped = client
        .post(format!("{url1}/v1/queues/jobs/tasks"))
        .json(&task_body("dropped"))
        .send()
        .await
        .expect("send POST task dropped");
    assert_eq!(
        post_dropped.status(),
        StatusCode::CREATED,
        "POST task dropped failed; stderr:\n{}",
        guard1.stderr_content()
    );

    let delete_dropped = client
        .delete(format!("{url1}/v1/queues/jobs/tasks/dropped"))
        .send()
        .await
        .expect("send DELETE task dropped");
    assert_eq!(
        delete_dropped.status(),
        StatusCode::NO_CONTENT,
        "DELETE task dropped failed; stderr:\n{}",
        guard1.stderr_content()
    );

    let pause_res = client
        .post(format!("{url1}/v1/queues/jobs/control"))
        .json(&serde_json::json!({"state": "Paused"}))
        .send()
        .await
        .expect("send POST control paused");
    assert_eq!(
        pause_res.status(),
        StatusCode::OK,
        "POST control paused failed; stderr:\n{}",
        guard1.stderr_content()
    );

    let queue_get1 = client
        .get(format!("{url1}/v1/queues/jobs"))
        .send()
        .await
        .expect("send GET /v1/queues/jobs");
    assert_eq!(
        queue_get1.status(),
        StatusCode::OK,
        "GET /v1/queues/jobs failed; stderr:\n{}",
        guard1.stderr_content()
    );
    let queue_body1: serde_json::Value = queue_get1.json().await.expect("parse queue JSON");
    assert_eq!(
        queue_body1["control_state"],
        "Paused",
        "queue control_state before restart is not Paused: {queue_body1}; stderr:\n{}",
        guard1.stderr_content()
    );

    let kept_get1 = client
        .get(format!("{url1}/v1/queues/jobs/tasks/kept"))
        .send()
        .await
        .expect("send GET task kept");
    assert_eq!(
        kept_get1.status(),
        StatusCode::OK,
        "GET task kept failed; stderr:\n{}",
        guard1.stderr_content()
    );
    let kept_body1: serde_json::Value = kept_get1.json().await.expect("parse kept task JSON");
    assert_eq!(
        kept_body1["status"],
        "Scheduled",
        "kept task status before restart is not Scheduled: {kept_body1}; stderr:\n{}",
        guard1.stderr_content()
    );

    let dropped_get1 = client
        .get(format!("{url1}/v1/queues/jobs/tasks/dropped"))
        .send()
        .await
        .expect("send GET task dropped");
    assert_eq!(
        dropped_get1.status(),
        StatusCode::OK,
        "GET task dropped failed; stderr:\n{}",
        guard1.stderr_content()
    );
    let dropped_body1: serde_json::Value = dropped_get1.json().await.expect("parse dropped task JSON");
    assert_eq!(
        dropped_body1["status"],
        "Canceled",
        "dropped task status before restart is not Canceled: {dropped_body1}; stderr:\n{}",
        guard1.stderr_content()
    );

    guard1.stop();

    let (bind_port2, bind_addr2) = allocate_free_port();
    let (raft_port2, _) = allocate_free_port();
    assert!(
        bind_port2 != 7141 && bind_port2 != 7142 && raft_port2 != 7141 && raft_port2 != 7142,
        "second free port allocation must not use default 7141 or 7142"
    );

    let stdout_path2 = dir.path().join("defer.stdout.2.log");
    let stderr_path2 = dir.path().join("defer.stderr.2.log");
    let mut guard2 = ChildGuard::spawn(
        &bind_addr2,
        raft_port2,
        &data_dir,
        &stdout_path2,
        &stderr_path2,
    );

    let url2 = format!("http://{bind_addr2}");
    wait_for_healthz(&client, &url2, &guard2).await;

    let queue_get2 = client
        .get(format!("{url2}/v1/queues/jobs"))
        .send()
        .await
        .expect("send GET /v1/queues/jobs after restart");
    assert_eq!(
        queue_get2.status(),
        StatusCode::OK,
        "GET /v1/queues/jobs after restart failed; stderr:\n{}",
        guard2.stderr_content()
    );
    let queue_body2: serde_json::Value = queue_get2.json().await.expect("parse queue JSON after restart");
    assert_eq!(
        queue_body2["control_state"],
        "Paused",
        "queue control_state after restart is not Paused: {queue_body2}; stderr:\n{}",
        guard2.stderr_content()
    );

    let kept_get2 = client
        .get(format!("{url2}/v1/queues/jobs/tasks/kept"))
        .send()
        .await
        .expect("send GET task kept after restart");
    assert_eq!(
        kept_get2.status(),
        StatusCode::OK,
        "GET task kept after restart failed; stderr:\n{}",
        guard2.stderr_content()
    );
    let kept_body2: serde_json::Value = kept_get2.json().await.expect("parse kept task JSON after restart");
    assert_eq!(
        kept_body2["status"],
        "Scheduled",
        "kept task status after restart is not Scheduled: {kept_body2}; stderr:\n{}",
        guard2.stderr_content()
    );

    let dropped_get2 = client
        .get(format!("{url2}/v1/queues/jobs/tasks/dropped"))
        .send()
        .await
        .expect("send GET task dropped after restart");
    assert_eq!(
        dropped_get2.status(),
        StatusCode::OK,
        "GET task dropped after restart failed; stderr:\n{}",
        guard2.stderr_content()
    );
    let dropped_body2: serde_json::Value = dropped_get2.json().await.expect("parse dropped task JSON after restart");
    assert_eq!(
        dropped_body2["status"],
        "Canceled",
        "dropped task status after restart is not Canceled: {dropped_body2}; stderr:\n{}",
        guard2.stderr_content()
    );

    let post_kept_again = client
        .post(format!("{url2}/v1/queues/jobs/tasks"))
        .json(&task_body("kept"))
        .send()
        .await
        .expect("send second POST task kept after restart");
    assert_eq!(
        post_kept_again.status(),
        StatusCode::CONFLICT,
        "second POST task kept after restart status is not 409 Conflict: {}; stderr:\n{}",
        post_kept_again.status(),
        guard2.stderr_content()
    );
    let conflict_body: serde_json::Value = post_kept_again.json().await.expect("parse conflict JSON");
    assert_eq!(
        conflict_body["error"],
        "conflict",
        "unexpected conflict error envelope: {conflict_body}; stderr:\n{}",
        guard2.stderr_content()
    );

    guard2.stop();
}
// HANDWRITE-END
