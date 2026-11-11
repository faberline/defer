// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-soak-script" tracker="#4" reason="Verify soak.sh autostart fail-closed isolation and syntax checks without running a live cluster or soak workload."
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn workspace_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

#[test]
fn autostart_fails_closed_when_upstream_is_occupied_plain_tcp() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp listener");
    let port = listener.local_addr().expect("local addr").port();
    let received_queue_request = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    let listener_stop = Arc::clone(&stop);
    let listener_flag = Arc::clone(&received_queue_request);
    listener.set_nonblocking(true).expect("set nonblocking");
    let server_thread = std::thread::spawn(move || {
        while !listener_stop.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buf = [0u8; 1024];
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
                    if let Ok(n) = stream.read(&mut buf) {
                        let req = String::from_utf8_lossy(&buf[..n]);
                        if req.contains("/v1/queues") {
                            listener_flag.store(true, Ordering::SeqCst);
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });

    let start = Instant::now();
    let output = Command::new("bash")
        .arg("scripts/soak.sh")
        .current_dir(workspace_root())
        .env("DEFER_SOAK_AUTOSTART", "1")
        .env("DEFER_UPSTREAM", format!("127.0.0.1:{port}"))
        .output()
        .expect("run scripts/soak.sh");
    let elapsed = start.elapsed();

    stop.store(true, Ordering::SeqCst);
    let _ = server_thread.join();

    assert!(!output.status.success(), "expected non-zero exit code");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("isolation"),
        "expected 'isolation' in stderr, got: {stderr}"
    );
    assert!(
        !received_queue_request.load(Ordering::SeqCst),
        "listener should not have received any request to /v1/queues"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "test run took too long: {:?}",
        elapsed
    );
}

#[test]
fn autostart_fails_closed_when_upstream_is_occupied_answering_healthz() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp listener");
    let port = listener.local_addr().expect("local addr").port();
    let received_queue_request = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    let listener_stop = Arc::clone(&stop);
    let listener_flag = Arc::clone(&received_queue_request);
    listener.set_nonblocking(true).expect("set nonblocking");
    let server_thread = std::thread::spawn(move || {
        while !listener_stop.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buf = [0u8; 1024];
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                    if let Ok(n) = stream.read(&mut buf) {
                        let req = String::from_utf8_lossy(&buf[..n]);
                        if req.contains("/v1/queues") {
                            listener_flag.store(true, Ordering::SeqCst);
                        }
                        if req.contains("/healthz") {
                            let response =
                                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
                            let _ = stream.write_all(response.as_bytes());
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });

    let start = Instant::now();
    let output = Command::new("bash")
        .arg("scripts/soak.sh")
        .current_dir(workspace_root())
        .env("DEFER_SOAK_AUTOSTART", "1")
        .env("DEFER_UPSTREAM", format!("127.0.0.1:{port}"))
        .output()
        .expect("run scripts/soak.sh");
    let elapsed = start.elapsed();

    stop.store(true, Ordering::SeqCst);
    let _ = server_thread.join();

    assert!(!output.status.success(), "expected non-zero exit code");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("isolation"),
        "expected 'isolation' in stderr, got: {stderr}"
    );
    assert!(
        !received_queue_request.load(Ordering::SeqCst),
        "listener should not have received any request to /v1/queues"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "test run took too long: {:?}",
        elapsed
    );
}

/// Runs `scripts/soak.sh` against a fake upstream that answers every request
/// with `200 ok`, so the script passes its reachability check and reaches the
/// argument validation. Each request is answered; the thread stops after the run.
fn run_with_fake_healthz(env: &[(&str, &str)]) -> std::process::Output {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp listener");
    let port = listener.local_addr().expect("local addr").port();
    let stop = Arc::new(AtomicBool::new(false));

    let listener_stop = Arc::clone(&stop);
    listener.set_nonblocking(true).expect("set nonblocking");
    let server_thread = std::thread::spawn(move || {
        while !listener_stop.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buf = [0u8; 1024];
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                    let _ = stream.read(&mut buf);
                    let response =
                        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
                    let _ = stream.write_all(response.as_bytes());
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });

    let mut command = Command::new("bash");
    command
        .arg("scripts/soak.sh")
        .current_dir(workspace_root())
        .env_remove("DEFER_SOAK_AUTOSTART")
        .env("DEFER_UPSTREAM", format!("127.0.0.1:{port}"))
        .env("DEFER_SOAK_PID", std::process::id().to_string());
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command.output().expect("run scripts/soak.sh");

    stop.store(true, Ordering::SeqCst);
    let _ = server_thread.join();
    output
}

#[test]
fn rejects_zero_cache_batch_rounds() {
    let output = run_with_fake_healthz(&[("DEFER_SOAK_CACHE_BATCH_ROUNDS", "0")]);
    assert!(!output.status.success(), "expected non-zero exit code");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("DEFER_SOAK_CACHE_BATCH_ROUNDS must be an integer >= 1"),
        "expected batch rounds validation in stderr, got: {stderr}"
    );
}

#[test]
fn rejects_non_numeric_cache_max_batches() {
    let output = run_with_fake_healthz(&[("DEFER_SOAK_CACHE_MAX_BATCHES", "abc")]);
    assert!(!output.status.success(), "expected non-zero exit code");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("DEFER_SOAK_CACHE_MAX_BATCHES must be an integer >= 1"),
        "expected max batches validation in stderr, got: {stderr}"
    );
}

#[test]
fn rejects_zero_rss_sample_secs() {
    let output = run_with_fake_healthz(&[("DEFER_SOAK_RSS_SAMPLE_SECS", "0")]);
    assert!(!output.status.success(), "expected non-zero exit code");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("DEFER_SOAK_RSS_SAMPLE_SECS must be an integer >= 1"),
        "expected RSS sample validation in stderr, got: {stderr}"
    );
}

#[test]
fn rejects_rss_sample_secs_longer_than_window() {
    let output = run_with_fake_healthz(&[
        ("DEFER_SOAK_DURATION_SECS", "2"),
        ("DEFER_SOAK_RSS_SAMPLE_SECS", "5"),
    ]);
    assert!(!output.status.success(), "expected non-zero exit code");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("DEFER_SOAK_RSS_SAMPLE_SECS must not exceed the window length"),
        "expected window length validation in stderr, got: {stderr}"
    );
}

#[test]
fn soak_script_passes_bash_n_syntax_check() {
    let output = Command::new("bash")
        .arg("-n")
        .arg("scripts/soak.sh")
        .current_dir(workspace_root())
        .output()
        .expect("run bash -n scripts/soak.sh");
    assert!(
        output.status.success(),
        "bash -n scripts/soak.sh failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Serves the queue state that `inspect_fixed` reads on a loopback port.
/// `task_count` is the value of `/v1/queues/soak-jobs`. Returns the port,
/// a stop flag and the server thread.
fn serve_fake_upstream(task_count: u64) -> (u16, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp listener");
    let port = listener.local_addr().expect("local addr").port();
    listener.set_nonblocking(true).expect("set nonblocking");
    let stop = Arc::new(AtomicBool::new(false));
    let listener_stop = Arc::clone(&stop);
    let server_thread = std::thread::spawn(move || {
        while !listener_stop.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
                    let mut buf = [0u8; 2048];
                    let n = stream.read(&mut buf).unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let path = request.split_whitespace().nth(1).unwrap_or("");
                    let body = match path {
                        "/v1/queues/soak-jobs" => {
                            format!("{{\"task_count\": {task_count}, \"terminal_count\": 1}}")
                        }
                        "/v1/queues/soak-jobs/tasks/fixed" => {
                            "{\"status\": \"Succeeded\"}".to_string()
                        }
                        "/v1/queues/soak-jobs/tasks/fixed-retry" => {
                            "{\"status\": \"Scheduled\"}".to_string()
                        }
                        _ => "ok".to_string(),
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes());
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });
    (port, stop, server_thread)
}

/// Runs the real `inspect_fixed` from `scripts/soak.sh` in an `if` context,
/// where bash turns off `set -e`, and returns its stdout. The marker is
/// `INSPECT_OK` or `INSPECT_FAILED`.
fn run_inspect_fixed(port: u16) -> String {
    let script = r#"set -euo pipefail
source <(grep '^healthy() ' scripts/soak.sh; awk '/^inspect_fixed\(\) \{/,/^}/' scripts/soak.sh)
UPSTREAM="127.0.0.1:$1"; QUEUE="soak-jobs"
if inspect_fixed; then echo INSPECT_OK; else echo INSPECT_FAILED; fi
"#;
    let output = Command::new("bash")
        .arg("-c")
        .arg(script)
        .arg("bash")
        .arg(port.to_string())
        .current_dir(workspace_root())
        .output()
        .expect("run bash for inspect_fixed");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(
        stdout.contains("INSPECT_OK") || stdout.contains("INSPECT_FAILED"),
        "inspect_fixed harness did not finish: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
}

#[test]
fn inspect_fixed_fails_when_queue_state_is_wrong() {
    let (port, stop, server_thread) = serve_fake_upstream(3);
    let stdout = run_inspect_fixed(port);
    stop.store(true, Ordering::SeqCst);
    let _ = server_thread.join();
    assert!(
        stdout.contains("INSPECT_FAILED") && !stdout.contains("INSPECT_OK"),
        "inspect_fixed must fail when task_count is 3, got: {stdout}"
    );
}

#[test]
fn inspect_fixed_passes_when_queue_state_is_right() {
    let (port, stop, server_thread) = serve_fake_upstream(2);
    let stdout = run_inspect_fixed(port);
    stop.store(true, Ordering::SeqCst);
    let _ = server_thread.join();
    assert!(
        stdout.contains("INSPECT_OK"),
        "inspect_fixed must pass when task_count is 2, got: {stdout}"
    );
}
// HANDWRITE-END
