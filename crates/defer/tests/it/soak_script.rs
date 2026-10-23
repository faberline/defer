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
// HANDWRITE-END
