// HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-quickstart-doc" tracker="#262" reason="Run the install.md quickstart commands against a real defer serve."
//! Runs the shell commands of the `## Quickstart` section of
//! `docs/operations/install.md` against a real `defer serve` process.
//!
//! Honest limit: CI builds the image from the same `cargo build` of
//! `-p defer --bin defer` (`Dockerfile`), but `cargo test --workspace` has no
//! Docker. This test runs `env!("CARGO_BIN_EXE_defer")`, not the image.

use std::fs::File;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const DOC: &str = "../../docs/operations/install.md";

/// Kills the child process on every exit path, including panic.
struct Guard(Option<Child>);

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn read_or_empty(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// The fenced ```bash blocks of the `## Quickstart` section, in order, with
/// up to 3 leading spaces removed from each line.
fn quickstart_blocks() -> Vec<String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(DOC);
    let text = std::fs::read_to_string(&path).expect("read install.md");
    let mut in_section = false;
    let mut current: Option<Vec<String>> = None;
    let mut blocks = Vec::new();
    for line in text.lines() {
        if let Some(block) = current.as_mut() {
            if line.trim() == "```" {
                blocks.push(block.join("\n"));
                current = None;
            } else {
                let spaces = line.len() - line.trim_start_matches(' ').len();
                block.push(line[spaces.min(3)..].to_string());
            }
        } else if line == "## Quickstart" {
            in_section = true;
        } else if line.starts_with("## ") {
            in_section = false;
        } else if in_section && line.trim() == "```bash" {
            current = Some(Vec::new());
        }
    }
    blocks
}

/// Ports are free at the moment of the call; all are held until the end so
/// they differ from each other.
fn free_ports(count: usize) -> Vec<u16> {
    let listeners: Vec<TcpListener> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().expect("local addr").port())
        .collect()
}

/// Replaces the doc's fixed ports (7141 defer, 9000 target) with free ones.
fn adapt(block: &str, http_port: u16, target_port: u16) -> String {
    block
        .replace("7141", &http_port.to_string())
        .replace("9000", &target_port.to_string())
}

async fn run_block(block: String) -> Output {
    tokio::task::spawn_blocking(move || {
        Command::new("bash")
            .arg("-c")
            .arg(block)
            .output()
            .expect("run bash")
    })
    .await
    .expect("join bash task")
}

async fn wait_for_healthz(url: &str, stderr_path: &Path) {
    let client = reqwest::Client::new();
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
        "defer serve not ready at {url}/healthz within 10s; stderr:\n{}",
        read_or_empty(stderr_path)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn install_md_quickstart_enqueues_and_dispatches() {
    let blocks = quickstart_blocks();
    assert_eq!(
        blocks.len(),
        4,
        "install.md Quickstart must have 4 bash blocks (target, create queue, create task, read task); the doc changed, so this test must follow it"
    );
    assert!(
        blocks[0].contains("9000") && blocks[0].contains("python3"),
        "block 0 must be the python3 target on port 9000"
    );
    for block in &blocks[1..] {
        assert!(
            block.contains("7141"),
            "blocks 1 to 3 must call defer on port 7141"
        );
    }

    let ports = free_ports(3);
    let (http_port, raft_port, target_port) = (ports[0], ports[1], ports[2]);
    assert!(
        ![7141, 7142, 9000].contains(&http_port)
            && ![7141, 7142, 9000].contains(&raft_port)
            && ![7141, 7142, 9000].contains(&target_port),
        "free ports must not be the doc's fixed ports"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    let serve_err_path = dir.path().join("serve.err");
    // The doc's --bind and --data-dir are replaced: the port must be free and
    // the data dir must be a temp dir. The dispatch tick is the default (100 ms).
    let serve = Command::new(env!("CARGO_BIN_EXE_defer"))
        .args([
            "serve",
            "--bind",
            &format!("127.0.0.1:{http_port}"),
            "--raft-port",
            &raft_port.to_string(),
            "--data-dir",
            data_dir.to_str().expect("utf8 data dir"),
        ])
        .stdout(Stdio::from(
            File::create(dir.path().join("serve.out")).expect("create serve.out"),
        ))
        .stderr(Stdio::from(
            File::create(&serve_err_path).expect("create serve.err"),
        ))
        .spawn()
        .expect("spawn defer serve");
    let _serve = Guard(Some(serve));
    wait_for_healthz(&format!("http://127.0.0.1:{http_port}"), &serve_err_path).await;

    // `exec` makes bash become python3, so killing the guard stops the target.
    let target_out_path = dir.path().join("target.out");
    let target_err_path = dir.path().join("target.err");
    let target = Command::new("bash")
        .arg("-c")
        .arg(format!(
            "exec {}",
            adapt(&blocks[0], http_port, target_port)
        ))
        .stdout(Stdio::from(
            File::create(&target_out_path).expect("create target.out"),
        ))
        .stderr(Stdio::from(
            File::create(&target_err_path).expect("create target.err"),
        ))
        .spawn()
        .expect("spawn target");
    let _target = Guard(Some(target));
    let target_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect(("127.0.0.1", target_port)).is_ok() {
            break;
        }
        if Instant::now() > target_deadline {
            panic!(
                "python3 is required for the install.md quickstart; target stderr:\n{}",
                read_or_empty(&target_err_path)
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let read_block = adapt(&blocks[3], http_port, target_port);

    // Negative check: before the create, the task does not exist yet.
    let before = run_block(read_block.clone()).await;
    let before_value = serde_json::from_slice::<serde_json::Value>(&before.stdout).ok();
    assert!(
        before_value.as_ref().and_then(|v| v["task_id"].as_str()) != Some("hello-1"),
        "hello-1 must not exist before the create block runs"
    );
    assert!(
        before_value.as_ref().and_then(|v| v["status"].as_str()) != Some("Succeeded"),
        "hello-1 must not be Succeeded before the create block runs"
    );

    // Create queue, then create task. The create-task response has no body
    // (201), so only its exit status is checked; the read below proves it.
    for (name, index) in [("create queue", 1), ("create task", 2)] {
        let out = run_block(adapt(&blocks[index], http_port, target_port)).await;
        assert!(
            out.status.success(),
            "{name} block failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // Poll the read until the dispatch tick runs the task.
    let deadline = Instant::now() + Duration::from_secs(15);
    let value = loop {
        let out = run_block(read_block.clone()).await;
        let last = String::from_utf8_lossy(&out.stdout).into_owned();
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&last) {
            if v["status"].as_str() == Some("Succeeded") {
                break v;
            }
        }
        if Instant::now() > deadline {
            panic!(
                "hello-1 did not reach Succeeded within 15s. last read:\n{last}\ntarget.out:\n{}\ntarget.err:\n{}\nserve.err:\n{}",
                read_or_empty(&target_out_path),
                read_or_empty(&target_err_path),
                read_or_empty(&serve_err_path)
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        value["task_id"].as_str(),
        Some("hello-1"),
        "the read must return task hello-1"
    );

    // The target prints one line per call it receives; flush may lag briefly.
    let mut target_out = String::new();
    for _ in 0..40 {
        target_out = read_or_empty(&target_out_path);
        if target_out.contains("target got /hook") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        target_out.contains("target got /hook"),
        "the target must receive /hook; target.out:\n{target_out}"
    );
}
// HANDWRITE-END
