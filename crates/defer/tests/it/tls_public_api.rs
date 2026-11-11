//! `defer serve --tls-cert-file` / `--tls-key-file`: the public API over HTTPS.

use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};

struct Material {
    _dir: tempfile::TempDir,
    cert: PathBuf,
    key: PathBuf,
    ca_pem: String,
}

/// A CA and a `localhost` leaf signed by it, written as PEM files.
fn material() -> Material {
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Defer test CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &ca, &ca_key).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("tls.crt");
    let key_path = dir.path().join("tls.key");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    Material {
        _dir: dir,
        cert: cert_path,
        key: key_path,
        ca_pem: ca.pem(),
    }
}

fn free_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_public_api_serves_https_and_refuses_plain_http() {
    let material = material();
    let work = tempfile::tempdir().unwrap();
    let data_dir = work.path().join("data");
    let stderr_path = work.path().join("stderr.log");
    let address = free_address();

    let mut child = Command::new(env!("CARGO_BIN_EXE_defer"))
        .args([
            "serve",
            "--bind",
            &address.to_string(),
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--tls-cert-file",
            material.cert.to_str().unwrap(),
            "--tls-key-file",
            material.key.to_str().unwrap(),
            "--dispatch-tick-ms",
            "1000",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).unwrap()))
        .spawn()
        .unwrap();

    let https = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .add_root_certificate(reqwest::Certificate::from_pem(material.ca_pem.as_bytes()).unwrap())
        .build()
        .unwrap();
    let https_url = format!("https://localhost:{}/healthz", address.port());
    let mut status = None;
    for _ in 0..200 {
        if let Ok(response) = https.get(&https_url).send().await {
            if response.status().is_success() {
                status = Some(response.status().as_u16());
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let plain = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
        .get(format!("http://127.0.0.1:{}/healthz", address.port()))
        .send()
        .await;

    let _ = child.kill();
    let _ = child.wait();
    let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();

    assert_eq!(
        status,
        Some(200),
        "HTTPS /healthz never answered; stderr: {stderr}"
    );
    assert!(
        plain.is_err(),
        "plain HTTP to the TLS port must get no HTTP answer, got {plain:?}"
    );
}

#[test]
fn tls_public_api_rejects_one_flag_alone() {
    let work = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_defer"))
        .args([
            "serve",
            "--bind",
            "127.0.0.1:0",
            "--data-dir",
            work.path().join("data").to_str().unwrap(),
            "--tls-cert-file",
            "/nonexistent/tls.crt",
        ])
        .output()
        .unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "one TLS flag alone must fail");
    assert!(
        stderr.contains("--tls-cert-file and --tls-key-file must be set together"),
        "stderr: {stderr}"
    );
}
