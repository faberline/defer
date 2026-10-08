// HANDWRITE-BEGIN gap="missing-generator:logic:defer-cli" tracker="#766" reason="Agent-facing Defer CLI, shared conventions, service startup, and HTTP domain client."
//! `defer serve`: the h2c + HTTP/1.1 service, the Raft host, and the
//! committed dispatch worker.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::ValueEnum;
use defer::{AuthConfig, DeferRaft, DeferScheduler, HttpDispatcher, TargetSigningKey};
use raft_runtime::Membership;
use server_lifecycle::{HookStage, HookStatus, TaskSupervisor};

#[derive(clap::Args)]
pub(crate) struct ServeArgs {
    #[arg(long, env = "DEFER_BIND", default_value = "0.0.0.0:7141")]
    bind: String,
    #[arg(long, env = "DEFER_DATA_DIR", default_value = ".defer/data")]
    data_dir: PathBuf,
    #[arg(long, env = "DEFER_PEER_SERVICE", default_value = "defer")]
    peer_service: String,
    #[arg(long, env = "DEFER_RAFT_PORT", default_value_t = 7142)]
    raft_port: u16,
    #[arg(long, env = "DEFER_GRACE_SECS", default_value_t = 10)]
    grace_secs: u64,
    /// Log output format. Kubernetes uses `json` for the shared
    /// `axiom.service.log.v1` collector contract; local development defaults
    /// to the human-readable formatter.
    #[arg(long, env = "DEFER_LOG_FORMAT", value_enum, default_value_t = LogFormat::Pretty)]
    log_format: LogFormat,
    #[arg(long, env = "DEFER_DISPATCH_TICK_MS", default_value_t = 100)]
    dispatch_tick_ms: u64,
    #[arg(long, env = "DEFER_DISPATCH_MAX_PER_QUEUE", default_value_t = 100)]
    dispatch_max_per_queue: usize,
    /// Maximum target HTTP requests in flight per process. The committed
    /// queue permits remain global across replicas.
    #[arg(long, env = "DEFER_DISPATCH_CONCURRENCY", default_value_t = 32)]
    dispatch_concurrency: usize,
    #[arg(long, env = "DEFER_TARGET_TIMEOUT_SECS", default_value_t = 30)]
    target_timeout_secs: u64,
    #[arg(long, env = "DEFER_TARGET_SIGNING_KEY_ID")]
    target_signing_key_id: Option<String>,
    #[arg(long, env = "DEFER_TARGET_SIGNING_SECRET_FILE")]
    target_signing_secret_file: Option<PathBuf>,
    #[arg(long, env = "DEFER_AUTH", default_value = "off")]
    auth: String,
    #[arg(long, env = "DEFER_TOKEN_REGISTRY_FILE")]
    token_registry_file: Option<PathBuf>,
    #[arg(long, env = "DEFER_OTLP_ENDPOINT")]
    otlp_endpoint: Option<String>,
    #[arg(long, env = "DEFER_BOOTSTRAP_SEED_URI")]
    bootstrap_seed_uri: Option<String>,
    /// Data-plane request body size limit (bytes). Requests with
    /// `Content-Length` exceeding this are rejected with 413; streamed bodies
    /// are bounded mid-read. Defaults to 8 MiB (`DEFER_BODY_LIMIT_BYTES`).
    #[arg(long, env = "DEFER_BODY_LIMIT_BYTES", default_value_t = 8 * 1024 * 1024)]
    body_limit_bytes: usize,
}

#[derive(Clone, Copy, ValueEnum)]
enum LogFormat {
    Pretty,
    Json,
}

pub(crate) async fn run(args: ServeArgs) -> Result<()> {
    let log_format = match args.log_format {
        LogFormat::Pretty => service_http::LogFormat::Pretty,
        LogFormat::Json => service_http::LogFormat::Json,
    };
    let config = service_http::HttpConfig::new(
        "127.0.0.1",
        0,
        "info",
        log_format,
        args.grace_secs,
        0,
        args.otlp_endpoint.clone(),
    );
    let identity = service_http::ServiceIdentity::new("defer", env!("CARGO_PKG_VERSION"))?;
    service_http::init_tracing_with_identity(&config, &identity)?;

    let auth = AuthConfig::resolve(
        &args.auth,
        args.token_registry_file
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned())
            .as_deref(),
        std::env::var(defer::auth::LEGACY_TOKENS_ENV)
            .ok()
            .as_deref(),
    )?;
    let admission = service_http::AdmissionConfig::from_env("DEFER")?.controller(
        "defer.read",
        "defer.write",
        "defer.admin",
    );
    if admission.is_some() {
        tracing::info!(
            "request admission enabled (DEFER_ADMISSION_*; probes and peer routes stay exempt)"
        );
    }
    let scheduler = Arc::new(Mutex::new(DeferScheduler::new()));
    let mut peer_transport = None;
    let raft = if raft_runtime::replica_mode() {
        let transport = defer::peer_tls::from_env()?
            .map(|config| defer::peer_tls::peer_transport(&config))
            .transpose()?;
        let (port, scheme) = match transport.as_ref() {
            Some(_) => (args.raft_port, "https"),
            None => (
                args.bind
                    .rsplit(':')
                    .next()
                    .context("DEFER_BIND requires a port")?
                    .parse()?,
                "http",
            ),
        };
        let topology = raft_runtime::ClusterTopology::from_env_with_scheme(
            "defer",
            &args.peer_service,
            port,
            "DEFER_PEERS",
            scheme,
        )?;
        if let Some(seed_uri) = args.bootstrap_seed_uri.as_deref() {
            let bytes = service_backup::fetch_backup_object(seed_uri)?;
            defer::raft::prepare_bootstrap_seed(&args.data_dir, topology.node_id, &bytes)?;
        }
        let raft = Arc::new(match transport.clone() {
            Some(transport) => DeferRaft::from_topology_with_peer_transport(
                scheduler,
                &args.data_dir,
                &topology,
                DeferRaft::host_config(defer::raft::SNAPSHOT_EVERY),
                transport,
            )?,
            None => DeferRaft::from_topology(
                scheduler,
                &args.data_dir,
                &topology,
                DeferRaft::host_config(defer::raft::SNAPSHOT_EVERY),
            )?,
        });
        peer_transport = transport;
        raft
    } else {
        if let Some(seed_uri) = args.bootstrap_seed_uri.as_deref() {
            let bytes = service_backup::fetch_backup_object(seed_uri)?;
            defer::raft::prepare_bootstrap_seed(&args.data_dir, 0, &bytes)?;
        }
        Arc::new(DeferRaft::spawn(
            scheduler,
            &args.data_dir.join("raft"),
            0,
            Membership {
                voters: vec![0],
                learners: vec![],
            },
            HashMap::new(),
            DeferRaft::host_config(defer::raft::SNAPSHOT_EVERY),
        )?)
    };

    let signing = match (args.target_signing_key_id, args.target_signing_secret_file) {
        (Some(key_id), Some(path)) => Some(TargetSigningKey::new(key_id, std::fs::read(path)?)?),
        (None, None) => None,
        _ => anyhow::bail!("target signing requires both key id and secret file"),
    };
    let dispatcher = HttpDispatcher::new(Duration::from_secs(args.target_timeout_secs), signing)?;
    let peer_router = peer_transport.as_ref().map(|_| raft.router());
    // One lifecycle owns readiness, listener drain, and the ordered shutdown
    // hooks below; the shutdown deadline is the configured grace window.
    let grace = Duration::from_secs(args.grace_secs.max(1));
    let reserve = Duration::from_secs((args.grace_secs / 10).min(5));
    let supervisor = TaskSupervisor::new(grace, reserve)?;
    let state = defer::server::AppState::new(raft.clone(), dispatcher, auth, args.body_limit_bytes)
        .with_lifecycle(supervisor.lifecycle());
    if let Some(path) = args.token_registry_file.as_deref() {
        std::mem::drop(service_auth::spawn_registry_file_watcher(
            state.verifier(),
            path,
        ));
    }

    let worker_state = state.clone();
    let mut worker_drain = state.drain_signal();
    let dispatch_tick_ms = args.dispatch_tick_ms;
    let dispatch_max_per_queue = args.dispatch_max_per_queue;
    let dispatch_concurrency = args.dispatch_concurrency;
    let dispatch_worker = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(dispatch_tick_ms));
        loop {
            // A tick already in flight finishes (and commits its acks);
            // draining only stops the next one from starting.
            tokio::select! {
                _ = worker_drain.changed() => break,
                _ = interval.tick() => {}
            }
            if let Err(error) = worker_state
                .dispatch_tick(dispatch_max_per_queue, dispatch_concurrency)
                .await
            {
                tracing::warn!(error = %error, "defer dispatch tick failed");
            }
        }
    });
    let dispatch_worker = Arc::new(tokio::sync::Mutex::new(Some(dispatch_worker)));
    supervisor.register_hook(
        HookStage::BackgroundStop,
        "defer-dispatch-worker",
        move |_| {
            let dispatch_worker = dispatch_worker.clone();
            async move {
                match dispatch_worker.lock().await.take() {
                    Some(worker) => worker.await.map_err(|error| error.to_string()),
                    None => Ok(()),
                }
            }
        },
    )?;

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    tracing::info!(
        event = "service_listening",
        addr = %listener.local_addr()?,
        "defer listening (HTTP/1.1 + HTTP/2 cleartext)"
    );
    let app = if peer_transport.is_some() {
        defer::server::router_without_raft_routes_with_admission(state.clone(), admission)
    } else {
        defer::server::router_with_admission(state.clone(), admission)
    };
    // The Raft peer listener stays up through BackgroundStop: the dispatch
    // worker's last acks still need it to commit. It closes in FinalFlush,
    // just before the Raft host itself.
    match (peer_transport, peer_router) {
        (Some(transport), Some(router)) => {
            let peer_bind = peer_bind_address(&args.bind, args.raft_port)?;
            let peer_listener = tokio::net::TcpListener::bind(&peer_bind).await?;
            let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
            let serve = tokio::spawn(async move {
                transport
                    .serve(peer_listener, router, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });
            supervisor.register_oneshot_task(
                HookStage::FinalFlush,
                "defer-raft-peer-listener",
                shutdown,
                serve,
            )?;
        }
        (None, None) => {}
        _ => unreachable!("peer transport and router are configured together"),
    }
    supervisor.register_hook(HookStage::FinalFlush, "defer-raft", move |_| {
        let raft = raft.clone();
        async move { raft.shutdown().await.map_err(|error| format!("{error:#}")) }
    })?;

    let signal_supervisor = supervisor.clone();
    let shutdown = tokio::spawn(async move {
        service_http::wait_shutdown_signal().await;
        signal_supervisor
            .shutdown("signal", "defer shutdown signal received")
            .await
    });
    let http_report = service_http::serve_with_lifecycle(
        listener,
        app,
        service_http::HttpServerOptions::default(),
        supervisor.lifecycle(),
    )
    .await;
    let shutdown_report = shutdown.await.context("join defer shutdown supervisor")?;
    let failures = shutdown_report
        .outcomes
        .iter()
        .filter(|outcome| outcome.status != HookStatus::Completed)
        .map(|outcome| match &outcome.error {
            Some(error) => format!("{}: {:?} ({error})", outcome.name, outcome.status),
            None => format!("{}: {:?}", outcome.name, outcome.status),
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        failures.is_empty(),
        "defer shutdown did not complete cleanly: {}",
        failures.join(", ")
    );
    tracing::info!(
        accepted = http_report.accepted,
        completed = http_report.completed,
        failed = http_report.failed,
        timed_out = http_report.timed_out,
        hooks = shutdown_report.outcomes.len(),
        "defer stopped"
    );
    Ok(())
}

fn peer_bind_address(bind: &str, raft_port: u16) -> Result<String> {
    let (host, _) = bind
        .rsplit_once(':')
        .with_context(|| format!("cannot derive peer bind from {bind}"))?;
    anyhow::ensure!(!host.is_empty(), "DEFER_BIND must include a host");
    Ok(format!("{host}:{raft_port}"))
}
// HANDWRITE-END
