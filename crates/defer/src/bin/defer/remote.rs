// HANDWRITE-BEGIN gap="missing-generator:logic:defer-cli" tracker="#766" reason="Agent-facing Defer CLI, shared conventions, service startup, and HTTP domain client."
//! Remote HTTP domain verbs: `defer queue`, `defer task`, and
//! `defer dispatch` against a running Defer service.

use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::{Subcommand, ValueEnum};
use defer::{CreateTask, QueueControlState, QueuePolicy, Target};
use serde_json::json;

#[derive(clap::Args)]
pub(crate) struct QueueArgs {
    #[command(subcommand)]
    command: QueueCommand,
}

#[derive(Subcommand)]
enum QueueCommand {
    Get(RemoteQueue),
    Put(QueuePutArgs),
    Control(QueueControlArgs),
}

#[derive(clap::Args)]
struct RemoteQueue {
    #[arg(long)]
    queue: String,
    #[command(flatten)]
    remote: Remote,
}

#[derive(clap::Args)]
struct QueuePutArgs {
    #[arg(long)]
    queue: String,
    #[arg(long, default_value_t = 100)]
    max_in_flight: usize,
    #[arg(long, default_value_t = 100)]
    max_dispatch_per_tick: usize,
    #[arg(long, default_value_t = 100)]
    max_dispatches_per_second: u32,
    #[arg(long, default_value_t = 100)]
    max_burst_size: usize,
    #[arg(long, default_value_t = 30_000)]
    lease_ttl_ms: u64,
    #[arg(long, default_value_t = 1_000)]
    retry_backoff_ms: u64,
    #[command(flatten)]
    remote: Remote,
}

#[derive(clap::Args)]
struct QueueControlArgs {
    #[arg(long)]
    queue: String,
    #[arg(long, value_enum)]
    state: QueueStateArg,
    #[command(flatten)]
    remote: Remote,
}

#[derive(Clone, Copy, ValueEnum)]
enum QueueStateArg {
    Running,
    Paused,
    Disabled,
}

#[derive(clap::Args)]
pub(crate) struct TaskArgs {
    #[command(subcommand)]
    command: TaskCommand,
}

#[derive(Subcommand)]
enum TaskCommand {
    Create(TaskCreateArgs),
    Status(TaskRef),
    Cancel(TaskRef),
}

#[derive(clap::Args)]
struct TaskCreateArgs {
    #[arg(long)]
    queue: String,
    #[arg(long)]
    task_id: String,
    #[arg(long)]
    target_url: String,
    #[arg(long, default_value = "POST")]
    method: String,
    #[arg(long, default_value = "null")]
    payload: String,
    #[arg(long)]
    schedule_at: Option<DateTime<Utc>>,
    #[arg(long, default_value_t = 10)]
    priority: u8,
    #[arg(long, default_value_t = 3)]
    max_attempts: u32,
    #[command(flatten)]
    remote: Remote,
}

#[derive(clap::Args)]
struct TaskRef {
    #[arg(long)]
    queue: String,
    #[arg(long)]
    task_id: String,
    #[command(flatten)]
    remote: Remote,
}

#[derive(clap::Args)]
pub(crate) struct DispatchArgs {
    #[arg(long)]
    queue: String,
    #[command(flatten)]
    remote: Remote,
}

#[derive(clap::Args, Clone)]
struct Remote {
    #[arg(long, env = "DEFER_URL", default_value = "http://127.0.0.1:7141")]
    url: String,
    #[arg(long, env = "DEFER_TOKEN")]
    token: Option<String>,
}

fn client(remote: &Remote) -> Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = &remote.token {
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token}").parse()?,
        );
    }
    Ok(reqwest::Client::builder()
        .http2_adaptive_window(true)
        .default_headers(headers)
        .build()?)
}

async fn print_response(response: reqwest::Response, next: &str) -> Result<()> {
    let status = response.status();
    let body = response.text().await?;
    anyhow::ensure!(status.is_success(), "Defer HTTP {status}: {body}");
    if !body.is_empty() {
        println!("{body}");
    }
    println!("next: {next}");
    Ok(())
}

pub(crate) async fn queue(args: QueueArgs) -> Result<()> {
    match args.command {
        QueueCommand::Get(args) => {
            let response = client(&args.remote)?
                .get(format!("{}/v1/queues/{}", args.remote.url, args.queue))
                .send()
                .await?;
            print_response(response, "done").await
        }
        QueueCommand::Put(args) => {
            let policy = QueuePolicy {
                max_in_flight: args.max_in_flight,
                max_dispatch_per_tick: args.max_dispatch_per_tick,
                max_dispatches_per_second: args.max_dispatches_per_second,
                max_burst_size: args.max_burst_size,
                lease_ttl_ms: args.lease_ttl_ms,
                retry_backoff_ms: args.retry_backoff_ms,
            };
            let response = client(&args.remote)?
                .put(format!("{}/v1/queues/{}", args.remote.url, args.queue))
                .json(&policy)
                .send()
                .await?;
            print_response(response, "done").await
        }
        QueueCommand::Control(args) => {
            let state = match args.state {
                QueueStateArg::Running => QueueControlState::Running,
                QueueStateArg::Paused => QueueControlState::Paused,
                QueueStateArg::Disabled => QueueControlState::Disabled,
            };
            let response = client(&args.remote)?
                .post(format!(
                    "{}/v1/queues/{}/control",
                    args.remote.url, args.queue
                ))
                .json(&json!({"state": state}))
                .send()
                .await?;
            print_response(response, "done").await
        }
    }
}

pub(crate) async fn task(args: TaskArgs) -> Result<()> {
    match args.command {
        TaskCommand::Create(args) => {
            let task = CreateTask {
                task_id: args.task_id,
                target: Target {
                    url: args.target_url,
                    method: args.method,
                    headers: Default::default(),
                },
                payload: serde_json::from_str(&args.payload)
                    .unwrap_or_else(|_| json!(args.payload)),
                schedule_at: args.schedule_at.unwrap_or_else(Utc::now),
                priority: args.priority,
                max_attempts: args.max_attempts,
            };
            let response = client(&args.remote)?
                .post(format!(
                    "{}/v1/queues/{}/tasks",
                    args.remote.url, args.queue
                ))
                .json(&task)
                .send()
                .await?;
            print_response(response, "done").await
        }
        TaskCommand::Status(args) => {
            let response = client(&args.remote)?
                .get(format!(
                    "{}/v1/queues/{}/tasks/{}",
                    args.remote.url, args.queue, args.task_id
                ))
                .send()
                .await?;
            print_response(response, "done").await
        }
        TaskCommand::Cancel(args) => {
            let response = client(&args.remote)?
                .delete(format!(
                    "{}/v1/queues/{}/tasks/{}",
                    args.remote.url, args.queue, args.task_id
                ))
                .send()
                .await?;
            print_response(response, "done").await
        }
    }
}

pub(crate) async fn dispatch(args: DispatchArgs) -> Result<()> {
    let response = client(&args.remote)?
        .post(format!(
            "{}/v1/queues/{}/dispatch",
            args.remote.url, args.queue
        ))
        .send()
        .await?;
    print_response(response, "done").await
}
// HANDWRITE-END
