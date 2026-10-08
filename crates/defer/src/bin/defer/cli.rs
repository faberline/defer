// HANDWRITE-BEGIN gap="missing-generator:logic:defer-cli" tracker="#766" reason="Agent-facing Defer CLI, shared conventions, service startup, and HTTP domain client."
//! The `defer` command surface: the top-level verbs and their dispatch.

use std::path::Path;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::backup::BackupArgs;
use crate::dockerfile::DockerfileArgs;
use crate::k8s::K8sArgs;
use crate::offline::{IssueArgs, LlmArgs, UpgradeArgs};
use crate::remote::{DispatchArgs, QueueArgs, TaskArgs};
use crate::serve::ServeArgs;
use crate::spec::SpecArgs;

#[derive(Parser)]
#[command(name = "defer", version, about = "Raft-backed delayed HTTP push queue")]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Run the h2c + HTTP/1.1 service and committed dispatch workers.
    Serve(ServeArgs),
    /// Print or generate clients from the exact served OpenAPI contract.
    Spec(SpecArgs),
    /// Print offline agent-driving documentation.
    Llm(LlmArgs),
    /// Self-update this binary from a Defer GitHub release.
    Upgrade(UpgradeArgs),
    /// Search, inspect, or create Defer issues.
    Issue(IssueArgs),
    /// Configure, inspect, or control a remote queue.
    Queue(QueueArgs),
    /// Create, inspect, or cancel a remote delayed task.
    Task(TaskArgs),
    /// Trigger one remote committed delivery attempt.
    Dispatch(DispatchArgs),
    /// Upload a consistent live state-machine snapshot to file:// or s3://.
    Backup(BackupArgs),
    /// Render or run the layered Kubernetes API/operator/instance surface.
    K8s(K8sArgs),
    /// Render source-build or release-download image Dockerfiles.
    Dockerfile(DockerfileArgs),
}

pub(crate) const TOOL: cli_std::ToolInfo = cli_std::ToolInfo {
    project: "defer",
    repo: "faberline/defer",
    target: env!("DEFER_TARGET"),
    version: env!("CARGO_PKG_VERSION"),
    git_sha: env!("DEFER_GIT_SHA"),
    built_at: env!("DEFER_BUILT_AT"),
};

pub(crate) async fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Serve(args) => crate::serve::run(args).await,
        Command::Spec(args) => crate::spec::run(args),
        Command::Llm(args) => crate::offline::llm(args),
        Command::Upgrade(args) => crate::offline::upgrade(args).await,
        Command::Issue(args) => crate::offline::issue(args).await,
        Command::Queue(args) => crate::remote::queue(args).await,
        Command::Task(args) => crate::remote::task(args).await,
        Command::Dispatch(args) => crate::remote::dispatch(args).await,
        Command::Backup(args) => crate::backup::run(args).await,
        Command::K8s(args) => crate::k8s::run(args).await,
        Command::Dockerfile(args) => crate::dockerfile::run(args),
    }
}

pub(crate) fn write_or_print(out: Option<&Path>, default_file: &str, body: &str) -> Result<()> {
    cli_std::artifact::write_or_print(out, default_file, body)?;
    println!("next: done");
    Ok(())
}
// HANDWRITE-END
