// HANDWRITE-BEGIN gap="missing-generator:logic:defer-cli" tracker="#766" reason="Agent-facing Defer CLI, shared conventions, service startup, and HTTP domain client."
//! Offline agent verbs: `defer llm`, `defer upgrade`, and `defer issue`.

use anyhow::Result;
use clap::Subcommand;

use crate::cli::TOOL;

#[derive(clap::Args)]
pub(crate) struct LlmArgs {
    #[arg(long, default_value = "outline")]
    topic: String,
    #[arg(long, default_value = "md")]
    format: String,
}

#[derive(clap::Args)]
pub(crate) struct UpgradeArgs {
    #[arg(long)]
    check: bool,
    #[arg(long = "version")]
    tag: Option<String>,
    #[arg(long)]
    force: bool,
    #[arg(short = 'y', long)]
    yes: bool,
}

#[derive(clap::Args)]
pub(crate) struct IssueArgs {
    #[command(subcommand)]
    command: IssueCommand,
}

#[derive(Subcommand)]
enum IssueCommand {
    Search {
        #[arg(value_name = "QUERY", num_args = 0..)]
        query: Vec<String>,
        #[arg(long, default_value = "open")]
        state: String,
        #[arg(long, default_value_t = 20)]
        limit: u32,
    },
    View {
        number: u64,
    },
    Create {
        #[arg(short = 't', long)]
        title: Option<String>,
        #[arg(value_name = "MSG", num_args = 0..)]
        message: Vec<String>,
        #[arg(long)]
        dry_run: bool,
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

const LLM_TOPICS: &[cli_std::llm::Topic] = &[
    cli_std::llm::Topic {
        id: "workflow",
        summary: "configure a queue, create a delayed task, inspect or cancel it",
        body: "Use `defer queue put --queue jobs`, then `defer task create --queue jobs --task-id ID --target-url URL --payload JSON`. The service leases due tasks through Raft before issuing HTTP. Every terminal CLI response ends with `next: done` or a runnable next command.",
    },
    cli_std::llm::Topic {
        id: "api",
        summary: "OpenAPI, h2c routes, typed clients, probes, and metrics",
        body: "`defer spec --format openapi` is the offline twin of `/openapi.json` and `/docs`. Generate clients with `defer spec gen --lang ts|py|rust --out DIR`. The one service port supports HTTP/1.1 and h2c; `/healthz`, `/readyz`, and `/metrics` are auth exempt.",
    },
    cli_std::llm::Topic {
        id: "delivery",
        summary: "push delivery, stable idempotency, HMAC signing, retry, and DLQ",
        body: "A committed lease contains executor node and fence epoch before any HTTP effect. Targets receive `Idempotency-Key`, attempt/fence headers, and optional `x-defer-signature`. Non-2xx or transport failure commits retry/DLQ; ambiguous external effects are at-least-once and use the task-stable key for dedupe.",
    },
    cli_std::llm::Topic {
        id: "ha",
        summary: "shards, replicas, committed executor ownership, snapshot, and recovery",
        body: "`SHARD_COUNT` owns storage partitioning; `REPLICAS_PER_SHARD` owns HA. Each replica applies identical scheduler commands. `POD_NAME`, `VOTER_COUNT`, and `DEFER_PEER_SERVICE` derive topology. Durable state lives under `DEFER_DATA_DIR`; lease expiry/reclaim is itself a committed transition.",
    },
    cli_std::llm::Topic {
        id: "auth",
        summary: "shared bearer registry, queue roles, and credential rotation",
        body: "Production sets `DEFER_AUTH=required` and `DEFER_TOKEN_REGISTRY_FILE`. Queue resources use read/write/admin roles and `*` wildcard grants. The live watcher keeps the last known good registry during atomic Secret rotation. Clients send `DEFER_TOKEN` as Bearer auth.",
    },
];

pub(crate) fn llm(args: LlmArgs) -> Result<()> {
    println!(
        "{}",
        cli_std::llm::render(
            TOOL.project,
            TOOL.version,
            LLM_TOPICS,
            &args.topic,
            cli_std::llm::Format::parse(&args.format),
        )?
    );
    println!("next: done");
    Ok(())
}

pub(crate) async fn upgrade(args: UpgradeArgs) -> Result<()> {
    cli_std::upgrade::run(
        &TOOL,
        cli_std::upgrade::Options {
            check: args.check,
            tag: args.tag,
            force: args.force,
            yes: args.yes,
        },
    )
    .await
}

pub(crate) async fn issue(args: IssueArgs) -> Result<()> {
    match args.command {
        IssueCommand::Search {
            query,
            state,
            limit,
        } => {
            cli_std::issue::search(
                &TOOL,
                cli_std::issue::SearchOptions {
                    query: (!query.is_empty()).then(|| query.join(" ")),
                    state,
                    limit,
                },
            )
            .await
        }
        IssueCommand::View { number } => cli_std::issue::view(&TOOL, number).await,
        IssueCommand::Create {
            title,
            message,
            dry_run,
            yes,
        } => {
            let message = (!message.is_empty()).then(|| message.join(" "));
            cli_std::issue::create(
                &TOOL,
                cli_std::issue::CreateOptions {
                    title: title.unwrap_or_else(|| "defer: issue report".into()),
                    message,
                    url: None,
                    repo: None,
                    label: vec!["app:defer".into()],
                    dry_run,
                    yes,
                },
            )
            .await
        }
    }
}
// HANDWRITE-END
