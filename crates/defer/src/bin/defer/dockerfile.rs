// HANDWRITE-BEGIN gap="missing-generator:logic:defer-cli" tracker="#766" reason="Agent-facing Defer CLI, shared conventions, service startup, and HTTP domain client."
//! `defer dockerfile`: render the source-build and release-download image
//! Dockerfiles.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Subcommand, ValueEnum};

use crate::cli::write_or_print;

#[derive(clap::Args, Debug)]
pub(crate) struct DockerfileArgs {
    #[command(subcommand)]
    command: DockerfileCommand,
}

#[derive(Subcommand, Debug)]
enum DockerfileCommand {
    Render(DockerfileRenderArgs),
}

#[derive(clap::Args, Debug)]
struct DockerfileRenderArgs {
    #[arg(long, value_enum, default_value_t = DockerfileVariant::Source)]
    variant: DockerfileVariant,
    #[arg(long)]
    version: Option<String>,
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DockerfileVariant {
    Source,
    Release,
}

pub(crate) fn run(args: DockerfileArgs) -> Result<()> {
    match args.command {
        DockerfileCommand::Render(args) => {
            let (name, body) = match args.variant {
                DockerfileVariant::Source => (
                    "Dockerfile",
                    cli_std::artifact::strip_source_ownership_markers(include_str!(
                        "../../../../../Dockerfile"
                    )),
                ),
                DockerfileVariant::Release => {
                    let tag = cli_std::artifact::release_tag(
                        "defer",
                        args.version.as_deref(),
                        env!("CARGO_PKG_VERSION"),
                    );
                    let template = cli_std::artifact::strip_source_ownership_markers(include_str!(
                        "../../../../../Dockerfile.release"
                    ));
                    let rendered = template
                        .lines()
                        .map(|line| {
                            if line.starts_with("ARG DEFER_VERSION=") {
                                format!("ARG DEFER_VERSION={tag}")
                            } else {
                                line.to_owned()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    (
                        "Dockerfile.release",
                        cli_std::artifact::ensure_trailing_newline(&rendered),
                    )
                }
            };
            write_or_print(args.out.as_deref(), name, &body)
        }
    }
}
// HANDWRITE-END
