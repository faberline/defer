// HANDWRITE-BEGIN gap="missing-generator:logic:defer-cli" tracker="#766" reason="Agent-facing Defer CLI, shared conventions, service startup, and HTTP domain client."
//! `defer`: the HTTP service, the remote domain client, and the offline
//! spec/deploy-artifact renders, all in one binary.

mod backup;
mod cli;
mod dockerfile;
mod k8s;
mod offline;
mod remote;
mod serve;
mod spec;

use anyhow::Result;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    peer_tls::install_default_crypto_provider();
    cli::dispatch(cli::Cli::parse()).await
}
// HANDWRITE-END
