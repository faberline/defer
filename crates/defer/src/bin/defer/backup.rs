// HANDWRITE-BEGIN gap="missing-generator:logic:defer-cli" tracker="#766" reason="Agent-facing Defer CLI, shared conventions, service startup, and HTTP domain client."
//! `defer backup`: ship a live node snapshot to a service-backup destination.

use anyhow::Result;

#[derive(clap::Args)]
pub(crate) struct BackupArgs {
    #[arg(long, env = "DEFER_URL", default_value = "http://127.0.0.1:7141")]
    url: String,
    #[arg(long, env = "DEFER_TOKEN")]
    token: Option<String>,
    #[arg(long)]
    dest: String,
    #[arg(long)]
    retention_secs: Option<u64>,
}

pub(crate) async fn run(args: BackupArgs) -> Result<()> {
    let destination = service_backup::BackupDestination::from_uri(&args.dest)?;
    let retention = match args.retention_secs {
        Some(seconds) => service_backup::RetentionPolicy::max_age_seconds(seconds),
        None => service_backup::RetentionPolicy::default(),
    };
    let result = service_backup::run_admin_snapshot_backup(
        &args.url,
        args.token.as_deref(),
        &destination,
        &retention,
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    println!("next: done");
    Ok(())
}
// HANDWRITE-END
