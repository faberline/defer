// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-raft-scheduler" tracker="#766" reason="Real three-node h2c Raft integration for committed task state and fenced dispatch ownership."

use super::common::{task, Cluster};
use chrono::{TimeZone, Utc};
use defer::raft::ImportQueueError;
use defer::scheduler::export::QueueImportError;
use defer::types::SchedulerError;
use defer::{QueuePolicy, TaskStatus};
use std::time::{Duration, Instant};

/// Wait until every live replica of `target` has the task `task_id` in the
/// queue `jobs`. A lagging follower may not have applied the import yet, so a
/// missing queue is "not yet", not an error.
async fn wait_scheduled_everywhere(cluster: &Cluster, task_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let converged = cluster.live().all(|(_, node)| {
            matches!(
                node.raft
                    .scheduler()
                    .lock()
                    .unwrap()
                    .status("jobs", task_id),
                Ok(Some(TaskStatus::Scheduled))
            )
        });
        if converged {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "task did not reach every replica"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// <HANDWRITE gap="missing-generator:unit-test" tracker="#282" reason="Whole-queue move between two raft groups through the committed log.">
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_moves_between_two_raft_groups() {
    let now = Utc.timestamp_millis_opt(10_000).unwrap();
    let mut source = Cluster::new(1).await;
    source.start_all();
    let sl = source.wait_leader().await;
    let mut target = Cluster::new(3).await;
    target.start_all();
    let tl = target.wait_leader().await;

    source
        .raft(sl)
        .configure_queue("jobs".into(), QueuePolicy::default())
        .await
        .unwrap();
    source
        .raft(sl)
        .create_task("jobs".into(), task("one", now))
        .await
        .unwrap();

    let export = source.raft(sl).export_queue("jobs").unwrap();
    target.raft(tl).import_queue(export.clone()).await.unwrap();
    wait_scheduled_everywhere(&target, "one").await;

    assert!(matches!(
        target.raft(tl).import_queue(export).await,
        Err(ImportQueueError::Refused(QueueImportError::QueueExists(_)))
    ));

    source
        .raft(sl)
        .remove_exported_queue("jobs".into())
        .await
        .unwrap();
    assert!(matches!(
        source.raft(sl).export_queue("jobs"),
        Err(SchedulerError::QueueMissing(_))
    ));
    wait_scheduled_everywhere(&target, "one").await;

    source.shutdown().await;
    target.shutdown().await;
}
// </HANDWRITE>

// HANDWRITE-END
