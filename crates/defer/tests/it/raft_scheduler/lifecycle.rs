// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-raft-scheduler" tracker="#766" reason="Real three-node h2c Raft integration for committed task state and fenced dispatch ownership."

use super::common::{task, Cluster};
use chrono::{TimeZone, Utc};
use defer::{QueuePolicy, TaskStatus};

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2221" reason="Own the primary observable that a follower-forwarded lease records executor identity and epoch, rejects stale settlement, survives leader loss, and completes the committed task lifecycle after reassignment.">
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_scheduler_converges_and_fences_cross_replica_effects() {
    let now = Utc.timestamp_millis_opt(10_000).unwrap();
    let mut cluster = Cluster::new(3).await;
    cluster.start_all();
    let leader = cluster.wait_leader().await;
    let follower = cluster
        .live()
        .map(|(id, _)| id)
        .find(|id| *id != leader)
        .unwrap();

    cluster
        .raft(leader)
        .configure_queue("jobs".into(), QueuePolicy::default())
        .await
        .unwrap();
    cluster
        .raft(leader)
        .create_task("jobs".into(), task("first", now))
        .await
        .unwrap();

    // A follower can forward the lease proposal, but the committed executor
    // is still that follower. Another replica cannot claim its HTTP result.
    let lease = cluster
        .raft(follower)
        .lease_due("jobs".into(), now, 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(lease.executor_node, follower as u64);
    cluster
        .wait_status("first", |status| {
            matches!(status, TaskStatus::Leased { executor_node, epoch, .. } if *executor_node == follower as u64 && *epoch == lease.epoch)
        })
        .await;
    assert!(!cluster
        .raft(leader)
        .ack("jobs".into(), lease.attempt_id.clone(), lease.epoch, now)
        .await
        .unwrap());
    assert!(cluster
        .raft(follower)
        .ack("jobs".into(), lease.attempt_id, lease.epoch, now)
        .await
        .unwrap());
    cluster
        .wait_status("first", |status| matches!(status, TaskStatus::Succeeded))
        .await;

    // If the executor dies with a live lease, the new primary preserves that
    // lease, rejects the stale attempt, and only reassigns after committed
    // expiry. The new assignment advances the fence epoch.
    cluster
        .raft(leader)
        .create_task("jobs".into(), task("second", now))
        .await
        .unwrap();
    let abandoned = cluster
        .raft(leader)
        .lease_due("jobs".into(), now, 1)
        .await
        .unwrap()
        .remove(0);
    cluster.kill(leader).await;
    let new_leader = cluster.wait_leader().await;
    assert!(!cluster
        .raft(new_leader)
        .ack(
            "jobs".into(),
            abandoned.attempt_id.clone(),
            abandoned.epoch,
            now,
        )
        .await
        .unwrap());
    let after_expiry = abandoned.expires_at + chrono::Duration::milliseconds(1);
    assert_eq!(
        cluster
            .raft(new_leader)
            .reclaim_expired("jobs".into(), after_expiry)
            .await
            .unwrap(),
        vec!["second".to_string()]
    );
    let retry_at = after_expiry + chrono::Duration::milliseconds(1_000);
    let reassigned = cluster
        .raft(new_leader)
        .lease_due("jobs".into(), retry_at, 1)
        .await
        .unwrap()
        .remove(0);
    assert!(reassigned.epoch > abandoned.epoch);
    assert_ne!(reassigned.attempt_id, abandoned.attempt_id);
    assert!(!cluster
        .raft(new_leader)
        .ack(
            "jobs".into(),
            abandoned.attempt_id,
            abandoned.epoch,
            retry_at
        )
        .await
        .unwrap());
    assert!(cluster
        .raft(new_leader)
        .ack(
            "jobs".into(),
            reassigned.attempt_id,
            reassigned.epoch,
            retry_at
        )
        .await
        .unwrap());
    cluster
        .wait_status("second", |status| matches!(status, TaskStatus::Succeeded))
        .await;

    // Recover the original primary from its durable raft state and require it
    // to converge before a second leader loss. The surviving quorum must still
    // accept and complete the full replicated task lifecycle.
    cluster.start_node(leader);
    cluster
        .wait_status("first", |status| matches!(status, TaskStatus::Succeeded))
        .await;
    cluster
        .wait_status("second", |status| matches!(status, TaskStatus::Succeeded))
        .await;
    let second_leader = cluster.wait_leader().await;
    cluster.kill(second_leader).await;
    let third_leader = cluster.wait_leader().await;
    assert_ne!(third_leader, second_leader);
    cluster
        .raft(third_leader)
        .create_task("jobs".into(), task("third", retry_at))
        .await
        .unwrap();
    let third_lease = cluster
        .raft(third_leader)
        .lease_due("jobs".into(), retry_at, 1)
        .await
        .unwrap()
        .remove(0);
    assert!(cluster
        .raft(third_leader)
        .ack(
            "jobs".into(),
            third_lease.attempt_id,
            third_lease.epoch,
            retry_at,
        )
        .await
        .unwrap());
    cluster
        .wait_status("third", |status| matches!(status, TaskStatus::Succeeded))
        .await;
    cluster.shutdown().await;
}
// </HANDWRITE>
// HANDWRITE-END
