// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-raft-scheduler" tracker="#766" reason="Real three-node h2c Raft integration for committed task state and fenced dispatch ownership."

use super::common::{task, Cluster};
use chrono::{TimeZone, Utc};
use defer::{QueuePolicy, TaskStatus};
use std::time::Instant;

// <HANDWRITE gap="missing-generator:unit-test" tracker="#12" reason="Own repeated leader failover, full cluster restart, and progress convergence across replicas.">
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_failover_and_full_restart_keep_committed_state_and_progress() {
    let now = Utc.timestamp_millis_opt(40_000).unwrap();
    let mut cluster = Cluster::new(3).await;
    cluster.start_all();
    let leader = cluster.wait_leader().await;

    cluster
        .raft(leader)
        .configure_queue("jobs".into(), QueuePolicy::default())
        .await
        .unwrap();
    cluster
        .raft(leader)
        .create_task("jobs".into(), task("t0", now))
        .await
        .unwrap();

    // Stale attempt taken before the leader is killed.
    let stale_lease = cluster
        .raft(leader)
        .lease_due("jobs".into(), now, 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(stale_lease.task_id, "t0");

    // Failover round 1: kill the leader, elect a new leader, and reclaim the abandoned lease.
    cluster.kill(leader).await;
    let second_leader = cluster.wait_leader().await;
    assert_ne!(second_leader, leader);

    let after_expiry = stale_lease.expires_at + chrono::Duration::milliseconds(1);
    let started = Instant::now();
    let reclaimed = cluster
        .raft(second_leader)
        .reclaim_expired("jobs".into(), after_expiry)
        .await;
    assert_eq!(
        cluster
            .expect_ok("reclaim_expired(t0)", started, reclaimed)
            .await,
        vec!["t0".to_string()]
    );
    cluster
        .wait_status("t0", |status| matches!(status, TaskStatus::Scheduled))
        .await;

    // Create t1 through the new leader and wait for Scheduled convergence across live nodes.
    let started = Instant::now();
    let created = cluster
        .raft(second_leader)
        .create_task("jobs".into(), task("t1", now))
        .await;
    cluster.expect_ok("create_task(t1)", started, created).await;
    cluster
        .wait_status("t1", |status| matches!(status, TaskStatus::Scheduled))
        .await;

    // Restart the killed node; all 3 nodes must list the earlier tasks.
    cluster.start_node(leader);
    cluster
        .wait_status("t0", |status| matches!(status, TaskStatus::Scheduled))
        .await;
    cluster
        .wait_status("t1", |status| matches!(status, TaskStatus::Scheduled))
        .await;

    // Failover round 2: kill the current leader, elect a new stable leader that is not the killed node.
    let third_leader_target = cluster.wait_leader().await;
    cluster.kill(third_leader_target).await;
    let third_leader = cluster.wait_leader().await;
    assert_ne!(third_leader, third_leader_target);

    // Create t2 through it and wait until Scheduled on every live node.
    let started = Instant::now();
    let created = cluster
        .raft(third_leader)
        .create_task("jobs".into(), task("t2", now))
        .await;
    cluster.expect_ok("create_task(t2)", started, created).await;
    cluster
        .wait_status("t2", |status| matches!(status, TaskStatus::Scheduled))
        .await;

    // Restart the killed node from round 2 and ensure all 3 nodes list all earlier tasks.
    cluster.start_node(third_leader_target);
    cluster
        .wait_status("t0", |status| matches!(status, TaskStatus::Scheduled))
        .await;
    cluster
        .wait_status("t1", |status| matches!(status, TaskStatus::Scheduled))
        .await;
    cluster
        .wait_status("t2", |status| matches!(status, TaskStatus::Scheduled))
        .await;

    // Full restart: kill all 3 nodes, start all 3 again on the same directories.
    for id in 0..3 {
        cluster.kill(id).await;
    }
    for id in 0..3 {
        cluster.start_node(id);
    }
    let post_restart_leader = cluster.wait_leader().await;

    // Assert on every node queue_snapshot("jobs") has task_count == 3 and scheduled_count == 3,
    // and that t0, t1, and t2 are still Scheduled.
    for (_, node) in cluster.live() {
        let snapshot = node
            .raft
            .scheduler()
            .lock()
            .unwrap()
            .queue_snapshot("jobs")
            .unwrap();
        assert_eq!(snapshot.task_count, 3);
        assert_eq!(snapshot.scheduled_count, 3);
        for task_id in ["t0", "t1", "t2"] {
            let status = node
                .raft
                .scheduler()
                .lock()
                .unwrap()
                .status("jobs", task_id)
                .unwrap();
            assert_eq!(status, Some(TaskStatus::Scheduled));
        }
    }

    // Stale fencing: the attempt taken before the leader kills is rejected with its old epoch after full restart.
    assert!(!cluster
        .raft(post_restart_leader)
        .ack(
            "jobs".into(),
            stale_lease.attempt_id.clone(),
            stale_lease.epoch,
            now,
        )
        .await
        .unwrap());
    assert_eq!(
        cluster
            .raft(post_restart_leader)
            .scheduler()
            .lock()
            .unwrap()
            .status("jobs", "t0")
            .unwrap(),
        Some(TaskStatus::Scheduled)
    );

    // Continued progress: a follower calls lease_due for t0 and acks it; all nodes converge.
    let follower = cluster
        .live()
        .map(|(id, _)| id)
        .find(|id| *id != post_restart_leader)
        .unwrap();
    let progress_time = after_expiry + chrono::Duration::milliseconds(1_000);
    let leased_t0 = cluster
        .raft(follower)
        .lease_due("jobs".into(), progress_time, 1)
        .await
        .unwrap();
    assert_eq!(leased_t0.len(), 1);
    let t0_attempt = &leased_t0[0];
    assert_eq!(t0_attempt.task_id, "t0");
    assert!(cluster
        .raft(follower)
        .ack(
            "jobs".into(),
            t0_attempt.attempt_id.clone(),
            t0_attempt.epoch,
            progress_time,
        )
        .await
        .unwrap());
    cluster
        .wait_status("t0", |status| matches!(status, TaskStatus::Succeeded))
        .await;

    // Fourth task t3 is created through the leader and converges.
    cluster
        .raft(post_restart_leader)
        .create_task("jobs".into(), task("t3", progress_time))
        .await
        .unwrap();
    cluster
        .wait_status("t3", |status| matches!(status, TaskStatus::Scheduled))
        .await;

    cluster.shutdown().await;
}
// </HANDWRITE>
// HANDWRITE-END
