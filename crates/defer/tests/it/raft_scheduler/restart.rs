// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-raft-scheduler" tracker="#766" reason="Real three-node h2c Raft integration for committed task state and fenced dispatch ownership."

use super::common::{task, Cluster};
use chrono::{TimeZone, Utc};
use defer::{NackOutcome, QueuePolicy, TaskStatus};
use std::time::{Duration, Instant};

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2221" reason="Own the same-directory restart and snapshot oracle that preserves terminal scheduler state and converged queue counts across replicas.">
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_letter_terminal_state_converges_and_survives_restart() {
    let now = Utc.timestamp_millis_opt(30_000).unwrap();
    let mut cluster = Cluster::new(3).await;
    cluster.start_all();
    let leader = cluster.wait_leader().await;
    let executor = cluster
        .live()
        .map(|(id, _)| id)
        .find(|id| *id != leader)
        .unwrap();

    cluster
        .raft(leader)
        .configure_queue(
            "jobs".into(),
            QueuePolicy {
                retry_backoff_ms: 0,
                ..QueuePolicy::default()
            },
        )
        .await
        .unwrap();
    let mut doomed = task("doomed", now);
    doomed.max_attempts = 2;
    cluster
        .raft(leader)
        .create_task("jobs".into(), doomed)
        .await
        .unwrap();

    let first = cluster
        .raft(executor)
        .lease_due("jobs".into(), now, 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        cluster
            .raft(executor)
            .nack("jobs".into(), first.attempt_id, first.epoch, now)
            .await
            .unwrap(),
        Some(NackOutcome::Retried { next_at: now })
    );
    let second = cluster
        .raft(executor)
        .lease_due("jobs".into(), now, 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(second.attempt, 2);
    assert_eq!(
        cluster
            .raft(executor)
            .nack("jobs".into(), second.attempt_id, second.epoch, now)
            .await
            .unwrap(),
        Some(NackOutcome::DeadLettered)
    );
    cluster
        .wait_status("doomed", |status| {
            matches!(status, TaskStatus::DeadLettered)
        })
        .await;

    let restart_id = cluster
        .live()
        .map(|(id, _)| id)
        .find(|id| *id != executor)
        .unwrap();
    cluster.kill(restart_id).await;
    cluster.start_node(restart_id);
    cluster
        .wait_status("doomed", |status| {
            matches!(status, TaskStatus::DeadLettered)
        })
        .await;

    for (_, node) in cluster.live() {
        let snapshot = node
            .raft
            .scheduler()
            .lock()
            .unwrap()
            .queue_snapshot("jobs")
            .unwrap();
        assert_eq!(snapshot.task_count, 1);
        assert_eq!(snapshot.terminal_count, 1);
        assert_eq!(snapshot.scheduled_count, 0);
        assert_eq!(snapshot.in_flight_count, 0);
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_purge_converges_on_followers_and_survives_restart() {
    let now = Utc.timestamp_millis_opt(30_000).unwrap();
    let mut cluster = Cluster::new(3).await;
    cluster.start_all();
    let leader = cluster.wait_leader().await;
    let follower = cluster
        .live()
        .map(|(id, _)| id)
        .find(|id| *id != leader)
        .unwrap();

    let custom_policy = QueuePolicy {
        max_in_flight: 42,
        ..QueuePolicy::default()
    };
    cluster
        .raft(leader)
        .configure_queue("jobs".into(), custom_policy.clone())
        .await
        .unwrap();
    cluster
        .raft(leader)
        .create_task("jobs".into(), task("task-one", now))
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let count = cluster
            .raft(follower)
            .scheduler()
            .lock()
            .unwrap()
            .queue_snapshot("jobs")
            .map(|s| s.task_count)
            .unwrap_or(0);
        if count == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "task did not appear on follower before purge"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let purged = cluster
        .raft(leader)
        .purge_queue("jobs".into())
        .await
        .unwrap();
    assert_eq!(purged, 1);

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let snap = cluster
            .raft(follower)
            .scheduler()
            .lock()
            .unwrap()
            .queue_snapshot("jobs");
        if let Ok(snap) = snap {
            if snap.task_count == 0 {
                assert_eq!(snap.policy, custom_policy);
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "queue did not become empty on follower"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // A task created after the purge is later in the log. Wait for it on the
    // restarted node before asserting the purge, so the check runs only after
    // that node has replayed past `PurgeQueue`.
    cluster
        .raft(leader)
        .create_task("jobs".into(), task("task-two", now))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let present = cluster
            .raft(follower)
            .scheduler()
            .lock()
            .unwrap()
            .status("jobs", "task-two")
            .unwrap()
            .is_some();
        if present {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "task-two did not appear on follower after purge"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let restart_id = follower;
    cluster.kill(restart_id).await;
    cluster.start_node(restart_id);
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let present = cluster
            .raft(restart_id)
            .scheduler()
            .lock()
            .unwrap()
            .status("jobs", "task-two")
            .unwrap()
            .is_some();
        if present {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "task-two did not replicate to restarted node"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    {
        let scheduler = cluster.raft(restart_id).scheduler();
        let scheduler = scheduler.lock().unwrap();
        let snap = scheduler.queue_snapshot("jobs").unwrap();
        assert_eq!(snap.task_count, 1, "purge was lost on restart");
        assert_eq!(snap.policy, custom_policy);
        assert!(scheduler.status("jobs", "task-one").unwrap().is_none());
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_prune_converges_on_followers_and_survives_restart() {
    let now = Utc.timestamp_millis_opt(30_000).unwrap();
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
        .configure_queue(
            "jobs".into(),
            QueuePolicy {
                terminal_retention_ms: Some(1_000),
                ..QueuePolicy::default()
            },
        )
        .await
        .unwrap();
    for id in ["old-a", "old-b"] {
        cluster
            .raft(leader)
            .create_task("jobs".into(), task(id, now))
            .await
            .unwrap();
    }
    cluster
        .raft(leader)
        .create_task(
            "jobs".into(),
            task("live", now + chrono::Duration::hours(1)),
        )
        .await
        .unwrap();

    let leases = cluster
        .raft(leader)
        .lease_due("jobs".into(), now, 2)
        .await
        .unwrap();
    assert_eq!(leases.len(), 2);
    for lease in &leases {
        assert!(cluster
            .raft(leader)
            .ack("jobs".into(), lease.attempt_id.clone(), lease.epoch, now)
            .await
            .unwrap());
    }
    cluster
        .wait_status("old-a", |status| matches!(status, TaskStatus::Succeeded))
        .await;
    cluster
        .wait_status("old-b", |status| matches!(status, TaskStatus::Succeeded))
        .await;

    // The first prune call stamps terminal_at = now on both records.
    let stamped = cluster
        .raft(leader)
        .prune_terminal("jobs".into(), now)
        .await
        .unwrap();
    assert_eq!(stamped, 0);

    // Before the retention ends, nothing is removed.
    let early = cluster
        .raft(leader)
        .prune_terminal("jobs".into(), now + chrono::Duration::milliseconds(999))
        .await
        .unwrap();
    assert_eq!(early, 0);
    assert!(cluster
        .raft(follower)
        .scheduler()
        .lock()
        .unwrap()
        .status("jobs", "old-a")
        .unwrap()
        .is_some());

    // At the end of the retention, both terminal records are removed.
    let pruned = cluster
        .raft(leader)
        .prune_terminal("jobs".into(), now + chrono::Duration::milliseconds(1_000))
        .await
        .unwrap();
    assert_eq!(pruned, 2);

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let ids: Vec<usize> = cluster.live().map(|(id, _)| id).collect();
        let converged = ids.iter().all(|id| {
            cluster
                .raft(*id)
                .scheduler()
                .lock()
                .unwrap()
                .queue_snapshot("jobs")
                .map(|s| s.task_count == 1)
                .unwrap_or(false)
        });
        if converged {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "terminal prune did not converge on every live node"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for id in cluster.live().map(|(id, _)| id).collect::<Vec<_>>() {
        let scheduler = cluster.raft(id).scheduler();
        let scheduler = scheduler.lock().unwrap();
        assert!(scheduler.status("jobs", "old-a").unwrap().is_none());
        assert!(scheduler.status("jobs", "old-b").unwrap().is_none());
        assert!(scheduler.status("jobs", "live").unwrap().is_some());
    }

    // A pruned id can be created again.
    cluster
        .raft(leader)
        .create_task("jobs".into(), task("old-a", now))
        .await
        .unwrap();

    // Restart the follower and prove it replayed past PruneTerminal.
    cluster
        .raft(leader)
        .create_task("jobs".into(), task("after", now))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let present = cluster
            .raft(follower)
            .scheduler()
            .lock()
            .unwrap()
            .status("jobs", "after")
            .unwrap()
            .is_some();
        if present {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "after did not appear on follower before restart"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let restart_id = follower;
    cluster.kill(restart_id).await;
    cluster.start_node(restart_id);
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let present = cluster
            .raft(restart_id)
            .scheduler()
            .lock()
            .unwrap()
            .status("jobs", "after")
            .unwrap()
            .is_some();
        if present {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "after did not replicate to restarted node"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    {
        let scheduler = cluster.raft(restart_id).scheduler();
        let scheduler = scheduler.lock().unwrap();
        assert!(
            scheduler.status("jobs", "old-b").unwrap().is_none(),
            "prune lost on restart"
        );
        assert!(scheduler.status("jobs", "live").unwrap().is_some());
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_delete_converges_on_followers_and_survives_restart() {
    let now = Utc.timestamp_millis_opt(30_000).unwrap();
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
        .create_task("jobs".into(), task("task-one", now))
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let count = cluster
            .raft(follower)
            .scheduler()
            .lock()
            .unwrap()
            .queue_snapshot("jobs")
            .map(|s| s.task_count)
            .unwrap_or(0);
        if count == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "task did not appear on follower before delete"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    cluster
        .raft(leader)
        .delete_queue("jobs".into())
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let exists = cluster
            .raft(follower)
            .scheduler()
            .lock()
            .unwrap()
            .queue_snapshot("jobs")
            .is_ok();
        if !exists {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "queue did not disappear on follower"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // A marker queue created after the delete is later in the log. Wait for it
    // on each node before asserting that `jobs` is gone, so the check runs only
    // after that node has replayed past the delete.
    cluster
        .raft(leader)
        .configure_queue("marker".into(), QueuePolicy::default())
        .await
        .unwrap();

    let restart_id = follower;
    cluster.kill(restart_id).await;
    cluster.start_node(restart_id);
    wait_queue_present(&cluster, restart_id, "marker").await;
    assert!(
        cluster
            .raft(restart_id)
            .scheduler()
            .lock()
            .unwrap()
            .queue_snapshot("jobs")
            .is_err(),
        "deleted queue reappeared on restarted node"
    );

    let live: Vec<usize> = cluster.live().map(|(id, _)| id).collect();
    for id in live {
        wait_queue_present(&cluster, id, "marker").await;
        assert!(
            cluster
                .raft(id)
                .scheduler()
                .lock()
                .unwrap()
                .queue_snapshot("jobs")
                .is_err(),
            "deleted queue must be gone on live node {id}"
        );
    }

    cluster.shutdown().await;
}

async fn wait_queue_present(cluster: &Cluster, id: usize, queue: &str) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let present = cluster
            .raft(id)
            .scheduler()
            .lock()
            .unwrap()
            .queue_snapshot(queue)
            .is_ok();
        if present {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "queue {queue} did not replicate to node {id}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
// </HANDWRITE>
// HANDWRITE-END
