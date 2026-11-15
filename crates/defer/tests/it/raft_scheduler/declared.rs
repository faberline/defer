// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-raft-scheduler" tracker="#766" reason="Real three-node h2c Raft integration for committed task state and fenced dispatch ownership."

use super::common::Cluster;
use defer::{
    ensure_declared_queues, DeclaredQueue, DeclaredQueueDecision, QueueControlState, QueuePolicy,
};
use std::time::{Duration, Instant};

// <HANDWRITE gap="missing-generator:unit-test" tracker="#17" reason="Own real three-node proofs that declared queues go through the Raft log, reach followers, survive restarts with no client traffic, and that a changed declaration converges on every node.">
async fn wait_queue(
    cluster: &Cluster,
    queue: &str,
    policy: &QueuePolicy,
    control: QueueControlState,
) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let converged = cluster.live().all(|(_, node)| {
            node.raft
                .scheduler()
                .lock()
                .unwrap()
                .queue_snapshot(queue)
                .is_ok_and(|s| s.policy == *policy && s.control_state == control)
        });
        if converged {
            return;
        }
        assert!(Instant::now() < deadline, "declared queue did not converge");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_queues_go_through_the_log_reach_followers_and_survive_restarts() {
    let declared = vec![
        DeclaredQueue {
            name: "jobs".into(),
            policy: QueuePolicy {
                retry_backoff_ms: 7,
                ..QueuePolicy::default()
            },
            control: QueueControlState::Paused,
        },
        DeclaredQueue {
            name: "mail".into(),
            policy: QueuePolicy::default(),
            control: QueueControlState::Running,
        },
    ];
    let mut cluster = Cluster::new(3).await;
    cluster.start_all();
    let leader = cluster.wait_leader().await;
    let follower = cluster
        .live()
        .map(|(id, _)| id)
        .find(|id| *id != leader)
        .unwrap();

    let before = cluster.raft(follower).applied_index();
    let outcomes = ensure_declared_queues(cluster.raft(leader).as_ref(), &declared)
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes
        .iter()
        .all(|outcome| outcome.decision == DeclaredQueueDecision::Created));

    // The queues exist on the follower, not only on the leader.
    wait_queue(
        &cluster,
        "jobs",
        &declared[0].policy,
        QueueControlState::Paused,
    )
    .await;
    wait_queue(
        &cluster,
        "mail",
        &declared[1].policy,
        QueueControlState::Running,
    )
    .await;

    // The follower's Raft log advanced: the state came through the log.
    let deadline = Instant::now() + Duration::from_secs(8);
    while cluster.raft(follower).applied_index() <= before {
        assert!(
            Instant::now() < deadline,
            "follower applied index did not advance"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // A restarted follower replays its log with no client traffic.
    cluster.kill(follower).await;
    cluster.start_node(follower);
    wait_queue(
        &cluster,
        "jobs",
        &declared[0].policy,
        QueueControlState::Paused,
    )
    .await;
    wait_queue(
        &cluster,
        "mail",
        &declared[1].policy,
        QueueControlState::Running,
    )
    .await;

    // A full restart of all three nodes keeps the queues from durable state only.
    for id in 0..3 {
        cluster.kill(id).await;
    }
    cluster.start_all();
    cluster.wait_leader().await;
    wait_queue(
        &cluster,
        "jobs",
        &declared[0].policy,
        QueueControlState::Paused,
    )
    .await;
    wait_queue(
        &cluster,
        "mail",
        &declared[1].policy,
        QueueControlState::Running,
    )
    .await;

    for (_, node) in cluster.live() {
        let task_count = node
            .raft
            .scheduler()
            .lock()
            .unwrap()
            .queue_snapshot("jobs")
            .unwrap()
            .task_count;
        assert_eq!(task_count, 0);
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_declaration_converges_on_every_node_after_a_restart() {
    let first = vec![DeclaredQueue {
        name: "jobs".into(),
        policy: QueuePolicy {
            retry_backoff_ms: 7,
            ..QueuePolicy::default()
        },
        control: QueueControlState::Paused,
    }];
    let mut cluster = Cluster::new(3).await;
    cluster.start_all();
    let leader = cluster.wait_leader().await;
    ensure_declared_queues(cluster.raft(leader).as_ref(), &first)
        .await
        .unwrap();
    wait_queue(
        &cluster,
        "jobs",
        &first[0].policy,
        QueueControlState::Paused,
    )
    .await;

    // Restart the leader, then change the declaration on the new leader.
    cluster.kill(leader).await;
    cluster.start_node(leader);
    let new_leader = cluster.wait_leader().await;

    let second = vec![DeclaredQueue {
        name: "jobs".into(),
        policy: QueuePolicy {
            retry_backoff_ms: 9,
            ..QueuePolicy::default()
        },
        control: QueueControlState::Running,
    }];
    let outcomes = ensure_declared_queues(cluster.raft(new_leader).as_ref(), &second)
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].decision, DeclaredQueueDecision::Updated);

    let changed_policy = QueuePolicy {
        retry_backoff_ms: 9,
        ..QueuePolicy::default()
    };
    wait_queue(
        &cluster,
        "jobs",
        &changed_policy,
        QueueControlState::Running,
    )
    .await;

    // The same declaration again changes nothing.
    let repeat = ensure_declared_queues(cluster.raft(new_leader).as_ref(), &second)
        .await
        .unwrap();
    assert_eq!(repeat.len(), 1);
    assert_eq!(repeat[0].decision, DeclaredQueueDecision::Unchanged);

    cluster.shutdown().await;
}
// </HANDWRITE>
// HANDWRITE-END
