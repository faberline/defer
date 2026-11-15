// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-raft-scheduler" tracker="#766" reason="Real three-node h2c Raft integration for committed task state and fenced dispatch ownership."

use super::common::{task, Cluster};
use chrono::{TimeZone, Utc};
use defer::QueuePolicy;

// <HANDWRITE gap="missing-generator:unit-test" tracker="#2216" reason="Own real three-node proofs that rate, burst, and in-flight limits are one committed aggregate and that DeadLettered converges and survives same-directory restart.">
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_queue_limits_survive_cross_replica_proposals_and_failover() {
    let now = Utc.timestamp_millis_opt(20_000).unwrap();
    let mut cluster = Cluster::new(3).await;
    cluster.start_all();
    let leader = cluster.wait_leader().await;
    let followers: Vec<_> = cluster
        .live()
        .map(|(id, _)| id)
        .filter(|id| *id != leader)
        .collect();
    let follower_a = followers[0];
    let follower_b = followers[1];

    cluster
        .raft(leader)
        .configure_queue(
            "rate-jobs".into(),
            QueuePolicy {
                max_in_flight: 10,
                max_dispatch_per_tick: 10,
                max_dispatches_per_second: 1,
                max_burst_size: 1,
                ..QueuePolicy::default()
            },
        )
        .await
        .unwrap();
    cluster
        .raft(leader)
        .configure_queue(
            "inflight-jobs".into(),
            QueuePolicy {
                max_in_flight: 1,
                max_dispatch_per_tick: 10,
                max_dispatches_per_second: 100,
                max_burst_size: 10,
                ..QueuePolicy::default()
            },
        )
        .await
        .unwrap();
    for (queue, ids) in [
        ("rate-jobs", ["rate-a", "rate-b"]),
        ("inflight-jobs", ["inflight-a", "inflight-b"]),
    ] {
        for id in ids {
            cluster
                .raft(leader)
                .create_task(queue.into(), task(id, now))
                .await
                .unwrap();
        }
    }

    let first_rate = cluster
        .raft(follower_a)
        .lease_due("rate-jobs".into(), now, 10)
        .await
        .unwrap();
    assert_eq!(first_rate.len(), 1);
    assert!(cluster
        .raft(follower_b)
        .lease_due("rate-jobs".into(), now, 10)
        .await
        .unwrap()
        .is_empty());

    let first_inflight = cluster
        .raft(follower_b)
        .lease_due("inflight-jobs".into(), now, 10)
        .await
        .unwrap()
        .remove(0);
    assert!(cluster
        .raft(follower_a)
        .lease_due("inflight-jobs".into(), now, 10)
        .await
        .unwrap()
        .is_empty());

    cluster.kill(leader).await;
    let new_leader = cluster.wait_leader().await;
    assert!(cluster
        .raft(new_leader)
        .lease_due(
            "rate-jobs".into(),
            now + chrono::Duration::milliseconds(999),
            10,
        )
        .await
        .unwrap()
        .is_empty());
    assert!(cluster
        .raft(new_leader)
        .lease_due(
            "inflight-jobs".into(),
            now + chrono::Duration::milliseconds(1_000),
            10,
        )
        .await
        .unwrap()
        .is_empty());

    assert!(cluster
        .raft(follower_b)
        .ack(
            "inflight-jobs".into(),
            first_inflight.attempt_id,
            first_inflight.epoch,
            now + chrono::Duration::milliseconds(1_000),
        )
        .await
        .unwrap());
    assert_eq!(
        cluster
            .raft(new_leader)
            .lease_due(
                "inflight-jobs".into(),
                now + chrono::Duration::milliseconds(1_000),
                10,
            )
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        cluster
            .raft(new_leader)
            .lease_due(
                "rate-jobs".into(),
                now + chrono::Duration::milliseconds(1_000),
                10,
            )
            .await
            .unwrap()
            .len(),
        1
    );

    cluster.shutdown().await;
}
// </HANDWRITE>
// HANDWRITE-END
