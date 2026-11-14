// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-raft-scheduler" tracker="#766" reason="Real three-node h2c Raft integration for committed task state and fenced dispatch ownership."

use super::common::Cluster;
use std::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "aw-diag raft_scheduler: op=probe")]
async fn expect_ok_prints_node_state_when_the_operation_fails() {
    let mut cluster = Cluster::new(1).await;
    cluster.start_all();
    cluster.wait_leader().await;
    cluster
        .expect_ok(
            "probe",
            Instant::now(),
            Err::<(), defer::raft::ProposeError>(anyhow::anyhow!("boom").into()),
        )
        .await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expect_ok_returns_the_value_and_prints_nothing_on_success() {
    let mut cluster = Cluster::new(1).await;
    cluster.start_all();
    cluster.wait_leader().await;
    assert_eq!(
        cluster
            .expect_ok(
                "probe",
                Instant::now(),
                Ok::<u32, defer::raft::ProposeError>(7)
            )
            .await,
        7
    );
    assert!(cluster
        .diagnostics()
        .await
        .starts_with("nodes=[node=0 is_leader=true"));
    assert!(cluster
        .diagnostics()
        .await
        .ends_with("term=unavailable commit=unavailable"));
    cluster.shutdown().await;
}
// HANDWRITE-END
