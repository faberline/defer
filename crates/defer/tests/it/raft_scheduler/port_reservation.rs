// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-raft-scheduler" tracker="#766" reason="Real three-node h2c Raft integration for committed task state and fenced dispatch ownership."

use super::common::Cluster;
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::time::Duration;

// <HANDWRITE gap="missing-generator:unit-test" tracker="#307" reason="A cluster binds its own loopback address so no parallel test can take the port of a killed node.">
fn host(cluster: &Cluster, id: u64) -> IpAddr {
    cluster.urls[&id]
        .strip_prefix("http://")
        .unwrap()
        .parse::<SocketAddr>()
        .unwrap()
        .ip()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clusters_bind_loopback_addresses_of_their_own() {
    let a = Cluster::new(3).await;
    let b = Cluster::new(3).await;
    let shared: IpAddr = "127.0.0.1".parse().unwrap();

    for id in 0..3u64 {
        assert_eq!(
            host(&a, id),
            host(&a, 0),
            "nodes of cluster a share one address"
        );
        assert_eq!(
            host(&b, id),
            host(&b, 0),
            "nodes of cluster b share one address"
        );
    }
    assert_ne!(
        host(&a, 0),
        host(&b, 0),
        "two clusters must not share an address"
    );
    assert_ne!(host(&a, 0), shared, "cluster a must not bind 127.0.0.1");
    assert_ne!(host(&b, 0), shared, "cluster b must not bind 127.0.0.1");
    assert!(host(&a, 0).is_loopback());
    assert!(host(&b, 0).is_loopback());

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_node_refuses_connections_and_restarts_on_its_address() {
    let mut cluster = Cluster::new(3).await;
    cluster.start_all();
    cluster.wait_leader().await;

    let id = 1usize;
    let address: SocketAddr = cluster.urls[&(id as u64)]
        .strip_prefix("http://")
        .unwrap()
        .parse()
        .unwrap();

    cluster.kill(id).await;
    match TcpStream::connect_timeout(&address, Duration::from_secs(1)) {
        Err(e) => assert_eq!(
            e.kind(),
            ErrorKind::ConnectionRefused,
            "a killed node must refuse, not hang"
        ),
        Ok(_) => panic!("a killed node accepted a connection"),
    }

    cluster.start_node(id);
    cluster.wait_leader().await;
    cluster.shutdown().await;
}
// </HANDWRITE>

// HANDWRITE-END
