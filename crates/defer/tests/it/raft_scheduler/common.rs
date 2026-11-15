// HANDWRITE-BEGIN gap="missing-generator:unit-test:defer-raft-scheduler" tracker="#766" reason="Real three-node h2c Raft integration for committed task state and fenced dispatch ownership."

use chrono::Utc;
use defer::{CreateTask, DeferRaft, DeferScheduler, Target, TaskStatus};
use raft_runtime::{Membership, NodeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(super) struct Node {
    pub(super) raft: Arc<DeferRaft>,
    pub(super) serve: tokio::task::JoinHandle<()>,
    pub(super) shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

pub(super) struct Cluster {
    pub(super) urls: HashMap<u64, String>,
    pub(super) listeners: Vec<Option<tokio::net::TcpListener>>,
    pub(super) nodes: Vec<Option<Node>>,
    pub(super) dirs: Vec<tempfile::TempDir>,
    pub(super) membership: Membership,
}

impl Cluster {
    pub(super) async fn new(size: usize) -> Self {
        let mut urls = HashMap::new();
        let mut listeners = Vec::new();
        for id in 0..size {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            urls.insert(
                id as u64,
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(Some(listener));
        }
        Self {
            urls,
            listeners,
            nodes: (0..size).map(|_| None).collect(),
            dirs: (0..size).map(|_| tempfile::tempdir().unwrap()).collect(),
            membership: Membership::new((0..size as u64).map(NodeId::new).collect(), vec![]),
        }
    }

    pub(super) fn start_node(&mut self, id: usize) {
        let peers = self
            .urls
            .iter()
            .filter(|(peer, _)| **peer != id as u64)
            .map(|(peer, url)| (NodeId::new(*peer), url.clone()))
            .collect();
        let raft = Arc::new(
            DeferRaft::spawn(
                Arc::new(Mutex::new(DeferScheduler::new())),
                &self.dirs[id].path().join("raft"),
                NodeId::new(id as u64),
                self.membership.clone(),
                peers,
                DeferRaft::host_config(8),
            )
            .unwrap(),
        );
        let app = raft.router();
        let listener = match self.listeners[id].take() {
            Some(listener) => listener,
            None => {
                let address = self.urls[&(id as u64)]
                    .strip_prefix("http://")
                    .unwrap()
                    .parse::<std::net::SocketAddr>()
                    .unwrap();
                let listener = std::net::TcpListener::bind(address).unwrap();
                listener.set_nonblocking(true).unwrap();
                tokio::net::TcpListener::from_std(listener).unwrap()
            }
        };
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        self.nodes[id] = Some(Node {
            raft,
            serve,
            shutdown: Some(shutdown),
        });
    }

    pub(super) fn start_all(&mut self) {
        for id in 0..self.nodes.len() {
            self.start_node(id);
        }
    }

    pub(super) fn raft(&self, id: usize) -> &Arc<DeferRaft> {
        &self.nodes[id].as_ref().unwrap().raft
    }

    pub(super) fn live(&self) -> impl Iterator<Item = (usize, &Node)> {
        self.nodes
            .iter()
            .enumerate()
            .filter_map(|(id, node)| node.as_ref().map(|node| (id, node)))
    }

    pub(super) async fn diagnostics(&self) -> String {
        let mut pieces = Vec::new();
        for (id, node) in self.live() {
            pieces.push(format!(
                "node={id} is_leader={} leader={:?} applied={}",
                node.raft.is_leader().await,
                node.raft.leader().await,
                node.raft.applied_index()
            ));
        }
        let joined = pieces.join("; ");
        format!("nodes=[{joined}] term=unavailable commit=unavailable")
    }

    pub(super) async fn expect_ok<T>(
        &self,
        what: &str,
        started: Instant,
        result: Result<T, defer::raft::ProposeError>,
    ) -> T {
        match result {
            Ok(value) => value,
            Err(error) => {
                let line = format!(
                    "aw-diag raft_scheduler: op={what} elapsed_ms={} error={error} {}",
                    started.elapsed().as_millis(),
                    self.diagnostics().await
                );
                eprintln!("{line}");
                panic!("{line}");
            }
        }
    }

    pub(super) async fn wait_leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut stable = None;
        let mut samples = 0;
        loop {
            let leader = {
                let mut found = None;
                for (id, node) in self.live() {
                    if node.raft.is_leader().await {
                        found = Some(id);
                        break;
                    }
                }
                found
            };
            if leader == stable && leader.is_some() {
                samples += 1;
            } else {
                stable = leader;
                samples = usize::from(stable.is_some());
            }
            if samples >= 3 {
                return stable.unwrap();
            }
            assert!(Instant::now() < deadline, "no stable leader elected");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub(super) async fn wait_status(&self, task_id: &str, expected: impl Fn(&TaskStatus) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let converged = self.live().all(|(_, node)| {
                node.raft
                    .scheduler()
                    .lock()
                    .unwrap()
                    .status("jobs", task_id)
                    .unwrap()
                    .as_ref()
                    .is_some_and(&expected)
            });
            if converged {
                return;
            }
            assert!(Instant::now() < deadline, "task status did not converge");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub(super) async fn kill(&mut self, id: usize) {
        if let Some(mut node) = self.nodes[id].take() {
            node.shutdown.take();
            node.serve.abort();
            let _ = node.serve.await;
            node.raft.shutdown().await.unwrap();
        }
    }

    pub(super) async fn shutdown(mut self) {
        for node in self.nodes.iter().flatten() {
            node.raft.shutdown().await.unwrap();
        }
        for node in &mut self.nodes {
            if let Some(mut node) = node.take() {
                if let Some(shutdown) = node.shutdown.take() {
                    let _ = shutdown.send(());
                }
                let _ = node.serve.await;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(super) fn task(id: &str, at: chrono::DateTime<Utc>) -> CreateTask {
    CreateTask {
        task_id: id.into(),
        target: Target {
            url: "http://target.test/jobs".into(),
            method: "POST".into(),
            headers: Default::default(),
        },
        payload: serde_json::json!({"task": id}),
        schedule_at: at,
        priority: 10,
        max_attempts: 3,
        dispatch_deadline_ms: None,
    }
}
// HANDWRITE-END
