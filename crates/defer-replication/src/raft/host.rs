// HANDWRITE-BEGIN gap="missing-generator:logic:defer-raft-scheduler" tracker="#766" reason="Raft-backed delayed-task state machine with durable snapshots and fenced dispatch ownership."
//! [`DeferRaft`]: the Raft host plus the async domain API that proposes
//! commands and returns their committed outcomes.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::Router;
use chrono::{DateTime, Utc};
use defer_shared_kernel::{
    AttemptSettlement, CreateTask, DeferScheduler, DispatchLease, NackOutcome, QueueControlState,
    QueuePolicy, QueueSnapshot, SettlementOutcome,
};
use raft_runtime::{
    ClusterTopology, HostConfig, Index, Membership, NodeId, PeerTransport, RaftHost,
    RaftStateMachine, SnapshotPolicy,
};

use super::command::{DeferCommand, DeferEnvelope, DeferOutcome, ProposalId};
use super::error::{ProposeError, ProposeResult};
use super::state_machine::DeferStateMachine;

pub struct DeferRaft {
    host: RaftHost,
    sm: Arc<DeferStateMachine>,
    node_id: NodeId,
    session: u64,
    proposal_sequence: AtomicU64,
}

impl DeferRaft {
    pub fn spawn(
        scheduler: Arc<Mutex<DeferScheduler>>,
        raft_dir: &Path,
        node_id: NodeId,
        membership: Membership,
        peers: HashMap<NodeId, String>,
        config: HostConfig,
    ) -> Result<Self> {
        Self::spawn_inner(
            scheduler, raft_dir, node_id, membership, peers, config, None,
        )
    }

    pub fn spawn_with_peer_transport(
        scheduler: Arc<Mutex<DeferScheduler>>,
        raft_dir: &Path,
        node_id: NodeId,
        membership: Membership,
        peers: HashMap<NodeId, String>,
        config: HostConfig,
        transport: PeerTransport,
    ) -> Result<Self> {
        Self::spawn_inner(
            scheduler,
            raft_dir,
            node_id,
            membership,
            peers,
            config,
            Some(transport),
        )
    }

    fn spawn_inner(
        scheduler: Arc<Mutex<DeferScheduler>>,
        raft_dir: &Path,
        node_id: NodeId,
        membership: Membership,
        peers: HashMap<NodeId, String>,
        config: HostConfig,
        transport: Option<PeerTransport>,
    ) -> Result<Self> {
        let store = defer_storage::open_raft_store(raft_dir, node_id)?;
        let sm = DeferStateMachine::new(scheduler, true);
        let host = match transport {
            Some(transport) => RaftHost::spawn_with_peer_transport(
                node_id,
                membership,
                peers,
                store,
                Arc::clone(&sm) as Arc<dyn RaftStateMachine>,
                config,
                transport,
            ),
            None => RaftHost::spawn(
                node_id,
                membership,
                peers,
                store,
                Arc::clone(&sm) as Arc<dyn RaftStateMachine>,
                config,
            ),
        };
        static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let session = wall
            ^ ((std::process::id() as u64) << 32)
            ^ NEXT_SESSION.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            host,
            sm,
            node_id,
            session,
            proposal_sequence: AtomicU64::new(1),
        })
    }

    pub fn from_topology(
        scheduler: Arc<Mutex<DeferScheduler>>,
        data_dir: &Path,
        topology: &ClusterTopology,
        config: HostConfig,
    ) -> Result<Self> {
        Self::spawn(
            scheduler,
            &defer_storage::raft_dir(data_dir),
            topology.node_id,
            topology.membership.clone(),
            topology.peers.clone(),
            config,
        )
    }

    pub fn from_topology_with_peer_transport(
        scheduler: Arc<Mutex<DeferScheduler>>,
        data_dir: &Path,
        topology: &ClusterTopology,
        config: HostConfig,
        transport: PeerTransport,
    ) -> Result<Self> {
        Self::spawn_with_peer_transport(
            scheduler,
            &defer_storage::raft_dir(data_dir),
            topology.node_id,
            topology.membership.clone(),
            topology.peers.clone(),
            config,
            transport,
        )
    }

    pub fn host_config(snapshot_every: u64) -> HostConfig {
        HostConfig {
            snapshot: SnapshotPolicy::EveryEntries(snapshot_every),
            ..HostConfig::default()
        }
    }

    /// Drain the shared Raft host's in-flight peer RPCs before its h2 client
    /// and peer listener are torn down.
    pub async fn shutdown(&self) -> Result<()> {
        self.host.shutdown().await
    }

    pub fn router(&self) -> Router {
        self.host.router()
    }

    async fn propose(&self, command: DeferCommand) -> Result<DeferOutcome> {
        let envelope = DeferEnvelope {
            proposal_id: ProposalId {
                node: self.node_id,
                session: self.session,
                sequence: self.proposal_sequence.fetch_add(1, Ordering::Relaxed),
            },
            command,
        };
        let index = self.host.propose(serde_json::to_vec(&envelope)?).await?;
        self.sm
            .claim_outcome(index)
            .with_context(|| format!("defer outcome for raft index {index} aged out"))
    }

    pub async fn configure_queue(
        &self,
        queue: String,
        policy: QueuePolicy,
    ) -> ProposeResult<QueueSnapshot> {
        match self
            .propose(DeferCommand::ConfigureQueue { queue, policy })
            .await?
        {
            DeferOutcome::Queue(result) => Ok(result?),
            other => Err(mismatch("configure queue", other)),
        }
    }

    pub async fn update_queue_policy(
        &self,
        queue: String,
        policy: QueuePolicy,
    ) -> ProposeResult<QueueSnapshot> {
        match self
            .propose(DeferCommand::UpdateQueuePolicy { queue, policy })
            .await?
        {
            DeferOutcome::Queue(result) => Ok(result?),
            other => Err(mismatch("update queue", other)),
        }
    }

    pub async fn set_queue_control(
        &self,
        queue: String,
        control: QueueControlState,
    ) -> ProposeResult<QueueSnapshot> {
        match self
            .propose(DeferCommand::SetQueueControl { queue, control })
            .await?
        {
            DeferOutcome::Queue(result) => Ok(result?),
            other => Err(mismatch("queue control", other)),
        }
    }

    pub async fn create_task(&self, queue: String, task: CreateTask) -> ProposeResult<()> {
        match self
            .propose(DeferCommand::CreateTask { queue, task })
            .await?
        {
            DeferOutcome::Created(result) => Ok(result?),
            other => Err(mismatch("create task", other)),
        }
    }

    pub async fn create_tasks(
        &self,
        queue: String,
        tasks: Vec<CreateTask>,
    ) -> ProposeResult<usize> {
        match self
            .propose(DeferCommand::CreateTasks { queue, tasks })
            .await?
        {
            DeferOutcome::CreatedBatch(result) => Ok(result?),
            other => Err(mismatch("create tasks", other)),
        }
    }

    pub async fn lease_due(
        &self,
        queue: String,
        now: DateTime<Utc>,
        requested: usize,
    ) -> ProposeResult<Vec<DispatchLease>> {
        match self
            .propose(DeferCommand::LeaseDue {
                queue,
                executor_node: self.node_id,
                now,
                requested,
            })
            .await?
        {
            DeferOutcome::Leased(result) => Ok(result?),
            other => Err(mismatch("lease", other)),
        }
    }

    pub async fn ack(
        &self,
        queue: String,
        attempt_id: String,
        epoch: u64,
        now: DateTime<Utc>,
    ) -> ProposeResult<bool> {
        match self
            .propose(DeferCommand::Ack {
                queue,
                attempt_id,
                executor_node: self.node_id,
                epoch,
                now,
            })
            .await?
        {
            DeferOutcome::Acked(result) => Ok(result?),
            other => Err(mismatch("ack", other)),
        }
    }

    pub async fn nack(
        &self,
        queue: String,
        attempt_id: String,
        epoch: u64,
        now: DateTime<Utc>,
    ) -> ProposeResult<Option<NackOutcome>> {
        match self
            .propose(DeferCommand::Nack {
                queue,
                attempt_id,
                executor_node: self.node_id,
                epoch,
                now,
            })
            .await?
        {
            DeferOutcome::Nacked(result) => Ok(result?),
            other => Err(mismatch("nack", other)),
        }
    }

    pub async fn settle_batch(
        &self,
        queue: String,
        attempts: Vec<AttemptSettlement>,
    ) -> ProposeResult<Vec<SettlementOutcome>> {
        match self
            .propose(DeferCommand::SettleBatch {
                queue,
                executor_node: self.node_id,
                attempts,
            })
            .await?
        {
            DeferOutcome::Settled(result) => Ok(result?),
            other => Err(mismatch("settle batch", other)),
        }
    }

    pub async fn reclaim_expired(
        &self,
        queue: String,
        now: DateTime<Utc>,
    ) -> ProposeResult<Vec<String>> {
        match self
            .propose(DeferCommand::ReclaimExpired { queue, now })
            .await?
        {
            DeferOutcome::Reclaimed(result) => Ok(result?),
            other => Err(mismatch("reclaim", other)),
        }
    }

    pub async fn cancel(&self, queue: String, task_id: String) -> ProposeResult<bool> {
        match self
            .propose(DeferCommand::Cancel { queue, task_id })
            .await?
        {
            DeferOutcome::Canceled(result) => Ok(result?),
            other => Err(mismatch("cancel", other)),
        }
    }

    pub fn scheduler(&self) -> Arc<Mutex<DeferScheduler>> {
        self.sm.scheduler()
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub async fn is_leader(&self) -> bool {
        self.host.is_leader().await
    }

    pub async fn leader(&self) -> Option<NodeId> {
        self.host.leader().await
    }

    pub fn applied_index(&self) -> Index {
        self.sm.applied_index()
    }

    pub fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.sm.snapshot(&mut buf)?;
        Ok(buf)
    }
}
fn mismatch(operation: &str, outcome: DeferOutcome) -> ProposeError {
    anyhow::anyhow!("{operation} outcome mismatch: {outcome:?}").into()
}
// HANDWRITE-END
