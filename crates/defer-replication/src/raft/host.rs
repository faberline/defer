// SPEC-MANAGED: tech-design/logic/core-scheduler-priority-rate-dispatch.md#logic
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
    ClusterTopology, FsyncPolicy as RaftFsyncPolicy, HostConfig, Index, Membership, NodeId,
    PeerTransport, RaftHost, RaftStateMachine, RaftStore, SnapshotPolicy,
};

use super::command::{DeferCommand, DeferEnvelope, DeferOutcome, ProposalId};
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
        std::fs::create_dir_all(raft_dir)?;
        let store = RaftStore::open(
            raft_dir
                .to_str()
                .context("raft data dir is not valid UTF-8")?,
            node_id,
            RaftFsyncPolicy::Always,
        )?;
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
            &data_dir.join("raft"),
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
            &data_dir.join("raft"),
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
    ) -> Result<QueueSnapshot> {
        match self
            .propose(DeferCommand::ConfigureQueue { queue, policy })
            .await?
        {
            DeferOutcome::Queue(result) => Ok(result?),
            other => anyhow::bail!("configure queue outcome mismatch: {other:?}"),
        }
    }

    pub async fn update_queue_policy(
        &self,
        queue: String,
        policy: QueuePolicy,
    ) -> Result<QueueSnapshot> {
        match self
            .propose(DeferCommand::UpdateQueuePolicy { queue, policy })
            .await?
        {
            DeferOutcome::Queue(result) => Ok(result?),
            other => anyhow::bail!("update queue outcome mismatch: {other:?}"),
        }
    }

    pub async fn set_queue_control(
        &self,
        queue: String,
        control: QueueControlState,
    ) -> Result<QueueSnapshot> {
        match self
            .propose(DeferCommand::SetQueueControl { queue, control })
            .await?
        {
            DeferOutcome::Queue(result) => Ok(result?),
            other => anyhow::bail!("queue control outcome mismatch: {other:?}"),
        }
    }

    pub async fn create_task(&self, queue: String, task: CreateTask) -> Result<()> {
        match self
            .propose(DeferCommand::CreateTask { queue, task })
            .await?
        {
            DeferOutcome::Created(result) => Ok(result?),
            other => anyhow::bail!("create task outcome mismatch: {other:?}"),
        }
    }

    pub async fn create_tasks(&self, queue: String, tasks: Vec<CreateTask>) -> Result<usize> {
        match self
            .propose(DeferCommand::CreateTasks { queue, tasks })
            .await?
        {
            DeferOutcome::CreatedBatch(result) => Ok(result?),
            other => anyhow::bail!("create tasks outcome mismatch: {other:?}"),
        }
    }

    pub async fn lease_due(
        &self,
        queue: String,
        now: DateTime<Utc>,
        requested: usize,
    ) -> Result<Vec<DispatchLease>> {
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
            other => anyhow::bail!("lease outcome mismatch: {other:?}"),
        }
    }

    pub async fn ack(
        &self,
        queue: String,
        attempt_id: String,
        epoch: u64,
        now: DateTime<Utc>,
    ) -> Result<bool> {
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
            other => anyhow::bail!("ack outcome mismatch: {other:?}"),
        }
    }

    pub async fn nack(
        &self,
        queue: String,
        attempt_id: String,
        epoch: u64,
        now: DateTime<Utc>,
    ) -> Result<Option<NackOutcome>> {
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
            other => anyhow::bail!("nack outcome mismatch: {other:?}"),
        }
    }

    pub async fn settle_batch(
        &self,
        queue: String,
        attempts: Vec<AttemptSettlement>,
    ) -> Result<Vec<SettlementOutcome>> {
        match self
            .propose(DeferCommand::SettleBatch {
                queue,
                executor_node: self.node_id,
                attempts,
            })
            .await?
        {
            DeferOutcome::Settled(result) => Ok(result?),
            other => anyhow::bail!("settle batch outcome mismatch: {other:?}"),
        }
    }

    pub async fn reclaim_expired(&self, queue: String, now: DateTime<Utc>) -> Result<Vec<String>> {
        match self
            .propose(DeferCommand::ReclaimExpired { queue, now })
            .await?
        {
            DeferOutcome::Reclaimed(result) => Ok(result?),
            other => anyhow::bail!("reclaim outcome mismatch: {other:?}"),
        }
    }

    pub async fn cancel(&self, queue: String, task_id: String) -> Result<bool> {
        match self
            .propose(DeferCommand::Cancel { queue, task_id })
            .await?
        {
            DeferOutcome::Canceled(result) => Ok(result?),
            other => anyhow::bail!("cancel outcome mismatch: {other:?}"),
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
// HANDWRITE-END
