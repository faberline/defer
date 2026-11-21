// HANDWRITE-BEGIN gap="missing-generator:logic:defer-core-scheduler" tracker="#766" reason="Queue export and import for a shard move (#269)."
//! Whole-queue export and import. A queue moves between schedulers as one
//! value: policy, control state, every task with its fence, the due and ready
//! heaps, the in-flight attempts and the dispatch bucket.

use serde::{Deserialize, Serialize};

use super::{DeferScheduler, QueueState};
use crate::types::{QueueName, SchedulerError, SchedulerResult};

/// One whole queue, ready to move to another scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueExport {
    queue: QueueName,
    state: QueueState,
}

impl QueueExport {
    pub fn queue(&self) -> &str {
        &self.queue
    }
}

/// Why an import was refused. The target scheduler is unchanged on refusal.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error, PartialEq, Eq)]
pub enum QueueImportError {
    #[error("queue `{0}` already exists")]
    QueueExists(String),
}

impl DeferScheduler {
    /// Copy one whole queue. The source keeps its queue.
    pub fn export_queue(&self, queue: &str) -> SchedulerResult<QueueExport> {
        let state = self
            .queues
            .get(queue)
            .ok_or_else(|| SchedulerError::QueueMissing(queue.to_string()))?;
        Ok(QueueExport {
            queue: queue.to_string(),
            state: state.clone(),
        })
    }

    /// Insert one whole queue. Refuses a name that already exists.
    pub fn import_queue(&mut self, export: QueueExport) -> Result<(), QueueImportError> {
        if self.queues.contains_key(&export.queue) {
            return Err(QueueImportError::QueueExists(export.queue));
        }
        self.queues.insert(export.queue, export.state);
        Ok(())
    }

    /// Remove the source copy after a successful import elsewhere.
    pub fn remove_exported_queue(&mut self, queue: &str) -> SchedulerResult<()> {
        self.delete_queue(queue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CreateTask, QueuePolicy, Target, TaskStatus, DEFAULT_PRIORITY};
    use chrono::{DateTime, TimeZone, Utc};

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    fn task(id: &str, at: DateTime<Utc>) -> CreateTask {
        CreateTask {
            task_id: id.into(),
            target: Target {
                url: "http://example.com/worker".into(),
                method: "POST".into(),
                headers: Default::default(),
            },
            payload: serde_json::json!({ "id": id }),
            schedule_at: at,
            priority: DEFAULT_PRIORITY,
            max_attempts: 3,
            dispatch_deadline_ms: None,
        }
    }

    fn custom_policy() -> QueuePolicy {
        QueuePolicy {
            max_in_flight: 2,
            ..QueuePolicy::default()
        }
    }

    fn permit_policy() -> QueuePolicy {
        QueuePolicy {
            max_burst_size: 3,
            max_dispatches_per_second: 1,
            ..QueuePolicy::default()
        }
    }

    fn ids() -> Vec<String> {
        (0..5).map(|i| format!("t{i}")).collect()
    }

    /// Scheduler A: queue `jobs` with 5 due tasks, one leased, one acked,
    /// one canceled, and the queue paused.
    fn populated() -> DeferScheduler {
        let mut a = DeferScheduler::new();
        a.configure_queue("jobs", custom_policy());
        for id in ids() {
            a.create_task("jobs", task(&id, now())).unwrap();
        }
        let leased = a.lease_due("jobs", now(), 1).unwrap();
        let acked = a.lease_due("jobs", now(), 1).unwrap();
        assert!(a.ack("jobs", &acked[0].attempt_id).unwrap());
        assert!(a.cancel("jobs", "t4").unwrap());
        assert_eq!(leased.len(), 1);
        a.pause_queue("jobs").unwrap();
        a
    }

    #[test]
    fn export_then_import_keeps_every_task() {
        let a = populated();
        let export = a.export_queue("jobs").unwrap();
        let mut b = DeferScheduler::new();
        b.import_queue(export).unwrap();

        assert_eq!(
            b.queue_snapshot("jobs").unwrap(),
            a.queue_snapshot("jobs").unwrap()
        );
        for id in ids() {
            assert_eq!(
                b.status("jobs", &id).unwrap(),
                a.status("jobs", &id).unwrap()
            );
        }
        assert_eq!(
            b.list_tasks("jobs", None, None, 100).unwrap(),
            a.list_tasks("jobs", None, None, 100).unwrap()
        );
    }

    #[test]
    fn imported_queue_keeps_the_permits() {
        let mut a = DeferScheduler::new();
        a.configure_queue("jobs", permit_policy());
        for id in ids() {
            a.create_task("jobs", task(&id, now())).unwrap();
        }
        assert_eq!(a.lease_due("jobs", now(), 3).unwrap().len(), 3);

        let export = a.export_queue("jobs").unwrap();
        let mut b = DeferScheduler::new();
        b.import_queue(export).unwrap();

        // The bucket is empty and nothing has refilled at the same instant.
        assert!(b.lease_due("jobs", now(), 3).unwrap().is_empty());
    }

    #[test]
    fn imported_in_flight_lease_can_still_be_acked() {
        let mut a = DeferScheduler::new();
        a.configure_queue("jobs", custom_policy());
        a.create_task("jobs", task("t0", now())).unwrap();
        let lease = a.lease_due("jobs", now(), 1).unwrap().remove(0);

        let mut b = DeferScheduler::new();
        b.import_queue(a.export_queue("jobs").unwrap()).unwrap();

        assert!(b.ack("jobs", &lease.attempt_id).unwrap());
        assert_eq!(b.status("jobs", "t0").unwrap(), Some(TaskStatus::Succeeded));
    }

    #[test]
    fn import_refuses_an_existing_queue() {
        let a = populated();
        let export = a.export_queue("jobs").unwrap();

        let mut b = DeferScheduler::new();
        b.configure_queue("jobs", QueuePolicy::default());
        b.create_task("jobs", task("keep", now())).unwrap();
        let before = b.queue_snapshot("jobs").unwrap();

        assert_eq!(
            b.import_queue(export),
            Err(QueueImportError::QueueExists("jobs".into()))
        );
        assert_eq!(b.queue_snapshot("jobs").unwrap(), before);
        assert_eq!(
            b.status("jobs", "keep").unwrap(),
            Some(TaskStatus::Scheduled)
        );
    }

    #[test]
    fn export_of_a_missing_queue_is_queue_missing() {
        let a = DeferScheduler::new();
        assert_eq!(
            a.export_queue("nope").unwrap_err(),
            SchedulerError::QueueMissing("nope".into())
        );
    }

    #[test]
    fn export_survives_a_json_round_trip() {
        let a = populated();
        let bytes = serde_json::to_vec(&a.export_queue("jobs").unwrap()).unwrap();
        let export: QueueExport = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(export.queue(), "jobs");

        let mut b = DeferScheduler::new();
        b.import_queue(export).unwrap();
        assert_eq!(
            b.queue_snapshot("jobs").unwrap(),
            a.queue_snapshot("jobs").unwrap()
        );
        assert_eq!(
            b.list_tasks("jobs", None, None, 100).unwrap(),
            a.list_tasks("jobs", None, None, 100).unwrap()
        );
    }
}
// HANDWRITE-END
