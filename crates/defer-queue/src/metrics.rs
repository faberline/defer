// HANDWRITE-BEGIN gap="missing-generator:logic:defer-prometheus" tracker="#766" reason="Defer metric names over shared lock-free Prometheus primitives."
use defer_shared_kernel::QueueSnapshot;
use metrics_prometheus::{
    render, render_labeled, Counter, Label, LabeledSample, Sample, SampleGroup,
};

#[derive(Debug, Default)]
pub struct DeferMetrics {
    pub requests: Counter,
    pub dispatch_acked: Counter,
    pub dispatch_retried: Counter,
    pub dispatch_dead_lettered: Counter,
    pub dispatch_lost_ownership: Counter,
}

impl DeferMetrics {
    pub fn render(&self) -> String {
        render(&[
            Sample::new(
                "defer_requests_total",
                "counter",
                "Total Defer data-plane requests.",
                self.requests.get(),
            ),
            Sample::new(
                "defer_dispatch_acked_total",
                "counter",
                "Target deliveries committed successful.",
                self.dispatch_acked.get(),
            ),
            Sample::new(
                "defer_dispatch_retried_total",
                "counter",
                "Target deliveries committed for retry.",
                self.dispatch_retried.get(),
            ),
            Sample::new(
                "defer_dispatch_dead_lettered_total",
                "counter",
                "Target deliveries moved to the DLQ terminal state.",
                self.dispatch_dead_lettered.get(),
            ),
            Sample::new(
                "defer_dispatch_lost_ownership_total",
                "counter",
                "Target delivery results rejected by executor fencing.",
                self.dispatch_lost_ownership.get(),
            ),
        ])
    }
}

/// Render per-queue task depth gauges across scheduled, in-flight, and terminal states.
pub fn render_queue_task_gauges(snapshots: &[QueueSnapshot]) -> String {
    let mut samples = Vec::with_capacity(snapshots.len() * 3);
    for s in snapshots {
        samples.push(LabeledSample::new(
            vec![
                Label::new("queue", &s.queue),
                Label::new("state", "scheduled"),
            ],
            s.scheduled_count as u64,
        ));
        samples.push(LabeledSample::new(
            vec![
                Label::new("queue", &s.queue),
                Label::new("state", "in_flight"),
            ],
            s.in_flight_count as u64,
        ));
        samples.push(LabeledSample::new(
            vec![
                Label::new("queue", &s.queue),
                Label::new("state", "terminal"),
            ],
            s.terminal_count as u64,
        ));
    }
    let group = SampleGroup::new(
        "defer_queue_tasks",
        "gauge",
        "Queue task depth by state.",
        &samples,
    );
    render_labeled(&[group])
}

#[cfg(test)]
mod tests {
    use super::*;
    use defer_shared_kernel::{QueueControlState, QueuePolicy};

    #[test]
    fn test_render_queue_task_gauges_empty() {
        let output = render_queue_task_gauges(&[]);
        assert!(output.contains("# TYPE defer_queue_tasks gauge"));
        assert!(output.contains("# HELP defer_queue_tasks Queue task depth by state."));
        assert!(!output.contains("defer_queue_tasks{"));
    }

    #[test]
    fn test_render_queue_task_gauges_with_queues() {
        let snapshots = vec![
            QueueSnapshot {
                queue: "alpha".to_string(),
                control_state: QueueControlState::Running,
                policy: QueuePolicy::default(),
                task_count: 5,
                scheduled_count: 2,
                in_flight_count: 1,
                terminal_count: 2,
            },
            QueueSnapshot {
                queue: "beta".to_string(),
                control_state: QueueControlState::Running,
                policy: QueuePolicy::default(),
                task_count: 0,
                scheduled_count: 0,
                in_flight_count: 0,
                terminal_count: 0,
            },
        ];
        let output = render_queue_task_gauges(&snapshots);
        assert!(output.contains("# TYPE defer_queue_tasks gauge"));
        assert!(output.contains("# HELP defer_queue_tasks Queue task depth by state."));
        assert!(output.contains("defer_queue_tasks{queue=\"alpha\",state=\"scheduled\"} 2"));
        assert!(output.contains("defer_queue_tasks{queue=\"alpha\",state=\"in_flight\"} 1"));
        assert!(output.contains("defer_queue_tasks{queue=\"alpha\",state=\"terminal\"} 2"));
        assert!(output.contains("defer_queue_tasks{queue=\"beta\",state=\"scheduled\"} 0"));
        assert!(output.contains("defer_queue_tasks{queue=\"beta\",state=\"in_flight\"} 0"));
        assert!(output.contains("defer_queue_tasks{queue=\"beta\",state=\"terminal\"} 0"));
    }
}
// HANDWRITE-END
