// HANDWRITE-BEGIN gap="missing-generator:logic:defer-http-api" tracker="#766" reason="Shared service-http/auth shell around Defer's Raft-backed domain commands."
//! Request and response bodies of the queue/task routes.

use defer_shared_kernel::{CreateTask, QueueControlState, TaskStatus};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema)]
pub struct QueueControlRequest {
    pub state: QueueControlState,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateTasksRequest {
    pub tasks: Vec<CreateTask>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CreateTasksResponse {
    pub created: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TaskStatusResponse {
    pub task_id: String,
    pub status: TaskStatus,
}
// HANDWRITE-END
