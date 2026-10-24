// HANDWRITE-BEGIN gap="missing-generator:logic:defer-http-api" tracker="#766" reason="Shared service-http/auth shell around Defer's Raft-backed domain commands."
//! Request and response bodies of the queue/task routes.

use defer_shared_kernel::{CreateTask, QueueControlState, QueueSnapshot, TaskStatus};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct QueueListResponse {
    pub queues: Vec<QueueSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

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
