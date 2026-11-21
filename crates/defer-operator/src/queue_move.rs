// HANDWRITE-BEGIN gap="missing-generator:logic:defer-queue-move" tracker="#284" reason="Queue move planner and HTTP actuator for a shard split or merge."
//! Queue move planner, runner and HTTP actuator for a shard split or merge.
//!
//! A queue moves in three steps: export from the old shard, import on the new
//! shard, remove from the old shard. The old shard removes a queue only after
//! the import returned success. A failed import leaves the queue on the old
//! shard; the next reconcile retries it.

use crate::crd::QueueMoveStatus;
use defer_queue::placement::QueuePlacement;
use serde_json::Value;
use shard_routing::{BucketMove, VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT};
use std::collections::HashSet;

/// A queue move step: recorded, running, or done (#285).
pub const MOVE_PENDING: &str = "pending";
pub const MOVE_MOVING: &str = "moving";
pub const MOVE_DONE: &str = "done";

/// One step of a queue move.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MoveStep {
    Export { queue: String, from_shard: u32 },
    Import { queue: String, to_shard: u32 },
    Remove { queue: String, from_shard: u32 },
}

/// Why a queue move step failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MoveError {
    Export(String),
    Import(String),
    Remove(String),
    List(String),
}

impl std::fmt::Display for MoveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Export(reason) => write!(f, "export failed: {reason}"),
            Self::Import(reason) => write!(f, "import failed: {reason}"),
            Self::Remove(reason) => write!(f, "remove failed: {reason}"),
            Self::List(reason) => write!(f, "list failed: {reason}"),
        }
    }
}

impl std::error::Error for MoveError {}

/// The three admin calls of a queue move.
#[allow(async_fn_in_trait)]
pub trait QueueMoveActuator {
    async fn export(&self, queue: &str, from_shard: u32) -> Result<Value, MoveError>;
    async fn import(&self, queue: &str, to_shard: u32, export: &Value) -> Result<(), MoveError>;
    async fn remove(&self, queue: &str, from_shard: u32) -> Result<(), MoveError>;
}

/// The result of a run: the queues that left the old shard, and the failures.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MoveReport {
    pub moved: Vec<String>,
    pub failed: Vec<(String, MoveError)>,
}

/// Turns the moves of a split into ordered steps: per queue, export, import, remove.
pub fn plan_queue_moves(moves: &[(String, BucketMove)]) -> Vec<MoveStep> {
    moves
        .iter()
        .flat_map(|(queue, bucket_move)| {
            [
                MoveStep::Export {
                    queue: queue.clone(),
                    from_shard: bucket_move.from_shard,
                },
                MoveStep::Import {
                    queue: queue.clone(),
                    to_shard: bucket_move.to_shard,
                },
                MoveStep::Remove {
                    queue: queue.clone(),
                    from_shard: bucket_move.from_shard,
                },
            ]
        })
        .collect()
}

/// Runs the steps in order. A failed step stops the remaining steps of its queue.
pub async fn run_queue_moves<A: QueueMoveActuator>(actuator: &A, steps: &[MoveStep]) -> MoveReport {
    let mut report = MoveReport::default();
    let mut exported: std::collections::HashMap<String, Value> = Default::default();
    let mut stopped: std::collections::HashSet<String> = Default::default();

    for step in steps {
        let queue = match step {
            MoveStep::Export { queue, .. }
            | MoveStep::Import { queue, .. }
            | MoveStep::Remove { queue, .. } => queue,
        };
        if stopped.contains(queue) {
            continue;
        }
        let result = match step {
            MoveStep::Export { queue, from_shard } => {
                match actuator.export(queue, *from_shard).await {
                    Ok(value) => {
                        exported.insert(queue.clone(), value);
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
            MoveStep::Import { queue, to_shard } => match exported.get(queue) {
                Some(value) => actuator.import(queue, *to_shard, value).await,
                None => Err(MoveError::Import("no export for queue".to_string())),
            },
            MoveStep::Remove { queue, from_shard } => actuator
                .remove(queue, *from_shard)
                .await
                .map(|()| report.moved.push(queue.clone())),
        };
        if let Err(error) = result {
            report.failed.push((queue.clone(), error));
            stopped.insert(queue.clone());
        }
    }
    report
}

/// Calls the three admin routes of the shard that owns the queue.
pub struct HttpQueueMoveActuator {
    client: reqwest::Client,
    /// Base URL (no trailing slash) of a pod of the given shard.
    shard_url: Box<dyn Fn(u32) -> String + Send + Sync>,
    /// Bearer token. An empty token sends no `Authorization` header.
    token: String,
}

impl HttpQueueMoveActuator {
    pub fn new(
        client: reqwest::Client,
        shard_url: impl Fn(u32) -> String + Send + Sync + 'static,
        token: impl Into<String>,
    ) -> Self {
        Self {
            client,
            shard_url: Box::new(shard_url),
            token: token.into(),
        }
    }
}

impl HttpQueueMoveActuator {
    /// Builds a request to `/v1/admin/queues/<queue>/<op>` on the shard. The
    /// queue name is percent-encoded as one path segment.
    fn build_request(
        &self,
        method: reqwest::Method,
        shard: u32,
        queue: &str,
        op: &str,
    ) -> Result<reqwest::RequestBuilder, String> {
        let base = (self.shard_url)(shard);
        let mut url = reqwest::Url::parse(&base).map_err(|error| format!("{base}: {error}"))?;
        url.path_segments_mut()
            .map_err(|()| format!("{base} is not a base URL"))?
            .extend(["v1", "admin", "queues", queue, op]);
        let mut request = self.client.request(method, url);
        if !self.token.is_empty() {
            request = request.bearer_auth(&self.token);
        }
        Ok(request)
    }
}

impl QueueMoveActuator for HttpQueueMoveActuator {
    async fn export(&self, queue: &str, from_shard: u32) -> Result<Value, MoveError> {
        let request = self
            .build_request(reqwest::Method::GET, from_shard, queue, "export")
            .map_err(MoveError::Export)?;
        let response = request
            .send()
            .await
            .map_err(|error| MoveError::Export(error.to_string()))?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<Value>()
                .await
                .map_err(|error| MoveError::Export(error.to_string()));
        }
        let text = response.text().await.unwrap_or_default();
        Err(MoveError::Export(format!("{status}: {text}")))
    }

    async fn import(&self, queue: &str, to_shard: u32, export: &Value) -> Result<(), MoveError> {
        let request = self
            .build_request(reqwest::Method::PUT, to_shard, queue, "import")
            .map_err(MoveError::Import)?;
        let response = request
            .json(export)
            .send()
            .await
            .map_err(|error| MoveError::Import(error.to_string()))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        // A conflict (409) is an error too: the queue stays on the old shard.
        let text = response.text().await.unwrap_or_default();
        Err(MoveError::Import(format!("{status}: {text}")))
    }

    async fn remove(&self, queue: &str, from_shard: u32) -> Result<(), MoveError> {
        let request = self
            .build_request(reqwest::Method::DELETE, from_shard, queue, "export")
            .map_err(MoveError::Remove)?;
        let response = request
            .send()
            .await
            .map_err(|error| MoveError::Remove(error.to_string()))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let text = response.text().await.unwrap_or_default();
        Err(MoveError::Remove(format!("{status}: {text}")))
    }
}

/// Lists the queues of one shard (the old shards and the new ones of a raise).
#[allow(async_fn_in_trait)]
pub trait QueueLister {
    async fn list_queues(&self, shard: u32) -> Result<Vec<String>, MoveError>;
}

impl QueueLister for HttpQueueMoveActuator {
    /// Follows `next_page_token` of `GET /v1/queues` until the last page.
    async fn list_queues(&self, shard: u32) -> Result<Vec<String>, MoveError> {
        let base = (self.shard_url)(shard);
        let mut names = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut url = reqwest::Url::parse(&base)
                .map_err(|error| MoveError::List(format!("{base}: {error}")))?;
            url.path_segments_mut()
                .map_err(|()| MoveError::List(format!("{base} is not a base URL")))?
                .extend(["v1", "queues"]);
            url.query_pairs_mut().append_pair("page_size", "1000");
            if let Some(token) = &page_token {
                url.query_pairs_mut().append_pair("page_token", token);
            }
            let mut request = self.client.get(url);
            if !self.token.is_empty() {
                request = request.bearer_auth(&self.token);
            }
            let response = request
                .send()
                .await
                .map_err(|error| MoveError::List(error.to_string()))?;
            let status = response.status();
            if !status.is_success() {
                let text = response.text().await.unwrap_or_default();
                return Err(MoveError::List(format!("{status}: {text}")));
            }
            let page: Value = response
                .json()
                .await
                .map_err(|error| MoveError::List(error.to_string()))?;
            let queues = page["queues"]
                .as_array()
                .ok_or_else(|| MoveError::List("no queues array".to_string()))?;
            for item in queues {
                let name = item["queue"]
                    .as_str()
                    .ok_or_else(|| MoveError::List("queue without a name".to_string()))?;
                names.push(name.to_string());
            }
            match page["next_page_token"].as_str().filter(|t| !t.is_empty()) {
                Some(token) => page_token = Some(token.to_string()),
                None => return Ok(names),
            }
        }
    }
}

/// The virtual bucket map of a Defer with `shards` shards: one shard, then one
/// single split for each further shard (the convention of `split_one_shard`).
pub fn shard_map(shards: u32) -> anyhow::Result<VirtualBucketShardMap> {
    anyhow::ensure!(shards >= 1, "shards must be at least 1");
    let mut map = VirtualBucketShardMap::balanced(1, DEFAULT_VIRTUAL_BUCKET_COUNT, 1)?;
    for version in 2..=shards {
        map = map.split_one_shard(u64::from(version))?;
    }
    Ok(map)
}

/// What one reconcile knows about a shard count change (#285).
pub struct QueueMoveInput<'a> {
    /// The queue move recorded in the status, if any.
    pub recorded: Option<&'a QueueMoveStatus>,
    /// Shards that the StatefulSet has now, before this reconcile's growth.
    pub live_shards: u32,
    /// `spec.cluster.shardCount`.
    pub desired_shards: u32,
    /// Settled voters of each shard after this reconcile's decision.
    pub settled_shards: &'a [u32],
    pub replicas_per_shard: u32,
    pub live_replicas: u32,
    pub ready_replicas: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueueMoveDecision {
    Nothing,
    Record(QueueMoveStatus),
    Run { from_shards: u32, to_shards: u32 },
}

/// Decides what a reconcile does about a queue move. Pure: no I/O.
pub fn plan_queue_move_step(input: &QueueMoveInput<'_>) -> QueueMoveDecision {
    if let Some(recorded) = input.recorded.filter(|r| r.step != MOVE_DONE) {
        let from = u32::try_from(recorded.from_shards).unwrap_or(0);
        let to = u32::try_from(recorded.to_shards).unwrap_or(0);
        let settled = input.settled_shards.len() == to as usize
            && input
                .settled_shards
                .iter()
                .all(|&voters| voters == input.replicas_per_shard);
        let sized = input.live_replicas == to * input.replicas_per_shard;
        if settled && input.ready_replicas == input.live_replicas && sized {
            return QueueMoveDecision::Run {
                from_shards: from,
                to_shards: to,
            };
        }
        return QueueMoveDecision::Nothing;
    }
    if input.live_shards >= 1 && input.live_shards < input.desired_shards {
        return QueueMoveDecision::Record(QueueMoveStatus {
            from_shards: input.live_shards as i32,
            to_shards: input.desired_shards as i32,
            step: MOVE_PENDING.to_string(),
        });
    }
    QueueMoveDecision::Nothing
}

/// Moves the queues of a raise from `from_shards` to `to_shards` (#285). Lists
/// the queues of the old shards and the new ones first, so a list error moves
/// nothing. A queue that already sits on a new shard (an earlier run imported
/// it and stopped) gets only the remove step.
pub async fn drive_queue_move<A: QueueMoveActuator + QueueLister>(
    actuator: &A,
    from_shards: u32,
    to_shards: u32,
) -> Result<MoveReport, MoveError> {
    let old = shard_map(from_shards).map_err(|error| MoveError::List(error.to_string()))?;
    let new = shard_map(to_shards).map_err(|error| MoveError::List(error.to_string()))?;
    let mut names = Vec::new();
    for shard in 0..from_shards {
        names.extend(actuator.list_queues(shard).await?);
    }
    names.sort();
    names.dedup();
    let mut on_target: HashSet<String> = HashSet::new();
    for shard in from_shards..to_shards {
        on_target.extend(actuator.list_queues(shard).await?);
    }
    let moves = QueuePlacement::new(old, 0)
        .queues_to_move(&new, &names)
        .map_err(|error| MoveError::List(error.to_string()))?;
    let mut steps = Vec::new();
    for (queue, bucket_move) in &moves {
        if on_target.contains(queue) {
            steps.push(MoveStep::Remove {
                queue: queue.clone(),
                from_shard: bucket_move.from_shard,
            });
        } else {
            steps.extend(plan_queue_moves(std::slice::from_ref(&(
                queue.clone(),
                *bucket_move,
            ))));
        }
    }
    Ok(run_queue_moves(actuator, &steps).await)
}

#[cfg(test)]
mod tests {
    use crate::crd::QueueMoveStatus;
    use crate::queue_move::{
        drive_queue_move, plan_queue_move_step, plan_queue_moves, run_queue_moves, shard_map,
        HttpQueueMoveActuator, MoveError, MoveReport, MoveStep, QueueLister, QueueMoveActuator,
        QueueMoveDecision, QueueMoveInput, MOVE_DONE, MOVE_MOVING, MOVE_PENDING,
    };
    use axum::{
        extract::{Query, State},
        http::{HeaderMap, Method, StatusCode, Uri},
        routing::{get, put},
        Router,
    };
    use defer_queue::placement::QueuePlacement;
    use serde_json::{json, Value};
    use shard_routing::{BucketMove, VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT};
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::{Arc, Mutex};

    /// The moves of a split from one shard to two, for 100 queues `q0..q99`.
    fn split_moves() -> Vec<(String, BucketMove)> {
        let old = VirtualBucketShardMap::balanced(1, DEFAULT_VIRTUAL_BUCKET_COUNT, 1).unwrap();
        let new = old.split_one_shard(2).unwrap();
        let names: Vec<String> = (0..100).map(|i| format!("q{i}")).collect();
        QueuePlacement::new(old, 0)
            .queues_to_move(&new, &names)
            .unwrap()
    }

    /// The three steps of one queue, from shard 0 to shard 1.
    fn steps(queue: &str) -> Vec<MoveStep> {
        vec![
            MoveStep::Export {
                queue: queue.to_string(),
                from_shard: 0,
            },
            MoveStep::Import {
                queue: queue.to_string(),
                to_shard: 1,
            },
            MoveStep::Remove {
                queue: queue.to_string(),
                from_shard: 0,
            },
        ]
    }

    #[derive(Default)]
    struct Fake {
        calls: Mutex<Vec<String>>,
        export_results: Mutex<VecDeque<Result<Value, MoveError>>>,
        import_results: Mutex<VecDeque<Result<(), MoveError>>>,
        remove_results: Mutex<VecDeque<Result<(), MoveError>>>,
    }

    impl Fake {
        fn record(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        fn with_import(results: Vec<Result<(), MoveError>>) -> Self {
            Self {
                import_results: Mutex::new(results.into()),
                ..Self::default()
            }
        }

        fn with_export(results: Vec<Result<Value, MoveError>>) -> Self {
            Self {
                export_results: Mutex::new(results.into()),
                ..Self::default()
            }
        }

        fn with_remove(results: Vec<Result<(), MoveError>>) -> Self {
            Self {
                remove_results: Mutex::new(results.into()),
                ..Self::default()
            }
        }
    }

    impl QueueMoveActuator for Fake {
        async fn export(&self, queue: &str, from_shard: u32) -> Result<Value, MoveError> {
            self.record(format!("export({queue},{from_shard})"));
            self.export_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(json!({"queue": queue})))
        }

        async fn import(
            &self,
            queue: &str,
            to_shard: u32,
            export: &Value,
        ) -> Result<(), MoveError> {
            self.record(format!("import({queue},{to_shard},{export})"));
            self.import_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(()))
        }

        async fn remove(&self, queue: &str, from_shard: u32) -> Result<(), MoveError> {
            self.record(format!("remove({queue},{from_shard})"));
            self.remove_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(()))
        }
    }

    #[test]
    fn plan_gives_export_import_remove_per_queue_in_order() {
        let moves = split_moves();
        assert!(!moves.is_empty(), "the split must move some queues");
        assert!(moves
            .iter()
            .all(|(_, m)| m.from_shard == 0 && m.to_shard == 1));

        let steps = plan_queue_moves(&moves);

        assert_eq!(steps.len(), 3 * moves.len());
        for (i, (queue, _)) in moves.iter().enumerate() {
            assert_eq!(
                steps[3 * i],
                MoveStep::Export {
                    queue: queue.clone(),
                    from_shard: 0
                }
            );
            assert_eq!(
                steps[3 * i + 1],
                MoveStep::Import {
                    queue: queue.clone(),
                    to_shard: 1
                }
            );
            assert_eq!(
                steps[3 * i + 2],
                MoveStep::Remove {
                    queue: queue.clone(),
                    from_shard: 0
                }
            );
        }
    }

    #[test]
    fn plan_of_no_moves_is_empty() {
        assert_eq!(plan_queue_moves(&[]), vec![]);
    }

    #[tokio::test]
    async fn run_calls_export_then_import_then_remove() {
        let fake = Fake::default();
        let mut all = steps("a");
        all.extend(steps("b"));

        let report = run_queue_moves(&fake, &all).await;

        assert_eq!(
            fake.calls(),
            vec![
                "export(a,0)",
                "import(a,1,{\"queue\":\"a\"})",
                "remove(a,0)",
                "export(b,0)",
                "import(b,1,{\"queue\":\"b\"})",
                "remove(b,0)",
            ]
        );
        assert_eq!(report.moved, vec!["a".to_string(), "b".to_string()]);
        assert!(report.failed.is_empty());
    }

    #[tokio::test]
    async fn failed_import_keeps_the_old_copy() {
        // The first import call is the import of `a`.
        let fake = Fake::with_import(vec![Err(MoveError::Import("boom".into()))]);
        let mut all = steps("a");
        all.extend(steps("b"));

        let report = run_queue_moves(&fake, &all).await;

        let calls = fake.calls();
        assert!(
            !calls.iter().any(|c| c == "remove(a,0)"),
            "a must stay on the old shard: {calls:?}"
        );
        assert_eq!(
            report,
            MoveReport {
                moved: vec!["b".to_string()],
                failed: vec![("a".to_string(), MoveError::Import("boom".into()))],
            }
        );
        assert!(calls.contains(&"remove(b,0)".to_string()));
    }

    #[tokio::test]
    async fn failed_export_skips_import_and_remove() {
        let fake = Fake::with_export(vec![Err(MoveError::Export("e".into()))]);
        let mut all = steps("a");
        all.extend(steps("b"));

        let report = run_queue_moves(&fake, &all).await;

        let calls = fake.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("import(a")),
            "{calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("remove(a")),
            "{calls:?}"
        );
        assert_eq!(
            report.failed,
            vec![("a".to_string(), MoveError::Export("e".into()))]
        );
        assert_eq!(report.moved, vec!["b".to_string()]);
    }

    #[tokio::test]
    async fn failed_remove_is_reported_not_moved() {
        let fake = Fake::with_remove(vec![Err(MoveError::Remove("x".into()))]);

        let report = run_queue_moves(&fake, &steps("a")).await;

        assert_eq!(
            report.failed,
            vec![("a".to_string(), MoveError::Remove("x".into()))]
        );
        assert!(!report.moved.contains(&"a".to_string()));
    }

    #[tokio::test]
    async fn retry_at_the_next_run_moves_the_queue() {
        let fake = Fake::with_import(vec![Err(MoveError::Import("boom".into()))]);

        // Run 1: the import fails, so the queue stays on the old shard.
        let first = run_queue_moves(&fake, &steps("a")).await;
        assert_eq!(first.failed.len(), 1);
        assert!(!fake.calls().contains(&"remove(a,0)".to_string()));

        // Run 2: the failing result is used up; the queue moves.
        let second = run_queue_moves(&fake, &steps("a")).await;

        let calls = fake.calls();
        assert_eq!(calls[calls.len() - 2], "import(a,1,{\"queue\":\"a\"})");
        assert_eq!(calls[calls.len() - 1], "remove(a,0)");
        assert_eq!(second.moved, vec!["a".to_string()]);
        assert!(second.failed.is_empty());
    }

    #[tokio::test]
    async fn import_without_export_is_reported() {
        let fake = Fake::default();
        let only_import = vec![MoveStep::Import {
            queue: "a".to_string(),
            to_shard: 1,
        }];

        let report = run_queue_moves(&fake, &only_import).await;

        assert_eq!(
            report.failed,
            vec![(
                "a".to_string(),
                MoveError::Import("no export for queue".to_string())
            )]
        );
        assert!(fake.calls().is_empty());
    }

    // ---- HTTP actuator ----

    #[derive(Debug, Clone, PartialEq)]
    struct Call {
        method: String,
        path: String,
        auth: Option<String>,
        body: Value,
    }

    type Seen = Arc<Mutex<Vec<Call>>>;

    #[derive(Clone)]
    struct Server {
        seen: Seen,
        status: StatusCode,
        body: String,
    }

    async fn handle(
        State(server): State<Server>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: String,
    ) -> (StatusCode, String) {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
        server.seen.lock().unwrap().push(Call {
            method: method.to_string(),
            path: uri.path().to_string(),
            auth,
            body,
        });
        (server.status, server.body.clone())
    }

    /// Starts a server that answers every queue route with `status` and `body`.
    async fn spawn(status: u16, body: &str) -> (String, Seen) {
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route(
                "/v1/admin/queues/{queue}/export",
                get(handle).delete(handle),
            )
            .route("/v1/admin/queues/{queue}/import", put(handle))
            .with_state(Server {
                seen: seen.clone(),
                status: StatusCode::from_u16(status).unwrap(),
                body: body.to_string(),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), seen)
    }

    fn actuator(url: String, token: &str) -> HttpQueueMoveActuator {
        HttpQueueMoveActuator::new(reqwest::Client::new(), move |_| url.clone(), token)
    }

    #[tokio::test]
    async fn http_export_gets_the_export_route_with_the_bearer_token() {
        let (url, seen) = spawn(200, r#"{"queue":"jobs","rows":[1,2]}"#).await;

        let result = actuator(url, "t0k").export("jobs", 0).await;

        assert_eq!(result, Ok(json!({"queue": "jobs", "rows": [1, 2]})));
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "GET");
        assert_eq!(seen[0].path, "/v1/admin/queues/jobs/export");
        assert_eq!(seen[0].auth.as_deref(), Some("Bearer t0k"));
    }

    #[tokio::test]
    async fn http_import_puts_the_value_on_the_import_route() {
        let (url, seen) = spawn(204, "").await;

        let result = actuator(url, "t0k")
            .import("jobs", 1, &json!({"queue": "jobs"}))
            .await;

        assert_eq!(result, Ok(()));
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "PUT");
        assert_eq!(seen[0].path, "/v1/admin/queues/jobs/import");
        assert_eq!(seen[0].body, json!({"queue": "jobs"}));
    }

    #[tokio::test]
    async fn http_remove_deletes_the_export_route() {
        let (url, seen) = spawn(204, "").await;

        let result = actuator(url, "t0k").remove("jobs", 0).await;

        assert_eq!(result, Ok(()));
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "DELETE");
        assert_eq!(seen[0].path, "/v1/admin/queues/jobs/export");
    }

    #[tokio::test]
    async fn http_import_conflict_is_an_error() {
        let (url, _seen) = spawn(409, "conflict").await;

        let result = actuator(url, "t0k")
            .import("jobs", 1, &json!({"queue": "jobs"}))
            .await;

        match result {
            Err(MoveError::Import(text)) => assert!(text.contains("409"), "text was: {text}"),
            other => panic!("expected an Import error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn http_remove_failure_status_is_an_error() {
        let (url, _seen) = spawn(500, "boom").await;

        let result = actuator(url, "t0k").remove("jobs", 0).await;

        match result {
            Err(MoveError::Remove(text)) => assert!(text.contains("500"), "text was: {text}"),
            other => panic!("expected a Remove error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn http_empty_token_sends_no_authorization_header() {
        let (url, seen) = spawn(204, "").await;

        let result = actuator(url, "").remove("jobs", 0).await;

        assert_eq!(result, Ok(()));
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].auth, None);
    }

    #[tokio::test]
    async fn http_queue_name_is_percent_encoded() {
        let (url, seen) = spawn(200, r#"{}"#).await;

        let _ = actuator(url, "t0k").export("a b", 0).await;

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, "/v1/admin/queues/a%20b/export");
    }

    #[tokio::test]
    async fn http_uses_the_url_of_the_shard() {
        let (url0, seen0) = spawn(200, r#"{}"#).await;
        let (url1, seen1) = spawn(204, "").await;
        let shards = [url0, url1];
        let actuator = HttpQueueMoveActuator::new(
            reqwest::Client::new(),
            move |shard| shards[shard as usize].clone(),
            "t0k",
        );

        assert!(actuator.export("jobs", 0).await.is_ok());
        assert_eq!(actuator.import("jobs", 1, &json!({})).await, Ok(()));

        let seen0 = seen0.lock().unwrap();
        assert_eq!(seen0.len(), 1);
        assert_eq!(seen0[0].method, "GET");
        let seen1 = seen1.lock().unwrap();
        assert_eq!(seen1.len(), 1);
        assert_eq!(seen1[0].method, "PUT");
    }

    // ---- shard map, list, planner and driver (#285) ----

    /// Queues per shard (index = shard number) with the calls made on them.
    struct Shards {
        queues: Mutex<Vec<Vec<String>>>,
        calls: Mutex<Vec<String>>,
        /// Queues whose next import fails once (the failure is used up).
        fail_import: Mutex<HashSet<String>>,
        fail_list: bool,
    }

    impl Shards {
        fn new(queues: Vec<Vec<String>>) -> Self {
            Self {
                queues: Mutex::new(queues),
                calls: Mutex::default(),
                fail_import: Mutex::default(),
                fail_list: false,
            }
        }

        fn record(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        /// The shards that hold the queue.
        fn holder(&self, queue: &str) -> Vec<u32> {
            self.queues
                .lock()
                .unwrap()
                .iter()
                .enumerate()
                .filter(|(_, names)| names.iter().any(|q| q == queue))
                .map(|(shard, _)| shard as u32)
                .collect()
        }
    }

    impl QueueLister for Shards {
        async fn list_queues(&self, shard: u32) -> Result<Vec<String>, MoveError> {
            if self.fail_list {
                return Err(MoveError::List("down".to_string()));
            }
            Ok(self
                .queues
                .lock()
                .unwrap()
                .get(shard as usize)
                .cloned()
                .unwrap_or_default())
        }
    }

    impl QueueMoveActuator for Shards {
        async fn export(&self, queue: &str, from_shard: u32) -> Result<Value, MoveError> {
            self.record(format!("export({queue},{from_shard})"));
            if self.holder(queue).contains(&from_shard) {
                Ok(json!({"queue": queue}))
            } else {
                Err(MoveError::Export("not on this shard".to_string()))
            }
        }

        async fn import(
            &self,
            queue: &str,
            to_shard: u32,
            _export: &Value,
        ) -> Result<(), MoveError> {
            self.record(format!("import({queue},{to_shard})"));
            if self.fail_import.lock().unwrap().remove(queue) {
                return Err(MoveError::Import("boom".to_string()));
            }
            let mut queues = self.queues.lock().unwrap();
            while queues.len() <= to_shard as usize {
                queues.push(Vec::new());
            }
            let target = &mut queues[to_shard as usize];
            if !target.iter().any(|q| q == queue) {
                target.push(queue.to_string());
            }
            Ok(())
        }

        async fn remove(&self, queue: &str, from_shard: u32) -> Result<(), MoveError> {
            self.record(format!("remove({queue},{from_shard})"));
            if let Some(names) = self.queues.lock().unwrap().get_mut(from_shard as usize) {
                names.retain(|q| q != queue);
            }
            Ok(())
        }
    }

    /// 100 queues `q0..q99`, all on shard 0.
    fn hundred_on_shard_zero() -> Vec<Vec<String>> {
        vec![(0..100).map(|i| format!("q{i}")).collect()]
    }

    /// The first of `q0..q99` that moves from shard 0 to shard 1 in a split to two shards.
    fn first_queue_moving_to_one() -> String {
        let two = shard_map(2).unwrap();
        (0..100)
            .map(|i| format!("q{i}"))
            .find(|q| two.route_key("queue", q.as_str()).shard == 1)
            .unwrap()
    }

    fn status(from: i32, to: i32, step: &str) -> QueueMoveStatus {
        QueueMoveStatus {
            from_shards: from,
            to_shards: to,
            step: step.to_string(),
        }
    }

    #[test]
    fn shard_map_one_is_single_and_each_step_is_one_split() {
        assert_eq!(shard_map(1).unwrap().physical_shard_count(), 1);
        assert_eq!(shard_map(3).unwrap().physical_shard_count(), 3);
        let stepped = shard_map(2).unwrap().split_one_shard(3).unwrap();
        for i in 0..100 {
            let q = format!("q{i}");
            assert_eq!(
                shard_map(3).unwrap().route_key("queue", &q).shard,
                stepped.route_key("queue", &q).shard,
                "{q}"
            );
        }
        assert!(shard_map(0).is_err());
    }

    #[tokio::test]
    async fn raise_moves_only_the_listed_queues() {
        let shards = Shards::new(hundred_on_shard_zero());

        let report = drive_queue_move(&shards, 1, 2).await.unwrap();

        assert!(report.failed.is_empty());
        let two = shard_map(2).unwrap();
        let expected: Vec<String> = (0..100)
            .map(|i| format!("q{i}"))
            .filter(|q| two.route_key("queue", q.as_str()).shard == 1)
            .collect();
        assert!(!expected.is_empty() && expected.len() < 100);
        for i in 0..100 {
            let q = format!("q{i}");
            let want = two.route_key("queue", &q).shard;
            assert_eq!(shards.holder(&q), vec![want], "{q}");
        }
        let exported = shards
            .calls()
            .into_iter()
            .filter(|c| c.starts_with("export("))
            .count();
        assert_eq!(exported, expected.len());
        assert_eq!(report.moved.len(), expected.len());
    }

    #[tokio::test]
    async fn failed_import_keeps_the_queue_on_the_old_shard_and_the_next_run_retries() {
        let moved = first_queue_moving_to_one();
        let shards = Shards::new(hundred_on_shard_zero());
        shards.fail_import.lock().unwrap().insert(moved.clone());

        let first = drive_queue_move(&shards, 1, 2).await.unwrap();

        assert_eq!(first.failed.len(), 1);
        assert_eq!(first.failed[0].0, moved);
        assert!(matches!(first.failed[0].1, MoveError::Import(_)));
        assert_eq!(shards.holder(&moved), vec![0]);
        assert!(!shards.calls().contains(&format!("remove({moved},0)")));

        let second = drive_queue_move(&shards, 1, 2).await.unwrap();

        assert!(second.failed.is_empty());
        assert_eq!(shards.holder(&moved), vec![1]);
    }

    #[tokio::test]
    async fn restart_after_import_before_remove_only_removes() {
        let moved = first_queue_moving_to_one();
        // A past run imported the queue to shard 1 and stopped before the remove.
        let mut queues = hundred_on_shard_zero();
        queues.push(vec![moved.clone()]);
        let shards = Shards::new(queues);

        let report = drive_queue_move(&shards, 1, 2).await.unwrap();

        assert!(report.failed.is_empty());
        let calls = shards.calls();
        let export_prefix = format!("export({moved},");
        let import_prefix = format!("import({moved},");
        assert!(calls.contains(&format!("remove({moved},0)")), "{calls:?}");
        assert!(!calls.iter().any(|c| c.starts_with(export_prefix.as_str())));
        assert!(!calls.iter().any(|c| c.starts_with(import_prefix.as_str())));
        assert_eq!(shards.holder(&moved), vec![1]);
    }

    #[tokio::test]
    async fn list_failure_moves_nothing() {
        let shards = Shards {
            fail_list: true,
            ..Shards::new(hundred_on_shard_zero())
        };

        let result = drive_queue_move(&shards, 1, 2).await;

        assert!(matches!(result, Err(MoveError::List(_))));
        assert!(shards.calls().is_empty());
    }

    #[tokio::test]
    async fn raise_by_two_lists_every_old_and_new_shard() {
        let old = shard_map(2).unwrap();
        let mut by_shard: Vec<Vec<String>> = vec![Vec::new(), Vec::new()];
        for i in 0..100 {
            let q = format!("q{i}");
            by_shard[old.route_key("queue", &q).shard as usize].push(q);
        }
        let shards = Shards::new(by_shard);

        let report = drive_queue_move(&shards, 2, 4).await.unwrap();

        assert!(report.failed.is_empty());
        let four = shard_map(4).unwrap();
        for i in 0..100 {
            let q = format!("q{i}");
            let want = four.route_key("queue", &q).shard;
            assert_eq!(shards.holder(&q), vec![want], "{q}");
        }
    }

    /// Answers `GET /v1/queues`: the first page is `a` and `b`, the page after `b` is `c`.
    async fn list_page(
        State(server): State<Server>,
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> (StatusCode, String) {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        server.seen.lock().unwrap().push(Call {
            method: "GET".to_string(),
            path: "/v1/queues".to_string(),
            auth,
            body: json!({
                "page_size": query.get("page_size"),
                "page_token": query.get("page_token"),
            }),
        });
        if !server.status.is_success() {
            return (server.status, server.body.clone());
        }
        let body = match query.get("page_token").map(String::as_str) {
            None => r#"{"queues":[{"queue":"a"},{"queue":"b"}],"next_page_token":"b"}"#,
            Some("b") => r#"{"queues":[{"queue":"c"}]}"#,
            Some(_) => r#"{"queues":[]}"#,
        };
        (server.status, body.to_string())
    }

    async fn spawn_list(status: u16, body: &str) -> (String, Seen) {
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/v1/queues", get(list_page))
            .with_state(Server {
                seen: seen.clone(),
                status: StatusCode::from_u16(status).unwrap(),
                body: body.to_string(),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), seen)
    }

    #[tokio::test]
    async fn http_list_queues_follows_the_page_token() {
        let (url, seen) = spawn_list(200, "").await;

        let result = actuator(url, "tok").list_queues(0).await;

        assert_eq!(
            result,
            Ok(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            seen[0].body,
            json!({"page_size": "1000", "page_token": null})
        );
        assert_eq!(
            seen[1].body,
            json!({"page_size": "1000", "page_token": "b"})
        );
        assert!(seen.iter().all(|c| c.auth.as_deref() == Some("Bearer tok")));
    }

    #[tokio::test]
    async fn http_list_queues_failure_status_is_an_error() {
        let (url, _seen) = spawn_list(500, "boom").await;

        let result = actuator(url, "tok").list_queues(0).await;

        assert!(matches!(result, Err(MoveError::List(_))));
    }

    #[test]
    fn queue_move_step_records_a_raise() {
        let decision = plan_queue_move_step(&QueueMoveInput {
            recorded: None,
            live_shards: 1,
            desired_shards: 2,
            settled_shards: &[],
            replicas_per_shard: 3,
            live_replicas: 3,
            ready_replicas: 3,
        });
        assert_eq!(
            decision,
            QueueMoveDecision::Record(status(1, 2, MOVE_PENDING))
        );
    }

    #[test]
    fn queue_move_step_does_nothing_without_a_raise() {
        let done = status(1, 2, MOVE_DONE);
        let base = QueueMoveInput {
            recorded: None,
            live_shards: 2,
            desired_shards: 2,
            settled_shards: &[3, 3],
            replicas_per_shard: 3,
            live_replicas: 6,
            ready_replicas: 6,
        };
        assert_eq!(plan_queue_move_step(&base), QueueMoveDecision::Nothing);
        let after_done = QueueMoveInput {
            recorded: Some(&done),
            ..base
        };
        assert_eq!(
            plan_queue_move_step(&after_done),
            QueueMoveDecision::Nothing
        );
    }

    #[test]
    fn queue_move_step_waits_until_every_shard_is_settled() {
        let pending = status(1, 2, MOVE_PENDING);
        let input = |settled: &'static [u32], ready: u32| QueueMoveInput {
            recorded: Some(&pending),
            live_shards: 2,
            desired_shards: 2,
            settled_shards: settled,
            replicas_per_shard: 3,
            live_replicas: 6,
            ready_replicas: ready,
        };
        assert_eq!(
            plan_queue_move_step(&input(&[3, 1], 6)),
            QueueMoveDecision::Nothing
        );
        assert_eq!(
            plan_queue_move_step(&input(&[3, 3], 5)),
            QueueMoveDecision::Nothing
        );
        assert_eq!(
            plan_queue_move_step(&input(&[3, 3], 6)),
            QueueMoveDecision::Run {
                from_shards: 1,
                to_shards: 2
            }
        );
    }

    #[test]
    fn queue_move_step_resumes_a_moving_record_and_starts_a_new_raise_after_done() {
        let moving = status(1, 2, MOVE_MOVING);
        let resumed = QueueMoveInput {
            recorded: Some(&moving),
            live_shards: 2,
            desired_shards: 2,
            settled_shards: &[3, 3],
            replicas_per_shard: 3,
            live_replicas: 6,
            ready_replicas: 6,
        };
        assert_eq!(
            plan_queue_move_step(&resumed),
            QueueMoveDecision::Run {
                from_shards: 1,
                to_shards: 2
            }
        );

        let done = status(1, 2, MOVE_DONE);
        let next_raise = QueueMoveInput {
            recorded: Some(&done),
            live_shards: 2,
            desired_shards: 3,
            settled_shards: &[3, 3],
            replicas_per_shard: 3,
            live_replicas: 6,
            ready_replicas: 6,
        };
        assert_eq!(
            plan_queue_move_step(&next_raise),
            QueueMoveDecision::Record(status(2, 3, MOVE_PENDING))
        );
    }
}

// HANDWRITE-END
