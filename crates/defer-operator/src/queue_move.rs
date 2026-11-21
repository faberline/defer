// HANDWRITE-BEGIN gap="missing-generator:logic:defer-queue-move" tracker="#284" reason="Queue move planner and HTTP actuator for a shard split or merge."
//! Queue move planner, runner and HTTP actuator for a shard split or merge.
//!
//! A queue moves in three steps: export from the old shard, import on the new
//! shard, remove from the old shard. The old shard removes a queue only after
//! the import returned success. A failed import leaves the queue on the old
//! shard; the next reconcile retries it.

use serde_json::Value;
use shard_routing::BucketMove;

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
}

impl std::fmt::Display for MoveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Export(reason) => write!(f, "export failed: {reason}"),
            Self::Import(reason) => write!(f, "import failed: {reason}"),
            Self::Remove(reason) => write!(f, "remove failed: {reason}"),
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

#[cfg(test)]
mod tests {
    use crate::queue_move::{
        plan_queue_moves, run_queue_moves, HttpQueueMoveActuator, MoveError, MoveReport, MoveStep,
        QueueMoveActuator,
    };
    use axum::{
        extract::State,
        http::{HeaderMap, Method, StatusCode, Uri},
        routing::{get, put},
        Router,
    };
    use defer_queue::placement::QueuePlacement;
    use serde_json::{json, Value};
    use shard_routing::{BucketMove, VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT};
    use std::collections::VecDeque;
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
}

// HANDWRITE-END
