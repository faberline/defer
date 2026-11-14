// HANDWRITE-BEGIN gap="missing-generator:logic:defer-membership-http" tracker="#87" reason="HTTP membership actuator for POST /v1/admin/membership on the leader."
//! Membership actuator that calls `POST /v1/admin/membership` on the leader.

use crate::membership_driver::{ActuatorError, MembershipActuator};
use serde_json::{json, Value};

/// Sends each membership step to the leader's admin route with the
/// operator's bearer token.
pub struct HttpMembershipActuator {
    client: reqwest::Client,
    /// Base URL (no trailing slash) of the pod with the given ordinal.
    base_url_for: Box<dyn Fn(u32) -> String + Send + Sync>,
    /// Number of pods; ordinals `0..ordinals` are asked. `0` means no
    /// discovery: one fixed target.
    ordinals: u32,
    /// Last ordinal that answered as the leader; `None` until one does.
    leader: std::sync::Mutex<Option<u32>>,
    /// Bearer token. An empty token sends no `Authorization` header (the
    /// server runs with auth off).
    token: String,
    peer_url: Box<dyn Fn(u32) -> String + Send + Sync>,
}

impl HttpMembershipActuator {
    /// `base_url` is the leader without a trailing slash. `peer_url` maps a
    /// StatefulSet ordinal to the peer URL of that replica.
    pub fn new(
        client: reqwest::Client,
        base_url: impl Into<String>,
        token: impl Into<String>,
        peer_url: impl Fn(u32) -> String + Send + Sync + 'static,
    ) -> Self {
        let base_url: String = base_url.into();
        Self {
            client,
            base_url_for: Box::new(move |_| base_url.clone()),
            ordinals: 0,
            leader: std::sync::Mutex::new(None),
            token: token.into(),
            peer_url: Box::new(peer_url),
        }
    }

    /// `base_url_for(ordinal)` is the base URL of pod `ordinal`, without a
    /// trailing slash. `ordinals` is the number of pods; ordinals
    /// `0..ordinals` are asked for the leader. A `409` with `not_leader`
    /// is followed to the leader named in the `x-defer-leader` header.
    /// When no pod accepts, it returns `ActuatorError::NotLeader`.
    pub fn with_leader_discovery(
        client: reqwest::Client,
        base_url_for: impl Fn(u32) -> String + Send + Sync + 'static,
        ordinals: u32,
        token: impl Into<String>,
        peer_url: impl Fn(u32) -> String + Send + Sync + 'static,
    ) -> Self {
        Self {
            client,
            base_url_for: Box::new(base_url_for),
            ordinals,
            leader: std::sync::Mutex::new(None),
            token: token.into(),
            peer_url: Box::new(peer_url),
        }
    }

    async fn post(&self, body: Value) -> Result<(), ActuatorError> {
        let tries = if self.ordinals == 0 {
            1
        } else {
            self.ordinals + 1
        };
        let mut ordinal = self.leader.lock().unwrap().unwrap_or(0);
        let mut last = String::new();
        for _ in 0..tries {
            let mut request = self
                .client
                .post(format!(
                    "{}/v1/admin/membership",
                    (self.base_url_for)(ordinal)
                ))
                .json(&body);
            if !self.token.is_empty() {
                request = request.bearer_auth(&self.token);
            }
            let response = request
                .send()
                .await
                .map_err(|error| ActuatorError::Hard(error.to_string()))?;
            let status = response.status();
            let hint = response
                .headers()
                .get("x-defer-leader")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let text = response.text().await.unwrap_or_default();
            if status.is_success() {
                *self.leader.lock().unwrap() = Some(ordinal);
                return Ok(());
            }
            if status == reqwest::StatusCode::CONFLICT && text.contains("retry_later") {
                *self.leader.lock().unwrap() = Some(ordinal);
                return Err(ActuatorError::NotCaughtUp(text));
            }
            if status == reqwest::StatusCode::CONFLICT
                && text.contains("not_leader")
                && self.ordinals > 0
            {
                last = format!("{status}: {text}");
                ordinal = match leader_from_hint(hint.as_deref()) {
                    Some(hinted) if hinted < self.ordinals => hinted,
                    _ => (ordinal + 1) % self.ordinals,
                };
                continue;
            }
            return Err(ActuatorError::Hard(format!("{status}: {text}")));
        }
        Err(ActuatorError::NotLeader(last))
    }
}

/// Parses the `x-defer-leader` header: the leader's node id, which equals
/// its pod ordinal.
fn leader_from_hint(header: Option<&str>) -> Option<u32> {
    header?.trim().parse().ok()
}

impl MembershipActuator for HttpMembershipActuator {
    async fn add_learner(&self, ordinal: u32) -> Result<(), ActuatorError> {
        let url = (self.peer_url)(ordinal);
        self.post(json!({"op": "add_learner", "node": u64::from(ordinal), "url": url}))
            .await
    }

    // The route has no catch-up probe. The `retry_later` answer of `promote`
    // is the catch-up check, and `run_steps` retries `Promote`.
    async fn wait_catch_up(&self, _ordinal: u32) -> Result<(), ActuatorError> {
        Ok(())
    }

    async fn promote(&self, ordinal: u32) -> Result<(), ActuatorError> {
        self.post(json!({"op": "promote", "node": u64::from(ordinal)}))
            .await
    }

    async fn demote(&self, ordinal: u32) -> Result<(), ActuatorError> {
        self.post(json!({"op": "demote", "node": u64::from(ordinal)}))
            .await
    }

    // The server has no `remove` op yet; follow-up #98 adds it. Until then
    // the server answers 4xx and this returns `Hard`.
    async fn remove(&self, ordinal: u32) -> Result<(), ActuatorError> {
        self.post(json!({"op": "remove", "node": u64::from(ordinal)}))
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::membership_driver::{ActuatorError, MembershipActuator};
    use crate::membership_http::HttpMembershipActuator;
    use axum::{
        extract::State,
        http::{HeaderMap, HeaderValue, StatusCode},
        routing::post,
        Router,
    };
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};

    type Seen = Arc<Mutex<Vec<(Option<String>, Value)>>>;

    #[derive(Clone)]
    struct Server {
        seen: Seen,
        status: StatusCode,
        body: String,
    }

    async fn handle(
        State(server): State<Server>,
        headers: HeaderMap,
        body: String,
    ) -> (StatusCode, String) {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let json: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
        server.seen.lock().unwrap().push((auth, json));
        (server.status, server.body.clone())
    }

    async fn spawn(status: u16, body: &str) -> (HttpMembershipActuator, Seen) {
        spawn_with_token(status, body, "t0k").await
    }

    async fn spawn_with_token(
        status: u16,
        body: &str,
        token: &str,
    ) -> (HttpMembershipActuator, Seen) {
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/v1/admin/membership", post(handle))
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
        let actuator = HttpMembershipActuator::new(
            reqwest::Client::new(),
            format!("http://{addr}"),
            token,
            |n| format!("http://defer-{n}.peers:9000"),
        );
        (actuator, seen)
    }

    #[tokio::test]
    async fn empty_token_sends_no_authorization_header() {
        let (actuator, seen) = spawn_with_token(200, "{}", "").await;

        let result = actuator.promote(2).await;

        assert_eq!(result, Ok(()));
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, None);
    }

    #[tokio::test]
    async fn token_sends_bearer_header() {
        let (actuator, seen) = spawn_with_token(200, "{}", "t0k").await;

        let result = actuator.promote(2).await;

        assert_eq!(result, Ok(()));
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0.as_deref(), Some("Bearer t0k"));
    }

    #[tokio::test]
    async fn add_learner_posts_op_node_and_url() {
        let (actuator, seen) = spawn(200, "{}").await;

        let result = actuator.add_learner(2).await;

        assert_eq!(result, Ok(()));
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0.as_deref(), Some("Bearer t0k"));
        assert_eq!(
            seen[0].1,
            json!({"op": "add_learner", "node": 2, "url": "http://defer-2.peers:9000"})
        );
    }

    #[tokio::test]
    async fn promote_demote_remove_post_their_op() {
        let (actuator, seen) = spawn(200, "{}").await;

        assert_eq!(actuator.promote(2).await, Ok(()));
        assert_eq!(actuator.demote(2).await, Ok(()));
        assert_eq!(actuator.remove(2).await, Ok(()));

        let bodies: Vec<Value> = seen
            .lock()
            .unwrap()
            .iter()
            .map(|(_, b)| b.clone())
            .collect();
        assert_eq!(
            bodies,
            vec![
                json!({"op": "promote", "node": 2}),
                json!({"op": "demote", "node": 2}),
                json!({"op": "remove", "node": 2}),
            ]
        );
    }

    #[tokio::test]
    async fn wait_catch_up_sends_no_request() {
        let (actuator, seen) = spawn(200, "{}").await;

        let result = actuator.wait_catch_up(2).await;

        assert_eq!(result, Ok(()));
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn conflict_retry_later_is_not_caught_up() {
        let (actuator, _seen) = spawn(409, r#"{"error":"retry_later"}"#).await;

        let result = actuator.promote(2).await;

        assert_eq!(
            result,
            Err(ActuatorError::NotCaughtUp(
                r#"{"error":"retry_later"}"#.to_string()
            ))
        );
    }

    #[tokio::test]
    async fn not_caught_up_error_carries_the_response_body() {
        let body = r#"{"error":{"kind":"retry_later","message":"NotCaughtUp { lag: 12 }"}}"#;
        let (actuator, _seen) = spawn(409, body).await;

        let result = actuator.promote(2).await;

        assert_eq!(result, Err(ActuatorError::NotCaughtUp(body.to_string())));
    }

    #[tokio::test]
    async fn other_errors_are_hard() {
        let (conflict, _seen) = spawn(409, r#"{"error":"not_leader"}"#).await;
        assert!(matches!(
            conflict.promote(2).await,
            Err(ActuatorError::Hard(_))
        ));

        let (unprocessable, _seen) = spawn(422, r#"{"error":"bad_request"}"#).await;
        assert!(matches!(
            unprocessable.promote(2).await,
            Err(ActuatorError::Hard(_))
        ));
    }

    #[derive(Clone)]
    struct Pod {
        seen: Seen,
        status: StatusCode,
        body: String,
        leader: Option<String>,
    }

    async fn handle_pod(
        State(pod): State<Pod>,
        headers: HeaderMap,
        body: String,
    ) -> (StatusCode, HeaderMap, String) {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let json: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
        pod.seen.lock().unwrap().push((auth, json));
        let mut out = HeaderMap::new();
        if let Some(leader) = &pod.leader {
            out.insert("x-defer-leader", HeaderValue::from_str(leader).unwrap());
        }
        (pod.status, out, pod.body.clone())
    }

    async fn spawn_pod(status: u16, body: &str, leader: Option<&str>) -> (String, Seen) {
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/v1/admin/membership", post(handle_pod))
            .with_state(Pod {
                seen: seen.clone(),
                status: StatusCode::from_u16(status).unwrap(),
                body: body.to_string(),
                leader: leader.map(str::to_string),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), seen)
    }

    fn discover(urls: Vec<String>) -> HttpMembershipActuator {
        let pods = urls.len() as u32;
        HttpMembershipActuator::with_leader_discovery(
            reqwest::Client::new(),
            move |n| urls[n as usize].clone(),
            pods,
            "t0k",
            |n| format!("http://defer-{n}.peers:9000"),
        )
    }

    #[tokio::test]
    async fn follows_the_leader_hint() {
        let (pod0, seen0) = spawn_pod(409, r#"{"error":"not_leader"}"#, Some("2")).await;
        let (pod1, seen1) = spawn_pod(500, "{}", None).await;
        let (pod2, seen2) = spawn_pod(200, "{}", None).await;
        let actuator = discover(vec![pod0, pod1, pod2]);

        assert_eq!(actuator.promote(3).await, Ok(()));

        assert_eq!(seen0.lock().unwrap().len(), 1);
        assert!(seen1.lock().unwrap().is_empty());
        {
            let seen = seen2.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(seen[0].0.as_deref(), Some("Bearer t0k"));
            assert_eq!(seen[0].1, json!({"op": "promote", "node": 3}));
        }

        // The leader is remembered: the next call starts at pod 2.
        assert_eq!(actuator.promote(3).await, Ok(()));
        assert_eq!(seen2.lock().unwrap().len(), 2);
        assert_eq!(seen0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tries_next_ordinal_without_hint() {
        let (pod0, seen0) = spawn_pod(409, r#"{"error":"not_leader"}"#, None).await;
        let (pod1, seen1) = spawn_pod(200, "{}", None).await;
        let actuator = discover(vec![pod0, pod1]);

        assert_eq!(actuator.demote(2).await, Ok(()));

        assert_eq!(seen0.lock().unwrap().len(), 1);
        let seen = seen1.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].1, json!({"op": "demote", "node": 2}));
    }

    #[tokio::test]
    async fn invalid_hint_falls_back_to_next_ordinal() {
        let (pod0, seen0) = spawn_pod(409, r#"{"error":"not_leader"}"#, Some("abc")).await;
        let (pod1, seen1) = spawn_pod(200, "{}", None).await;
        let actuator = discover(vec![pod0, pod1]);

        assert_eq!(actuator.promote(2).await, Ok(()));

        assert_eq!(seen0.lock().unwrap().len(), 1);
        assert_eq!(seen1.lock().unwrap().len(), 1);
    }

    // Negative test: without the loop limit this never returns.
    #[tokio::test]
    async fn gives_up_after_all_ordinals() {
        let (pod0, seen0) = spawn_pod(409, r#"{"error":"not_leader"}"#, None).await;
        let (pod1, seen1) = spawn_pod(409, r#"{"error":"not_leader"}"#, None).await;
        let (pod2, seen2) = spawn_pod(409, r#"{"error":"not_leader"}"#, None).await;
        let actuator = discover(vec![pod0, pod1, pod2]);

        match actuator.promote(3).await {
            Err(ActuatorError::NotLeader(text)) => assert!(text.contains("not_leader")),
            other => panic!("expected NotLeader, got {other:?}"),
        }

        let total =
            seen0.lock().unwrap().len() + seen1.lock().unwrap().len() + seen2.lock().unwrap().len();
        assert_eq!(total, 4);
    }

    #[tokio::test]
    async fn retry_later_is_still_not_caught_up() {
        let (pod0, seen0) = spawn_pod(409, r#"{"error":"retry_later"}"#, None).await;
        let actuator = discover(vec![pod0]);

        assert_eq!(
            actuator.promote(2).await,
            Err(ActuatorError::NotCaughtUp(
                r#"{"error":"retry_later"}"#.to_string()
            ))
        );

        assert_eq!(seen0.lock().unwrap().len(), 1);
    }
}

// HANDWRITE-END
