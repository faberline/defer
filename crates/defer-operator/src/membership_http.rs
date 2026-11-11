// HANDWRITE-BEGIN gap="missing-generator:logic:defer-membership-http" tracker="#87" reason="HTTP membership actuator for POST /v1/admin/membership on the leader."
//! Membership actuator that calls `POST /v1/admin/membership` on the leader.

use crate::membership_driver::{ActuatorError, MembershipActuator};
use serde_json::{json, Value};

/// Sends each membership step to the leader's admin route with the
/// operator's bearer token.
pub struct HttpMembershipActuator {
    client: reqwest::Client,
    base_url: String,
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
        Self {
            client,
            base_url: base_url.into(),
            token: token.into(),
            peer_url: Box::new(peer_url),
        }
    }

    async fn post(&self, body: Value) -> Result<(), ActuatorError> {
        let response = self
            .client
            .post(format!("{}/v1/admin/membership", self.base_url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|error| ActuatorError::Hard(error.to_string()))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let text = response.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::CONFLICT && text.contains("retry_later") {
            return Err(ActuatorError::NotCaughtUp);
        }
        Err(ActuatorError::Hard(format!("{status}: {text}")))
    }
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
        http::{HeaderMap, StatusCode},
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
            "t0k",
            |n| format!("http://defer-{n}.peers:9000"),
        );
        (actuator, seen)
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

        assert_eq!(result, Err(ActuatorError::NotCaughtUp));
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
}

// HANDWRITE-END
