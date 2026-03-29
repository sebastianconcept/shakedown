//! HTTP bridge server + [`run_scenario`] integration (`bridges` feature).
//!
//! Reference scenarios for registering bridges, issuing real HTTP requests against localhost
//! stubs, and asserting on [`BridgeHandle`] recordings. No Docker — only `127.0.0.1` listeners.

use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;

use shakedown::{
    run_scenario, BridgeContext, BridgeRequest, BridgeResponse, Context, EventLog, HttpBridge,
    StubBridge,
};

/// **Given** a stateful counter. **When** each request hits any path, **then** return JSON
/// `{"n": <incrementing count>}` so callers can observe ordering via recorded response bodies.
struct CountingJsonBridge {
    hits: AtomicU32,
}

#[async_trait]
impl HttpBridge for CountingJsonBridge {
    async fn handle(&self, _request: BridgeRequest, _context: &BridgeContext) -> BridgeResponse {
        let next = self.hits.fetch_add(1, Ordering::SeqCst) + 1;
        BridgeResponse::json(&serde_json::json!({ "n": next })).expect("json body")
    }
}

#[tokio::test]
async fn stub_bridge_multiple_routes_register_request_assert() {
    // **Given** a [`StubBridge`] with several routes (plain status, JSON POST, glob GET) and a
    // default 404 for unknown paths.
    let stub = StubBridge::new("auth-api")
        .on("GET", "/health")
        .respond(200)
        .on("POST", "/v1/validate")
        .respond_json(&serde_json::json!({ "valid": true }))
        .expect("stub json route")
        .on("GET", "/v1/users/*/profile")
        .respond_json(&serde_json::json!({ "name": "test" }))
        .expect("stub json route")
        .default_response(404);

    let mut ctx = Context::new();
    ctx.register_http_bridge("auth-api", stub)
        .expect("register_http_bridge");

    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "stub_bridge_multiple_routes_register_request_assert",
        |context| {
            Box::pin(async move {
                let client = reqwest::Client::new();
                let base = context.bridge_url("auth-api");

                // **When** the scenario exercises multiple endpoints (and leaves one path unused).
                let health = client
                    .get(format!("{base}/health"))
                    .send()
                    .await
                    .expect("GET /health");
                assert_eq!(health.status(), 200);

                let validate = client
                    .post(format!("{base}/v1/validate"))
                    .json(&serde_json::json!({ "email": "user@example.com" }))
                    .send()
                    .await
                    .expect("POST /v1/validate");
                assert_eq!(validate.status(), 200);

                let profile = client
                    .get(format!("{base}/v1/users/42/profile"))
                    .send()
                    .await
                    .expect("GET profile");
                assert_eq!(profile.status(), 200);

                let handle = context.bridge("auth-api");

                // **Then** total traffic, a POST body predicate, and absence of an unused route
                // are all assertable.
                handle
                    .assert_called_times(3)
                    .expect("three inbound requests");
                handle
                    .assert_called_with("POST", "/v1/validate", |call| {
                        match call.json::<serde_json::Value>() {
                            Ok(value) => {
                                value.get("email").and_then(|email| email.as_str())
                                    == Some("user@example.com")
                            }
                            Err(_) => false,
                        }
                    })
                    .expect("POST body matches");
                handle
                    .assert_path_not_called("GET", "/admin")
                    .expect("admin was not called");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");

    let messages: Vec<String> = event_log
        .events()
        .map(|event| event.message.clone())
        .collect();
    assert!(
        messages
            .iter()
            .any(|message| message.contains("bridge 'auth-api' started on")),
        "start event missing: {messages:?}"
    );
    assert!(
        messages.iter().any(|message| {
            message.contains("bridge 'auth-api' stopped") && message.contains("calls recorded")
        }),
        "stop event missing: {messages:?}"
    );
}

#[tokio::test]
async fn coded_http_bridge_stateful_calls_matching_order() {
    // **Given** a coded [`HttpBridge`] that returns a different JSON body on each call.
    let mut ctx = Context::new();
    ctx.register_http_bridge(
        "counter",
        CountingJsonBridge {
            hits: AtomicU32::new(0),
        },
    )
    .expect("register_http_bridge");

    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "coded_http_bridge_stateful_calls_matching_order",
        |context| {
            Box::pin(async move {
                let client = reqwest::Client::new();
                let base = context.bridge_url("counter");

                // **When** two GETs hit the same path, the handler advances internal state.
                let first = client
                    .get(format!("{base}/"))
                    .send()
                    .await
                    .expect("first GET");
                assert_eq!(first.status(), 200);
                let second = client
                    .get(format!("{base}/"))
                    .send()
                    .await
                    .expect("second GET");
                assert_eq!(second.status(), 200);

                let handle = context.bridge("counter");
                let matched = handle.calls_matching("GET", "/");
                assert_eq!(matched.len(), 2, "both GETs recorded for path /");

                // **Then** [`BridgeHandle::calls_matching`] preserves arrival order for inspection.
                let first_n: u64 = serde_json::from_slice(matched[0].response_body().as_ref())
                    .map(|value: serde_json::Value| {
                        value
                            .get("n")
                            .and_then(|value| value.as_u64())
                            .expect("n field")
                    })
                    .expect("first response json");
                let second_n: u64 = serde_json::from_slice(matched[1].response_body().as_ref())
                    .map(|value: serde_json::Value| {
                        value
                            .get("n")
                            .and_then(|value| value.as_u64())
                            .expect("n field")
                    })
                    .expect("second response json");
                assert_eq!(first_n, 1);
                assert_eq!(second_n, 2);
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}
