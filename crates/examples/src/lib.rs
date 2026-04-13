//! Shared steps for the **service + mock + Postgres + Kafka** adoption walkthrough.
//!
//! This crate’s `[features]` in `Cargo.toml` are **marker flags** so commands like
//! `cargo test -p shakedown-examples --features kafka,…` stay valid and match `shakedown`’s feature
//! names in docs; they do not change how the `shakedown` path dependency is built (that is fixed in
//! `Cargo.toml`).
//!
//! The canonical macro-driven entry point is the integration test `tests/service_with_mock.rs`.
//! `examples/service_with_mock.rs` runs the same phases via [`shakedown::run_scenario`] for
//! `cargo check --example` / `cargo run --example` workflows.
//!
//! **Sections in [`adoption_example_body`]:** HTTP client checks against the declarative mock and
//! `traefik/whoami`, env echo proving [`EnvMapping`] (including namespaced Kafka topic names), a
//! Kafka `produce_json` → `consume_one` round-trip on the logical `events` topic, then Postgres
//! helpers on the scenario database and a final service health check.

use std::time::Duration;

use serde_json::json;
use shakedown::{
    Context, EnvMapping, HealthCheck, HttpMockService, HttpMockServiceResponse, ServiceUnderTest,
    StatusCode, HOST_DOCKER_INTERNAL,
};

/// **Prelude** (runs after resources are registered, before the scenario body): register the
/// declarative HTTP mock and the service-under-test with custom [`EnvMapping`] entries.
pub fn register_adoption_example_prelude(ctx: &mut Context) -> shakedown::Result<()> {
    let quote_stub = HttpMockService::new("quote-api")
        .on_prefix("GET", "/v1/quote")
        .respond_with(
            HttpMockServiceResponse::builder()
                .status(StatusCode::OK)
                .header("set-cookie", "session=demo; Path=/")
                .header("x-custom-header", "shakedown-example")
                .body_bytes(br#"{"symbol":"DEMO","price":42}"#),
        )
        .default_response(StatusCode::NOT_FOUND.into());
    ctx.register_http_mock_service("quote-api", quote_stub)?;

    let specification = ServiceUnderTest::docker("traefik/whoami:v1.10")
        .name("app")
        .port(80)
        .health_check(HealthCheck::http("/"))
        .env_mapping(EnvMapping::kafka_topic("MY_KAFKA_TOPIC", "events"))
        .env_mapping(EnvMapping::mock_service("quote-api", "QUOTE_API_BASE_URL"))
        .build()?;

    ctx.register_service(specification)?;
    Ok(())
}

/// **Scenario body**: HTTP checks against the mock and service-under-test, assertions that env
/// mappings (including Kafka topic namespacing) reach the container, a Kafka JSON produce/consume
/// round-trip, then relational assertions via Postgres helpers on the harness-provisioned database.
///
/// Structured with Given / When / Then comments for the adoption walkthrough (same narrative as
/// integration tests that call this function).
pub async fn adoption_example_body(ctx: &mut Context) {
    // Given the app and mock are reachable on loopback after readiness.
    let base_url = ctx
        .service_url("app")
        .expect("service base URL after readiness; register ServiceUnderTest before run_scenario");

    assert!(
        base_url.starts_with("http://127.0.0.1:"),
        "assertions use the test host (loopback), not host.docker.internal: {base_url}"
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client");

    let mock_base_url = ctx
        .mock_service_url("quote-api")
        .expect("mock base URL after lifecycle started the stub");

    // When we hit the declarative mock for a quoted path.
    let quote_response = client
        .get(format!("{mock_base_url}/v1/quote/AAPL"))
        .send()
        .await
        .expect("GET /v1/quote/AAPL on declarative mock");
    // Then the response status and configured headers match the mock.
    assert_eq!(quote_response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        quote_response
            .headers()
            .get("x-custom-header")
            .and_then(|value| value.to_str().ok()),
        Some("shakedown-example"),
        "respond_with headers must reach the real HTTP client"
    );
    assert!(
        quote_response
            .headers()
            .get("set-cookie")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|cookie| cookie.contains("session=demo")),
        "Set-Cookie from respond_with should be visible to the client"
    );

    // When we GET the app root and the env echo endpoint.
    let root = client.get(&base_url).send().await.expect("GET /");
    assert_eq!(root.status(), reqwest::StatusCode::OK);

    let env_url = format!("{base_url}/?env=true");
    let env_response = client.get(env_url).send().await.expect("GET /?env=true");
    assert_eq!(env_response.status(), reqwest::StatusCode::OK);
    let body = env_response.text().await.expect("whoami body");

    let expected_kafka_topic = ctx
        .kafka_bindings()
        .expect("kafka bindings after provision")
        .topic_namespace("events");
    assert_ne!(
        expected_kafka_topic.as_str(),
        "events",
        "logical topic name must be scenario-namespaced for isolation"
    );

    // Then env mappings and DATABASE_URL (with host rewrite) appear in the whoami body.
    assert!(
        body.contains("QUOTE_API_BASE_URL="),
        "whoami should echo mock base URL from EnvMapping::mock_service: {body}"
    );
    let kafka_topic_from_env = body
        .split("MY_KAFKA_TOPIC=")
        .nth(1)
        .expect("MY_KAFKA_TOPIC in whoami body")
        .lines()
        .next()
        .expect("MY_KAFKA_TOPIC value")
        .trim_end_matches('\r');
    assert_eq!(
        kafka_topic_from_env, expected_kafka_topic.as_str(),
        "EnvMapping::kafka_topic should inject the namespaced topic (not the bare logical name 'events')"
    );
    assert!(
        body.contains("DATABASE_URL=") && body.contains(HOST_DOCKER_INTERNAL),
        "Postgres convention injection should still reach the container (rewritten for Docker): {body}"
    );

    // When we publish JSON to the logical "events" topic and read it back with the typed helpers.
    let kafka_payload = json!({"demo": "kafka-round-trip", "n": 1});
    ctx.kafka()
        .expect("kafka helpers")
        .produce_json("events", "test-key", &kafka_payload)
        .await
        .expect("produce_json to scenario-scoped events topic");
    let consumed = ctx
        .kafka()
        .expect("kafka helpers")
        .consume_one("events", Some(Duration::from_secs(30)))
        .await
        .expect("consume_one from events");
    let consumed_json = consumed
        .payload_json::<serde_json::Value>()
        .expect("JSON payload round-trip");
    assert_eq!(consumed_json, kafka_payload);

    // When we create a table, insert a row, and assert it exists on the scenario database.
    ctx.postgres()
        .expect("postgres helpers")
        .execute(
            "CREATE TABLE adoption_demo (id SERIAL PRIMARY KEY, label TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE adoption_demo");
    ctx.postgres()
        .expect("postgres helpers")
        .execute(
            "INSERT INTO adoption_demo (label) VALUES ($1)",
            &[&"integrations"],
        )
        .await
        .expect("INSERT adoption_demo");
    ctx.postgres()
        .expect("postgres helpers")
        .assert_row_exists(
            "SELECT 1 FROM adoption_demo WHERE label = $1",
            &[&"integrations"],
        )
        .await
        .expect("row should exist after insert");

    // Then the registered service still reports healthy.
    let handle = ctx
        .service_handle("app")
        .expect("service handle after lifecycle");
    assert!(handle.is_healthy());
}
