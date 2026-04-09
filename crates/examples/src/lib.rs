//! Shared steps for the **service + mock + Postgres + Kafka** adoption walkthrough.
//!
//! The canonical macro-driven entry point is the integration test `tests/service_with_mock.rs`.
//! `examples/service_with_mock.rs` runs the same phases via [`shakedown::run_scenario`] for
//! `cargo check --example` / `cargo run --example` workflows.

use std::time::Duration;

use shakedown::{
    Context, EnvMapping, HealthCheck, HttpMockService, HttpMockServiceResponse, ServiceUnderTest,
    HOST_DOCKER_INTERNAL,
};

/// **Prelude** (runs after resources are registered, before the scenario body): register the
/// declarative HTTP mock and the service-under-test with custom [`EnvMapping`] entries.
pub fn register_adoption_example_prelude(ctx: &mut Context) -> shakedown::Result<()> {
    let quote_stub = HttpMockService::new("quote-api")
        .on_prefix("GET", "/v1/quote")
        .respond_with(
            HttpMockServiceResponse::builder()
                .status(200)
                .header("set-cookie", "session=demo; Path=/")
                .header("x-custom-header", "shakedown-example")
                .body_bytes(br#"{"symbol":"DEMO","price":42}"#),
        )
        .default_response(404);
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

/// **Scenario body**: HTTP assertions against the running container, then relational assertions
/// via Postgres helpers on the same scenario-scoped database the harness provisioned.
pub async fn adoption_example_body(ctx: &mut Context) {
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
    let quote_response = client
        .get(format!("{mock_base_url}/v1/quote/AAPL"))
        .send()
        .await
        .expect("GET /v1/quote/AAPL on declarative mock");
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

    let root = client.get(&base_url).send().await.expect("GET /");
    assert_eq!(root.status(), reqwest::StatusCode::OK);

    let env_url = format!("{base_url}/?env=true");
    let env_response = client.get(env_url).send().await.expect("GET /?env=true");
    assert_eq!(env_response.status(), reqwest::StatusCode::OK);
    let body = env_response.text().await.expect("whoami body");

    assert!(
        body.contains("MY_KAFKA_TOPIC=") && body.contains("QUOTE_API_BASE_URL="),
        "whoami should echo renamed bindings from EnvMapping::kafka_topic and EnvMapping::mock_service: {body}"
    );
    assert!(
        body.contains("DATABASE_URL=") && body.contains(HOST_DOCKER_INTERNAL),
        "Postgres convention injection should still reach the container (rewritten for Docker): {body}"
    );

    ctx.postgres()
        .execute(
            "CREATE TABLE adoption_demo (id SERIAL PRIMARY KEY, label TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE adoption_demo");
    ctx.postgres()
        .execute(
            "INSERT INTO adoption_demo (label) VALUES ($1)",
            &[&"integrations"],
        )
        .await
        .expect("INSERT adoption_demo");
    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM adoption_demo WHERE label = $1",
            &[&"integrations"],
        )
        .await
        .expect("row should exist after insert");

    let handle = ctx
        .service_handle("app")
        .expect("service handle after lifecycle");
    assert!(handle.is_healthy());
}
