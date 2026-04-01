//! Multi-service end-to-end — Postgres + two `traefik/whoami` with `depends_on`.
//!
//! Proves partitioned `DATABASE_URL` (same env var name, distinct DB path per service),
//! inter-service `UPSTREAM_URL` injection, startup order (upstream healthy before downstream),
//! and HTTP 200 + env echo on both `service_url()` endpoints.
//!
//! ```text
//! cargo test -p shakedown --features postgres,services --test services_multi_postgres_whoami -- --ignored
//! ```

use std::time::Duration;

use reqwest::StatusCode;
use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, HealthCheck, PostgresResource,
    ServiceUnderTest, HOST_DOCKER_INTERNAL,
};

fn database_url_assignment_line(body: &str) -> Option<&str> {
    body.lines()
        .map(str::trim)
        .find(|line| line.starts_with("DATABASE_URL="))
}

fn database_name_from_postgres_url_line(line: &str) -> Option<&str> {
    let value = line.strip_prefix("DATABASE_URL=")?;
    let without_query = value.split('?').next()?;
    without_query.rsplit('/').next()
}

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features postgres,services --test services_multi_postgres_whoami -- --ignored"]
async fn postgres_partitioned_database_urls_depends_on_and_env_echo() {
    let upstream_spec = ServiceUnderTest::docker("traefik/whoami:v1.10")
        .name("upstream")
        .port(80)
        .health_check(HealthCheck::http("/"))
        .build()
        .expect("upstream spec");

    let downstream_spec = ServiceUnderTest::docker("traefik/whoami:v1.10")
        .name("downstream")
        .port(80)
        .depends_on("upstream")
        .health_check(HealthCheck::http("/"))
        .build()
        .expect("downstream spec");

    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Postgres(
        PostgresResource::new().with_image_tag_from_env(),
    ));
    context
        .register_service(upstream_spec)
        .expect("register upstream");
    context
        .register_service(downstream_spec)
        .expect("register downstream");

    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "postgres_multi_service_partitioned_db_depends_on_whoami",
        |scenario_context| {
            Box::pin(async move {
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()
                    .expect("reqwest client");

                let upstream_base = scenario_context
                    .service_url("upstream")
                    .expect("upstream URL after readiness");
                let upstream_root = client
                    .get(&upstream_base)
                    .send()
                    .await
                    .expect("GET upstream /");
                assert_eq!(upstream_root.status(), StatusCode::OK);

                let upstream_env = client
                    .get(format!("{upstream_base}/?env=true"))
                    .send()
                    .await
                    .expect("GET upstream env");
                assert_eq!(upstream_env.status(), StatusCode::OK);
                let upstream_body = upstream_env.text().await.expect("upstream body");

                let downstream_base = scenario_context
                    .service_url("downstream")
                    .expect("downstream URL after readiness");
                let downstream_root = client
                    .get(&downstream_base)
                    .send()
                    .await
                    .expect("GET downstream /");
                assert_eq!(downstream_root.status(), StatusCode::OK);

                let downstream_env = client
                    .get(format!("{downstream_base}/?env=true"))
                    .send()
                    .await
                    .expect("GET downstream env");
                assert_eq!(downstream_env.status(), StatusCode::OK);
                let downstream_body = downstream_env.text().await.expect("downstream body");

                let upstream_line = database_url_assignment_line(&upstream_body).unwrap_or_else(|| {
                    panic!("upstream: expected DATABASE_URL in whoami body: {upstream_body}")
                });
                let downstream_line =
                    database_url_assignment_line(&downstream_body).unwrap_or_else(|| {
                        panic!("downstream: expected DATABASE_URL in whoami body: {downstream_body}")
                    });

                assert_ne!(
                    upstream_line, downstream_line,
                    "partitioned DATABASE_URL must differ per service (same var name, different values)"
                );

                for (label, line, expected_suffix) in [
                    ("upstream", upstream_line, "_upstream"),
                    ("downstream", downstream_line, "_downstream"),
                ] {
                    assert!(
                        line.contains(HOST_DOCKER_INTERNAL),
                        "{label}: DATABASE_URL should use {HOST_DOCKER_INTERNAL}: {line}"
                    );
                    let database_name = database_name_from_postgres_url_line(line).unwrap_or_else(|| {
                        panic!("{label}: could not parse database segment from {line:?}")
                    });
                    assert!(
                        database_name.ends_with(expected_suffix),
                        "{label}: expected database name ending with {expected_suffix}, got {database_name} (line: {line})"
                    );
                }

                assert!(
                    downstream_body.contains("UPSTREAM_URL=") && downstream_body.contains(HOST_DOCKER_INTERNAL),
                    "downstream should receive inter-service URL for depends_on target: {downstream_body}"
                );
                assert!(
                    !upstream_body.contains("DOWNSTREAM_URL="),
                    "upstream must not receive dependent service URL via convention: {upstream_body}"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");

    let healthy_messages: Vec<&str> = event_log
        .events()
        .map(|event| event.message.as_str())
        .filter(|message| message.contains("is healthy"))
        .collect();

    let upstream_healthy_at = healthy_messages
        .iter()
        .position(|message| message.contains("service 'upstream'"));
    let downstream_healthy_at = healthy_messages
        .iter()
        .position(|message| message.contains("service 'downstream'"));
    assert!(
        upstream_healthy_at.is_some() && downstream_healthy_at.is_some(),
        "expected health log lines, got: {healthy_messages:?}"
    );
    assert!(
        upstream_healthy_at.unwrap() < downstream_healthy_at.unwrap(),
        "upstream must become healthy before downstream; order: {healthy_messages:?}"
    );
}
