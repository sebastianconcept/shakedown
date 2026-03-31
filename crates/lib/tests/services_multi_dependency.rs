//! Multi-service `depends_on` ordering and inter-service env injection (`services` feature).
//!
//! ```text
//! cargo test -p shakedown --features services --test services_multi_dependency -- --ignored
//! ```

use shakedown::{run_scenario, Context, EventLog, HealthCheck, ServiceUnderTest};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features services --test services_multi_dependency -- --ignored"]
async fn two_whoami_services_dependency_order_and_upstream_url_in_env() {
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
    context
        .register_service(upstream_spec)
        .expect("register upstream");
    context
        .register_service(downstream_spec)
        .expect("register downstream");

    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "services_multi_dependency_whoami",
        |scenario_context| {
            Box::pin(async move {
                let downstream_base = scenario_context
                    .service_url("downstream")
                    .expect("downstream URL after readiness");
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .expect("client");

                let response = client
                    .get(format!("{downstream_base}/?env=true"))
                    .send()
                    .await
                    .expect("GET downstream env echo");
                assert_eq!(response.status(), reqwest::StatusCode::OK);
                let body = response.text().await.expect("body");
                assert!(
                    body.contains("UPSTREAM_URL="),
                    "expected UPSTREAM_URL in whoami env listing, got: {body}"
                );
                assert!(
                    body.contains("host.docker.internal"),
                    "service container should see rewritten host for upstream: {body}"
                );

                let upstream_base = scenario_context
                    .service_url("upstream")
                    .expect("upstream URL");
                let upstream_response = client
                    .get(format!("{upstream_base}/?env=true"))
                    .send()
                    .await
                    .expect("GET upstream");
                assert_eq!(upstream_response.status(), reqwest::StatusCode::OK);
                let upstream_body = upstream_response.text().await.expect("upstream body");
                assert!(
                    !upstream_body.contains("DOWNSTREAM_URL="),
                    "upstream should not receive dependent URL via convention: {upstream_body}"
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
