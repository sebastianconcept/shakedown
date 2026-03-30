//! Service-under-test Docker lifecycle (`services` feature, US-009).
//!
//! ```text
//! cargo test -p shakedown --features services --test services_lifecycle -- --ignored
//! ```

use shakedown::{run_scenario, Context, EventLog, HealthCheck, ServiceUnderTest};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features services --test services_lifecycle -- --ignored"]
async fn traefik_whoami_single_service_http_health_and_teardown() {
    let specification = ServiceUnderTest::docker("traefik/whoami:v1.10")
        .name("web")
        .port(80)
        .health_check(HealthCheck::http("/"))
        .auto_inject(false)
        .build()
        .expect("valid spec");

    let mut context = Context::new();
    context.register_service(specification).expect("register");
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "services_lifecycle_whoami",
        |scenario_context| {
            Box::pin(async move {
                let base_url = scenario_context
                    .service_url("web")
                    .expect("service URL after readiness");
                assert!(
                    base_url.starts_with("http://127.0.0.1:"),
                    "scenario uses loopback host mapping: {base_url}"
                );

                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .expect("client");
                let response = client.get(base_url).send().await.expect("GET whoami");
                assert_eq!(response.status(), reqwest::StatusCode::OK);

                let handle = scenario_context
                    .service_handle("web")
                    .expect("service handle");
                assert!(handle.is_healthy());
                assert!(handle.container_id().is_some());
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}
