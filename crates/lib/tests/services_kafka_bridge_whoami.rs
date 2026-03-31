//! Single-service end-to-end scenario — Kafka + HTTP bridge env injection + whoami + diagnostics.
//!
//! Proves convention injection (`KAFKA_BOOTSTRAP_SERVERS`, `{BRIDGE}_URL`), `host.docker.internal` rewrite
//! inside the service container, HTTP health, and bollard-merged diagnostics for the service label.
//!
//! ```text
//! cargo test -p shakedown --features kafka,services --test services_kafka_bridge_whoami -- --ignored
//! ```

use std::time::Duration;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
use shakedown::diagnostics::labels::{
    correlation_name_from_container_summary, SHAKEDOWN_DOCKER_LABEL_KEY,
    SHAKEDOWN_DOCKER_LABEL_VALUE, SHAKEDOWN_SERVICE_LABEL_KEY,
};
use shakedown::event::{InfraEvent, LogEntry};
use shakedown::{
    render_failure_timeline, run_scenario, BuiltinResource, Context, EventLog,
    FailureTimelineOptions, HealthCheck, KafkaResource, ServiceUnderTest, StubBridge,
    HOST_DOCKER_INTERNAL,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka,services --test services_kafka_bridge_whoami -- --ignored"]
async fn kafka_http_bridge_whoami_env_injection_and_diagnostics() {
    std::env::set_var("SHAKEDOWN_STATS_INTERVAL_MS", "150");

    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Kafka(
        KafkaResource::new().with_image_tag_from_env(),
    ));

    let order_gateway_stub = StubBridge::new("order-gateway")
        .on("GET", "/health")
        .respond(200)
        .default_response(404);
    context
        .register_http_bridge("order-gateway", order_gateway_stub)
        .expect("register_http_bridge");

    let specification = ServiceUnderTest::docker("traefik/whoami:v1.10")
        .name("order")
        .port(80)
        .health_check(HealthCheck::http("/"))
        .build()
        .expect("valid ServiceSpec");

    context
        .register_service(specification)
        .expect("register_service");

    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "us012_kafka_bridge_whoami",
        |scenario_context| {
            Box::pin(async move {
                let base_url = scenario_context
                    .service_url("order")
                    .expect("service URL after readiness");
                assert!(
                    base_url.starts_with("http://127.0.0.1:"),
                    "harness uses loopback for assertions: {base_url}"
                );

                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(15))
                    .build()
                    .expect("reqwest client");

                let root = client.get(&base_url).send().await.expect("GET /");
                assert_eq!(root.status(), reqwest::StatusCode::OK);

                let env_url = format!("{base_url}/?env=true");
                let env_response = client.get(env_url).send().await.expect("GET /?env=true");
                assert_eq!(env_response.status(), reqwest::StatusCode::OK);
                let body = env_response.text().await.expect("whoami body");
                assert!(
                    body.contains("KAFKA_BOOTSTRAP_SERVERS=")
                        && body.contains(HOST_DOCKER_INTERNAL),
                    "whoami env dump should include Kafka bootstrap rewritten to {HOST_DOCKER_INTERNAL}: {body}"
                );
                assert!(
                    body.contains("ORDER_GATEWAY_URL=")
                        && body.contains(HOST_DOCKER_INTERNAL),
                    "Bridge base URL inside the container should use {HOST_DOCKER_INTERNAL}: {body}"
                );

                let handle = scenario_context
                    .service_handle("order")
                    .expect("service handle");
                assert!(handle.is_healthy());
                assert!(handle.container_id().is_some());

                #[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
                {
                    use std::collections::HashMap;

                    let diagnostics_handle = shakedown::suite_diagnostics_handle()
                        .await
                        .expect("suite diagnostics");
                    let docker = diagnostics_handle.docker();
                    let list = docker
                        .list_containers(Some(
                            bollard::query_parameters::ListContainersOptionsBuilder::default()
                                .all(true)
                                .filters(&HashMap::from([
                                    (
                                        "label".to_string(),
                                        vec![format!(
                                            "{SHAKEDOWN_DOCKER_LABEL_KEY}={SHAKEDOWN_DOCKER_LABEL_VALUE}"
                                        )],
                                    ),
                                    ("status".to_string(), vec!["running".to_string()]),
                                ]))
                                .build(),
                        ))
                        .await
                        .expect("list containers");
                    let order_summary = list
                        .iter()
                        .find(|summary| {
                            summary.labels.as_ref().is_some_and(|labels| {
                                labels.get(SHAKEDOWN_SERVICE_LABEL_KEY).map(|v| v.as_str())
                                    == Some("order")
                            })
                        })
                        .expect("running container with io.shakedown.service=order");
                    assert_eq!(
                        correlation_name_from_container_summary(order_summary),
                        "order",
                        "diagnostics correlation name should match registered service name"
                    );
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");

    let mut saw_infra_for_order = false;
    for entry in event_log.entries() {
        if let LogEntry::Infrastructure(infra) = entry {
            match infra {
                InfraEvent::ResourceSample { container, .. } if container == "order" => {
                    saw_infra_for_order = true;
                }
                InfraEvent::ContainerLog { container, .. } if container == "order" => {
                    saw_infra_for_order = true;
                }
                _ => {}
            }
        }
    }
    assert!(
        saw_infra_for_order,
        "expected merged diagnostics (stats and/or logs) for logical service name `order`"
    );

    let timeline = render_failure_timeline(
        &event_log,
        &FailureTimelineOptions::new("us012_kafka_bridge_whoami"),
    );
    assert!(
        timeline.contains("order"),
        "failure timeline should reference service name `order`: {timeline}"
    );
}
