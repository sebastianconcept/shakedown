//! Chapter 18: Bollard diagnostics — resource budgets and failure timelines
//!
//! **Resources:** Postgres, Redis, Kafka, MongoDB
//! **Services:** `order-service` + `fulfillment-service`
//! **Assertions:** `Context::assert_resource(..).peak_memory_under`, `avg_cpu_under`,
//!   `render_failure_timeline`, inspection of merged `LogEntry` / `InfraEvent` records
//! **Concepts:** suite diagnostics (`SHAKEDOWN_STATS_INTERVAL_MS`), logical service names on
//!   `ResourceSample` / `ContainerLog`, building a readable timeline string from `EventLog`
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! `#[shakedown::test]` keeps the scenario `EventLog` inside the harness closure. This chapter
//! uses `run_scenario` with a caller-owned [`shakedown::EventLog`] so we can call
//! [`shakedown::render_failure_timeline`] and scan [`shakedown::event::InfraEvent`] rows after the
//! run — the same pattern as Shakedown’s `services_diagnostics` integration test.
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,mongodb,kafka,redis,diagnostics --test ch18_diagnostics -- --ignored`

use std::time::{Duration, Instant};

use mongodb::bson::doc;
use shakedown::error::ShakedownError;
use shakedown::event::{InfraEvent, LogEntry};
use shakedown::{
    render_failure_timeline, run_scenario, BuiltinResource, Context, EnvMapping, EventLog,
    FailureTimelineOptions, HealthCheck, KafkaResource, MongoResource, PostgresResource,
    PullPolicy, RedisResource, ServiceUnderTest,
};
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

async fn await_peak_memory_under(
    ctx: &mut shakedown::Context,
    container_name: &str,
    threshold_bytes: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        match ctx
            .assert_resource(container_name)
            .peak_memory_under(threshold_bytes)
        {
            Ok(()) => return,
            Err(ShakedownError::ResourceAssertionFailed {
                ref measured,
                ref metric,
                ..
            }) if metric == "peak_memory" && measured == "no samples" => {
                if Instant::now() >= deadline {
                    panic!(
                        "timed out waiting for resource stats samples for container {container_name:?}"
                    );
                }
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
            Err(other) => panic!("peak_memory_under: {other}"),
        }
    }
}

async fn await_avg_cpu_under(
    ctx: &mut shakedown::Context,
    container_name: &str,
    limit_pct: f64,
) -> Result<(), ShakedownError> {
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        match ctx.assert_resource(container_name).avg_cpu_under(limit_pct) {
            Ok(()) => return Ok(()),
            Err(ShakedownError::ResourceAssertionFailed {
                ref measured,
                ref metric,
                ..
            }) if metric == "avg_cpu" && measured == "no samples" => {
                if Instant::now() >= deadline {
                    return Err(ShakedownError::ResourceAssertionFailed {
                        container: container_name.to_string(),
                        metric: "avg_cpu".to_string(),
                        measured: "no samples (timeout)".to_string(),
                        threshold: format!("{limit_pct:.1}%"),
                        phase: shakedown::Phase::Run,
                    });
                }
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
            Err(other) => return Err(other),
        }
    }
}

#[tokio::test]
#[ignore = "requires Docker + locally-built order-service:latest and fulfillment-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,mongodb,kafka,redis,diagnostics --test ch18_diagnostics -- --ignored"]
async fn both_services_resource_budgets_and_failure_timeline_string() {
    // `set_var` is unsafe in multi-threaded programs (Rust RFC 3312); tests run single-threaded.
    unsafe {
        std::env::set_var("SHAKEDOWN_STATS_INTERVAL_MS", "150");
    }

    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Postgres(PostgresResource::new()));
    context.register_builtin(BuiltinResource::Redis(RedisResource::new()));
    context.register_builtin(BuiltinResource::Kafka(KafkaResource::new()));
    context.register_builtin(BuiltinResource::Mongo(MongoResource::new()));

    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "ch18_diagnostics_both_services",
        |scenario_context| {
            Box::pin(async move {
                scenario_context
                    .postgres()
                    .execute(ORDERS_SCHEMA_SQL, &[])
                    .await
                    .expect("seed orders schema");

                let fulfillment_spec = ServiceUnderTest::docker("fulfillment-service:latest")
                    .name("fulfillment-service")
                    .port(3001)
                    .port_named("grpc", 50051)
                    .health_check(HealthCheck::http("/health"))
                    .pull_policy(PullPolicy::Never)
                    .build()
                    .expect("fulfillment service spec");

                scenario_context
                    .register_service(fulfillment_spec)
                    .expect("register fulfillment-service");

                let order_spec = ServiceUnderTest::docker("order-service:latest")
                    .name("order-service")
                    .port(3000)
                    .health_check(HealthCheck::http("/health"))
                    .depends_on("fulfillment-service")
                    .env_mapping(EnvMapping::service_named_port(
                        "fulfillment-service",
                        "grpc",
                        "FULFILLMENT_GRPC_URI",
                    ))
                    .pull_policy(PullPolicy::Never)
                    .build()
                    .expect("order service spec");

                scenario_context
                    .register_service(order_spec)
                    .expect("register order-service");

                let order_base_url = scenario_context
                    .service_url("order-service")
                    .expect("order service URL");

                let http_client = reqwest::Client::new();
                let create_response = http_client
                    .post(format!("{order_base_url}/orders"))
                    .json(&sample_order("customer-ch18"))
                    .send()
                    .await
                    .expect("POST /orders");

                assert_eq!(create_response.status(), reqwest::StatusCode::CREATED);
                let created: serde_json::Value =
                    create_response.json().await.expect("created JSON");
                let order_id = created["id"].as_str().expect("order id");

                let fulfill_response = http_client
                    .post(format!("{order_base_url}/orders/{order_id}/fulfill"))
                    .send()
                    .await
                    .expect("POST /orders/:id/fulfill");

                assert_eq!(
                    fulfill_response.status(),
                    reqwest::StatusCode::OK,
                    "expected order-service to call fulfillment-service"
                );

                scenario_context
                    .mongo()
                    .assert_document_exists("fulfillments", doc! { "order_id": order_id })
                    .await
                    .expect("fulfillment document after HTTP handoff");

                tokio::time::sleep(Duration::from_secs(2)).await;

                const MEMORY_CEILING_BYTES: u64 = 512 * 1024 * 1024;
                const CPU_CEILING_PCT: f64 = 95.0;

                await_peak_memory_under(scenario_context, "order-service", MEMORY_CEILING_BYTES)
                    .await;
                await_peak_memory_under(
                    scenario_context,
                    "fulfillment-service",
                    MEMORY_CEILING_BYTES,
                )
                .await;

                scenario_context
                    .assert_resource("order-service")
                    .peak_memory_under(MEMORY_CEILING_BYTES)?;
                scenario_context
                    .assert_resource("fulfillment-service")
                    .peak_memory_under(MEMORY_CEILING_BYTES)?;

                await_avg_cpu_under(scenario_context, "order-service", CPU_CEILING_PCT).await?;
                scenario_context
                    .assert_resource("order-service")
                    .avg_cpu_under(CPU_CEILING_PCT)?;
                await_avg_cpu_under(scenario_context, "fulfillment-service", CPU_CEILING_PCT)
                    .await?;
                scenario_context
                    .assert_resource("fulfillment-service")
                    .avg_cpu_under(CPU_CEILING_PCT)?;

                Ok::<(), ShakedownError>(())
            })
        },
        &mut event_log,
    )
    .await
    .expect("ch18 diagnostics scenario should pass");

    let mut order_infra_observations = 0_u32;
    let mut fulfillment_infra_observations = 0_u32;

    for entry in event_log.entries() {
        if let LogEntry::Infrastructure(infra) = entry {
            match infra {
                InfraEvent::ResourceSample { container, .. }
                | InfraEvent::ContainerLog { container, .. }
                    if container == "order-service" =>
                {
                    order_infra_observations += 1;
                }
                InfraEvent::ResourceSample { container, .. }
                | InfraEvent::ContainerLog { container, .. }
                    if container == "fulfillment-service" =>
                {
                    fulfillment_infra_observations += 1;
                }
                InfraEvent::DockerEvent { container, .. }
                    if container == "order-service" || container == "fulfillment-service" =>
                {
                    if container == "order-service" {
                        order_infra_observations += 1;
                    } else {
                        fulfillment_infra_observations += 1;
                    }
                }
                _ => {}
            }
        }
    }

    assert!(
        order_infra_observations > 0 || fulfillment_infra_observations > 0,
        "expected merged diagnostics for at least one logical service name (stats and/or logs/events); order={order_infra_observations} fulfillment={fulfillment_infra_observations}"
    );

    let timeline = render_failure_timeline(
        &event_log,
        &FailureTimelineOptions::new("ch18_diagnostics_both_services")
            .with_failure_summary("successful run — timeline string includes merged infra rows"),
    );
    assert!(
        timeline.contains("order-service") || timeline.contains("fulfillment-service"),
        "failure timeline renderer should mention at least one service label; got:\n{timeline}"
    );
}
