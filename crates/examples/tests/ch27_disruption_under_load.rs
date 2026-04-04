//! Chapter 27: Mongo primary step-down while load hits Fulfillment
//!
//! **Resources:** `MongoReplicaSet`, Kafka, RabbitMQ
//! **Service:** `fulfillment-service`
//! **Cluster:** [`shakedown::MongoClusterOps::step_down_primary`], [`shakedown::MongoClusterOps::await_new_primary`]
//! **Load:** [`shakedown::LoadProfile::open_model`] with [`shakedown::LoadPhase::warmup`] then sustained open-model
//!   arrivals during a named disruption phase.
//! **Recovery:** [`shakedown::LoadHandle::await_recovery_with_config`] with [`shakedown::RecoveryConfig::new`],
//!   [`shakedown::RecoveryConfig::with_error_rate_threshold`], [`shakedown::RecoveryConfig::with_rolling_window`].
//! **Assertions:** [`shakedown::LoadMetricsAssertBuilder::error_rate_under`], [`shakedown::LoadMetricsAssertBuilder::recovery_under`],
//!   [`shakedown::LoadMetricsAssertBuilder::error_burst_under`], [`shakedown::LoadMetricsAssertBuilder::p99_under`].
//! **Mode 3:** When Mongo elects a new primary, transient write failures are **accidental complexity** (infra);
//!   Shakedown’s load driver records burst + recovery timing so SLO assertions stay tied to observable recovery,
//!   not silent retries inside the client under test.
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,mongodb-cluster,mongodb,kafka,rabbitmq,load --test ch27_disruption_under_load -- --ignored`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serial_test::serial;
use shakedown::ExchangeKind;
use shakedown::{LoadPhase, LoadProfile, Rate, RecoveryConfig};
use shakedown_examples::sample_fulfillment;

#[shakedown::test(resources = [MongoReplicaSet, Kafka, Rabbit])]
#[serial]
#[ignore = "requires Docker + locally-built fulfillment-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,mongodb-cluster,mongodb,kafka,rabbitmq,load --test ch27_disruption_under_load -- --ignored"]
async fn fulfillment_load_survives_mongo_primary_step_down(ctx: &mut shakedown::Context) {
    ctx.rabbit()
        .declare_topology(
            "fulfillments",
            ExchangeKind::Direct,
            "fulfillments-assert",
            "#",
        )
        .await
        .expect("declare RabbitMQ topology");

    let fulfillment_spec = shakedown::ServiceUnderTest::docker("fulfillment-service:latest")
        .name("fulfillment-service")
        .port(3001)
        .health_check(shakedown::HealthCheck::http("/health"))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("fulfillment service spec");

    ctx.register_service(fulfillment_spec)
        .expect("register fulfillment-service");

    let base_url = ctx
        .service_url("fulfillment-service")
        .expect("fulfillment URL after readiness");

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("HTTP client");

    let sequence = Arc::new(AtomicU64::new(0));

    let profile = LoadProfile::open_model()
        .phase(LoadPhase::warmup(
            Duration::from_secs(2),
            Rate::per_second(8.0),
        ))
        .phase(LoadPhase::constant(Duration::from_secs(4), Rate::per_second(12.0)).named("steady"))
        .phase(
            LoadPhase::constant(Duration::from_secs(22), Rate::per_second(18.0))
                .named("disruption"),
        )
        .workers(20)
        .max_workers(40)
        .build()
        .expect("profile");

    let load_handle = ctx
        .start_load("fulfillment-api", profile, {
            let http_client = http_client.clone();
            let base_url = base_url.clone();
            let sequence = sequence.clone();
            move || {
                let http_client = http_client.clone();
                let base_url = base_url.clone();
                let sequence = sequence.clone();
                async move {
                    let index = sequence.fetch_add(1, Ordering::Relaxed);
                    let order_label = format!("order-ch27-{index}");
                    let response = http_client
                        .post(format!("{base_url}/fulfill"))
                        .json(&sample_fulfillment(&order_label))
                        .send()
                        .await
                        .map_err(|error| error.to_string())?;
                    if response.status().is_success() {
                        Ok(())
                    } else {
                        Err(format!("HTTP {}", response.status()))
                    }
                }
            }
        })
        .await
        .expect("start_load");

    load_handle
        .await_phase("disruption")
        .await
        .expect("disruption phase");

    let mongo_cluster = ctx.mongo_cluster().expect("mongo cluster ops");
    mongo_cluster
        .step_down_primary()
        .await
        .expect("step down primary under load");
    mongo_cluster
        .await_new_primary(Duration::from_secs(90))
        .await
        .expect("await new primary");

    let recovery_config = RecoveryConfig::new(Duration::from_secs(60))
        .with_error_rate_threshold(0.0)
        .with_rolling_window(Duration::from_secs(1));

    load_handle
        .await_recovery_with_config(recovery_config)
        .await
        .expect("await rolling error-rate recovery after election");

    let metrics = load_handle.stop().await.expect("load stop");

    let fulfillment_assert = metrics.assert("fulfillment-api");
    fulfillment_assert
        .error_rate_under(0.35)
        .expect("error rate bounded across election window");
    fulfillment_assert
        .recovery_under(Duration::from_secs(5))
        .expect("recovery latency after failures");
    fulfillment_assert
        .error_burst_under(Duration::from_secs(2))
        .expect("failure burst duration");
    fulfillment_assert
        .p99_under(Duration::from_secs(45))
        .expect("tail latency after disruption");
    fulfillment_assert.store_for_report();
    fulfillment_assert.print_summary_to_stderr();

    metrics
        .write_report("fulfillment-api", "ch27_disruption_under_load")
        .expect("write report");
}
