//! Chapter 28: Resilience capstone — replica-set elections, inter-node partition, load SLOs, baselines
//!
//! **Resources:** `Toxiproxy` (register **before** [`shakedown::MongoReplicaSetResource`] for inter-node mesh),
//!   Kafka, RabbitMQ
//! **Service:** `fulfillment-service` (single service under test per DQ-9)
//! **Cluster + mesh:** [`shakedown::MongoClusterOps::step_down_primary`] during load;
//!   [`shakedown::ClusterHandle::partition_nodes`] across the Toxiproxy mesh, then
//!   [`shakedown::ClusterHandle::reset_topology`] to heal.
//! **Load:** Repeated named phases (`baseline`, `disruption_1`, `recovery_1`, `stable`, `disruption_2`,
//!   `cooldown`) with [`shakedown::LoadHandle::await_phase`], [`shakedown::RecoveryConfig`] rolling windows,
//!   and cumulative [`shakedown::LoadMetricsAssertBuilder`] checks (`recovery_under`, `error_burst_under`,
//!   `tail_ratio_under`, `error_rate_under`).
//! **Baselines:** Two **identical** capstone passes — first [`shakedown::LoadMetrics::save_baseline`]
//!   (`capstone-v1`), second [`shakedown::LoadMetricsAssertBuilder::no_regression_from`] with
//!   [`shakedown::Tolerance::percent`] + [`shakedown::Tolerance::error_rate_floor`].
//! **Diagnostics:** [`shakedown::ResourceAssertBuilder::peak_memory_under`] on `fulfillment-service` across
//!   the full scenario (Mode 3: classify infra pressure vs application SLO breaches).
//! **Artifacts:** Terminal summary + JSON run report under `target/shakedown/reports/`.
//! **Timeline:** [`shakedown::run_scenario`] with a caller-owned [`shakedown::EventLog`] so we can
//!   [`shakedown::render_failure_timeline`] and assert dense **cluster** + **load** correlation rows.
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,mongodb-cluster,kafka,rabbitmq,load,fault-injection,diagnostics --test ch28_capstone -- --ignored`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serial_test::serial;
use shakedown::error::ShakedownError;
use shakedown::{render_failure_timeline, ExchangeKind};
use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, FailureTimelineOptions, HealthCheck,
    KafkaResource, LoadPhase, LoadProfile, MongoReplicaSetMarker, MongoReplicaSetResource,
    PullPolicy, RabbitResource, Rate, RecoveryConfig, ServiceUnderTest, Tolerance,
    ToxiproxyResource, CLUSTER_EVENT_SOURCE,
};
use shakedown_examples::sample_fulfillment;

async fn await_peak_memory_under_fulfillment(ctx: &mut Context, threshold_bytes: u64) {
    let deadline = Instant::now() + Duration::from_secs(35);
    loop {
        match ctx
            .assert_resource("fulfillment-service")
            .peak_memory_under(threshold_bytes)
        {
            Ok(()) => return,
            Err(ShakedownError::ResourceAssertionFailed {
                ref measured,
                ref metric,
                ..
            }) if metric == "peak_memory" && measured == "no samples" => {
                if Instant::now() >= deadline {
                    panic!("timed out waiting for diagnostics samples for fulfillment-service");
                }
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
            Err(other) => panic!("peak_memory_under: {other}"),
        }
    }
}

fn capstone_load_profile() -> LoadProfile {
    LoadProfile::open_model()
        .phase(LoadPhase::warmup(
            Duration::from_secs(2),
            Rate::per_second(6.0),
        ))
        .phase(
            LoadPhase::constant(Duration::from_secs(3), Rate::per_second(10.0)).named("baseline"),
        )
        .phase(
            LoadPhase::constant(Duration::from_secs(9), Rate::per_second(16.0))
                .named("disruption_1"),
        )
        .phase(
            LoadPhase::constant(Duration::from_secs(2), Rate::per_second(8.0)).named("recovery_1"),
        )
        .phase(LoadPhase::constant(Duration::from_secs(3), Rate::per_second(12.0)).named("stable"))
        .phase(
            LoadPhase::constant(Duration::from_secs(7), Rate::per_second(14.0))
                .named("disruption_2"),
        )
        .phase(
            LoadPhase::constant(Duration::from_secs(2), Rate::per_second(7.0)).named("recovery_2"),
        )
        .phase(LoadPhase::constant(Duration::from_secs(2), Rate::per_second(6.0)).named("cooldown"))
        .workers(18)
        .max_workers(36)
        .build()
        .expect("capstone profile")
}

#[tokio::test]
#[serial]
#[ignore = "requires Docker + locally-built fulfillment-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,mongodb-cluster,kafka,rabbitmq,load,fault-injection,diagnostics --test ch28_capstone -- --ignored"]
async fn fulfillment_capstone_load_cluster_mesh_and_baseline() {
    // `set_var` is unsafe in multi-threaded programs (Rust RFC 3312); tests run single-threaded.
    unsafe {
        std::env::set_var("SHAKEDOWN_STATS_INTERVAL_MS", "180");
    }

    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Toxiproxy(ToxiproxyResource::new()));
    context.register_builtin(BuiltinResource::MongoReplicaSet(
        MongoReplicaSetResource::new(),
    ));
    context.register_builtin(BuiltinResource::Kafka(KafkaResource::new()));
    context.register_builtin(BuiltinResource::Rabbit(RabbitResource::new()));

    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "ch28_capstone",
        |scenario_context| {
            Box::pin(async move {
                scenario_context
                    .rabbit()
                    .declare_topology(
                        "fulfillments",
                        ExchangeKind::Direct,
                        "fulfillments-assert",
                        "#",
                    )
                    .await
                    .expect("rabbit topology");

                let fulfillment_spec = ServiceUnderTest::docker("fulfillment-service:latest")
                    .name("fulfillment-service")
                    .port(3001)
                    .health_check(HealthCheck::http("/health"))
                    .pull_policy(PullPolicy::Never)
                    .build()
                    .expect("fulfillment spec");

                scenario_context
                    .register_service(fulfillment_spec)
                    .expect("register fulfillment-service");

                let base_url = scenario_context
                    .service_url("fulfillment-service")
                    .expect("fulfillment URL");

                let http_client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(25))
                    .build()
                    .expect("HTTP client");

                let profile = capstone_load_profile();

                let make_work = {
                    let http_client = http_client.clone();
                    let base_url = base_url.clone();
                    move |pass_id: u32| {
                        let http_client = http_client.clone();
                        let base_url = base_url.clone();
                        let sequence = Arc::new(AtomicU64::new(0));
                        move || {
                            let http_client = http_client.clone();
                            let base_url = base_url.clone();
                            let sequence = sequence.clone();
                            async move {
                                let index = sequence.fetch_add(1, Ordering::Relaxed);
                                let order_label = format!("order-ch28-p{pass_id}-{index}");
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
                    }
                };

                let first_handle = scenario_context
                    .start_load("capstone", profile.clone(), make_work(1))
                    .await
                    .expect("start_load pass1");

                first_handle
                    .await_phase("disruption_1")
                    .await
                    .expect("await disruption_1 pass1");
                tokio::time::sleep(Duration::from_secs(1)).await;
                scenario_context
                    .mongo_cluster()
                    .expect("mongo cluster ops")
                    .step_down_primary()
                    .await
                    .expect("step_down_primary pass1");
                scenario_context
                    .mongo_cluster()
                    .expect("mongo cluster ops")
                    .await_new_primary(Duration::from_secs(90))
                    .await
                    .expect("await_new_primary pass1");
                first_handle
                    .await_recovery_with_config(
                        RecoveryConfig::new(Duration::from_secs(50))
                            .with_error_rate_threshold(0.0)
                            .with_rolling_window(Duration::from_secs(1)),
                    )
                    .await
                    .expect("await_recovery pass1");

                first_handle
                    .await_phase("disruption_2")
                    .await
                    .expect("await disruption_2 pass1");
                tokio::time::sleep(Duration::from_millis(600)).await;
                scenario_context
                    .cluster_handle::<MongoReplicaSetMarker>()
                    .expect("mongo cluster handle")
                    .partition_nodes(0, 1)
                    .await
                    .expect("partition_nodes pass1");
                tokio::time::sleep(Duration::from_secs(2)).await;
                scenario_context
                    .cluster_handle::<MongoReplicaSetMarker>()
                    .expect("mongo cluster handle")
                    .reset_topology()
                    .await
                    .expect("reset_topology pass1");

                let first_metrics = first_handle.stop().await.expect("stop pass1");

                first_metrics
                    .save_baseline("capstone-v1", "ch28_capstone")
                    .expect("save capstone baseline");

                let second_handle = scenario_context
                    .start_load("capstone", profile, make_work(2))
                    .await
                    .expect("start_load pass2");

                second_handle
                    .await_phase("disruption_1")
                    .await
                    .expect("await disruption_1 pass2");
                tokio::time::sleep(Duration::from_secs(1)).await;
                scenario_context
                    .mongo_cluster()
                    .expect("mongo cluster ops")
                    .step_down_primary()
                    .await
                    .expect("step_down_primary pass2");
                scenario_context
                    .mongo_cluster()
                    .expect("mongo cluster ops")
                    .await_new_primary(Duration::from_secs(90))
                    .await
                    .expect("await_new_primary pass2");
                second_handle
                    .await_recovery_with_config(
                        RecoveryConfig::new(Duration::from_secs(50))
                            .with_error_rate_threshold(0.0)
                            .with_rolling_window(Duration::from_secs(1)),
                    )
                    .await
                    .expect("await_recovery pass2");

                second_handle
                    .await_phase("disruption_2")
                    .await
                    .expect("await disruption_2 pass2");
                tokio::time::sleep(Duration::from_millis(600)).await;
                scenario_context
                    .cluster_handle::<MongoReplicaSetMarker>()
                    .expect("mongo cluster handle")
                    .partition_nodes(0, 1)
                    .await
                    .expect("partition_nodes pass2");
                tokio::time::sleep(Duration::from_secs(2)).await;
                scenario_context
                    .cluster_handle::<MongoReplicaSetMarker>()
                    .expect("mongo cluster handle")
                    .reset_topology()
                    .await
                    .expect("reset_topology pass2");

                let second_metrics = second_handle.stop().await.expect("stop pass2");

                const MEMORY_CEILING_BYTES: u64 = 512 * 1024 * 1024;
                await_peak_memory_under_fulfillment(scenario_context, MEMORY_CEILING_BYTES).await;
                scenario_context
                    .assert_resource("fulfillment-service")
                    .peak_memory_under(MEMORY_CEILING_BYTES)
                    .expect("fulfillment-service memory budget");

                let phase_anchor =
                    LoadPhase::constant(Duration::from_secs(1), Rate::per_second(1.0));
                let capstone_assert = second_metrics.assert_with_phase("capstone", phase_anchor);
                capstone_assert
                    .error_rate_under(0.35)
                    .expect("cumulative error rate across elections + partition");
                capstone_assert
                    .recovery_under(Duration::from_secs(8))
                    .expect("recovery budget");
                capstone_assert
                    .error_burst_under(Duration::from_secs(3))
                    .expect("burst budget");
                capstone_assert.tail_ratio_under(120.0).expect("tail ratio");
                capstone_assert
                    .no_regression_from(
                        "capstone-v1",
                        "ch28_capstone",
                        Tolerance::percent(0.10).error_rate_floor(0.02),
                    )
                    .expect("regression vs first capstone pass");
                capstone_assert.store_for_report();
                capstone_assert.print_summary_to_stderr();
                second_metrics
                    .write_report("capstone", "ch28_capstone")
                    .expect("write capstone JSON report");
            })
        },
        &mut event_log,
    )
    .await
    .expect("capstone scenario");

    let correlated: Vec<_> = event_log
        .events()
        .filter(|record| record.source == CLUSTER_EVENT_SOURCE || record.source == "load")
        .collect();
    assert!(
        correlated.iter().all(|record| record.timestamp > 0),
        "expected positive timestamps on correlated rows"
    );
    assert!(
        correlated
            .iter()
            .any(|record| record.source == CLUSTER_EVENT_SOURCE),
        "expected cluster topology/load rows"
    );
    assert!(
        correlated.iter().any(|record| record.source == "load"),
        "expected load driver rows"
    );

    let timeline = render_failure_timeline(
        &event_log,
        &FailureTimelineOptions::new("ch28_capstone")
            .with_failure_summary(
                "successful capstone — correlated cluster + load events for Mode 3 classification",
            )
            .with_verdict("passed"),
    );
    assert!(
        timeline.contains("load") || timeline.to_lowercase().contains("cluster"),
        "timeline should surface load and/or cluster correlation; got:\n{timeline}"
    );
}
