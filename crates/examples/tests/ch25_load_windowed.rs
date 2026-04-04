//! Chapter 25: Named phases, wall-clock windows, and distribution shape
//!
//! **Resources:** Postgres, Redis, Kafka
//! **Service:** `example-order-service`
//! **Load API:** Multi-phase profile with [`shakedown::LoadPhase::named`] labels (`baseline`, `peak`),
//!   [`shakedown::LoadHandle::await_phase`], then windowed [`shakedown::LoadMetrics::p99_during`],
//!   [`shakedown::LoadMetrics::error_rate_during`], [`shakedown::LoadMetrics::throughput_during`],
//!   [`shakedown::LoadMetrics::p99_after`].
//! **Assertions:** [`shakedown::LoadMetricsAssertBuilder::tail_ratio_under`],
//!   [`shakedown::LoadMetricsAssertBuilder::throughput_above`] (steady-state gate),
//!   [`shakedown::LoadMetrics::assert_with_phase`] for phase-correlated failure metadata on SLO breaks.
//! **Saturation:** [`shakedown::SaturationPolicy::Drop`] vs [`shakedown::SaturationPolicy::Queue`] —
//!   `Drop` sheds arrivals when every worker is busy (open-model realism); `Queue` buffers until the harness
//!   queue fills, trading tail latency for fewer dropped iterations. [`shakedown::SaturationPolicy::Error`]
//!   fails the iteration immediately when saturated (strict open-model).
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,redis,kafka,load --test ch25_load_windowed -- --ignored`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use shakedown::{LoadPhase, LoadProfile, Rate, SaturationPolicy};
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(resources = [Postgres, Redis, Kafka])]
#[ignore = "requires Docker + locally-built example-order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,redis,kafka,load --test ch25_load_windowed -- --ignored"]
async fn order_service_windowed_metrics_and_tail_ratio(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    let order_spec = shakedown::ServiceUnderTest::docker("example-order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("order service spec");

    ctx.register_service(order_spec)
        .expect("register order-service");

    let base_url = ctx
        .service_url("order-service")
        .expect("order service URL after readiness");

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("HTTP client");

    let sequence = Arc::new(AtomicU64::new(0));

    let profile = LoadProfile::open_model()
        .phase(LoadPhase::warmup(Duration::from_millis(800), Rate::per_second(10.0)).named("warm"))
        .phase(LoadPhase::constant(Duration::from_secs(3), Rate::per_second(8.0)).named("baseline"))
        .phase(LoadPhase::constant(Duration::from_secs(4), Rate::per_second(22.0)).named("peak"))
        .workers(10)
        .max_workers(20)
        .on_saturation(SaturationPolicy::Queue)
        .build()
        .expect("load profile");

    let load_handle = ctx
        .start_load("orders-windowed", profile, {
            let http_client = http_client.clone();
            let base_url = base_url.clone();
            let sequence = sequence.clone();
            move || {
                let http_client = http_client.clone();
                let base_url = base_url.clone();
                let sequence = sequence.clone();
                async move {
                    let index = sequence.fetch_add(1, Ordering::Relaxed);
                    let customer_label = format!("customer-ch25-{index}");
                    let response = http_client
                        .post(format!("{base_url}/orders"))
                        .json(&sample_order(&customer_label))
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
        .await_phase("baseline")
        .await
        .expect("baseline phase");
    let baseline_window_start = Instant::now();
    tokio::time::sleep(Duration::from_millis(900)).await;
    let baseline_window_end = Instant::now();

    load_handle.await_phase("peak").await.expect("peak phase");
    let peak_window_start = Instant::now();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let peak_window_end = Instant::now();

    let metrics = load_handle.stop().await.expect("load stop");

    let baseline_err = metrics.error_rate_during(&(baseline_window_start..baseline_window_end));
    assert!(
        baseline_err <= 0.12,
        "baseline window error rate should stay bounded (got {baseline_err})"
    );

    let peak_p99 = metrics.p99_during(&(peak_window_start..peak_window_end));
    assert!(
        peak_p99 < Duration::from_secs(60),
        "peak-window p99 should remain finite under local Docker (got {peak_p99:?})"
    );

    let peak_throughput = metrics.throughput_during(&(peak_window_start..peak_window_end));
    let baseline_throughput =
        metrics.throughput_during(&(baseline_window_start..baseline_window_end));
    assert!(
        peak_throughput >= baseline_throughput * 0.85 || peak_throughput > 2.0,
        "peak window should sustain similar or higher completion rate than baseline (peak={peak_throughput}, base={baseline_throughput})"
    );

    let late_only_p99 = metrics.p99_after(peak_window_start);
    assert!(
        late_only_p99 <= Duration::from_secs(90),
        "p99_after should summarize late-run latency ({late_only_p99:?})"
    );

    let phase_for_messages = LoadPhase::constant(Duration::from_secs(1), Rate::per_second(1.0));
    let windowed = metrics.assert_with_phase("orders-windowed", phase_for_messages);
    windowed.tail_ratio_under(80.0).expect("tail ratio");
    windowed.throughput_above(0.5).expect("throughput floor");
    windowed
        .error_rate_under(0.12)
        .expect("steady-state error rate");
    windowed.store_for_report();
    windowed.print_summary_to_stderr();

    metrics
        .write_report("orders-windowed", "ch25_load_windowed")
        .expect("write report");
}
