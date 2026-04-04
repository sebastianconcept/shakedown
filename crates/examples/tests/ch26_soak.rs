//! Chapter 26: Soak-style profile, baselines, drift windows, and infra budgets
//!
//! **Resources:** Postgres, Redis, Kafka
//! **Service:** `example-order-service`
//! **Diagnostics:** `SHAKEDOWN_STATS_INTERVAL_MS` + [`shakedown::Context::assert_resource`] for
//!   [`shakedown::ResourceAssertBuilder::peak_memory_under`] on the logical service container.
//! **Load:** Warmup → ramp → sustained constant “soak” phase; two sequential runs — first seeds
//!   [`shakedown::LoadMetrics::save_baseline`] under `target/shakedown/baselines/`, second enforces
//!   [`shakedown::LoadMetricsAssertBuilder::no_regression_from`] with [`shakedown::Tolerance::percent`].
//! **Drift:** Compare [`shakedown::LoadMetrics::p99_during`] on early vs late wall-clock slices of the
//!   soak phase to catch latency creep while throughput stays flat.
//! **Assertions:** [`shakedown::LoadMetricsAssertBuilder::error_rate_under`] (0% failures expected for
//!   healthy Order API), terminal summary + JSON report.
//! **Mode 3:** When a soak breaches an SLO, Shakedown’s performance assertion surfaces pair with
//!   diagnostics samples so reviewers can tell **essential** (service regression) from **accidental**
//!   (container memory pressure) complexity — the same `peak_memory_under` gate used here.
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,kafka,redis,load,diagnostics --test ch26_soak -- --ignored`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use shakedown::error::ShakedownError;
use shakedown::{LoadPhase, LoadProfile, Rate, Tolerance};
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

async fn await_peak_memory_under_order_service(ctx: &mut shakedown::Context, threshold_bytes: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match ctx
            .assert_resource("order-service")
            .peak_memory_under(threshold_bytes)
        {
            Ok(()) => return,
            Err(ShakedownError::ResourceAssertionFailed {
                ref measured,
                ref metric,
                ..
            }) if metric == "peak_memory" && measured == "no samples" => {
                if Instant::now() >= deadline {
                    panic!("timed out waiting for diagnostics samples for order-service");
                }
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
            Err(other) => panic!("peak_memory_under: {other}"),
        }
    }
}

fn soak_profile() -> LoadProfile {
    LoadProfile::open_model()
        .phase(LoadPhase::warmup(
            Duration::from_millis(900),
            Rate::per_second(10.0),
        ))
        .phase(LoadPhase::ramp(
            Duration::from_secs(2),
            Rate::per_second(4.0),
            Rate::per_second(16.0),
        ))
        .phase(
            LoadPhase::constant(Duration::from_secs(12), Rate::per_second(14.0))
                .named("soak_steady"),
        )
        .workers(12)
        .max_workers(24)
        .build()
        .expect("soak profile")
}

#[shakedown::test(resources = [Postgres, Redis, Kafka])]
#[ignore = "requires Docker + locally-built example-order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,kafka,redis,load,diagnostics --test ch26_soak -- --ignored"]
async fn order_service_soak_baseline_and_drift(ctx: &mut shakedown::Context) {
    std::env::set_var("SHAKEDOWN_STATS_INTERVAL_MS", "200");

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

    let work_factory = {
        let http_client = http_client.clone();
        let base_url = base_url.clone();
        let sequence = sequence.clone();
        move || {
            let http_client = http_client.clone();
            let base_url = base_url.clone();
            let sequence = sequence.clone();
            async move {
                let index = sequence.fetch_add(1, Ordering::Relaxed);
                let customer_label = format!("customer-ch26-{index}");
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
    };

    let profile = soak_profile();

    let first_handle = ctx
        .start_load("order-soak", profile.clone(), work_factory.clone())
        .await
        .expect("first soak start");
    let first_metrics = first_handle.stop().await.expect("first soak stop");

    first_metrics
        .assert("order-soak")
        .error_rate_under(0.0)
        .expect("golden soak should not observe HTTP failures");
    first_metrics
        .save_baseline("v1.0", "ch26_soak")
        .expect("persist baseline under target/shakedown/baselines/");

    let second_handle = ctx
        .start_load("order-soak", profile, work_factory)
        .await
        .expect("second soak start");

    second_handle
        .await_phase("soak_steady")
        .await
        .expect("enter soak phase");

    let early_start = Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let early_end = Instant::now();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let late_start = Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let late_end = Instant::now();

    let second_metrics = second_handle.stop().await.expect("second soak stop");

    let early_p99 = second_metrics.p99_during(&(early_start..early_end));
    let late_p99 = second_metrics.p99_during(&(late_start..late_end));
    let drift_ratio = if early_p99.is_zero() {
        1.0
    } else {
        late_p99.as_secs_f64() / early_p99.as_secs_f64().max(1e-9)
    };
    assert!(
        drift_ratio < 4.0,
        "late-window p99 should not drift orders of magnitude vs early soak (early={early_p99:?} late={late_p99:?})"
    );

    const MEMORY_CEILING_BYTES: u64 = 512 * 1024 * 1024;
    await_peak_memory_under_order_service(ctx, MEMORY_CEILING_BYTES).await;
    ctx.assert_resource("order-service")
        .peak_memory_under(MEMORY_CEILING_BYTES)
        .expect("order-service RSS should stay under teaching ceiling");

    let soak_assert = second_metrics.assert("order-soak");
    soak_assert
        .error_rate_under(0.0)
        .expect("soak must stay green on happy path");
    soak_assert
        .no_regression_from("v1.0", "ch26_soak", Tolerance::percent(0.05))
        .expect("second soak should match first within 5% tolerance");
    soak_assert.store_for_report();
    soak_assert.print_summary_to_stderr();

    second_metrics
        .write_report("order-soak", "ch26_soak")
        .expect("write JSON soak report");
}
