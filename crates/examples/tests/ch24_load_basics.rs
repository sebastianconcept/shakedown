//! Chapter 24: Open-model load against the Order Service
//!
//! **Resources:** Postgres, Redis, Kafka
//! **Service:** `order-service`
//! **Load API:** [`shakedown::LoadProfile::open_model`], [`shakedown::LoadPhase::warmup`],
//!   [`shakedown::LoadPhase::constant`], [`shakedown::LoadPhase::ramp`], [`shakedown::Rate::per_second`],
//!   [`shakedown::Context::start_load`], [`shakedown::LoadHandle`], live [`shakedown::LoadHandle::metrics`]
//!   snapshots, then final [`shakedown::LoadMetrics`] after [`shakedown::LoadHandle::stop`].
//! **Assertions:** [`shakedown::LoadMetrics::total_iterations`], [`shakedown::LoadMetrics::successful_operations`],
//!   [`shakedown::LoadMetrics::p50`], [`shakedown::LoadMetrics::p99`], [`shakedown::LoadMetrics::error_rate`],
//!   [`shakedown::LoadMetrics::throughput`], [`shakedown::LoadMetricsAssertBuilder`] (`p99_under`, `p95_under`, `p50_under`,
//!   `error_rate_under`, `no_drops`), [`shakedown::LoadMetricsAssertBuilder::print_summary_to_stderr`],
//!   [`shakedown::LoadMetricsAssertBuilder::store_for_report`] + [`shakedown::LoadMetrics::write_report`]
//!   → `target/shakedown/reports/`.
//! **Runtime hints:** [`shakedown::SaturationPolicy`] (default `Drop`) and [`shakedown::LoadRuntime`] are selected
//!   on the profile builder — here we keep defaults (`Drop`, shared Tokio runtime) so overload surfaces as
//!   dropped arrivals rather than unbounded queuing.
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,redis,kafka,load --test ch24_load_basics -- --ignored`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use shakedown::{LoadPhase, LoadProfile, LoadRuntime, Rate, SaturationPolicy};
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(resources = [Postgres, Redis, Kafka])]
#[ignore = "requires Docker + locally-built order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,redis,kafka,load --test ch24_load_basics -- --ignored"]
async fn order_service_load_warmup_ramp_and_slo_report(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    let order_spec = shakedown::ServiceUnderTest::docker("order-service:latest")
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
        // Warmup iterations are excluded from steady-state percentiles when the driver records warmup end.
        .phase(LoadPhase::warmup(
            Duration::from_secs(2),
            Rate::per_second(12.0),
        ))
        .phase(LoadPhase::constant(
            Duration::from_secs(4),
            Rate::per_second(18.0),
        ))
        .phase(LoadPhase::ramp(
            Duration::from_secs(3),
            Rate::per_second(6.0),
            Rate::per_second(20.0),
        ))
        .workers(8)
        .max_workers(16)
        // Default open-model behavior: `SaturationPolicy::Drop` — see also `Queue` for back-pressure experiments.
        .on_saturation(SaturationPolicy::Drop)
        .runtime(LoadRuntime::Shared)
        .build()
        .expect("load profile");

    let load_handle = ctx
        .start_load("order-api", profile, {
            let http_client = http_client.clone();
            let base_url = base_url.clone();
            let sequence = sequence.clone();
            move || {
                let http_client = http_client.clone();
                let base_url = base_url.clone();
                let sequence = sequence.clone();
                async move {
                    let index = sequence.fetch_add(1, Ordering::Relaxed);
                    let customer_label = format!("customer-ch24-{index}");
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

    let mid_run = load_handle.metrics();
    assert!(
        mid_run.total_iterations() > 0,
        "live metrics should show progress while the profile is running"
    );
    assert!(
        mid_run.successful_operations() + mid_run.failed_operations() <= mid_run.total_iterations(),
        "completed ops should reconcile with totals"
    );

    let metrics = load_handle.stop().await.expect("load stop");

    assert!(metrics.total_iterations() > 0, "expected load iterations");
    assert!(
        metrics.successful_operations() > 0,
        "expected at least one successful POST /orders"
    );

    assert!(metrics.throughput() >= 0.0);
    assert!(metrics.p50() <= metrics.p99() || metrics.p99().is_zero());

    let order_assertions = metrics.assert("order-api");
    order_assertions
        .p99_under(Duration::from_secs(45))
        .expect("p99 SLO");
    order_assertions
        .p95_under(Duration::from_secs(42))
        .expect("p95 SLO");
    order_assertions
        .p50_under(Duration::from_secs(30))
        .expect("p50 SLO");
    order_assertions
        .error_rate_under(0.08)
        .expect("error rate SLO");
    order_assertions.no_drops().expect("no saturation drops");
    order_assertions.store_for_report();
    order_assertions.print_summary_to_stderr();

    let report_path = metrics
        .write_report("order-api", "ch24_load_basics")
        .expect("write JSON run report under target/shakedown/reports/");
    assert!(
        report_path
            .as_os_str()
            .to_string_lossy()
            .contains("reports"),
        "report path should live under reports/: {report_path:?}"
    );
}
