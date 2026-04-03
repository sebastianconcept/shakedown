//! Tests for [`super::LoadMetricsAssertBuilder`] chains and baseline regression integration.

use std::sync::Arc;
use std::time::Duration;

use crate::error::ShakedownError;
use crate::load::baseline::{
    resolve_load_baseline_path, write_load_baseline_to_path, LoadBaseline, Tolerance,
};
use crate::load::metrics::LoadMetricsAccumulator;
use crate::load::{LoadMetrics, Phase, Rate};

fn sample_metrics() -> LoadMetrics {
    let epoch = std::time::Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for index in 0..50u64 {
        let completion = epoch + Duration::from_millis(index);
        accumulator.record_success(completion, Duration::from_micros(500 + index * 10));
    }
    LoadMetrics::new(accumulator)
}

#[test]
fn assertion_chain_passes() {
    // Given sample metrics with modest latencies and no failures
    let metrics = sample_metrics();
    // When we chain every assertion type with generous thresholds
    let assertions = metrics.assert("checkout");
    assertions
        .p99_under(Duration::from_millis(50))
        .expect("p99")
        .p50_under(Duration::from_millis(5))
        .expect("p50")
        .throughput_above(1.0)
        .expect("throughput")
        .error_rate_under(0.5)
        .expect("error_rate")
        .no_drops()
        .expect("no_drops")
        .recovery_under(Duration::from_secs(10))
        .expect("recovery")
        .error_burst_under(Duration::from_secs(10))
        .expect("burst")
        .tail_ratio_under(100.0)
        .expect("tail");
    // Then every verdict records a pass
    let verdicts = assertions.verdicts_snapshot();
    assert!(verdicts.iter().all(|verdict| verdict.passed));
}

#[test]
fn assertion_chain_question_mark_operator() {
    // Given a helper that uses `?` on the assertion chain
    fn run(metrics: &LoadMetrics) -> Result<(), ShakedownError> {
        metrics
            .assert("q")
            .p99_under(Duration::from_secs(1))?
            .error_rate_under(1.0)?;
        Ok(())
    }
    // When we run it on sample metrics
    // Then it completes without error
    run(&sample_metrics()).expect("chain with ?");
}

#[test]
fn p99_under_fails_with_structured_error() {
    // Given sample metrics and an impossibly tight p99 ceiling
    let metrics = sample_metrics();
    let phase = Phase::constant(Duration::from_secs(1), Rate::per_second(1.0));
    // When we assert p99 under 1ns with a named load phase
    let error = metrics
        .assert_with_phase("cart", phase.clone())
        .p99_under(Duration::from_nanos(1))
        .expect_err("expected failure");
    // Then we get PerformanceAssertionFailed with matching metadata
    match error {
        ShakedownError::PerformanceAssertionFailed(details) => {
            assert_eq!(details.load_name, "cart");
            assert_eq!(details.metric, "p99");
            assert!(!details.measured.is_empty());
            assert!(details.threshold.contains('<'));
            assert_eq!(details.phase, phase);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn drops_under_records_failure() {
    // Given metrics with two dropped iterations
    let epoch = std::time::Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    accumulator.record_dropped(epoch);
    accumulator.record_dropped(epoch);
    let metrics = LoadMetrics::new(accumulator);
    // When we require zero drops
    let error = metrics.assert("x").drops_under(0).expect_err("drops");
    // Then the failure names the drops metric
    assert!(matches!(
        error,
        ShakedownError::PerformanceAssertionFailed(details) if details.metric == "drops"
    ));
}

#[test]
fn no_regression_from_succeeds_against_saved_baseline() {
    // Given identical steady-state data written as a baseline on disk
    let epoch = std::time::Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for index in 0..40u64 {
        accumulator.record_success(
            epoch + Duration::from_millis(index + 2),
            Duration::from_micros(500 + index * 2),
        );
    }
    let baseline_metrics = LoadMetrics::new(accumulator.clone());
    let scenario_key = format!("noregr_ok_{}", std::process::id());
    let mut baseline =
        LoadBaseline::from_load_metrics(&baseline_metrics, "v1", &scenario_key).expect("baseline");
    // Wall-clock throughput differs between baseline capture and comparison; zero skips the row.
    baseline.summary.throughput = 0.0;
    let path = resolve_load_baseline_path(&scenario_key, "v1");
    write_load_baseline_to_path(&baseline, &path).expect("write baseline");

    let current = LoadMetrics::new(accumulator);
    // When we compare current metrics to that baseline with 25% tolerance
    current
        .assert("checkout")
        .no_regression_from("v1", &scenario_key, Tolerance::percent(0.25))
        .expect("within tolerance");

    // Then no error is returned (cleanup artifact)
    let _ = std::fs::remove_file(&path);
}

#[test]
fn no_regression_from_fails_with_comparison_table() {
    // Given a fast baseline and a much slower current run on disk
    let epoch = std::time::Instant::now();
    let accumulator_fast = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for index in 0..25u64 {
        accumulator_fast.record_success(
            epoch + Duration::from_millis(index + 1),
            Duration::from_micros(80 + index),
        );
    }
    let scenario_key = format!("noregr_fail_{}", std::process::id());
    let mut baseline = LoadBaseline::from_load_metrics(
        &LoadMetrics::new(accumulator_fast.clone()),
        "fast",
        &scenario_key,
    )
    .expect("baseline");
    baseline.summary.throughput = 0.0;
    let path = resolve_load_baseline_path(&scenario_key, "fast");
    write_load_baseline_to_path(&baseline, &path).expect("write baseline");

    let accumulator_slow = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for index in 0..25u64 {
        accumulator_slow.record_success(
            epoch + Duration::from_millis(40 + index),
            Duration::from_millis(8),
        );
    }
    let current = LoadMetrics::new(accumulator_slow);
    // When we compare with 1% tolerance
    let error = current
        .assert("slow")
        .no_regression_from("fast", &scenario_key, Tolerance::percent(0.01))
        .expect_err("regression expected");

    // Then we fail with baseline_regression and a markdown table
    match error {
        ShakedownError::PerformanceAssertionFailed(details) => {
            assert_eq!(details.metric, "baseline_regression");
            let table = details
                .comparison_table
                .as_deref()
                .expect("comparison markdown");
            assert!(table.contains('|'));
            assert!(table.contains("p99") || table.contains("p50"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let _ = std::fs::remove_file(&path);
}
