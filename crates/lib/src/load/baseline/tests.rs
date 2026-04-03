//! Baseline JSON round-trip, path conventions, tolerance math, and markdown rendering.

use std::sync::Arc;
use std::time::Duration;

use crate::load::metrics::LoadMetricsAccumulator;
use crate::timeline::resolve_target_dir;

use super::{
    compare_load_metrics_to_baseline, decode_baseline_histogram, read_load_baseline_from_path,
    render_comparison_markdown, write_load_baseline_to_path, LoadBaseline, LoadMetrics,
    ReportMetrics, Tolerance, LOAD_BASELINE_SCHEMA_VERSION,
};

#[test]
fn load_baseline_round_trip_file_and_histogram() {
    // Given metrics with a known profile summary and latency spread
    let epoch = std::time::Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    accumulator.set_load_profile_summary("Constant(1s, 8 it/s)".to_string());
    for index in 0..24u64 {
        let completion = epoch + Duration::from_millis(index * 4);
        accumulator.record_success(completion, Duration::from_micros(300 + index * 25));
    }
    let metrics = LoadMetrics::new(accumulator);

    let temp = std::env::temp_dir().join(format!(
        "shakedown-load-baseline-test-{}.json",
        std::process::id()
    ));
    let baseline =
        LoadBaseline::from_load_metrics(&metrics, "steady_v1", "scenario_b").expect("build");
    // When we write JSON to a temp file and read it back
    write_load_baseline_to_path(&baseline, &temp).expect("write");
    let loaded = read_load_baseline_from_path(&temp).expect("read");

    // Then metadata and histogram quantiles match the live snapshot
    assert_eq!(loaded.schema_version, LOAD_BASELINE_SCHEMA_VERSION);
    assert_eq!(loaded.scenario_id, "scenario_b");
    assert_eq!(loaded.baseline_name, "steady_v1");
    assert_eq!(loaded.load_profile_summary, "Constant(1s, 8 it/s)");
    assert_eq!(loaded.summary.successful_operations, 24);
    assert_eq!(loaded.summary.total_iterations, 24);

    let restored = decode_baseline_histogram(&loaded).expect("histogram");
    assert_eq!(restored.len(), 24);
    let from_live = metrics.clone_latency_histogram_for_report();
    assert_eq!(restored.len(), from_live.len());
    assert_eq!(
        restored.value_at_quantile(0.99),
        from_live.value_at_quantile(0.99)
    );
    assert_eq!(
        restored.value_at_quantile(0.5),
        from_live.value_at_quantile(0.5)
    );

    let _ = std::fs::remove_file(&temp);
}

#[test]
fn save_baseline_uses_target_shakedown_baselines_path() {
    // Given trivial metrics
    let accumulator = Arc::new(LoadMetricsAccumulator::default());
    accumulator.record_success(std::time::Instant::now(), Duration::from_micros(120));
    let metrics = LoadMetrics::new(accumulator);
    // When we save under scenario `s2` and name `mainline`
    let path = metrics
        .save_baseline("mainline", "s2")
        .expect("save_baseline");
    // Then the path is under target/shakedown/baselines and reloads
    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some("mainline.json")
    );
    assert_eq!(
        path.parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str()),
        Some("s2")
    );
    assert!(
        path.strip_prefix(resolve_target_dir()).is_ok(),
        "baseline path should live under the workspace target directory"
    );
    let loaded = LoadBaseline::load("s2", "mainline").expect("load");
    assert_eq!(loaded.baseline_name, "mainline");
    assert_eq!(loaded.scenario_id, "s2");
    let _ = std::fs::remove_file(&path);
}

fn synthetic_baseline(summary: ReportMetrics, name: &str) -> LoadBaseline {
    LoadBaseline {
        schema_version: LOAD_BASELINE_SCHEMA_VERSION,
        scenario_id: "unit".to_string(),
        baseline_name: name.to_string(),
        created_at: "2026-01-01T00:00:00Z".to_string(),
        load_profile_summary: "test".to_string(),
        summary,
        histogram_v2_base64: String::new(),
    }
}

fn metrics_with_latency_and_throughput(latency: Duration, throughput_scale: u64) -> LoadMetrics {
    let epoch = std::time::Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for index in 0..throughput_scale {
        let completion = epoch + Duration::from_millis(index + 10);
        accumulator.record_success(completion, latency);
    }
    LoadMetrics::new(accumulator)
}

#[test]
fn regression_not_detected_within_tolerance() {
    // Given a synthetic baseline and current run with similar latencies
    let baseline_summary = ReportMetrics {
        total_iterations: 50,
        successful_operations: 50,
        failed_operations: 0,
        dropped_iterations: 0,
        error_rate: 0.0,
        throughput: 0.0,
        p50_ns: Duration::from_millis(2).as_nanos() as u64,
        p90_ns: Duration::from_millis(3).as_nanos() as u64,
        p95_ns: Duration::from_millis(4).as_nanos() as u64,
        p99_ns: Duration::from_millis(5).as_nanos() as u64,
        min_latency_ns: 1,
        max_latency_ns: Duration::from_millis(6).as_nanos() as u64,
    };
    let baseline = synthetic_baseline(baseline_summary, "steady");
    let current = metrics_with_latency_and_throughput(Duration::from_millis(2), 50);
    let tolerance = Tolerance::percent(0.10);
    // When we compare at 10% tolerance
    let result = compare_load_metrics_to_baseline(&current, &baseline, &tolerance);
    // Then comparison passes
    assert!(
        result.passed,
        "similar run should pass: {:?}",
        result.metrics
    );
}

#[test]
fn regression_detected_when_latency_exceeds_tolerance() {
    // Given a fast baseline summary and a 10ms current run
    let baseline_summary = ReportMetrics {
        total_iterations: 20,
        successful_operations: 20,
        failed_operations: 0,
        dropped_iterations: 0,
        error_rate: 0.0,
        throughput: 0.0,
        p50_ns: Duration::from_millis(1).as_nanos() as u64,
        p90_ns: Duration::from_millis(2).as_nanos() as u64,
        p95_ns: Duration::from_millis(2).as_nanos() as u64,
        p99_ns: Duration::from_millis(3).as_nanos() as u64,
        min_latency_ns: 1,
        max_latency_ns: Duration::from_millis(4).as_nanos() as u64,
    };
    let baseline = synthetic_baseline(baseline_summary, "fast");
    let current = metrics_with_latency_and_throughput(Duration::from_millis(10), 20);
    let tolerance = Tolerance::percent(0.05);
    // When we compare at 5% tolerance
    let result = compare_load_metrics_to_baseline(&current, &baseline, &tolerance);
    // Then p99 fails the comparison
    assert!(!result.passed);
    let p99_row = result
        .metrics
        .iter()
        .find(|row| row.name == "p99")
        .expect("p99 row");
    assert!(!p99_row.passed);
}

#[test]
fn error_rate_near_zero_uses_absolute_floor() {
    // Given a baseline with ~0.1% error rate and tolerance with 1% floor
    let baseline_summary = ReportMetrics {
        total_iterations: 100,
        successful_operations: 99,
        failed_operations: 1,
        dropped_iterations: 0,
        error_rate: 0.001,
        throughput: 0.0,
        p50_ns: 1000,
        p90_ns: 1000,
        p95_ns: 1000,
        p99_ns: 1000,
        min_latency_ns: 500,
        max_latency_ns: 2000,
    };
    let baseline = synthetic_baseline(baseline_summary, "low_err");
    let epoch = std::time::Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for index in 0..100u64 {
        let completion = epoch + Duration::from_millis(index + 1);
        accumulator.record_success(completion, Duration::from_micros(1));
    }
    accumulator.record_failure(epoch + Duration::from_millis(200), Duration::from_micros(1));
    let current = LoadMetrics::new(accumulator);
    let tolerance = Tolerance::percent(0.05).error_rate_floor(0.01);
    // When current has 1% errors
    let result = compare_load_metrics_to_baseline(&current, &baseline, &tolerance);
    // Then it passes the absolute ceiling
    assert!(
        result.passed,
        "1% error rate should pass 1% absolute ceiling: {:?}",
        result.metrics
    );

    // And when current has 20% errors
    let accumulator_bad = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for index in 0..80u64 {
        let completion = epoch + Duration::from_millis(index + 1);
        accumulator_bad.record_success(completion, Duration::from_micros(1));
    }
    for index in 0..20u64 {
        let completion = epoch + Duration::from_millis(100 + index);
        accumulator_bad.record_failure(completion, Duration::from_micros(1));
    }
    let current_bad = LoadMetrics::new(accumulator_bad);
    let result_bad = compare_load_metrics_to_baseline(&current_bad, &baseline, &tolerance);
    // Then error_rate row fails
    assert!(!result_bad.passed);
    let err_row = result_bad
        .metrics
        .iter()
        .find(|row| row.name == "error_rate")
        .expect("error_rate row");
    assert!(!err_row.passed);
}

#[test]
fn render_comparison_markdown_includes_table_and_verdicts() {
    // Given a failing comparison result (slow current vs tight baseline)
    let baseline_summary = ReportMetrics {
        total_iterations: 10,
        successful_operations: 10,
        failed_operations: 0,
        dropped_iterations: 0,
        error_rate: 0.0,
        throughput: 0.0,
        p50_ns: 1_000,
        p90_ns: 2_000,
        p95_ns: 2_000,
        p99_ns: 3_000,
        min_latency_ns: 500,
        max_latency_ns: 4_000,
    };
    let baseline = synthetic_baseline(baseline_summary, "m");
    let current = metrics_with_latency_and_throughput(Duration::from_secs(1), 10);
    let result = compare_load_metrics_to_baseline(&current, &baseline, &Tolerance::percent(0.01));
    // When we render markdown
    let md = render_comparison_markdown(&result);
    // Then the table has headers, metrics, and pass/fail marks
    assert!(md.contains("| metric |"));
    assert!(md.contains("p99"));
    assert!(md.contains("throughput"));
    assert!(md.contains("✗") || md.contains('✓'));
}
