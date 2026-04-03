//! JSON report round-trip and path convention tests.

use std::sync::Arc;
use std::time::Duration;

use crate::load::metrics::LoadMetricsAccumulator;
use crate::timeline::resolve_target_dir;

use super::{
    decode_report_histogram, read_load_run_report_from_path, write_load_run_report_to_path,
    write_report, LoadMetrics, LoadRunReport, ReportAssertion, ReportComparison,
    LOAD_RUN_REPORT_SCHEMA_VERSION,
};

#[test]
fn load_run_report_round_trip_file_and_histogram() {
    // Given metrics with assertions/comparison metadata attached
    let epoch = std::time::Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    accumulator.set_load_profile_summary("Constant(1s, 10 it/s)".to_string());
    for index in 0..30u64 {
        let completion = epoch + Duration::from_millis(index * 3);
        accumulator.record_success(completion, Duration::from_micros(400 + index * 20));
    }
    let metrics = LoadMetrics::new(accumulator);
    metrics.set_report_assertions(vec![ReportAssertion {
        metric: "p99".to_string(),
        measured: "1.20ms".to_string(),
        threshold: "<= 5.00ms".to_string(),
        passed: true,
    }]);
    metrics.set_report_comparison(ReportComparison {
        baseline_name: "main".to_string(),
        passed: true,
        tolerance_percent: 0.05,
        metrics: vec![crate::load::ReportComparisonMetric {
            name: "p99".to_string(),
            baseline_value: "1.00ms".to_string(),
            current_value: "1.10ms".to_string(),
            change_pct: 0.1,
            threshold_pct: 0.05,
            passed: true,
        }],
    });

    let temp = std::env::temp_dir().join(format!(
        "shakedown-load-report-test-{}.json",
        std::process::id()
    ));
    let report =
        LoadRunReport::from_load_metrics(&metrics, "checkout", "scenario_a").expect("build report");
    // When we write JSON to a temp file and read it back
    write_load_run_report_to_path(&report, &temp).expect("write");
    let loaded = read_load_run_report_from_path(&temp).expect("read");

    // Then scalar fields, optional blocks, and histogram length match
    assert_eq!(loaded.schema_version, LOAD_RUN_REPORT_SCHEMA_VERSION);
    assert_eq!(loaded.scenario_id, "scenario_a");
    assert_eq!(loaded.load_name, "checkout");
    assert_eq!(loaded.load_profile_summary, "Constant(1s, 10 it/s)");
    assert_eq!(loaded.metrics.successful_operations, 30);
    assert_eq!(loaded.metrics.total_iterations, 30);
    assert!(loaded
        .assertions
        .as_ref()
        .is_some_and(|rows| rows.len() == 1));
    assert!(loaded
        .comparison
        .as_ref()
        .is_some_and(|comparison| comparison.passed));

    let restored = decode_report_histogram(&loaded).expect("histogram");
    assert_eq!(restored.len(), 30);
    let from_live = metrics.clone_latency_histogram_for_report();
    assert_eq!(restored.len(), from_live.len());

    let _ = std::fs::remove_file(&temp);
}

#[test]
fn write_report_uses_target_shakedown_reports_path() {
    // Given minimal metrics
    let accumulator = Arc::new(LoadMetricsAccumulator::default());
    accumulator.record_success(std::time::Instant::now(), Duration::from_micros(100));
    let metrics = LoadMetrics::new(accumulator);
    // When we write via the conventional API
    let path = write_report(&metrics, "my_load", "s1").expect("write_report");
    // Then the path is under target/shakedown/reports and round-trips
    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some("my_load.json")
    );
    assert_eq!(
        path.parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str()),
        Some("s1")
    );
    assert!(
        path.strip_prefix(resolve_target_dir()).is_ok(),
        "report path should live under the workspace target directory"
    );
    let loaded = read_load_run_report_from_path(&path).expect("read back");
    assert_eq!(loaded.load_name, "my_load");
    let _ = std::fs::remove_file(&path);
}
