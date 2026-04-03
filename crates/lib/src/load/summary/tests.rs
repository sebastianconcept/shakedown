//! Tests for plain and styled summary rendering (assertions and baseline blocks).

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::super::baseline::{ComparisonResult, MetricComparison, Tolerance};
use super::super::metrics::LoadMetricsAccumulator;
use super::{
    render_summary, render_summary_styled, render_summary_with_comparison,
    render_summary_with_verdicts, AssertionVerdict, LoadMetrics,
};

fn sample_metrics() -> LoadMetrics {
    let epoch = Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for index in 0..20u64 {
        let completion = epoch + Duration::from_millis(index);
        accumulator.record_success(completion, Duration::from_micros(800 + index * 50));
    }
    LoadMetrics::new(accumulator)
}

#[test]
fn render_summary_contains_expected_fields() {
    // Given sample latency metrics
    let metrics = sample_metrics();
    // When we render a plain summary for a named load
    let text = render_summary("my-load", &metrics);
    // Then expected section labels appear
    assert!(text.contains("my-load"));
    assert!(text.contains("iterations"));
    assert!(text.contains("successful"));
    assert!(text.contains("failed"));
    assert!(text.contains("dropped"));
    assert!(text.contains("error rate"));
    assert!(text.contains("p50"));
    assert!(text.contains("p95"));
    assert!(text.contains("p99"));
    assert!(text.contains("min"));
    assert!(text.contains("max"));
    assert!(text.contains("Load Summary"));
}

#[test]
fn render_summary_plain_has_no_ansi() {
    // Given plain-text summary rendering
    let text = render_summary("x", &sample_metrics());
    // Then no ANSI escape sequences are present
    assert!(!text.contains('\x1b'));
}

#[test]
fn render_summary_styled_with_color_has_escape_codes() {
    // Given styled rendering with color enabled
    let text = render_summary_styled("x", &sample_metrics(), None, None, true);
    // Then escape codes are present
    assert!(text.contains('\x1b'));
}

#[test]
fn render_summary_includes_baseline_comparison_section() {
    // Given metrics and a synthetic passing comparison result
    let metrics = sample_metrics();
    let comparison = ComparisonResult {
        baseline_name: "main".to_string(),
        tolerance: Tolerance::percent(0.05),
        passed: true,
        metrics: vec![MetricComparison {
            name: "p99".to_string(),
            baseline_value: "1.00ms".to_string(),
            current_value: "1.05ms".to_string(),
            change_pct: 0.05,
            threshold_pct: 0.05,
            passed: true,
            limit_description: "≤ 1.10ms (+5.0% vs baseline)".to_string(),
        }],
    };
    // When we render with comparison block
    let text = render_summary_with_comparison("checkout", &metrics, &[], &comparison);
    // Then baseline section and row appear
    assert!(text.contains("Baseline comparison"));
    assert!(text.contains("main"));
    assert!(text.contains("p99"));
    assert!(text.contains("✓"));
}

#[test]
fn render_summary_with_verdicts_shows_assertion_section() {
    // Given mixed pass/fail assertion verdicts
    let metrics = sample_metrics();
    let verdicts = vec![
        AssertionVerdict {
            metric: "p99".to_string(),
            measured: "12.00ms".to_string(),
            threshold: "<= 50.00ms".to_string(),
            passed: true,
        },
        AssertionVerdict {
            metric: "error_rate".to_string(),
            measured: "0.00%".to_string(),
            threshold: "<= 1.00%".to_string(),
            passed: false,
        },
    ];
    // When we render with verdict rows
    let text = render_summary_with_verdicts("checkout", &metrics, &verdicts);
    // Then Assertions section shows both checkmarks
    assert!(text.contains("Assertions"));
    assert!(text.contains("✓"));
    assert!(text.contains("✗"));
    assert!(text.contains("p99"));
    assert!(text.contains("error_rate"));
    assert!(text.contains("measured:"));
    assert!(text.contains("threshold:"));
}
