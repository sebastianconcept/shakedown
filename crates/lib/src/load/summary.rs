//! Terminal-friendly load run summaries (US-004).

use super::baseline::ComparisonResult;
use super::metrics::LoadMetrics;
use super::AssertionVerdict;

mod ansi {
    pub const BOLD: &str = "\x1b[1m";
    pub const DIM: &str = "\x1b[2m";
    pub const GREEN: &str = "\x1b[32m";
    pub const RED: &str = "\x1b[31m";
    pub const RESET: &str = "\x1b[0m";
}

const LINE_WIDTH: usize = 58;

/// `true` unless `SHAKEDOWN_LOAD_SUMMARY` is exactly `"0"`.
pub(crate) fn load_summary_print_enabled() -> bool {
    !matches!(
        std::env::var("SHAKEDOWN_LOAD_SUMMARY"),
        Ok(value) if value == "0"
    )
}

/// Plain-text summary (no ANSI): load name, counts, error rate, throughput, percentiles, min/max.
pub fn render_summary(load_name: &str, metrics: &LoadMetrics) -> String {
    render_summary_styled(load_name, metrics, None, None, false)
}

/// Plain-text summary including per-assertion verdict rows when `verdicts` is non-empty.
pub fn render_summary_with_verdicts(
    load_name: &str,
    metrics: &LoadMetrics,
    verdicts: &[AssertionVerdict],
) -> String {
    render_summary_styled(load_name, metrics, Some(verdicts), None, false)
}

/// Summary with verdict rows and an optional baseline comparison block (US-007).
pub fn render_summary_with_comparison(
    load_name: &str,
    metrics: &LoadMetrics,
    verdicts: &[AssertionVerdict],
    comparison: &ComparisonResult,
) -> String {
    render_summary_styled(load_name, metrics, Some(verdicts), Some(comparison), false)
}

/// Summary with optional ANSI styling (`use_color` should be `stderr.is_terminal()` at call sites).
pub(crate) fn render_summary_styled(
    load_name: &str,
    metrics: &LoadMetrics,
    verdicts: Option<&[AssertionVerdict]>,
    comparison: Option<&ComparisonResult>,
    use_color: bool,
) -> String {
    let mut lines = Vec::new();
    let title = format!("Load Summary: {load_name}");
    lines.push(format_rule_line(&title, use_color));
    lines.push(String::new());

    let total = metrics.total_iterations();
    let throughput = metrics.throughput();
    let iterations_value = format!("{} ({})", total, format_throughput(throughput));
    lines.push(row_plain("iterations", &iterations_value));
    lines.push(row_plain(
        "successful",
        &format!("{}", metrics.successful_operations()),
    ));
    lines.push(row_plain(
        "failed",
        &format!("{}", metrics.failed_operations()),
    ));
    lines.push(row_plain(
        "dropped",
        &format!("{}", metrics.dropped_iterations()),
    ));
    lines.push(row_plain(
        "error rate",
        &format_rate_pct(metrics.error_rate()),
    ));
    lines.push(String::new());

    lines.push(row_plain("p50", &format_duration(metrics.p50())));
    lines.push(row_plain("p95", &format_duration(metrics.p95())));
    lines.push(row_plain("p99", &format_duration(metrics.p99())));
    lines.push(row_plain("min", &format_duration(metrics.min_latency())));
    lines.push(row_plain("max", &format_duration(metrics.max_latency())));

    if let Some(rows) = verdicts {
        if !rows.is_empty() {
            lines.push(String::new());
            lines.push(indent_line("Assertions:", use_color, true));
            for verdict in rows {
                let rest = format!(
                    " {} (measured: {}, threshold: {})",
                    verdict.metric, verdict.measured, verdict.threshold
                );
                let line = if use_color {
                    let (color, mark) = if verdict.passed {
                        (ansi::GREEN, "✓")
                    } else {
                        (ansi::RED, "✗")
                    };
                    format!("    {color}{mark}{}{rest}", ansi::RESET)
                } else {
                    let mark = if verdict.passed { "✓" } else { "✗" };
                    format!("    {mark}{rest}")
                };
                lines.push(line);
            }
        }
    }

    if let Some(result) = comparison {
        lines.push(String::new());
        lines.push(indent_line("Baseline comparison:", use_color, true));
        lines.push(format!(
            "    baseline '{}'  (tolerance ±{:.1}% rel; err floor {:.2}%)",
            result.baseline_name,
            result.tolerance.percent * 100.0,
            result.tolerance.error_rate_floor * 100.0
        ));
        for row in &result.metrics {
            let verdict = if row.passed { "✓" } else { "✗" };
            let line = format!(
                "    {verdict} {}  {} → {}  ({})",
                row.name, row.baseline_value, row.current_value, row.limit_description
            );
            lines.push(line);
        }
    }

    lines.push(String::new());
    lines.push(format_closing_rule(use_color));
    lines.join("\n")
}

fn format_rule_line(title: &str, use_color: bool) -> String {
    let decorated = if use_color {
        format!("{}{title}{}", ansi::BOLD, ansi::RESET)
    } else {
        title.to_string()
    };
    let inner_len = 4 + title.len();
    let dash_count = LINE_WIDTH.saturating_sub(inner_len);
    let dashes = "─".repeat(dash_count.max(8));
    format!("──── {decorated} {dashes}")
}

fn format_closing_rule(use_color: bool) -> String {
    let dashes = "─".repeat(LINE_WIDTH);
    if use_color {
        format!("{}{dashes}{}", ansi::DIM, ansi::RESET)
    } else {
        dashes
    }
}

const LABEL_WIDTH: usize = 14;

fn row_plain(label: &str, value: &str) -> String {
    let prefix = "  ";
    let dots = ".".repeat(LABEL_WIDTH.saturating_sub(label.len()).max(1));
    format!("{prefix}{label} {dots} {value}")
}

fn indent_line(text: &str, use_color: bool, bold: bool) -> String {
    let prefix = "  ";
    if use_color && bold {
        format!("{prefix}{}{text}{}", ansi::BOLD, ansi::RESET)
    } else {
        format!("{prefix}{text}")
    }
}

fn format_duration(duration: std::time::Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds >= 1.0 {
        format!("{seconds:.2}s")
    } else if duration.subsec_millis() > 0 || seconds >= 0.001 {
        format!("{:.2}ms", seconds * 1000.0)
    } else if duration.subsec_micros() > 0 {
        format!("{}µs", duration.subsec_micros())
    } else {
        format!("{duration:?}")
    }
}

fn format_throughput(ops_per_s: f64) -> String {
    format!("{ops_per_s:.2}/s")
}

fn format_rate_pct(rate: f64) -> String {
    format!("{:.2}%", rate * 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::super::metrics::LoadMetricsAccumulator;

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
        let metrics = sample_metrics();
        let text = render_summary("my-load", &metrics);
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
        let text = render_summary("x", &sample_metrics());
        assert!(!text.contains('\x1b'));
    }

    #[test]
    fn render_summary_styled_with_color_has_escape_codes() {
        let text = render_summary_styled("x", &sample_metrics(), None, None, true);
        assert!(text.contains('\x1b'));
    }

    #[test]
    fn render_summary_includes_baseline_comparison_section() {
        use super::super::baseline::{ComparisonResult, MetricComparison, Tolerance};

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
        let text = render_summary_with_comparison("checkout", &metrics, &[], &comparison);
        assert!(text.contains("Baseline comparison"));
        assert!(text.contains("main"));
        assert!(text.contains("p99"));
        assert!(text.contains("✓"));
    }

    #[test]
    fn render_summary_with_verdicts_shows_assertion_section() {
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
        let text = render_summary_with_verdicts("checkout", &metrics, &verdicts);
        assert!(text.contains("Assertions"));
        assert!(text.contains("✓"));
        assert!(text.contains("✗"));
        assert!(text.contains("p99"));
        assert!(text.contains("error_rate"));
        assert!(text.contains("measured:"));
        assert!(text.contains("threshold:"));
    }
}
