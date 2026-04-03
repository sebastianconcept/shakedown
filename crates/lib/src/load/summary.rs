//! Terminal-friendly load run summaries.

use super::baseline::ComparisonResult;
use super::fmt::{format_duration, format_rate_pct, format_throughput};
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

/// Summary with verdict rows and an optional baseline comparison block.
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

#[cfg(test)]
mod tests;
