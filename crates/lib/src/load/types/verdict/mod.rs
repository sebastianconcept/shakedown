//! Assertion and report DTOs shared across summaries, JSON reports, and the metrics assert builder.
//!
//! [`AssertionVerdict`] is the in-memory shape used while chaining checks; the `Report*` types
//! mirror optional sections in [`super::super::report::LoadRunReport`] for serialization.

/// One evaluated performance check for terminal summary / JSON report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssertionVerdict {
    /// Metric name (e.g. `p99`, `throughput`).
    pub metric: String,
    /// Observed value as displayed to the user.
    pub measured: String,
    /// Bound that was asserted.
    pub threshold: String,
    /// Whether the check passed.
    pub passed: bool,
}

/// One assertion row in a [`super::super::report::LoadRunReport`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReportAssertion {
    /// Metric name (e.g. `p99`, `throughput`).
    pub metric: String,
    /// Observed value as displayed to the user.
    pub measured: String,
    /// Bound that was asserted.
    pub threshold: String,
    /// Whether the check passed.
    pub passed: bool,
}

impl From<&AssertionVerdict> for ReportAssertion {
    fn from(verdict: &AssertionVerdict) -> Self {
        Self {
            metric: verdict.metric.clone(),
            measured: verdict.measured.clone(),
            threshold: verdict.threshold.clone(),
            passed: verdict.passed,
        }
    }
}

/// Baseline comparison block in a [`super::super::report::LoadRunReport`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReportComparison {
    /// Baseline artifact name compared against.
    pub baseline_name: String,
    /// Whether every compared metric stayed within tolerance.
    pub passed: bool,
    /// Configured relative tolerance (fraction, e.g. `0.05` for 5%).
    pub tolerance_percent: f64,
    /// Per-metric old vs new rows.
    pub metrics: Vec<ReportComparisonMetric>,
}

/// One metric row inside [`ReportComparison`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReportComparisonMetric {
    /// Metric identifier (e.g. `p99`, `throughput`).
    pub name: String,
    /// Baseline value as a string (same formatting as summaries).
    pub baseline_value: String,
    /// Current run value as a string.
    pub current_value: String,
    /// Relative change as a fraction (signed).
    pub change_pct: f64,
    /// Allowed relative tolerance as a fraction for this comparison direction.
    pub threshold_pct: f64,
    /// Whether this row passed.
    pub passed: bool,
}
