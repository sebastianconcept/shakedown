//! Named performance baseline artifacts under `target/shakedown/baselines/`.
//! Regression detection vs saved baselines ([`Tolerance`], [`compare_load_metrics_to_baseline`]).

use std::fs::File;
#[cfg(test)]
use std::io::Cursor;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
#[cfg(test)]
use hdrhistogram::serialization::Deserializer;
use hdrhistogram::serialization::{Serializer, V2Serializer};
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::error::InternalError;
use crate::timeline::{resolve_target_dir, sanitize_scenario_id_for_filename};
use crate::ShakedownError;

use super::metrics::LoadMetrics;
use super::{ReportComparison, ReportComparisonMetric, ReportMetrics};

/// Schema version for [`LoadBaseline::schema_version`] (bump when fields are incompatible).
pub(crate) const LOAD_BASELINE_SCHEMA_VERSION: u32 = 1;

/// Serializable performance snapshot for regression comparison.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoadBaseline {
    pub schema_version: u32,
    pub scenario_id: String,
    pub baseline_name: String,
    /// When the baseline was captured (RFC 3339, UTC).
    pub created_at: String,
    pub load_profile_summary: String,
    pub summary: ReportMetrics,
    /// HdrHistogram V2 payload, base64-encoded.
    pub histogram_v2_base64: String,
}

impl LoadBaseline {
    /// Build a baseline from a [`LoadMetrics`] snapshot (histogram matches aggregate view / warmup rules).
    pub fn from_load_metrics(
        metrics: &LoadMetrics,
        baseline_name: impl Into<String>,
        scenario_id: impl Into<String>,
    ) -> Result<Self, ShakedownError> {
        let baseline_name = baseline_name.into();
        let scenario_id = scenario_id.into();
        let inner = metrics.accumulator();
        let load_profile_summary = inner
            .load_profile_summary()
            .unwrap_or_else(|| "(load profile summary not recorded)".to_string());
        let histogram = metrics.clone_latency_histogram_for_report();
        let mut raw = Vec::new();
        V2Serializer::new()
            .serialize(&histogram, &mut raw)
            .map_err(|error| {
                ShakedownError::Internal(InternalError::Bug(format!(
                    "hdrhistogram V2 serialize (baseline): {error}"
                )))
            })?;
        let histogram_v2_base64 = STANDARD.encode(raw);
        let created_at = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string());
        Ok(Self {
            schema_version: LOAD_BASELINE_SCHEMA_VERSION,
            scenario_id,
            baseline_name,
            created_at,
            load_profile_summary,
            summary: ReportMetrics::from(metrics),
            histogram_v2_base64,
        })
    }

    /// Read a baseline from `target/shakedown/baselines/{scenario_id}/{name}.json`.
    pub fn load(
        scenario_id: impl AsRef<str>,
        baseline_name: impl AsRef<str>,
    ) -> Result<Self, ShakedownError> {
        let path = resolve_load_baseline_path(scenario_id.as_ref(), baseline_name.as_ref());
        read_load_baseline_from_path(&path)
    }
}

/// Relative tolerance for [`LoadMetricsAssertBuilder::no_regression_from`](crate::load::LoadMetricsAssertBuilder::no_regression_from).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tolerance {
    /// Relative fraction (e.g. `0.05` for 5% slack on latency increase, throughput decrease, or error-rate increase).
    pub percent: f64,
    /// When baseline error rate is below this, the current run is compared against this absolute ceiling instead of a relative blow-up from ~0.
    pub error_rate_floor: f64,
}

impl Tolerance {
    /// Relative tolerance as a fraction (e.g. `0.05` for 5%). Must be finite and non-negative.
    pub fn percent(percent: f64) -> Self {
        Self {
            percent,
            error_rate_floor: 0.01,
        }
    }

    /// Set the absolute error-rate ceiling used when the baseline error rate is below this value (default `0.01` = 1%).
    pub fn error_rate_floor(mut self, floor: f64) -> Self {
        self.error_rate_floor = floor;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if !self.percent.is_finite() || self.percent < 0.0 {
            return Err(format!(
                "tolerance percent must be finite and ≥ 0 (got {})",
                self.percent
            ));
        }
        if !self.error_rate_floor.is_finite() || self.error_rate_floor < 0.0 {
            return Err(format!(
                "error_rate_floor must be finite and ≥ 0 (got {})",
                self.error_rate_floor
            ));
        }
        Ok(())
    }
}

/// One row in a [`ComparisonResult`] / Markdown regression table.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricComparison {
    /// Metric identifier (`p50`, `p95`, `p99`, `throughput`, `error_rate`).
    pub name: String,
    pub baseline_value: String,
    pub current_value: String,
    /// Signed relative delta when baseline is non-zero (`(current - baseline) / baseline`).
    pub change_pct: f64,
    /// Relative tolerance fraction used for this comparison direction (from [`Tolerance::percent`]).
    pub threshold_pct: f64,
    pub passed: bool,
    /// Human-readable bound that was evaluated.
    pub limit_description: String,
}

/// Outcome of comparing live [`LoadMetrics`] to a [`LoadBaseline`].
#[derive(Debug, Clone, PartialEq)]
pub struct ComparisonResult {
    pub baseline_name: String,
    pub tolerance: Tolerance,
    pub metrics: Vec<MetricComparison>,
    pub passed: bool,
}

impl From<&ComparisonResult> for ReportComparison {
    fn from(value: &ComparisonResult) -> Self {
        Self {
            baseline_name: value.baseline_name.clone(),
            passed: value.passed,
            tolerance_percent: value.tolerance.percent,
            metrics: value
                .metrics
                .iter()
                .map(|row| ReportComparisonMetric {
                    name: row.name.clone(),
                    baseline_value: row.baseline_value.clone(),
                    current_value: row.current_value.clone(),
                    change_pct: row.change_pct,
                    threshold_pct: row.threshold_pct,
                    passed: row.passed,
                })
                .collect(),
        }
    }
}

/// Compare steady-state aggregates from `current` to frozen `baseline` numbers using `tolerance`.
pub fn compare_load_metrics_to_baseline(
    current: &LoadMetrics,
    baseline: &LoadBaseline,
    tolerance: &Tolerance,
) -> ComparisonResult {
    let summary = &baseline.summary;
    let mut rows = Vec::new();
    let threshold_pct = tolerance.percent;

    rows.push(compare_latency_ns_row(
        "p50",
        summary.p50_ns,
        current.p50().as_nanos() as u64,
        threshold_pct,
    ));
    rows.push(compare_latency_ns_row(
        "p95",
        summary.p95_ns,
        current.p95().as_nanos() as u64,
        threshold_pct,
    ));
    rows.push(compare_latency_ns_row(
        "p99",
        summary.p99_ns,
        current.p99().as_nanos() as u64,
        threshold_pct,
    ));
    rows.push(compare_throughput_row(
        summary.throughput,
        current.throughput(),
        threshold_pct,
    ));
    rows.push(compare_error_rate_row(
        summary.error_rate,
        current.error_rate(),
        tolerance,
    ));

    let passed = rows.iter().all(|row| row.passed);
    ComparisonResult {
        baseline_name: baseline.baseline_name.clone(),
        tolerance: *tolerance,
        metrics: rows,
        passed,
    }
}

fn compare_latency_ns_row(
    name: &'static str,
    baseline_ns: u64,
    current_ns: u64,
    tolerance_pct: f64,
) -> MetricComparison {
    let (passed, limit_description) = if baseline_ns == 0 {
        let pass = current_ns == 0;
        (pass, "≤ 0 (zero baseline)".to_string())
    } else {
        let allowed = ((baseline_ns as f64) * (1.0 + tolerance_pct))
            .clamp(0.0, u64::MAX as f64)
            .ceil() as u64;
        let pass = current_ns <= allowed;
        let limit = format!(
            "≤ {} (+{:.1}% vs baseline)",
            format_latency_ns(allowed),
            tolerance_pct * 100.0
        );
        (pass, limit)
    };
    let change_pct = relative_change_u64(baseline_ns, current_ns);
    MetricComparison {
        name: name.to_string(),
        baseline_value: format_latency_ns(baseline_ns),
        current_value: format_latency_ns(current_ns),
        change_pct,
        threshold_pct: tolerance_pct,
        passed,
        limit_description,
    }
}

fn compare_throughput_row(
    baseline_tp: f64,
    current_tp: f64,
    tolerance_pct: f64,
) -> MetricComparison {
    let (passed, limit_description) = if baseline_tp <= 1e-12 {
        (true, "n/a (zero baseline)".to_string())
    } else {
        let floor_tp = baseline_tp * (1.0 - tolerance_pct);
        let pass = current_tp >= floor_tp;
        let limit = format!(
            "≥ {:.2}/s (−{:.1}% vs baseline)",
            floor_tp,
            tolerance_pct * 100.0
        );
        (pass, limit)
    };
    MetricComparison {
        name: "throughput".to_string(),
        baseline_value: format_throughput(baseline_tp),
        current_value: format_throughput(current_tp),
        change_pct: relative_change_f64(baseline_tp, current_tp),
        threshold_pct: tolerance_pct,
        passed,
        limit_description,
    }
}

fn compare_error_rate_row(
    baseline_rate: f64,
    current_rate: f64,
    tolerance: &Tolerance,
) -> MetricComparison {
    let (passed, limit_description) = if baseline_rate < tolerance.error_rate_floor {
        let pass = current_rate <= tolerance.error_rate_floor;
        let limit = format!(
            "≤ {:.2}% absolute (baseline {:.2}% < {:.2}% floor)",
            tolerance.error_rate_floor * 100.0,
            baseline_rate * 100.0,
            tolerance.error_rate_floor * 100.0
        );
        (pass, limit)
    } else {
        let allowed = baseline_rate * (1.0 + tolerance.percent);
        let pass = current_rate <= allowed;
        let limit = format!(
            "≤ {:.2}% (baseline +{:.1}%)",
            allowed * 100.0,
            tolerance.percent * 100.0
        );
        (pass, limit)
    };
    MetricComparison {
        name: "error_rate".to_string(),
        baseline_value: format_rate_pct(baseline_rate),
        current_value: format_rate_pct(current_rate),
        change_pct: relative_change_f64(baseline_rate, current_rate),
        threshold_pct: tolerance.percent,
        passed,
        limit_description,
    }
}

fn relative_change_u64(baseline: u64, current: u64) -> f64 {
    if baseline > 0 {
        (current as f64 - baseline as f64) / baseline as f64
    } else if current == 0 {
        0.0
    } else {
        1.0
    }
}

fn relative_change_f64(baseline: f64, current: f64) -> f64 {
    if baseline.abs() > 1e-15 {
        (current - baseline) / baseline
    } else if current.abs() <= 1e-15 {
        0.0
    } else {
        1.0
    }
}

fn format_latency_ns(ns: u64) -> String {
    let duration = Duration::from_nanos(ns);
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

/// Markdown table of baseline vs current regression rows.
pub fn render_comparison_markdown(result: &ComparisonResult) -> String {
    let mut lines = Vec::new();
    lines.push("| metric | baseline | current | change % | limit | verdict |".to_string());
    lines.push("|--------|----------|---------|----------|-------|---------|".to_string());
    for row in &result.metrics {
        let verdict = if row.passed { "✓" } else { "✗" };
        let change = format_change_pct(row.change_pct);
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} |",
            row.name,
            row.baseline_value,
            row.current_value,
            change,
            row.limit_description.replace('|', "\\|"),
            verdict
        ));
    }
    lines.join("\n")
}

fn format_change_pct(change_pct: f64) -> String {
    if change_pct.is_finite() {
        format!("{:.1}%", change_pct * 100.0)
    } else {
        "n/a".to_string()
    }
}

/// Resolve `target/shakedown/baselines/{scenario_id}/{baseline_name}.json`.
pub(crate) fn resolve_load_baseline_path(scenario_id: &str, baseline_name: &str) -> PathBuf {
    let scenario_dir = sanitize_scenario_id_for_filename(scenario_id);
    let mut file_stem = sanitize_scenario_id_for_filename(baseline_name);
    if !file_stem.ends_with(".json") {
        file_stem.push_str(".json");
    }
    resolve_target_dir()
        .join("shakedown")
        .join("baselines")
        .join(scenario_dir)
        .join(file_stem)
}

/// Write pretty JSON for `baseline` to `path` (creates parent directories).
pub(crate) fn write_load_baseline_to_path(
    baseline: &LoadBaseline,
    path: &Path,
) -> Result<(), ShakedownError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            ShakedownError::Internal(InternalError::LoadArtifactIo {
                operation: "create baseline directory",
                detail: source.to_string(),
            })
        })?;
    }
    let file = File::create(path).map_err(|source| {
        ShakedownError::Internal(InternalError::LoadArtifactIo {
            operation: "create baseline file",
            detail: source.to_string(),
        })
    })?;
    serde_json::to_writer_pretty(BufWriter::new(file), baseline).map_err(|source| {
        ShakedownError::Internal(InternalError::LoadArtifactIo {
            operation: "serialize baseline JSON",
            detail: source.to_string(),
        })
    })
}

/// Read and deserialize a baseline written by [`write_load_baseline_to_path`].
pub(crate) fn read_load_baseline_from_path(path: &Path) -> Result<LoadBaseline, ShakedownError> {
    let file = File::open(path).map_err(|source| {
        ShakedownError::Internal(InternalError::LoadArtifactIo {
            operation: "open baseline file",
            detail: source.to_string(),
        })
    })?;
    let baseline: LoadBaseline =
        serde_json::from_reader(BufReader::new(file)).map_err(|source| {
            ShakedownError::Internal(InternalError::LoadArtifactIo {
                operation: "deserialize baseline JSON",
                detail: source.to_string(),
            })
        })?;
    Ok(baseline)
}

/// Persist [`LoadBaseline`] for `metrics` to the conventional baselines path.
pub(crate) fn save_baseline(
    metrics: &LoadMetrics,
    baseline_name: &str,
    scenario_id: &str,
) -> Result<PathBuf, ShakedownError> {
    let path = resolve_load_baseline_path(scenario_id, baseline_name);
    let baseline = LoadBaseline::from_load_metrics(metrics, baseline_name, scenario_id)?;
    write_load_baseline_to_path(&baseline, &path)?;
    Ok(path)
}

#[cfg(test)]
pub(super) fn decode_baseline_histogram(
    baseline: &LoadBaseline,
) -> Result<hdrhistogram::Histogram<u64>, ShakedownError> {
    let bytes = STANDARD
        .decode(baseline.histogram_v2_base64.as_bytes())
        .map_err(|error| {
            ShakedownError::Internal(InternalError::Bug(format!(
                "base64-decode load baseline histogram: {error}"
            )))
        })?;
    let mut cursor = Cursor::new(bytes);
    Deserializer::new()
        .deserialize(&mut cursor)
        .map_err(|error| {
            ShakedownError::Internal(InternalError::Bug(format!(
                "hdrhistogram V2 deserialize (baseline): {error}"
            )))
        })
}

#[cfg(test)]
mod tests;
