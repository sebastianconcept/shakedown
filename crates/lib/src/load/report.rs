//! JSON load run reports under `target/shakedown/reports/`.

use std::fs::File;
#[cfg(test)]
use std::io::Cursor;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

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
use super::{ReportAssertion, ReportComparison};

/// Schema version for [`LoadRunReport::schema_version`] (bump when fields are incompatible).
pub(crate) const LOAD_RUN_REPORT_SCHEMA_VERSION: u32 = 1;

/// Scalar metrics embedded in a [`LoadRunReport`] (nanoseconds for latency fields).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReportMetrics {
    /// All arrivals: successes, failures, and scheduling drops.
    pub total_iterations: u64,
    /// Iterations that completed without returning an error.
    pub successful_operations: u64,
    /// Iterations that returned an error.
    pub failed_operations: u64,
    /// Iterations dropped by the open-model scheduler (backpressure saturation).
    pub dropped_iterations: u64,
    /// Failed / completed ratio (0..1).
    pub error_rate: f64,
    /// Completed iterations per second (steady-state window when warmup is configured).
    pub throughput: f64,
    /// 50th percentile latency in nanoseconds.
    pub p50_ns: u64,
    /// 90th percentile latency in nanoseconds.
    pub p90_ns: u64,
    /// 95th percentile latency in nanoseconds.
    pub p95_ns: u64,
    /// 99th percentile latency in nanoseconds.
    pub p99_ns: u64,
    /// Minimum recorded latency in nanoseconds.
    pub min_latency_ns: u64,
    /// Maximum recorded latency in nanoseconds.
    pub max_latency_ns: u64,
}

impl From<&LoadMetrics> for ReportMetrics {
    fn from(metrics: &LoadMetrics) -> Self {
        Self {
            total_iterations: metrics.total_iterations(),
            successful_operations: metrics.successful_operations(),
            failed_operations: metrics.failed_operations(),
            dropped_iterations: metrics.dropped_iterations(),
            error_rate: metrics.error_rate(),
            throughput: metrics.throughput(),
            p50_ns: metrics.p50().as_nanos() as u64,
            p90_ns: metrics.p90().as_nanos() as u64,
            p95_ns: metrics.p95().as_nanos() as u64,
            p99_ns: metrics.p99().as_nanos() as u64,
            min_latency_ns: metrics.min_latency().as_nanos() as u64,
            max_latency_ns: metrics.max_latency().as_nanos() as u64,
        }
    }
}

/// Machine-readable record of a single load run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoadRunReport {
    /// Format version; bump when the JSON shape changes incompatibly.
    pub schema_version: u32,
    /// Scenario identifier under which this load run executed.
    pub scenario_id: String,
    /// Author-supplied label for this load profile (from [`crate::load::LoadHandle`]).
    pub load_name: String,
    /// Wall time when the report was built (RFC 3339, UTC).
    pub timestamp: String,
    /// Wall milliseconds from load accumulator origin until report build.
    pub duration_ms: u64,
    /// Human-readable description of the load profile phases.
    pub load_profile_summary: String,
    /// Scalar metric snapshot for this run.
    pub metrics: ReportMetrics,
    /// HdrHistogram V2 payload, base64-encoded.
    pub histogram_v2_base64: String,
    /// Assertion verdicts when [`crate::load::LoadMetricsAssertBuilder::store_for_report`] was called.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assertions: Option<Vec<ReportAssertion>>,
    /// Baseline comparison results when [`crate::load::LoadMetricsAssertBuilder::no_regression_from`] was called.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison: Option<ReportComparison>,
}

impl LoadRunReport {
    /// Build a report from a live [`LoadMetrics`] snapshot (histogram matches aggregate view / warmup rules).
    pub fn from_load_metrics(
        metrics: &LoadMetrics,
        load_name: impl Into<String>,
        scenario_id: impl Into<String>,
    ) -> Result<Self, ShakedownError> {
        let load_name = load_name.into();
        let scenario_id = scenario_id.into();
        let inner = metrics.accumulator();
        let duration_ms = inner
            .wall_clock_origin()
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let load_profile_summary = inner
            .load_profile_summary()
            .unwrap_or_else(|| "(load profile summary not recorded)".to_string());
        let histogram = metrics.clone_latency_histogram_for_report();
        let mut raw = Vec::new();
        V2Serializer::new()
            .serialize(&histogram, &mut raw)
            .map_err(|error| {
                ShakedownError::Internal(InternalError::Bug(format!(
                    "hdrhistogram V2 serialize: {error}"
                )))
            })?;
        let histogram_v2_base64 = STANDARD.encode(raw);
        let timestamp = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string());
        let assertions = inner.report_assertions_snapshot();
        let comparison = inner.report_comparison_snapshot();
        Ok(Self {
            schema_version: LOAD_RUN_REPORT_SCHEMA_VERSION,
            scenario_id,
            load_name,
            timestamp,
            duration_ms,
            load_profile_summary,
            metrics: ReportMetrics::from(metrics),
            histogram_v2_base64,
            assertions,
            comparison,
        })
    }
}

/// Resolve `target/shakedown/reports/{scenario_id}/{load_name}.json`.
pub(crate) fn resolve_load_report_path(scenario_id: &str, load_name: &str) -> PathBuf {
    let scenario_dir = sanitize_scenario_id_for_filename(scenario_id);
    let mut file_stem = sanitize_scenario_id_for_filename(load_name);
    if !file_stem.ends_with(".json") {
        file_stem.push_str(".json");
    }
    resolve_target_dir()
        .join("shakedown")
        .join("reports")
        .join(scenario_dir)
        .join(file_stem)
}

/// Write pretty JSON for `report` to `path` (creates parent directories).
pub(crate) fn write_load_run_report_to_path(
    report: &LoadRunReport,
    path: &Path,
) -> Result<(), ShakedownError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            ShakedownError::Internal(InternalError::LoadArtifactIo {
                operation: "create report directory",
                detail: source.to_string(),
            })
        })?;
    }
    let file = File::create(path).map_err(|source| {
        ShakedownError::Internal(InternalError::LoadArtifactIo {
            operation: "create report file",
            detail: source.to_string(),
        })
    })?;
    serde_json::to_writer_pretty(BufWriter::new(file), report).map_err(|source| {
        ShakedownError::Internal(InternalError::LoadArtifactIo {
            operation: "serialize report JSON",
            detail: source.to_string(),
        })
    })
}

/// Read and deserialize a report written by [`write_load_run_report_to_path`].
///
/// Used by unit tests and for ad-hoc inspection; not referenced from non-test lib code (suppresses `dead_code`).
#[allow(dead_code)]
pub(crate) fn read_load_run_report_from_path(path: &Path) -> Result<LoadRunReport, ShakedownError> {
    let file = File::open(path).map_err(|source| {
        ShakedownError::Internal(InternalError::LoadArtifactIo {
            operation: "open report file",
            detail: source.to_string(),
        })
    })?;
    let report: LoadRunReport =
        serde_json::from_reader(BufReader::new(file)).map_err(|source| {
            ShakedownError::Internal(InternalError::LoadArtifactIo {
                operation: "deserialize report JSON",
                detail: source.to_string(),
            })
        })?;
    Ok(report)
}

/// Persist [`LoadRunReport`] for `metrics` to the conventional reports path.
pub fn write_report(
    metrics: &LoadMetrics,
    load_name: &str,
    scenario_id: &str,
) -> Result<PathBuf, ShakedownError> {
    let path = resolve_load_report_path(scenario_id, load_name);
    let report = LoadRunReport::from_load_metrics(metrics, load_name, scenario_id)?;
    write_load_run_report_to_path(&report, &path)?;
    Ok(path)
}

/// Decode and deserialize the V2 histogram from a report (for round-trip tests).
#[cfg(test)]
pub(super) fn decode_report_histogram(
    report: &LoadRunReport,
) -> Result<hdrhistogram::Histogram<u64>, ShakedownError> {
    let bytes = STANDARD
        .decode(report.histogram_v2_base64.as_bytes())
        .map_err(|error| {
            ShakedownError::Internal(InternalError::Bug(format!(
                "base64-decode load report histogram: {error}"
            )))
        })?;
    let mut cursor = Cursor::new(bytes);
    Deserializer::new()
        .deserialize(&mut cursor)
        .map_err(|error| {
            ShakedownError::Internal(InternalError::Bug(format!(
                "hdrhistogram V2 deserialize: {error}"
            )))
        })
}

#[cfg(test)]
mod tests;
