//! JSON load run reports under `target/shakedown/reports/` (US-005).

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
pub const LOAD_RUN_REPORT_SCHEMA_VERSION: u32 = 1;

/// Scalar metrics embedded in a [`LoadRunReport`] (nanoseconds for latency fields).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReportMetrics {
    /// All arrivals: successes, failures, and scheduling drops.
    pub total_iterations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub dropped_iterations: u64,
    /// Failed / completed (0..1).
    pub error_rate: f64,
    /// Completed iterations per second (steady-state window when warmup is configured).
    pub throughput: f64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub min_latency_ns: u64,
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

/// Machine-readable record of a single load run (US-005).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoadRunReport {
    pub schema_version: u32,
    pub scenario_id: String,
    pub load_name: String,
    /// Wall time when the report was built (RFC 3339, UTC).
    pub timestamp: String,
    /// Wall milliseconds from load accumulator origin until report build.
    pub duration_ms: u64,
    pub load_profile_summary: String,
    pub metrics: ReportMetrics,
    /// HdrHistogram V2 payload, base64-encoded.
    pub histogram_v2_base64: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assertions: Option<Vec<ReportAssertion>>,
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
pub fn resolve_load_report_path(scenario_id: &str, load_name: &str) -> PathBuf {
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
pub fn write_load_run_report_to_path(
    report: &LoadRunReport,
    path: &Path,
) -> Result<(), ShakedownError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            ShakedownError::Internal(InternalError::Bug(format!(
                "create load report directory: {source}"
            )))
        })?;
    }
    let file = File::create(path).map_err(|source| {
        ShakedownError::Internal(InternalError::Bug(format!(
            "create load report file: {source}"
        )))
    })?;
    serde_json::to_writer_pretty(BufWriter::new(file), report).map_err(|source| {
        ShakedownError::Internal(InternalError::Bug(format!(
            "serialize load report JSON: {source}"
        )))
    })
}

/// Read and deserialize a report written by [`write_load_run_report_to_path`].
pub fn read_load_run_report_from_path(path: &Path) -> Result<LoadRunReport, ShakedownError> {
    let file = File::open(path).map_err(|source| {
        ShakedownError::Internal(InternalError::Bug(format!(
            "open load report file: {source}"
        )))
    })?;
    let report: LoadRunReport =
        serde_json::from_reader(BufReader::new(file)).map_err(|source| {
            ShakedownError::Internal(InternalError::Bug(format!(
                "deserialize load report JSON: {source}"
            )))
        })?;
    Ok(report)
}

/// Persist [`LoadRunReport`] for `metrics` to the conventional reports path (US-005).
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
fn decode_report_histogram(
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
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::load::metrics::LoadMetricsAccumulator;

    #[test]
    fn load_run_report_round_trip_file_and_histogram() {
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
        let report = LoadRunReport::from_load_metrics(&metrics, "checkout", "scenario_a")
            .expect("build report");
        write_load_run_report_to_path(&report, &temp).expect("write");
        let loaded = read_load_run_report_from_path(&temp).expect("read");

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
        let accumulator = Arc::new(LoadMetricsAccumulator::default());
        accumulator.record_success(std::time::Instant::now(), Duration::from_micros(100));
        let metrics = LoadMetrics::new(accumulator);
        let path = write_report(&metrics, "my_load", "s1").expect("write_report");
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
}
