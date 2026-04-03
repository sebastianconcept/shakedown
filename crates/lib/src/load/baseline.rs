//! Named performance baseline artifacts under `target/shakedown/baselines/` (US-006).

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
use super::ReportMetrics;

/// Schema version for [`LoadBaseline::schema_version`] (bump when fields are incompatible).
pub const LOAD_BASELINE_SCHEMA_VERSION: u32 = 1;

/// Serializable performance snapshot for regression comparison (US-006, US-007).
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

/// Resolve `target/shakedown/baselines/{scenario_id}/{baseline_name}.json`.
pub fn resolve_load_baseline_path(scenario_id: &str, baseline_name: &str) -> PathBuf {
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
pub fn write_load_baseline_to_path(
    baseline: &LoadBaseline,
    path: &Path,
) -> Result<(), ShakedownError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            ShakedownError::Internal(InternalError::Bug(format!(
                "create load baseline directory: {source}"
            )))
        })?;
    }
    let file = File::create(path).map_err(|source| {
        ShakedownError::Internal(InternalError::Bug(format!(
            "create load baseline file: {source}"
        )))
    })?;
    serde_json::to_writer_pretty(BufWriter::new(file), baseline).map_err(|source| {
        ShakedownError::Internal(InternalError::Bug(format!(
            "serialize load baseline JSON: {source}"
        )))
    })
}

/// Read and deserialize a baseline written by [`write_load_baseline_to_path`].
pub fn read_load_baseline_from_path(path: &Path) -> Result<LoadBaseline, ShakedownError> {
    let file = File::open(path).map_err(|source| {
        ShakedownError::Internal(InternalError::Bug(format!(
            "open load baseline file: {source}"
        )))
    })?;
    let baseline: LoadBaseline =
        serde_json::from_reader(BufReader::new(file)).map_err(|source| {
            ShakedownError::Internal(InternalError::Bug(format!(
                "deserialize load baseline JSON: {source}"
            )))
        })?;
    Ok(baseline)
}

/// Persist [`LoadBaseline`] for `metrics` to the conventional baselines path (US-006).
pub fn save_baseline(
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
fn decode_baseline_histogram(
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
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::load::metrics::LoadMetricsAccumulator;

    #[test]
    fn load_baseline_round_trip_file_and_histogram() {
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
        write_load_baseline_to_path(&baseline, &temp).expect("write");
        let loaded = read_load_baseline_from_path(&temp).expect("read");

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
        let accumulator = Arc::new(LoadMetricsAccumulator::default());
        accumulator.record_success(std::time::Instant::now(), Duration::from_micros(120));
        let metrics = LoadMetrics::new(accumulator);
        let path = metrics
            .save_baseline("mainline", "s2")
            .expect("save_baseline");
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
}
