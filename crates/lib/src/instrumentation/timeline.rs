//! Human-readable failure timeline for [`crate::event::EventLog`].
//!
//! On scenario failure, the harness sorts application and infrastructure records by time,
//! prints an interleaved view (excluding dense [`crate::event::InfraEvent::ResourceSample`] rows —
//! those appear only in the resource snapshot), and summarizes the latest resource stats per
//! container.
//!
//! A structured JSON artifact is written under `target/shakedown/timelines/` (see
//! [`write_failure_timeline_json`]) for CI and tooling. When infrastructure
//! signals may explain the failure, [`build_failure_timeline_document`] fills
//! [`FailureTimelineDocument::hints`] and [`render_failure_timeline`] appends a
//! `HINT:` section (root-cause analysis).

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::error::ShakedownError;
use crate::event::{
    DockerEventKind, EventLevel, EventLog, InfraEvent, InfraSeverity, LogEntry, ScenarioLogEntry,
};
use crate::lifecycle::Phase;

/// Suggestion emitted when an infrastructure event may explain a later failure (root-cause analysis).
#[derive(Debug, Clone, Serialize)]
pub struct RootCauseHint {
    /// Machine hint class (`oom_before_failure`, `exit_before_failure`, `health_before_failure`).
    #[serde(rename = "type")]
    pub hint_type: String,
    /// Container name or ID associated with the infrastructure signal.
    pub container: String,
    /// Milliseconds since the first log timestamp when the infrastructure event occurred.
    pub event_offset_ms: u64,
    /// Milliseconds since the first log timestamp when the scenario failure was observed.
    pub assertion_offset_ms: u64,
    /// Wall-clock gap between the infrastructure event and the failure (`assertion_offset_ms - event_offset_ms`).
    pub gap_ms: u64,
    /// Human-readable explanation (terminal output prefixes this with `HINT:`).
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem_limit: Option<u64>,
}

/// Structured load / SLO assertion failure for timeline JSON and terminal output (only when the `load` feature is enabled).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerformanceAssertionFailureJson {
    /// Load handle name from the scenario.
    pub load_name: String,
    /// Aggregate under test (e.g. `p99`, `throughput`).
    pub metric: String,
    /// Observed value (human-readable).
    pub measured: String,
    /// Asserted bound (human-readable).
    pub threshold: String,
    /// Debug label of the load profile phase (distinct from lifecycle [`Phase`]).
    pub load_phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional regression comparison table from baseline / tolerance checks.
    pub comparison_table: Option<String>,
}

/// JSON artifact for a failed scenario (machine-readable timeline).
#[derive(Debug, Clone, Serialize)]
pub struct FailureTimelineDocument {
    /// Scenario metadata (id, duration, verdict).
    pub scenario: ScenarioMetadataJson,
    /// Chronological timeline rows (application + infrastructure; excludes raw stats rows).
    pub events: Vec<TimelineEventJson>,
    /// Latest resource sample per container at failure.
    pub resource_snapshots: ResourceSnapshotsJson,
    /// Root-cause hints: infrastructure events that may explain the failure.
    pub hints: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Present when the failure was a load performance assertion (`load` feature).
    pub performance_assertion_failure: Option<PerformanceAssertionFailureJson>,
}

/// Scenario section of [`FailureTimelineDocument`].
#[derive(Debug, Clone, Serialize)]
pub struct ScenarioMetadataJson {
    /// Stable scenario identifier (matches harness / test name).
    pub id: String,
    /// Display name (same as id when no separate label exists).
    pub name: String,
    /// Wall-clock span covered by log events (milliseconds).
    pub duration_ms: u64,
    /// High-level outcome label (e.g. `failed`, `timeout`, `infrastructure_error`).
    pub verdict: String,
}

/// Per-container stats at failure for JSON export.
#[derive(Debug, Clone, Serialize)]
pub struct ResourceSnapshotJson {
    /// CPU usage percentage
    pub cpu_pct: f64,
    /// Resident memory in bytes
    pub mem_bytes: u64,
    /// Memory limit in bytes when known
    pub mem_limit: u64,
    /// Cumulative network bytes received
    pub net_rx: u64,
    /// Cumulative network bytes transmitted
    pub net_tx: u64,
}

/// Resource snapshot section in exported failure timelines.
#[derive(Debug, Clone, Serialize)]
pub struct ResourceSnapshotsJson {
    /// Latest sample per container name
    pub at_failure: HashMap<String, ResourceSnapshotJson>,
}

/// One timeline row for JSON (tagged with `type`).
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TimelineEventJson {
    /// Harness / application [`crate::event::Event`]
    Application {
        /// RFC 3339 UTC timestamp
        timestamp: String,
        /// Milliseconds since the earliest event in this log
        offset_ms: u64,
        /// Event source label
        source: String,
        /// Log level
        level: String,
        /// Message body
        message: String,
    },
    /// Container stdout/stderr line
    ContainerLog {
        /// RFC 3339 UTC timestamp
        timestamp: String,
        offset_ms: u64,
        /// Lifecycle phase
        phase: String,
        /// Container name
        container: String,
        /// Log line text
        line: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        /// Stream when known (`stdout` / `stderr`)
        stream: Option<String>,
    },
    /// Docker engine event (OOM, exit, health, …)
    DockerEvent {
        /// RFC 3339 UTC timestamp
        timestamp: String,
        offset_ms: u64,
        /// Lifecycle phase
        phase: String,
        /// Container name
        container: String,
        /// Classified kind
        kind: String,
        /// Severity tier
        severity: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        exit_code: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        oom_killed: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

/// Options for [`render_failure_timeline`] and [`build_failure_timeline_document`].
#[derive(Debug, Clone)]
pub struct FailureTimelineOptions {
    /// Scenario name or id (shown in the header).
    pub scenario_name: String,
    /// Short failure reason (e.g. error display string).
    pub failure_summary: Option<String>,
    /// Whether to emit ANSI colors for severity (stderr when supported).
    pub use_color: bool,
    /// Verdict for JSON metadata (e.g. `failed`, `infrastructure_error`).
    pub verdict: String,
    /// Optional scenario timeout (seconds) for hint eligibility: events must fall in
    /// `[scenario_start, scenario_start + timeout]`. When `None`, the observed span
    /// from first to last log timestamp is used.
    pub scenario_timeout_secs: Option<u64>,
    /// When set, terminal render and JSON include a performance assertion section.
    pub performance_assertion_failure: Option<PerformanceAssertionFailureJson>,
}

impl FailureTimelineOptions {
    /// Build options with [`Self::use_color`] set from stderr terminal detection.
    pub fn new(scenario_name: impl Into<String>) -> Self {
        Self {
            scenario_name: scenario_name.into(),
            failure_summary: None,
            use_color: io::stderr().is_terminal(),
            verdict: "failed".to_string(),
            scenario_timeout_secs: None,
            performance_assertion_failure: None,
        }
    }

    /// Sets an explicit scenario timeout (seconds) used when evaluating hint time windows.
    pub fn with_scenario_timeout_secs(mut self, timeout_secs: u64) -> Self {
        self.scenario_timeout_secs = Some(timeout_secs);
        self
    }

    /// Sets the failure summary line (shown in the header).
    pub fn with_failure_summary(mut self, summary: impl Into<String>) -> Self {
        self.failure_summary = Some(summary.into());
        self
    }

    /// Sets the verdict string written to JSON [`ScenarioMetadataJson::verdict`].
    pub fn with_verdict(mut self, verdict: impl Into<String>) -> Self {
        self.verdict = verdict.into();
        self
    }
}

fn log_timestamp_bounds(entries: &[(usize, &LogEntry)]) -> (u64, u64) {
    let mut min_ts = u64::MAX;
    let mut max_ts = 0_u64;
    for (_, entry) in entries {
        let ts = entry.timestamp_secs();
        min_ts = min_ts.min(ts);
        max_ts = max_ts.max(ts);
    }
    if min_ts == u64::MAX {
        (0, 0)
    } else {
        (min_ts, max_ts)
    }
}

fn docker_event_triggers_root_cause_hint(kind: &DockerEventKind, severity: InfraSeverity) -> bool {
    match severity {
        InfraSeverity::Critical => {
            matches!(kind, DockerEventKind::OomKilled | DockerEventKind::Exit)
        }
        InfraSeverity::Warning => matches!(kind, DockerEventKind::HealthUnhealthy),
        InfraSeverity::Info => false,
    }
}

fn timeout_suggestion_phrase(options: &FailureTimelineOptions) -> &'static str {
    let verdict_timeout = options.verdict == "timeout";
    let summary_timeout = options
        .failure_summary
        .as_ref()
        .is_some_and(|summary| summary.to_lowercase().contains("timeout"));
    if verdict_timeout || summary_timeout {
        "This may have caused the timeout."
    } else {
        "This may have contributed to this failure."
    }
}

fn short_docker_event_description(
    kind: &DockerEventKind,
    exit_code: Option<i64>,
    oom_killed: Option<bool>,
) -> String {
    match kind {
        DockerEventKind::OomKilled => {
            let mut s = "OOMKilled".to_string();
            if let Some(code) = exit_code {
                s.push_str(&format!(" (exit {code})"));
            }
            if oom_killed == Some(true) {
                s.push_str(" [oom_killed=true]");
            }
            s
        }
        DockerEventKind::Exit => {
            let mut s = "non-zero exit".to_string();
            if let Some(code) = exit_code {
                s.push_str(&format!(" (exit {code})"));
            }
            s
        }
        DockerEventKind::HealthUnhealthy => "health check failure (unhealthy)".to_string(),
        DockerEventKind::Restart => "restart".to_string(),
        DockerEventKind::Other(raw) => raw.clone(),
    }
}

fn generate_root_cause_hints(
    entries: &[(usize, &LogEntry)],
    baseline_secs: u64,
    failure_secs: u64,
    options: &FailureTimelineOptions,
    resource_at_failure: &HashMap<String, ResourceSnapshotJson>,
) -> Vec<RootCauseHint> {
    let observed_span = failure_secs.saturating_sub(baseline_secs).max(1);
    let window_secs = options.scenario_timeout_secs.unwrap_or(observed_span);
    let window_end_secs = baseline_secs.saturating_add(window_secs);
    let suggestion = timeout_suggestion_phrase(options);

    let mut out: Vec<RootCauseHint> = Vec::new();

    for (_, entry) in entries {
        let LogEntry::Infrastructure(InfraEvent::DockerEvent {
            container,
            timestamp,
            kind,
            severity,
            exit_code,
            oom_killed,
            ..
        }) = entry
        else {
            continue;
        };

        if !docker_event_triggers_root_cause_hint(kind, *severity) {
            continue;
        }

        if *timestamp >= failure_secs {
            continue;
        }

        if *timestamp < baseline_secs || *timestamp > window_end_secs {
            continue;
        }

        let event_offset_ms = timestamp.saturating_sub(baseline_secs).saturating_mul(1000);
        let assertion_offset_ms = failure_secs
            .saturating_sub(baseline_secs)
            .saturating_mul(1000);
        let gap_ms = assertion_offset_ms.saturating_sub(event_offset_ms);

        let event_sec = (*timestamp - baseline_secs) as f64;
        let failure_sec = (failure_secs - baseline_secs) as f64;
        let gap_sec = (failure_secs - *timestamp) as f64;

        let snap = resource_at_failure.get(container);
        let (mem_bytes, mem_limit) = snap
            .map(|resource_snapshot| {
                (
                    Some(resource_snapshot.mem_bytes),
                    Some(resource_snapshot.mem_limit),
                )
            })
            .unwrap_or((None, None));

        let metrics = match (snap, kind) {
            (Some(resource_snapshot), DockerEventKind::OomKilled)
                if resource_snapshot.mem_limit > 0 =>
            {
                Some(format!(
                    "Memory limit {} / peak {}. ",
                    format_bytes(resource_snapshot.mem_limit),
                    format_bytes(resource_snapshot.mem_bytes),
                ))
            }
            (Some(resource_snapshot), _) if resource_snapshot.mem_limit > 0 => Some(format!(
                "Memory {} / limit {}. ",
                format_bytes(resource_snapshot.mem_bytes),
                format_bytes(resource_snapshot.mem_limit),
            )),
            _ => None,
        };

        let desc = short_docker_event_description(kind, *exit_code, *oom_killed);
        let metrics = metrics.unwrap_or_default();
        let message = format!(
            "Container \"{container}\" reported {desc} at T+{event_sec:.1}s, {gap_sec:.1}s before failure at T+{failure_sec:.1}s. {metrics}{suggestion}",
        );

        let hint_type = match kind {
            DockerEventKind::OomKilled => "oom_before_failure",
            DockerEventKind::Exit => "exit_before_failure",
            DockerEventKind::HealthUnhealthy => "health_before_failure",
            DockerEventKind::Restart | DockerEventKind::Other(_) => "infra_before_failure",
        }
        .to_string();

        out.push(RootCauseHint {
            hint_type,
            container: container.clone(),
            event_offset_ms,
            assertion_offset_ms,
            gap_ms,
            message,
            mem_bytes,
            mem_limit,
        });
    }

    out.sort_by(|left, right| {
        left.event_offset_ms
            .cmp(&right.event_offset_ms)
            .then_with(|| left.container.cmp(&right.container))
    });
    out
}

/// Mode 3 hints: classify load saturation (accidental) vs service errors during cluster work (essential).
fn generate_mode_three_load_cluster_hints(
    entries: &[(usize, &LogEntry)],
    baseline_secs: u64,
    failure_secs: u64,
) -> Vec<RootCauseHint> {
    let mut out = Vec::new();
    let assertion_offset_ms = failure_secs
        .saturating_sub(baseline_secs)
        .saturating_mul(1000);

    let cluster_before_failure = entries.iter().any(|(_, entry)| {
        matches!(
            entry,
            LogEntry::Application(event)
                if event.source == "cluster"
                    && event.timestamp < failure_secs
                    && (event.message.contains("step_down_primary")
                        || event.message.contains("await_new_primary"))
        )
    });

    let load_failures_while_cluster = entries.iter().any(|(_, entry)| {
        matches!(
            entry,
            LogEntry::Application(event)
                if event.source == "load"
                    && event.timestamp < failure_secs
                    && event.message.contains("iteration failed")
        )
    }) && cluster_before_failure;

    if load_failures_while_cluster {
        out.push(RootCauseHint {
            hint_type: "service_errors_during_cluster_topology".into(),
            container: "cluster+load".into(),
            event_offset_ms: 0,
            assertion_offset_ms,
            gap_ms: assertion_offset_ms,
            message: "Load iterations failed while cluster topology operations (e.g. step_down / election) were in flight — review service retry/driver behavior (essential complexity vs cluster stabilization)."
                .into(),
            mem_bytes: None,
            mem_limit: None,
        });
    }

    let dropped_during_baseline = entries.iter().find_map(|(_, entry)| {
        if let LogEntry::Application(event) = entry {
            if event.source == "load"
                && event.timestamp < failure_secs
                && event.message.contains("iteration dropped")
                && event.message.contains("baseline")
            {
                let event_offset_ms = event
                    .timestamp
                    .saturating_sub(baseline_secs)
                    .saturating_mul(1000);
                return Some(event_offset_ms);
            }
        }
        None
    });

    if let Some(event_offset_ms) = dropped_during_baseline {
        out.push(RootCauseHint {
            hint_type: "load_saturation_baseline_accidental".into(),
            container: "load".into(),
            event_offset_ms,
            assertion_offset_ms,
            gap_ms: assertion_offset_ms.saturating_sub(event_offset_ms),
            message: "Load generator dropped iterations during a phase named baseline — workers saturated; raise max_workers or lower rate (test infrastructure / accidental complexity)."
                .into(),
            mem_bytes: None,
            mem_limit: None,
        });
    }

    out
}

fn root_cause_hints_to_json_values(hints: &[RootCauseHint]) -> Vec<serde_json::Value> {
    hints
        .iter()
        .filter_map(|hint| serde_json::to_value(hint).ok())
        .collect()
}

type TimelineRowsAndSamples<'a> = (u64, Vec<(u64, usize, TimelineRow)>, Vec<&'a InfraEvent>);

fn collect_timeline_rows<'a>(entries: &'a [(usize, &'a LogEntry)]) -> TimelineRowsAndSamples<'a> {
    let baseline_secs = entries
        .iter()
        .map(|(_, entry)| entry.timestamp_secs())
        .min()
        .unwrap_or(0);

    let mut timeline_rows: Vec<(u64, usize, TimelineRow)> = Vec::new();
    let mut samples: Vec<&InfraEvent> = Vec::new();

    for (index, entry) in entries {
        match entry {
            LogEntry::Application(event) => {
                timeline_rows.push((
                    event.timestamp,
                    *index,
                    TimelineRow::Application {
                        offset_secs: event.timestamp.saturating_sub(baseline_secs),
                        level: event.level,
                        source: event.source.clone(),
                        message: event.message.clone(),
                    },
                ));
            }
            LogEntry::Infrastructure(infra) => match infra {
                InfraEvent::ResourceSample { .. } => samples.push(infra),
                InfraEvent::ContainerLog {
                    container,
                    phase,
                    timestamp,
                    line,
                    stream,
                } => {
                    timeline_rows.push((
                        *timestamp,
                        *index,
                        TimelineRow::ContainerLog {
                            offset_secs: timestamp.saturating_sub(baseline_secs),
                            phase: *phase,
                            container: container.clone(),
                            line: line.clone(),
                            stream: stream.clone(),
                        },
                    ));
                }
                InfraEvent::DockerEvent {
                    container,
                    phase,
                    timestamp,
                    kind,
                    severity,
                    exit_code,
                    oom_killed,
                    detail,
                    ..
                } => {
                    timeline_rows.push((
                        *timestamp,
                        *index,
                        TimelineRow::Docker {
                            offset_secs: timestamp.saturating_sub(baseline_secs),
                            phase: *phase,
                            container: container.clone(),
                            kind: kind.clone(),
                            severity: *severity,
                            exit_code: *exit_code,
                            oom_killed: *oom_killed,
                            detail: detail.clone(),
                        },
                    ));
                }
            },
        }
    }

    (baseline_secs, timeline_rows, samples)
}

/// Builds a structured [`FailureTimelineDocument`] from the event log (no I/O).
pub fn build_failure_timeline_document(
    event_log: &EventLog,
    options: &FailureTimelineOptions,
) -> FailureTimelineDocument {
    let entries: Vec<(usize, &LogEntry)> = event_log.entries().iter().enumerate().collect();
    let (baseline_secs, mut timeline_rows, samples) = collect_timeline_rows(&entries);
    timeline_rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let min_ts = entries
        .iter()
        .map(|(_, e)| e.timestamp_secs())
        .min()
        .unwrap_or(0);
    let max_ts = entries
        .iter()
        .map(|(_, e)| e.timestamp_secs())
        .max()
        .unwrap_or(0);
    let duration_ms = max_ts.saturating_sub(min_ts).saturating_mul(1000);

    let events: Vec<TimelineEventJson> = timeline_rows
        .into_iter()
        .map(|(_, _, row)| timeline_row_to_json(row, baseline_secs))
        .collect();

    let snapshot_map = latest_resource_samples_by_container(&samples);
    let mut at_failure: HashMap<String, ResourceSnapshotJson> = HashMap::new();
    for (name, sample) in snapshot_map {
        if let InfraEvent::ResourceSample {
            cpu_pct,
            mem_bytes,
            mem_limit,
            net_rx,
            net_tx,
            ..
        } = sample
        {
            at_failure.insert(
                name,
                ResourceSnapshotJson {
                    cpu_pct: *cpu_pct,
                    mem_bytes: *mem_bytes,
                    mem_limit: *mem_limit,
                    net_rx: *net_rx,
                    net_tx: *net_tx,
                },
            );
        }
    }

    let (_, failure_secs) = log_timestamp_bounds(&entries);
    let mut root_cause_hints =
        generate_root_cause_hints(&entries, baseline_secs, failure_secs, options, &at_failure);
    root_cause_hints.extend(generate_mode_three_load_cluster_hints(
        &entries,
        baseline_secs,
        failure_secs,
    ));
    let hints = root_cause_hints_to_json_values(&root_cause_hints);

    FailureTimelineDocument {
        scenario: ScenarioMetadataJson {
            id: options.scenario_name.clone(),
            name: options.scenario_name.clone(),
            duration_ms,
            verdict: options.verdict.clone(),
        },
        events,
        resource_snapshots: ResourceSnapshotsJson { at_failure },
        hints,
        performance_assertion_failure: options.performance_assertion_failure.clone(),
    }
}

fn timeline_row_to_json(row: TimelineRow, baseline_secs: u64) -> TimelineEventJson {
    match row {
        TimelineRow::Application {
            offset_secs,
            level,
            source,
            message,
        } => {
            let ts = baseline_secs.saturating_add(offset_secs);
            TimelineEventJson::Application {
                timestamp: unix_secs_to_rfc3339(ts),
                offset_ms: offset_secs.saturating_mul(1000),
                source,
                level: level.to_string(),
                message,
            }
        }
        TimelineRow::ContainerLog {
            offset_secs,
            phase,
            container,
            line,
            stream,
        } => {
            let ts = baseline_secs.saturating_add(offset_secs);
            TimelineEventJson::ContainerLog {
                timestamp: unix_secs_to_rfc3339(ts),
                offset_ms: offset_secs.saturating_mul(1000),
                phase: phase_as_str(phase).to_string(),
                container,
                line,
                stream,
            }
        }
        TimelineRow::Docker {
            offset_secs,
            phase,
            container,
            kind,
            severity,
            exit_code,
            oom_killed,
            detail,
        } => {
            let ts = baseline_secs.saturating_add(offset_secs);
            TimelineEventJson::DockerEvent {
                timestamp: unix_secs_to_rfc3339(ts),
                offset_ms: offset_secs.saturating_mul(1000),
                phase: phase_as_str(phase).to_string(),
                container,
                kind: docker_kind_label(&kind),
                severity: infra_severity_label(severity),
                exit_code,
                oom_killed,
                detail,
            }
        }
    }
}

fn phase_as_str(phase: Phase) -> &'static str {
    match phase {
        Phase::Provision => "provision",
        Phase::Readiness => "readiness",
        Phase::Seed => "seed",
        Phase::Run => "run",
        Phase::Assert => "assert",
        Phase::Teardown => "teardown",
    }
}

fn docker_kind_label(kind: &DockerEventKind) -> String {
    match kind {
        DockerEventKind::OomKilled => "oom_killed".to_string(),
        DockerEventKind::Exit => "exit".to_string(),
        DockerEventKind::HealthUnhealthy => "health_unhealthy".to_string(),
        DockerEventKind::Restart => "restart".to_string(),
        DockerEventKind::Other(raw) => raw.clone(),
    }
}

fn infra_severity_label(severity: InfraSeverity) -> String {
    match severity {
        InfraSeverity::Info => "info".to_string(),
        InfraSeverity::Warning => "warning".to_string(),
        InfraSeverity::Critical => "critical".to_string(),
    }
}

fn unix_secs_to_rfc3339(secs: u64) -> String {
    OffsetDateTime::from_unix_timestamp(secs as i64)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&Rfc3339)
        .unwrap_or_else(|_| format!("{secs}"))
}

/// Resolves `target/shakedown/timelines` under the active Cargo workspace / target directory.
pub fn resolve_target_timelines_dir() -> PathBuf {
    resolve_target_dir().join("shakedown").join("timelines")
}

/// Resolves the Cargo workspace `target` directory (same rules as artifact paths under `target/shakedown/`).
pub fn resolve_target_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("CARGO_TARGET_DIR") {
        return PathBuf::from(dir);
    }
    if let Ok(manifest) = std::env::var("CARGO_MANIFEST_DIR") {
        let path = PathBuf::from(manifest);
        if let Some(workspace_root) = path.parent().and_then(|parent| parent.parent()) {
            return workspace_root.join("target");
        }
    }
    PathBuf::from("target")
}

/// Sanitizes a scenario id for use as a single path segment (filename).
pub fn sanitize_scenario_id_for_filename(scenario_id: &str) -> String {
    scenario_id
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' | '\0' => '_',
            c => c,
        })
        .collect()
}

/// Writes [`FailureTimelineDocument`] as pretty JSON via [`serde_json::to_writer_pretty`].
pub fn write_failure_timeline_json_to_path(
    document: &FailureTimelineDocument,
    path: &Path,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    serde_json::to_writer_pretty(file, document)?;
    Ok(())
}

/// Builds a failure timeline document and writes it to
/// `{target}/shakedown/timelines/{sanitized_scenario_id}.json`.
///
/// I/O errors are returned to the caller; harness code typically logs and ignores them so
/// diagnostics never fail the test run.
pub fn write_failure_timeline_json(
    event_log: &EventLog,
    options: &FailureTimelineOptions,
    scenario_id_for_filename: &str,
) -> io::Result<PathBuf> {
    let document = build_failure_timeline_document(event_log, options);
    let dir = resolve_target_timelines_dir();
    let filename = format!(
        "{}.json",
        sanitize_scenario_id_for_filename(scenario_id_for_filename)
    );
    let path = dir.join(filename);
    write_failure_timeline_json_to_path(&document, &path)?;
    Ok(path)
}

/// Renders a multi-section failure timeline to a string (no I/O).
pub fn render_failure_timeline(event_log: &EventLog, options: &FailureTimelineOptions) -> String {
    let mut out = String::new();

    let header_reason = options
        .failure_summary
        .as_deref()
        .unwrap_or("scenario failed");
    out.push_str(&format!(
        "\nFAILURE TIMELINE — {} ({})\n\n",
        options.scenario_name, header_reason
    ));

    let entries: Vec<(usize, &LogEntry)> = event_log.entries().iter().enumerate().collect();
    if entries.is_empty() {
        out.push_str("(no events recorded)\n");
    } else {
        let (baseline_secs, mut timeline_rows, samples) = collect_timeline_rows(&entries);

        timeline_rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        out.push_str("--- Events (chronological) ---\n");
        for (_, _, row) in &timeline_rows {
            out.push_str(&row.format_line(options.use_color));
            out.push('\n');
        }

        out.push_str("\n--- RESOURCE SNAPSHOT at failure ---\n");
        let snapshot = latest_resource_samples_by_container(&samples);
        if snapshot.is_empty() {
            out.push_str("(no resource samples recorded)\n");
        } else {
            let mut names: Vec<_> = snapshot.keys().cloned().collect();
            names.sort();
            for name in names {
                let Some(sample) = snapshot.get(&name) else {
                    continue;
                };
                let InfraEvent::ResourceSample {
                    cpu_pct,
                    mem_bytes,
                    mem_limit,
                    net_rx,
                    net_tx,
                    ..
                } = sample
                else {
                    continue;
                };
                let mem_pct = if *mem_limit > 0 {
                    (*mem_bytes as f64 / *mem_limit as f64) * 100.0
                } else {
                    0.0
                };
                let warn = *mem_limit > 0 && mem_pct >= 95.0;
                let flag = if warn { " ⚠" } else { "" };
                out.push_str(&format!(
                    "  {name}  CPU: {:5.1}%  MEM: {}/{} ({:.0}%){flag}  NET rx: {} tx: {}\n",
                    cpu_pct,
                    format_bytes(*mem_bytes),
                    format_bytes(*mem_limit),
                    mem_pct,
                    format_bytes(*net_rx),
                    format_bytes(*net_tx),
                ));
            }
        }

        let (_, failure_secs) = log_timestamp_bounds(&entries);
        let snapshot_map = latest_resource_samples_by_container(&samples);
        let mut at_failure_for_hints: HashMap<String, ResourceSnapshotJson> = HashMap::new();
        for (name, sample) in snapshot_map {
            if let InfraEvent::ResourceSample {
                cpu_pct,
                mem_bytes,
                mem_limit,
                net_rx,
                net_tx,
                ..
            } = sample
            {
                at_failure_for_hints.insert(
                    name,
                    ResourceSnapshotJson {
                        cpu_pct: *cpu_pct,
                        mem_bytes: *mem_bytes,
                        mem_limit: *mem_limit,
                        net_rx: *net_rx,
                        net_tx: *net_tx,
                    },
                );
            }
        }
        let root_cause_hints = generate_root_cause_hints(
            &entries,
            baseline_secs,
            failure_secs,
            options,
            &at_failure_for_hints,
        );
        if !root_cause_hints.is_empty() {
            out.push_str("\n--- HINTS ---\n");
            for hint in &root_cause_hints {
                out.push_str("HINT: ");
                out.push_str(&hint.message);
                out.push('\n');
            }
        }
    }

    if let Some(perf) = &options.performance_assertion_failure {
        out.push_str("\n--- PERFORMANCE ASSERTION ---\n");
        out.push_str(&format!("  load .......... {}\n", perf.load_name));
        out.push_str(&format!("  metric ........ {}\n", perf.metric));
        out.push_str(&format!("  measured ...... {}\n", perf.measured));
        out.push_str(&format!("  threshold ..... {}\n", perf.threshold));
        out.push_str(&format!("  load phase .... {}\n", perf.load_phase));
        if let Some(table) = &perf.comparison_table {
            out.push('\n');
            out.push_str(table);
            out.push('\n');
        }
    }

    out.push('\n');
    out
}

/// Writes a rendered failure timeline to stderr (for harness use on error paths) and a JSON
/// artifact under `target/shakedown/timelines/{scenario_id}.json` when writing succeeds.
pub fn emit_failure_timeline_to_stderr(
    event_log: &EventLog,
    scenario_id: &str,
    failure_summary: &str,
) {
    let options = FailureTimelineOptions {
        scenario_name: scenario_id.to_string(),
        failure_summary: Some(failure_summary.to_string()),
        use_color: io::stderr().is_terminal(),
        verdict: "infrastructure_error".to_string(),
        scenario_timeout_secs: None,
        performance_assertion_failure: None,
    };
    eprintln!("{}", render_failure_timeline(event_log, &options));
    if let Err(error) = write_failure_timeline_json(event_log, &options, scenario_id) {
        tracing::warn!(
            target: "shakedown::harness",
            error = %error,
            "shakedown: could not write failure timeline JSON artifact"
        );
    }
}

fn scenario_failure_verdict(error: &ShakedownError) -> String {
    match error {
        #[cfg(feature = "load")]
        ShakedownError::PerformanceAssertionFailed(_) => "performance_assertion_failed".to_string(),
        ShakedownError::Infrastructure(_) => "infrastructure_error".to_string(),
        ShakedownError::Scenario(_) => "scenario_error".to_string(),
        ShakedownError::Internal(_) => "internal_error".to_string(),
        ShakedownError::Outbox(_) => "outbox_error".to_string(),
        ShakedownError::PayloadSerialisation(_) => "payload_serialisation_error".to_string(),
        ShakedownError::ResourceAssertionFailed { .. } => "resource_assertion_failed".to_string(),
        ShakedownError::HelperTimeout { .. } => "helper_timeout".to_string(),
    }
}

fn performance_assertion_failure_from_error(
    error: &ShakedownError,
) -> Option<PerformanceAssertionFailureJson> {
    #[cfg(feature = "load")]
    {
        match error {
            ShakedownError::PerformanceAssertionFailed(details) => {
                Some(PerformanceAssertionFailureJson {
                    load_name: details.load_name.clone(),
                    metric: details.metric.clone(),
                    measured: details.measured.clone(),
                    threshold: details.threshold.clone(),
                    load_phase: format!("{:?}", details.phase),
                    comparison_table: details.comparison_table.clone(),
                })
            }
            _ => None,
        }
    }
    #[cfg(not(feature = "load"))]
    {
        let _ = error;
        None
    }
}

/// Renders a failure timeline and writes JSON for a harness [`ShakedownError`] after scenario teardown (including performance assertion failures when applicable).
///
/// When the error is [`ShakedownError::PerformanceAssertionFailed`] and the `load` feature is enabled,
/// [`FailureTimelineDocument::performance_assertion_failure`] is populated for tooling.
pub fn emit_failure_timeline_for_shakedown_error(
    event_log: &EventLog,
    scenario_id: &str,
    error: &ShakedownError,
) {
    let options = FailureTimelineOptions {
        scenario_name: scenario_id.to_string(),
        failure_summary: Some(error.to_string()),
        use_color: io::stderr().is_terminal(),
        verdict: scenario_failure_verdict(error),
        scenario_timeout_secs: None,
        performance_assertion_failure: performance_assertion_failure_from_error(error),
    };
    eprintln!("{}", render_failure_timeline(event_log, &options));
    if let Err(write_error) = write_failure_timeline_json(event_log, &options, scenario_id) {
        tracing::warn!(
            target: "shakedown::harness",
            error = %write_error,
            "shakedown: could not write failure timeline JSON artifact"
        );
    }
}

fn latest_resource_samples_by_container<'a>(
    samples: &'a [&'a InfraEvent],
) -> HashMap<String, &'a InfraEvent> {
    let mut map: HashMap<String, &'a InfraEvent> = HashMap::new();
    for sample in samples {
        let InfraEvent::ResourceSample {
            container,
            timestamp,
            ..
        } = sample
        else {
            continue;
        };
        let replace = match map.get(container) {
            None => true,
            Some(existing) => *timestamp >= existing.timestamp_secs(),
        };
        if replace {
            map.insert(container.clone(), sample);
        }
    }
    map
}

enum TimelineRow {
    Application {
        offset_secs: u64,
        level: EventLevel,
        source: String,
        message: String,
    },
    ContainerLog {
        offset_secs: u64,
        phase: Phase,
        container: String,
        line: String,
        stream: Option<String>,
    },
    Docker {
        offset_secs: u64,
        phase: Phase,
        container: String,
        kind: DockerEventKind,
        severity: InfraSeverity,
        exit_code: Option<i64>,
        oom_killed: Option<bool>,
        detail: Option<String>,
    },
}

impl TimelineRow {
    fn format_line(&self, use_color: bool) -> String {
        match self {
            TimelineRow::Application {
                offset_secs,
                level,
                source,
                message,
            } => {
                let tag = phase_tag_for_source(source);
                format!(
                    "T+{offset}s  {tag}  {source}  | [{level}] {message}",
                    offset = *offset_secs,
                    tag = tag,
                )
            }
            TimelineRow::ContainerLog {
                offset_secs,
                phase,
                container,
                line,
                stream,
            } => {
                let stream_note = stream
                    .as_deref()
                    .map(|s| format!(" ({s})"))
                    .unwrap_or_default();
                let phase = phase_bracket(*phase);
                format!(
                    "T+{offset}s  {phase}  {container}{stream_note}  | {line}",
                    offset = *offset_secs,
                )
            }
            TimelineRow::Docker {
                offset_secs,
                phase,
                container,
                kind,
                severity,
                exit_code,
                oom_killed,
                detail,
            } => {
                let phase = phase_bracket(*phase);
                let body = format_docker_body(kind, *exit_code, *oom_killed, detail.as_deref());
                let marker = docker_marker(*severity, use_color);
                format!(
                    "T+{offset}s  {phase}  {marker}{container}  | CONTAINER EVENT: {body}",
                    offset = *offset_secs,
                )
            }
        }
    }
}

fn phase_tag_for_source(source: &str) -> String {
    format!("[{source}]")
}

fn phase_bracket(phase: Phase) -> String {
    let label = match phase {
        Phase::Provision => "provision",
        Phase::Readiness => "readiness",
        Phase::Seed => "seed",
        Phase::Run => "run",
        Phase::Assert => "assert",
        Phase::Teardown => "teardown",
    };
    format!("[{label}]")
}

fn format_docker_body(
    kind: &DockerEventKind,
    exit_code: Option<i64>,
    oom_killed: Option<bool>,
    detail: Option<&str>,
) -> String {
    let mut base = match kind {
        DockerEventKind::OomKilled => {
            let mut s = "OOMKilled".to_string();
            if let Some(code) = exit_code {
                s.push_str(&format!(" (exit {code})"));
            }
            if oom_killed == Some(true) {
                s.push_str(" [oom_killed=true]");
            }
            s
        }
        DockerEventKind::Exit => {
            let mut s = "non-zero exit".to_string();
            if let Some(code) = exit_code {
                s.push_str(&format!(" (exit {code})"));
            }
            s
        }
        DockerEventKind::HealthUnhealthy => "health check unhealthy".to_string(),
        DockerEventKind::Restart => "restart".to_string(),
        DockerEventKind::Other(raw) => raw.clone(),
    };
    if let Some(d) = detail {
        if !d.is_empty() && !base.contains(d) {
            base.push_str(&format!(" [{d}]"));
        }
    }
    base
}

fn docker_marker(severity: InfraSeverity, use_color: bool) -> String {
    match severity {
        InfraSeverity::Info => String::new(),
        InfraSeverity::Warning => {
            if use_color {
                "\x1b[33m⚠\x1b[0m ".to_string()
            } else {
                "⚠ ".to_string()
            }
        }
        InfraSeverity::Critical => {
            if use_color {
                "\x1b[31m✗\x1b[0m ".to_string()
            } else {
                "✗ ".to_string()
            }
        }
    }
}

fn format_bytes(n: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if n >= GB {
        format!("{:.1}GB", n as f64 / GB as f64)
    } else if n >= MB {
        format!("{:.1}MB", n as f64 / MB as f64)
    } else if n >= KB {
        format!("{:.1}KB", n as f64 / KB as f64)
    } else {
        format!("{n}B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;

    #[test]
    fn fault_injection_application_events_merge_with_infra_and_json_export() {
        use crate::event::{EventLevel, LogEntry};

        let mut log = EventLog::new();
        log.append_entry(LogEntry::Application(Event {
            timestamp: 1,
            level: EventLevel::Info,
            source: "fault-injection".into(),
            message: "action=added proxy='p' upstream=127.0.0.1:9 toxic='t' kind=Latency(50, 0) direction=downstream".into(),
        }));
        log.append_infra(InfraEvent::ContainerLog {
            container: "c".into(),
            phase: Phase::Seed,
            timestamp: 2,
            line: "infra line".into(),
            stream: None,
        });
        log.append_entry(LogEntry::Application(Event {
            timestamp: 3,
            level: EventLevel::Info,
            source: "fault-injection".into(),
            message: "action=removed proxy='p' upstream=127.0.0.1:9 toxic='t'".into(),
        }));

        let options = FailureTimelineOptions {
            scenario_name: "faulty_case".into(),
            failure_summary: Some("assertion failed".into()),
            use_color: false,
            verdict: "failed".into(),
            scenario_timeout_secs: None,
            performance_assertion_failure: None,
        };
        let rendered = render_failure_timeline(&log, &options);
        let first = rendered.find("action=added").expect("added");
        let mid = rendered.find("infra line").expect("infra");
        let last = rendered.find("action=removed").expect("removed");
        assert!(first < mid && mid < last);

        let document = build_failure_timeline_document(&log, &options);
        let json = serde_json::to_string(&document).expect("json");
        assert!(json.contains("\"source\":\"fault-injection\""));
        assert!(json.contains("action=added"));
        assert!(json.contains("action=removed"));
    }

    #[test]
    fn sorts_mixed_entries_and_interleaves() {
        let mut log = EventLog::new();
        log.append_infra(InfraEvent::ContainerLog {
            container: "c".into(),
            phase: Phase::Run,
            timestamp: 3,
            line: "late".into(),
            stream: None,
        });
        log.append_infra(InfraEvent::ContainerLog {
            container: "a".into(),
            phase: Phase::Seed,
            timestamp: 1,
            line: "early".into(),
            stream: Some("stdout".into()),
        });

        let options = FailureTimelineOptions {
            scenario_name: "s".into(),
            failure_summary: None,
            use_color: false,
            verdict: "failed".into(),
            scenario_timeout_secs: None,
            performance_assertion_failure: None,
        };
        let rendered = render_failure_timeline(&log, &options);
        let early_pos = rendered.find("early").expect("early");
        let late_pos = rendered.find("late").expect("late");
        assert!(early_pos < late_pos);
    }

    #[test]
    fn timeline_renders_docker_warning_and_critical_markers() {
        let mut log = EventLog::new();
        log.append_infra(InfraEvent::DockerEvent {
            container: "kafka".into(),
            phase: Phase::Run,
            timestamp: 10,
            scenario_id: "s".into(),
            kind: DockerEventKind::HealthUnhealthy,
            severity: InfraSeverity::Warning,
            exit_code: None,
            oom_killed: None,
            detail: Some("health_status: unhealthy".into()),
        });
        log.append_infra(InfraEvent::DockerEvent {
            container: "kafka".into(),
            phase: Phase::Run,
            timestamp: 11,
            scenario_id: "s".into(),
            kind: DockerEventKind::OomKilled,
            severity: InfraSeverity::Critical,
            exit_code: Some(137),
            oom_killed: Some(true),
            detail: Some("die".into()),
        });
        log.append_infra(InfraEvent::ResourceSample {
            container: "kafka".into(),
            phase: Phase::Run,
            timestamp: 11,
            scenario_id: "s".into(),
            cpu_pct: 12.0,
            mem_bytes: 500 * 1024 * 1024,
            mem_limit: 512 * 1024 * 1024,
            net_rx: 1000,
            net_tx: 2000,
        });

        let options = FailureTimelineOptions {
            scenario_name: "my_scenario".into(),
            failure_summary: Some("readiness failed".into()),
            use_color: false,
            verdict: "failed".into(),
            scenario_timeout_secs: None,
            performance_assertion_failure: None,
        };
        let rendered = render_failure_timeline(&log, &options);
        assert!(rendered.contains("FAILURE TIMELINE — my_scenario"));
        assert!(rendered.contains("⚠"));
        assert!(rendered.contains("✗"));
        assert!(rendered.contains("CONTAINER EVENT:"));
        assert!(rendered.contains("RESOURCE SNAPSHOT"));
        assert!(rendered.contains("CPU:"));
        assert!(rendered.contains("--- HINTS ---"));
        assert!(rendered.contains("HINT:"));
    }

    #[test]
    fn resource_snapshot_picks_latest_sample_per_container() {
        let mut log = EventLog::new();
        log.append_infra(InfraEvent::ResourceSample {
            container: "a".into(),
            phase: Phase::Seed,
            timestamp: 1,
            scenario_id: String::new(),
            cpu_pct: 1.0,
            mem_bytes: 100,
            mem_limit: 200,
            net_rx: 0,
            net_tx: 0,
        });
        log.append_infra(InfraEvent::ResourceSample {
            container: "a".into(),
            phase: Phase::Run,
            timestamp: 5,
            scenario_id: String::new(),
            cpu_pct: 9.0,
            mem_bytes: 150,
            mem_limit: 200,
            net_rx: 1,
            net_tx: 2,
        });

        let options = FailureTimelineOptions {
            scenario_name: "x".into(),
            failure_summary: None,
            use_color: false,
            verdict: "failed".into(),
            scenario_timeout_secs: None,
            performance_assertion_failure: None,
        };
        let rendered = render_failure_timeline(&log, &options);
        assert!(rendered.contains("9.0%"));
        assert!(!rendered.contains("1.0%"));
    }

    #[test]
    fn failure_timeline_document_serializes_to_parseable_json() {
        let mut log = EventLog::new();
        log.append_raw(EventLevel::Info, "lifecycle", "Starting provision phase");
        log.append_infra(InfraEvent::ContainerLog {
            container: "kafka".into(),
            phase: Phase::Seed,
            timestamp: 1_700_000_000,
            line: "broker up".into(),
            stream: Some("stdout".into()),
        });
        let options =
            FailureTimelineOptions::new("my/scenario").with_verdict("infrastructure_error");
        let document = build_failure_timeline_document(&log, &options);
        assert_eq!(document.scenario.verdict, "infrastructure_error");
        assert!(document.hints.is_empty());
        let json = serde_json::to_string_pretty(&document).expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert!(value.get("scenario").is_some());
        assert!(value.get("events").is_some());
        assert!(value.get("resource_snapshots").is_some());
        assert!(value.get("hints").is_some());
    }

    #[test]
    fn write_failure_timeline_json_to_path_creates_parent_dirs() {
        let temp =
            std::env::temp_dir().join(format!("shakedown-timeline-json-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp);
        let path = temp.join("nested").join("t.json");
        let mut log = EventLog::new();
        log.append_raw(EventLevel::Error, "lifecycle", "fail");
        let options = FailureTimelineOptions::new("x");
        let document = build_failure_timeline_document(&log, &options);
        write_failure_timeline_json_to_path(&document, &path).expect("write");
        assert!(path.exists());
        let bytes = std::fs::read(&path).expect("read");
        let _: serde_json::Value = serde_json::from_slice(&bytes).expect("parse");
        let _ = std::fs::remove_dir_all(&temp);
    }

    #[test]
    fn sanitize_scenario_id_replaces_path_separators() {
        assert_eq!(
            sanitize_scenario_id_for_filename("suite/case:a"),
            "suite_case_a"
        );
    }

    #[test]
    fn root_cause_hints_include_oom_and_metrics_in_terminal_and_json() {
        let mut log = EventLog::new();
        log.append(Event {
            timestamp: 100,
            level: EventLevel::Info,
            source: "lifecycle".into(),
            message: "Starting readiness phase".into(),
        });
        log.append_infra(InfraEvent::DockerEvent {
            container: "kafka".into(),
            phase: Phase::Readiness,
            timestamp: 103,
            scenario_id: "s".into(),
            kind: DockerEventKind::OomKilled,
            severity: InfraSeverity::Critical,
            exit_code: Some(137),
            oom_killed: Some(true),
            detail: Some("die".into()),
        });
        log.append_infra(InfraEvent::ResourceSample {
            container: "kafka".into(),
            phase: Phase::Readiness,
            timestamp: 103,
            scenario_id: "s".into(),
            cpu_pct: 1.0,
            mem_bytes: 508 * 1024 * 1024,
            mem_limit: 512 * 1024 * 1024,
            net_rx: 0,
            net_tx: 0,
        });
        log.append(Event {
            timestamp: 110,
            level: EventLevel::Error,
            source: "lifecycle".into(),
            message: "Readiness failed: broker not up".into(),
        });

        let options = FailureTimelineOptions {
            scenario_name: "oom_scenario".into(),
            failure_summary: Some("Readiness failed".into()),
            use_color: false,
            verdict: "infrastructure_error".into(),
            scenario_timeout_secs: None,
            performance_assertion_failure: None,
        };
        let rendered = render_failure_timeline(&log, &options);
        assert!(rendered.contains("--- HINTS ---"));
        assert!(rendered.contains("HINT:"));
        assert!(rendered.contains("This may have contributed to this failure."));
        assert!(rendered.contains("OOMKilled"));

        let document = build_failure_timeline_document(&log, &options);
        assert_eq!(document.hints.len(), 1);
        let hint = &document.hints[0];
        assert_eq!(
            hint.get("type").and_then(|v| v.as_str()),
            Some("oom_before_failure")
        );
        assert_eq!(
            hint.get("container").and_then(|v| v.as_str()),
            Some("kafka")
        );
        assert!(hint.get("message").and_then(|v| v.as_str()).is_some());
        assert_eq!(hint.get("gap_ms").and_then(|v| v.as_u64()), Some(7000));
    }

    #[test]
    fn root_cause_hints_skips_infra_events_at_or_after_failure_time() {
        let mut log = EventLog::new();
        log.append(Event {
            timestamp: 1,
            level: EventLevel::Info,
            source: "lifecycle".into(),
            message: "start".into(),
        });
        log.append(Event {
            timestamp: 5,
            level: EventLevel::Error,
            source: "lifecycle".into(),
            message: "failed early".into(),
        });
        log.append_infra(InfraEvent::DockerEvent {
            container: "late".into(),
            phase: Phase::Run,
            timestamp: 10,
            scenario_id: "s".into(),
            kind: DockerEventKind::OomKilled,
            severity: InfraSeverity::Critical,
            exit_code: Some(137),
            oom_killed: Some(true),
            detail: None,
        });

        let options = FailureTimelineOptions {
            scenario_name: "x".into(),
            failure_summary: None,
            use_color: false,
            verdict: "failed".into(),
            scenario_timeout_secs: None,
            performance_assertion_failure: None,
        };
        let rendered = render_failure_timeline(&log, &options);
        assert!(!rendered.contains("--- HINTS ---"));
        let document = build_failure_timeline_document(&log, &options);
        assert!(document.hints.is_empty());
    }

    #[test]
    fn root_cause_hints_timeout_verdict_uses_suggested_timeout_phrase() {
        let mut log = EventLog::new();
        log.append(Event {
            timestamp: 1,
            level: EventLevel::Info,
            source: "lifecycle".into(),
            message: "Starting assert phase".into(),
        });
        log.append_infra(InfraEvent::DockerEvent {
            container: "pg".into(),
            phase: Phase::Assert,
            timestamp: 2,
            scenario_id: "s".into(),
            kind: DockerEventKind::HealthUnhealthy,
            severity: InfraSeverity::Warning,
            exit_code: None,
            oom_killed: None,
            detail: None,
        });
        log.append(Event {
            timestamp: 12,
            level: EventLevel::Error,
            source: "scenario".into(),
            message: "await_until timed out".into(),
        });

        let options = FailureTimelineOptions {
            scenario_name: "t".into(),
            failure_summary: None,
            use_color: false,
            verdict: "timeout".into(),
            scenario_timeout_secs: None,
            performance_assertion_failure: None,
        };
        let rendered = render_failure_timeline(&log, &options);
        assert!(rendered.contains("This may have caused the timeout."));
    }

    #[test]
    fn performance_assertion_section_appears_in_render_and_json() {
        let log = EventLog::new();
        let options = FailureTimelineOptions {
            scenario_name: "perf_case".into(),
            failure_summary: Some("p99 too high".into()),
            use_color: false,
            verdict: "performance_assertion_failed".into(),
            scenario_timeout_secs: None,
            performance_assertion_failure: Some(PerformanceAssertionFailureJson {
                load_name: "checkout".into(),
                metric: "p99".into(),
                measured: "120ms".into(),
                threshold: "50ms".into(),
                load_phase: "Constant { duration: 5s, rate: Rate(10.0) }".into(),
                comparison_table: Some("| m | ok |\n|---|--|".into()),
            }),
        };
        let rendered = render_failure_timeline(&log, &options);
        assert!(rendered.contains("--- PERFORMANCE ASSERTION ---"));
        assert!(rendered.contains("checkout"));
        assert!(rendered.contains("p99"));
        assert!(rendered.contains("| m | ok |"));

        let document = build_failure_timeline_document(&log, &options);
        assert_eq!(document.scenario.verdict, "performance_assertion_failed");
        let perf = document
            .performance_assertion_failure
            .as_ref()
            .expect("performance block");
        assert_eq!(perf.load_name, "checkout");
        let json = serde_json::to_value(&document).expect("json");
        assert_eq!(
            json.get("performance_assertion_failure")
                .and_then(|v| v.get("metric"))
                .and_then(|v| v.as_str()),
            Some("p99")
        );
    }
}
