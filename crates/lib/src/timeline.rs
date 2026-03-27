//! Human-readable failure timeline for [`crate::event::EventLog`].
//!
//! On scenario failure, the harness sorts application and infrastructure records by time,
//! prints an interleaved view (excluding dense [`crate::event::InfraEvent::ResourceSample`] rows —
//! those appear only in the resource snapshot), and summarizes the latest resource stats per
//! container.

use std::collections::HashMap;
use std::io::{self, IsTerminal};

use crate::event::{
    DockerEventKind, EventLevel, EventLog, InfraEvent, InfraSeverity, LogEntry, ScenarioLogEntry,
};
use crate::phase::Phase;

/// Options for [`render_failure_timeline`].
#[derive(Debug, Clone)]
pub struct FailureTimelineOptions {
    /// Scenario name or id (shown in the header).
    pub scenario_name: String,
    /// Short failure reason (e.g. error display string).
    pub failure_summary: Option<String>,
    /// Whether to emit ANSI colors for severity (stderr when supported).
    pub use_color: bool,
}

impl FailureTimelineOptions {
    /// Build options with [`Self::use_color`] set from stderr terminal detection.
    pub fn new(scenario_name: impl Into<String>) -> Self {
        Self {
            scenario_name: scenario_name.into(),
            failure_summary: None,
            use_color: io::stderr().is_terminal(),
        }
    }

    /// Sets the failure summary line (shown in the header).
    pub fn with_failure_summary(mut self, summary: impl Into<String>) -> Self {
        self.failure_summary = Some(summary.into());
        self
    }
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
        return out;
    }

    let baseline_secs = entries
        .iter()
        .map(|(_, entry)| entry.timestamp_secs())
        .min()
        .unwrap_or(0);

    let mut timeline_rows: Vec<(u64, usize, TimelineRow)> = Vec::new();
    let mut samples: Vec<&InfraEvent> = Vec::new();

    for (index, entry) in &entries {
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

    out.push('\n');
    out
}

/// Writes a rendered failure timeline to stderr (for harness use on error paths).
pub fn emit_failure_timeline_to_stderr(
    event_log: &EventLog,
    scenario_id: &str,
    failure_summary: &str,
) {
    let options = FailureTimelineOptions {
        scenario_name: scenario_id.to_string(),
        failure_summary: Some(failure_summary.to_string()),
        use_color: io::stderr().is_terminal(),
    };
    eprintln!("{}", render_failure_timeline(event_log, &options));
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
        };
        let rendered = render_failure_timeline(&log, &options);
        assert!(rendered.contains("FAILURE TIMELINE — my_scenario"));
        assert!(rendered.contains("⚠"));
        assert!(rendered.contains("✗"));
        assert!(rendered.contains("CONTAINER EVENT:"));
        assert!(rendered.contains("RESOURCE SNAPSHOT"));
        assert!(rendered.contains("CPU:"));
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
        };
        let rendered = render_failure_timeline(&log, &options);
        assert!(rendered.contains("9.0%"));
        assert!(!rendered.contains("1.0%"));
    }
}
