//! Event logging for scenario orchestration.
//!
//! What: [`Event`] records, [`InfraEvent`] infrastructure records, [`LogEntry`] union, and
//! [`EventLog`] / [`EventSnapshot`] types for ordered timelines.
//!
//! Why: when a scenario fails, operators need level, source, and message without attaching a full
//! logger to every test; infrastructure diagnostics use the same append-only log via
//! [`LogEntry::Infrastructure`].
//!
//! How: the lifecycle appends phase transitions; tests and resources may append additional entries;
//! inspect [`EventLog::entries`] after [`crate::lifecycle::run_scenario`] completes for the full
//! timeline, or [`EventLog::events`] for application-level [`Event`] records only.

use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::lifecycle::Phase;

/// Severity for a single [`Event`] (mirrors common log levels).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, displaydoc::Display)]
#[ignore_extra_doc_attributes]
pub enum EventLevel {
    /// DEBUG
    Debug,
    /// INFO
    Info,
    /// WARN
    Warn,
    /// ERROR
    Error,
}

/// One timestamped record in the scenario timeline.
///
/// Exists so callers log with a consistent shape (level, source, message, time) whether the
/// emitter is the harness, a resource, or the scenario. Created via [`Event::new`] or helpers
/// [`Event::info`], [`Event::debug`], etc., and stored in [`EventLog`] as [`LogEntry::Application`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Timestamp when the event occurred (UNIX epoch seconds).
    pub timestamp: u64,
    /// Log level of this event
    pub level: EventLevel,
    /// Source of the event (e.g., "scenario", "resource:fake")
    pub source: String,
    /// Message describing the event
    pub message: String,
}

impl Event {
    /// Create a new event with the current timestamp.
    pub fn new(level: EventLevel, source: impl Into<String>, message: impl Into<String>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            timestamp,
            level,
            source: source.into(),
            message: message.into(),
        }
    }

    /// Create a debug event.
    pub fn debug(source: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(EventLevel::Debug, source, message)
    }

    /// Create an info event.
    pub fn info(source: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(EventLevel::Info, source, message)
    }

    /// Create a warning event.
    pub fn warn(source: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(EventLevel::Warn, source, message)
    }

    /// Create an error event.
    pub fn error(source: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(EventLevel::Error, source, message)
    }
}

/// Severity for Docker-side infrastructure events (distinct from [`EventLevel`]).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InfraSeverity {
    /// Informational (e.g. container restart)
    Info,
    /// Warning (e.g. health check unhealthy)
    Warning,
    /// Critical (e.g. OOM, non-zero exit)
    Critical,
}

/// Classified Docker engine / container event for [`InfraEvent::DockerEvent`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DockerEventKind {
    /// Container was OOM-killed
    OomKilled,
    /// Container exited
    Exit,
    /// Health status became unhealthy
    HealthUnhealthy,
    /// Container restarted
    Restart,
    /// Other action (raw descriptor)
    Other(String),
}

/// Infrastructure diagnostic record (container logs, Docker events, resource samples).
///
/// Exists so the harness stores Docker-side observations in the same [`EventLog`] as scenario
/// messages without conflating them with [`Event`] (application-level) entries.
///
/// Emitted by infrastructure diagnostics (stats, logs, Docker engine events; the `instrumentation::diagnostics`
/// submodule when a Docker-backed resource or `services` is enabled) and merged via [`LogEntry::Infrastructure`];
/// serde-stable for failure timeline JSON and snapshots.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InfraEvent {
    /// One line of container stdout/stderr
    ContainerLog {
        /// Container name or identifier
        container: String,
        /// Active lifecycle phase when the line was observed
        phase: Phase,
        /// UNIX epoch seconds when observed
        timestamp: u64,
        /// Log line text
        line: String,
        /// Stream name when known (`stdout` / `stderr`)
        #[serde(skip_serializing_if = "Option::is_none")]
        stream: Option<String>,
    },
    /// Docker engine event (OOM, exit, health, restart, …)
    DockerEvent {
        /// Container name or identifier
        container: String,
        /// Active lifecycle phase when the event was observed
        phase: Phase,
        /// UNIX epoch seconds when observed
        timestamp: u64,
        /// Scenario id from the harness when the event was observed
        #[serde(default, skip_serializing_if = "String::is_empty")]
        scenario_id: String,
        /// Classified event kind
        kind: DockerEventKind,
        /// Severity for timeline highlighting
        severity: InfraSeverity,
        /// Process exit code when applicable
        #[serde(skip_serializing_if = "Option::is_none")]
        exit_code: Option<i64>,
        /// Whether the kernel reported OOM kill
        #[serde(skip_serializing_if = "Option::is_none")]
        oom_killed: Option<bool>,
        /// Extra detail (e.g. raw Docker action)
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
    /// Sample of CPU, memory, and network usage for a container
    ResourceSample {
        /// Container name or identifier
        container: String,
        /// Active lifecycle phase when the sample was taken
        phase: Phase,
        /// UNIX epoch seconds when sampled
        timestamp: u64,
        /// Harness scenario id when the sample was taken
        #[serde(default, skip_serializing_if = "String::is_empty")]
        scenario_id: String,
        /// CPU usage as a percentage (host-relative)
        cpu_pct: f64,
        /// Resident memory usage in bytes
        mem_bytes: u64,
        /// Memory limit in bytes when known
        mem_limit: u64,
        /// Network bytes received
        net_rx: u64,
        /// Network bytes transmitted
        net_tx: u64,
    },
}

impl InfraEvent {
    /// UNIX timestamp for this record (seconds).
    pub fn timestamp_secs(&self) -> u64 {
        match self {
            InfraEvent::ContainerLog { timestamp, .. }
            | InfraEvent::DockerEvent { timestamp, .. }
            | InfraEvent::ResourceSample { timestamp, .. } => *timestamp,
        }
    }
}

/// Union of application-level and infrastructure events in one append-only log.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEntry {
    /// Harness / resource / scenario [`Event`]
    Application(Event),
    /// Docker-backed diagnostic record
    Infrastructure(InfraEvent),
}

/// Common interface for entries stored in [`EventLog`].
pub trait ScenarioLogEntry: Debug + Clone + Serialize {
    /// UNIX timestamp (seconds) for ordering timelines.
    fn timestamp_secs(&self) -> u64;
}

impl ScenarioLogEntry for Event {
    fn timestamp_secs(&self) -> u64 {
        self.timestamp
    }
}

impl ScenarioLogEntry for InfraEvent {
    fn timestamp_secs(&self) -> u64 {
        InfraEvent::timestamp_secs(self)
    }
}

impl ScenarioLogEntry for LogEntry {
    fn timestamp_secs(&self) -> u64 {
        match self {
            LogEntry::Application(event) => event.timestamp_secs(),
            LogEntry::Infrastructure(infra) => infra.timestamp_secs(),
        }
    }
}

/// Append-only in-memory collection of [`LogEntry`] values for one scenario run.
///
/// Why in-memory: tests should stay hermetic and fast; this type is enough for assertions and
/// debugging without configuring `tracing` or `log` in every crate.
#[derive(Debug, Default, Clone)]
pub struct EventLog {
    entries: Vec<LogEntry>,
}

impl EventLog {
    /// Create a new empty event log.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Append an application [`Event`].
    pub fn append(&mut self, event: Event) {
        self.entries.push(LogEntry::Application(event));
    }

    /// Append an infrastructure [`InfraEvent`].
    pub fn append_infra(&mut self, event: InfraEvent) {
        self.entries.push(LogEntry::Infrastructure(event));
    }

    /// Append an application event with the given level, source, and message.
    pub fn append_raw(
        &mut self,
        level: EventLevel,
        source: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.append(Event::new(level, source, message));
    }

    /// Append any [`LogEntry`] (application or infrastructure).
    pub fn append_entry(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    /// Full chronological log (application and infrastructure).
    pub fn entries(&self) -> &[LogEntry] {
        &self.entries
    }

    /// Iterator over application [`Event`] records only (excludes [`LogEntry::Infrastructure`]).
    pub fn events(&self) -> impl DoubleEndedIterator<Item = &Event> + '_ {
        self.entries.iter().filter_map(|entry| match entry {
            LogEntry::Application(event) => Some(event),
            LogEntry::Infrastructure(_) => None,
        })
    }

    /// Get the number of entries in the log (application and infrastructure).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all events from the log.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Immutable copy of an [`EventLog`] at a point in time.
///
/// Use when you need to retain events while continuing to append to the original log, or to pass
/// a cheap clone across threads after the run.
#[derive(Debug, Clone)]
pub struct EventSnapshot {
    entries: Vec<LogEntry>,
}

impl EventSnapshot {
    /// Create a new snapshot from an event log.
    pub fn from_log(log: &EventLog) -> Self {
        Self {
            entries: log.entries().to_vec(),
        }
    }

    /// Full chronological log at snapshot time.
    pub fn entries(&self) -> &[LogEntry] {
        &self.entries
    }

    /// Application events only.
    pub fn events(&self) -> impl DoubleEndedIterator<Item = &Event> + '_ {
        self.entries.iter().filter_map(|entry| match entry {
            LogEntry::Application(event) => Some(event),
            LogEntry::Infrastructure(_) => None,
        })
    }

    /// Get the number of entries in the snapshot.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the snapshot is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl From<EventLog> for Vec<Event> {
    fn from(log: EventLog) -> Self {
        log.entries
            .into_iter()
            .filter_map(|entry| match entry {
                LogEntry::Application(event) => Some(event),
                LogEntry::Infrastructure(_) => None,
            })
            .collect()
    }
}

impl From<EventLog> for Vec<LogEntry> {
    fn from(log: EventLog) -> Self {
        log.entries
    }
}

impl From<EventLog> for EventSnapshot {
    fn from(log: EventLog) -> Self {
        Self::from_log(&log)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        // Given an info-level event from the scenario source
        let event = Event::info("scenario", "starting scenario");
        // When we inspect its fields
        // Then level, source, message, and a positive timestamp are set
        assert_eq!(event.level, EventLevel::Info);
        assert_eq!(event.source, "scenario");
        assert_eq!(event.message, "starting scenario");
        assert!(event.timestamp > 0);
    }

    #[test]
    fn test_event_levels() {
        // Given one event per log level from the same source
        let debug = Event::debug("source", "debug message");
        let info = Event::info("source", "info message");
        let warn = Event::warn("source", "warn message");
        let error = Event::error("source", "error message");
        // When we read their levels
        // Then each matches the constructor used
        assert_eq!(debug.level, EventLevel::Debug);
        assert_eq!(info.level, EventLevel::Info);
        assert_eq!(warn.level, EventLevel::Warn);
        assert_eq!(error.level, EventLevel::Error);
    }

    #[test]
    fn test_event_display() {
        // Given an info-level event
        let event = Event::new(EventLevel::Info, "scenario", "test message");
        // When we format its level for display
        let level_display = format!("{}", event.level);
        // Then we get the uppercase level label
        assert_eq!(level_display, "INFO");
    }

    #[test]
    fn test_event_log_append() {
        // Given an empty event log
        let mut log = EventLog::new();
        assert!(log.is_empty());
        // When we append two structured events
        let event1 = Event::info("scenario", "first event");
        let event2 = Event::debug("resource:fake", "second event");
        log.append(event1);
        log.append(event2);
        // Then the log length reflects both entries
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_event_log_append_raw() {
        // Given an empty event log
        let mut log = EventLog::new();
        // When we append raw entries at info and error levels
        log.append_raw(EventLevel::Info, "scenario", "test message");
        log.append_raw(EventLevel::Error, "resource:fake", "error occurred");
        // Then both messages are stored with the expected source and text
        assert_eq!(log.len(), 2);
        let events: Vec<_> = log.events().collect();
        assert_eq!(events[0].message, "test message");
        assert_eq!(events[1].source, "resource:fake");
    }

    #[test]
    fn test_event_log_snapshot() {
        // Given a log with one event
        let mut log = EventLog::new();
        log.append(Event::info("scenario", "test event"));
        // When we take a snapshot and then consume the log into a vector of application events
        let snapshot = EventSnapshot::from_log(&log);
        let events: Vec<Event> = log.into();
        // Then the snapshot mirrors the single event and the consumed vec matches
        assert_eq!(snapshot.len(), 1);
        assert!(!snapshot.is_empty());
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_event_log_clear() {
        // Given a log with one event
        let mut log = EventLog::new();
        log.append(Event::info("scenario", "test"));
        assert_eq!(log.len(), 1);
        // When we clear the log
        log.clear();
        // Then it is empty again
        assert!(log.is_empty());
    }

    #[test]
    fn test_event_log_iter() {
        // Given a log with five numbered scenario events
        let mut log = EventLog::new();
        for i in 0..5 {
            log.append(Event::info("scenario", format!("event {}", i)));
        }
        assert_eq!(log.len(), 5);
        // When we collect messages in order
        let collected: Vec<_> = log.events().map(|e| e.message.clone()).collect();
        // Then they are the sequence event 0 through event 4
        assert_eq!(
            collected,
            vec!["event 0", "event 1", "event 2", "event 3", "event 4"]
        );
    }

    #[test]
    fn test_scenario_log_entry_trait() {
        let app = Event::info("harness", "x");
        assert_eq!(ScenarioLogEntry::timestamp_secs(&app), app.timestamp);

        let infra = InfraEvent::ContainerLog {
            container: "kafka".into(),
            phase: Phase::Run,
            timestamp: 42,
            line: "ready".into(),
            stream: Some("stdout".into()),
        };
        assert_eq!(ScenarioLogEntry::timestamp_secs(&infra), 42);

        let combined = LogEntry::Infrastructure(infra.clone());
        assert_eq!(ScenarioLogEntry::timestamp_secs(&combined), 42);
    }

    #[test]
    fn test_append_infra_mixed_timeline() {
        let mut log = EventLog::new();
        log.append(Event::info("lifecycle", "seed"));
        log.append_infra(InfraEvent::ResourceSample {
            container: "pg".into(),
            phase: Phase::Seed,
            timestamp: 10,
            scenario_id: String::new(),
            cpu_pct: 1.0,
            mem_bytes: 100,
            mem_limit: 200,
            net_rx: 0,
            net_tx: 0,
        });
        assert_eq!(log.len(), 2);
        assert_eq!(log.entries()[1].timestamp_secs(), 10);
        let app_only: Vec<_> = log.events().collect();
        assert_eq!(app_only.len(), 1);
    }

    #[test]
    fn test_infra_event_serde_round_trip() {
        let sample = InfraEvent::DockerEvent {
            container: "c".into(),
            phase: Phase::Assert,
            timestamp: 99,
            scenario_id: "scenario_x".into(),
            kind: DockerEventKind::OomKilled,
            severity: InfraSeverity::Critical,
            exit_code: Some(137),
            oom_killed: Some(true),
            detail: None,
        };
        let json = serde_json::to_string(&sample).expect("serialize");
        let back: InfraEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.timestamp_secs(), 99);
    }
}
