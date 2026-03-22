//! Event logging for scenario orchestration.
//!
//! Shakedown records structured events during scenario runs, enabling
//! failure timeline reconstruction and diagnostics.

use std::time::{SystemTime, UNIX_EPOCH};

/// Represents a log level for events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventLevel {
    /// Debug-level events for detailed tracing
    Debug,
    /// Informational events during scenario execution
    Info,
    /// Warning about potential issues
    Warn,
    /// Error in a resource or phase
    Error,
}

impl std::fmt::Display for EventLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventLevel::Debug => write!(f, "DEBUG"),
            EventLevel::Info => write!(f, "INFO"),
            EventLevel::Warn => write!(f, "WARN"),
            EventLevel::Error => write!(f, "ERROR"),
        }
    }
}

/// Represents a structured event in the scenario lifecycle.
#[derive(Debug, Clone)]
pub struct Event {
    /// Timestamp when the event occurred
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

/// A simple in-memory event log.
#[derive(Debug, Default, Clone)]
pub struct EventLog {
    /// The collected events
    events: Vec<Event>,
}

impl EventLog {
    /// Create a new empty event log.
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    /// Append an event to the log.
    pub fn append(&mut self, event: Event) {
        self.events.push(event);
    }

    /// Append an event with the given level, source, and message.
    pub fn append_raw(
        &mut self,
        level: EventLevel,
        source: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.events.push(Event::new(level, source, message));
    }

    /// Get a reference to all events in the log.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Get the number of events in the log.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Clear all events from the log.
    pub fn clear(&mut self) {
        self.events.clear();
    }
}

/// A read-only view of an event log.
#[derive(Debug, Clone)]
pub struct EventSnapshot {
    /// The events at the time of snapshot
    events: Vec<Event>,
}

impl EventSnapshot {
    /// Create a new snapshot from an event log.
    pub fn from_log(log: &EventLog) -> Self {
        Self {
            events: log.events().to_vec(),
        }
    }

    /// Get a reference to the snapshot's events.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Get the number of events in the snapshot.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if the snapshot is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl From<EventLog> for Vec<Event> {
    fn from(log: EventLog) -> Self {
        log.events
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
        let event = Event::info("scenario", "starting scenario");

        assert_eq!(event.level, EventLevel::Info);
        assert_eq!(event.source, "scenario");
        assert_eq!(event.message, "starting scenario");
        assert!(event.timestamp > 0);
    }

    #[test]
    fn test_event_levels() {
        let debug = Event::debug("source", "debug message");
        let info = Event::info("source", "info message");
        let warn = Event::warn("source", "warn message");
        let error = Event::error("source", "error message");

        assert_eq!(debug.level, EventLevel::Debug);
        assert_eq!(info.level, EventLevel::Info);
        assert_eq!(warn.level, EventLevel::Warn);
        assert_eq!(error.level, EventLevel::Error);
    }

    #[test]
    fn test_event_display() {
        let event = Event::new(EventLevel::Info, "scenario", "test message");

        assert_eq!(format!("{}", event.level), "INFO");
    }

    #[test]
    fn test_event_log_append() {
        let mut log = EventLog::new();

        assert!(log.is_empty());

        let event1 = Event::info("scenario", "first event");
        let event2 = Event::debug("resource:fake", "second event");

        log.append(event1);
        log.append(event2);

        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_event_log_append_raw() {
        let mut log = EventLog::new();

        log.append_raw(EventLevel::Info, "scenario", "test message");
        log.append_raw(EventLevel::Error, "resource:fake", "error occurred");

        assert_eq!(log.len(), 2);

        let events = log.events();
        assert_eq!(events[0].message, "test message");
        assert_eq!(events[1].source, "resource:fake");
    }

    #[test]
    fn test_event_log_snapshot() {
        let mut log = EventLog::new();

        log.append(Event::info("scenario", "test event"));

        let snapshot = EventSnapshot::from_log(&log);

        assert_eq!(snapshot.len(), 1);
        assert!(!snapshot.is_empty());

        let events: Vec<Event> = log.into();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_event_log_clear() {
        let mut log = EventLog::new();

        log.append(Event::info("scenario", "test"));
        assert_eq!(log.len(), 1);

        log.clear();
        assert!(log.is_empty());
    }

    #[test]
    fn test_event_log_iter() {
        let mut log = EventLog::new();

        for i in 0..5 {
            log.append(Event::info("scenario", format!("event {}", i)));
        }

        assert_eq!(log.len(), 5);

        let collected: Vec<_> = log.events().iter().map(|e| e.message.clone()).collect();
        assert_eq!(
            collected,
            vec!["event 0", "event 1", "event 2", "event 3", "event 4"]
        );
    }
}
