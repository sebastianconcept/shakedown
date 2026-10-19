//! Timeline rows the load driver and handle publish to the scenario event log.

use std::sync::mpsc::Sender;

use crate::event::Event;

const LOAD_EVENT_SOURCE: &str = "load";

/// Publish `message` as a `load` timeline row prefixed with `load_name`, when the run has a sink.
pub(super) fn try_emit_load_event(
    events: Option<&Sender<Event>>,
    load_name: &str,
    message: impl Into<String>,
) {
    if let Some(sender) = events {
        let text = format!("{load_name}: {}", message.into());
        let _ = sender.send(Event::info(LOAD_EVENT_SOURCE, text));
    }
}
