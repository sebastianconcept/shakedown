//! Docker Engine `/events` stream → [`crate::event::InfraEvent`] records.

use std::collections::HashMap;
use std::sync::Arc;

use bollard::models::EventMessage;
use bollard::query_parameters::EventsOptionsBuilder;
use bollard::Docker;
use futures_util::StreamExt;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;

use crate::event::{DockerEventKind, InfraEvent, InfraSeverity};
use crate::phase::Phase;

use super::labels::shakedown_docker_label_filter;

/// Background task: stream filtered container events into `sink`.
pub(crate) struct DockerEventsMonitor {
    join: JoinHandle<()>,
    sink: Arc<Mutex<Vec<InfraEvent>>>,
}

impl DockerEventsMonitor {
    pub(crate) fn spawn(
        docker: Docker,
        scenario_id: String,
    ) -> (Self, watch::Sender<(Phase, String)>) {
        let (phase_tx, phase_rx) = watch::channel((Phase::Provision, scenario_id));
        let sink = Arc::new(Mutex::new(Vec::new()));
        let sink_clone = Arc::clone(&sink);
        let join = tokio::spawn(run_docker_event_stream(docker, sink_clone, phase_rx));
        (Self { join, sink }, phase_tx)
    }

    pub(crate) async fn shutdown_and_drain(self, event_log: &mut crate::event::EventLog) {
        let DockerEventsMonitor { join, sink } = self;
        join.abort();
        let _ = join.await;
        let mut guard = sink.lock().await;
        for record in guard.drain(..) {
            event_log.append_infra(record);
        }
    }
}

pub(crate) fn push_lifecycle_phase(
    phase_sender: &watch::Sender<(Phase, String)>,
    phase: Phase,
    scenario_id: &str,
) {
    let _ = phase_sender.send((phase, scenario_id.to_string()));
}

fn events_options() -> bollard::query_parameters::EventsOptions {
    let label = shakedown_docker_label_filter();
    let mut filters = HashMap::new();
    filters.insert("type".to_string(), vec!["container".to_string()]);
    filters.insert("label".to_string(), vec![label]);
    EventsOptionsBuilder::new().filters(&filters).build()
}

fn event_timestamp_secs(message: &EventMessage) -> u64 {
    if let Some(nano) = message.time_nano {
        if nano > 0 {
            return (nano / 1_000_000_000).max(0) as u64;
        }
    }
    message.time.unwrap_or(0).max(0) as u64
}

fn container_name_from_actor(message: &EventMessage) -> String {
    let Some(actor) = &message.actor else {
        return String::new();
    };
    let Some(attrs) = &actor.attributes else {
        return actor.id.clone().unwrap_or_default();
    };
    attrs
        .get("name")
        .map(|name| name.trim_start_matches('/').to_string())
        .filter(|name| !name.is_empty())
        .or_else(|| actor.id.clone())
        .unwrap_or_default()
}

fn parse_exit_code(attributes: &HashMap<String, String>) -> Option<i64> {
    attributes
        .get("exitCode")
        .and_then(|value| value.parse().ok())
}

fn parse_oom_killed(attributes: &HashMap<String, String>) -> Option<bool> {
    match attributes.get("oomKilled")?.as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// Classify a Docker [`EventMessage`] into a Shakedown infrastructure record, if relevant.
fn classify_container_event(
    message: &EventMessage,
    phase: Phase,
    scenario_id: &str,
) -> Option<InfraEvent> {
    let action = message.action.as_deref()?;
    let timestamp = event_timestamp_secs(message);
    let container = container_name_from_actor(message);

    if action == "restart" {
        return Some(InfraEvent::DockerEvent {
            container,
            phase,
            timestamp,
            scenario_id: scenario_id.to_string(),
            kind: DockerEventKind::Restart,
            severity: InfraSeverity::Info,
            exit_code: None,
            oom_killed: None,
            detail: Some("restart".to_string()),
        });
    }

    if action == "health_status: unhealthy" {
        return Some(InfraEvent::DockerEvent {
            container,
            phase,
            timestamp,
            scenario_id: scenario_id.to_string(),
            kind: DockerEventKind::HealthUnhealthy,
            severity: InfraSeverity::Warning,
            exit_code: None,
            oom_killed: None,
            detail: Some(action.to_string()),
        });
    }

    if action == "die" {
        let (exit_code, oom_killed) = message
            .actor
            .as_ref()
            .and_then(|actor| actor.attributes.as_ref())
            .map(|attributes| (parse_exit_code(attributes), parse_oom_killed(attributes)))
            .unwrap_or((None, None));

        let oom = oom_killed == Some(true);
        if oom {
            return Some(InfraEvent::DockerEvent {
                container,
                phase,
                timestamp,
                scenario_id: scenario_id.to_string(),
                kind: DockerEventKind::OomKilled,
                severity: InfraSeverity::Critical,
                exit_code,
                oom_killed: Some(true),
                detail: Some("die".to_string()),
            });
        }

        let non_zero = exit_code.is_some_and(|code| code != 0);
        if non_zero {
            return Some(InfraEvent::DockerEvent {
                container,
                phase,
                timestamp,
                scenario_id: scenario_id.to_string(),
                kind: DockerEventKind::Exit,
                severity: InfraSeverity::Critical,
                exit_code,
                oom_killed: oom_killed.or(Some(false)),
                detail: Some("die".to_string()),
            });
        }

        return None;
    }

    None
}

async fn run_docker_event_stream(
    docker: Docker,
    sink: Arc<Mutex<Vec<InfraEvent>>>,
    phase_receiver: watch::Receiver<(Phase, String)>,
) {
    let options = events_options();
    let mut stream = docker.events(Some(options));
    while let Some(item) = stream.next().await {
        match item {
            Ok(message) => {
                let (phase, scenario_id) = phase_receiver.borrow().clone();
                if let Some(record) = classify_container_event(&message, phase, &scenario_id) {
                    let mut guard = sink.lock().await;
                    guard.push(record);
                }
            }
            Err(error) => {
                tracing::warn!(
                    target: "shakedown::harness",
                    error = %error,
                    "shakedown diagnostics: Docker events stream error; stopping infrastructure event monitor"
                );
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bollard::models::EventActor;
    use std::collections::HashMap;

    #[test]
    fn die_non_zero_is_critical_exit() {
        let mut attributes = HashMap::new();
        attributes.insert("exitCode".to_string(), "1".to_string());
        attributes.insert("oomKilled".to_string(), "false".to_string());
        let message = EventMessage {
            action: Some("die".to_string()),
            actor: Some(EventActor {
                id: Some("abc".to_string()),
                attributes: Some(attributes),
            }),
            time: Some(100),
            time_nano: None,
            typ: None,
            scope: None,
        };
        let record = classify_container_event(&message, Phase::Run, "my_scenario").expect("record");
        match record {
            InfraEvent::DockerEvent {
                kind: DockerEventKind::Exit,
                severity: InfraSeverity::Critical,
                exit_code: Some(1),
                scenario_id,
                ..
            } => assert_eq!(scenario_id, "my_scenario"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn die_oom_is_critical_oom() {
        let mut attributes = HashMap::new();
        attributes.insert("exitCode".to_string(), "137".to_string());
        attributes.insert("oomKilled".to_string(), "true".to_string());
        let message = EventMessage {
            action: Some("die".to_string()),
            actor: Some(EventActor {
                id: Some("abc".to_string()),
                attributes: Some(attributes),
            }),
            time: Some(100),
            time_nano: None,
            typ: None,
            scope: None,
        };
        let record = classify_container_event(&message, Phase::Seed, "s").expect("record");
        match record {
            InfraEvent::DockerEvent {
                kind: DockerEventKind::OomKilled,
                severity: InfraSeverity::Critical,
                oom_killed: Some(true),
                ..
            } => {}
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn die_zero_exit_is_ignored() {
        let mut attributes = HashMap::new();
        attributes.insert("exitCode".to_string(), "0".to_string());
        let message = EventMessage {
            action: Some("die".to_string()),
            actor: Some(EventActor {
                id: Some("abc".to_string()),
                attributes: Some(attributes),
            }),
            time: Some(100),
            time_nano: None,
            typ: None,
            scope: None,
        };
        assert!(classify_container_event(&message, Phase::Run, "s").is_none());
    }

    #[test]
    fn restart_is_info() {
        let message = EventMessage {
            action: Some("restart".to_string()),
            actor: Some(EventActor {
                id: Some("abc".to_string()),
                attributes: Some(HashMap::from([("name".to_string(), "/fancy".to_string())])),
            }),
            time: Some(10),
            time_nano: None,
            typ: None,
            scope: None,
        };
        let record = classify_container_event(&message, Phase::Provision, "sid").expect("record");
        match record {
            InfraEvent::DockerEvent {
                kind: DockerEventKind::Restart,
                severity: InfraSeverity::Info,
                container,
                ..
            } => assert_eq!(container, "fancy"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn health_unhealthy_is_warning() {
        let message = EventMessage {
            action: Some("health_status: unhealthy".to_string()),
            actor: Some(EventActor {
                id: Some("x".to_string()),
                attributes: None,
            }),
            time: Some(5),
            time_nano: None,
            typ: None,
            scope: None,
        };
        let record = classify_container_event(&message, Phase::Assert, "a").expect("record");
        match record {
            InfraEvent::DockerEvent {
                kind: DockerEventKind::HealthUnhealthy,
                severity: InfraSeverity::Warning,
                ..
            } => {}
            other => panic!("unexpected {other:?}"),
        }
    }
}
