//! Stream container stdout/stderr via bollard [`Docker::logs`] into [`InfraEvent::ContainerLog`] records.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bollard::container::LogOutput;
use bollard::query_parameters::LogsOptionsBuilder;
use bollard::Docker;
use futures_util::StreamExt;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration, MissedTickBehavior};

use crate::event::InfraEvent;
use crate::phase::Phase;

use super::labels::shakedown_docker_label_filter;

/// Background task: follow logs for Shakedown-labeled running containers; drain into [`EventLog`] at shutdown.
pub(crate) struct ContainerLogsMonitor {
    join: JoinHandle<()>,
    sink: Arc<Mutex<Vec<InfraEvent>>>,
}

impl ContainerLogsMonitor {
    pub(crate) fn spawn(docker: Docker, phase_receiver: watch::Receiver<(Phase, String)>) -> Self {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let sink_clone = Arc::clone(&sink);
        let join = tokio::spawn(run_log_supervisor(docker, phase_receiver, sink_clone));
        Self { join, sink }
    }

    pub(crate) async fn shutdown_and_drain(self, event_log: &mut crate::event::EventLog) {
        let ContainerLogsMonitor { join, sink } = self;
        join.abort();
        let _ = join.await;
        let mut guard = sink.lock().await;
        let mut collected: Vec<InfraEvent> = guard.drain(..).collect();
        collected.sort_by_key(|event| event.timestamp_secs());
        for event in collected {
            event_log.append_infra(event);
        }
    }
}

async fn run_log_supervisor(
    docker: Docker,
    phase_receiver: watch::Receiver<(Phase, String)>,
    sink: Arc<Mutex<Vec<InfraEvent>>>,
) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut ticker = interval(Duration::from_millis(400));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let containers = match list_shakedown_running_containers(&docker).await {
            Ok(list) => list,
            Err(error) => {
                tracing::debug!(
                    target: "shakedown::harness",
                    error = %error,
                    "shakedown diagnostics: listing containers for log streaming failed"
                );
                continue;
            }
        };
        for summary in containers {
            let Some(container_id) = summary.id.clone() else {
                continue;
            };
            if !seen.insert(container_id.clone()) {
                continue;
            }
            let display_name = container_display_name(&summary);
            let docker_clone = docker.clone();
            let phase_rx = phase_receiver.clone();
            let sink_clone = Arc::clone(&sink);
            tokio::spawn(async move {
                follow_container_logs(
                    docker_clone,
                    container_id,
                    display_name,
                    phase_rx,
                    sink_clone,
                )
                .await;
            });
        }
    }
}

async fn list_shakedown_running_containers(
    docker: &Docker,
) -> Result<Vec<bollard::models::ContainerSummary>, bollard::errors::Error> {
    let mut filters = std::collections::HashMap::new();
    filters.insert("label".to_string(), vec![shakedown_docker_label_filter()]);
    filters.insert("status".to_string(), vec!["running".to_string()]);
    let options = bollard::query_parameters::ListContainersOptionsBuilder::default()
        .all(true)
        .filters(&filters)
        .build();
    docker.list_containers(Some(options)).await
}

fn container_display_name(summary: &bollard::models::ContainerSummary) -> String {
    summary
        .names
        .as_ref()
        .and_then(|names| names.first())
        .map(|name| name.trim_start_matches('/').to_string())
        .filter(|name| !name.is_empty())
        .or_else(|| summary.id.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

async fn follow_container_logs(
    docker: Docker,
    container_id: String,
    display_name: String,
    phase_receiver: watch::Receiver<(Phase, String)>,
    sink: Arc<Mutex<Vec<InfraEvent>>>,
) {
    let options = LogsOptionsBuilder::default()
        .stdout(true)
        .stderr(true)
        .follow(true)
        .timestamps(true)
        .build();
    let mut stream = docker.logs(&container_id, Some(options));
    while let Some(item) = stream.next().await {
        let Ok(output) = item else {
            break;
        };
        let (stream_label, payload): (&str, &[u8]) = match &output {
            LogOutput::StdOut { message } => ("stdout", message.as_ref()),
            LogOutput::StdErr { message } => ("stderr", message.as_ref()),
            LogOutput::StdIn { message } => ("stdin", message.as_ref()),
            LogOutput::Console { message } => ("console", message.as_ref()),
        };
        let text = String::from_utf8_lossy(payload);
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            let (phase, _) = phase_receiver.borrow().clone();
            let timestamp = unix_timestamp_secs();
            let record = InfraEvent::ContainerLog {
                container: display_name.clone(),
                phase,
                timestamp,
                line: line.to_string(),
                stream: Some(stream_label.to_string()),
            };
            let mut guard = sink.lock().await;
            guard.push(record);
        }
    }
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
