//! Poll `/containers/{id}/stats` via bollard and store bounded [`InfraEvent::ResourceSample`] rings.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bollard::models::{ContainerNetworkStats, ContainerStatsResponse};
use bollard::query_parameters::{ListContainersOptionsBuilder, StatsOptionsBuilder};
use bollard::Docker;
use futures_util::StreamExt;
use std::sync::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};

use crate::event::InfraEvent;
use crate::phase::Phase;

use super::labels::shakedown_docker_label_filter;
use super::ring_buffer::RingBuffer;

/// Shared per-container rings keyed by Docker container id (see [`ResourceStatsMonitor`]).
pub(crate) type SharedResourceStatsRings =
    std::sync::Arc<Mutex<HashMap<String, RingBuffer<InfraEvent>>>>;

/// Polling interval and per-container ring depth (defaults: 1s, 60 samples).
#[derive(Debug, Clone)]
pub(crate) struct StatsConfig {
    pub poll_interval: Duration,
    pub buffer_depth: usize,
}

impl StatsConfig {
    pub(crate) fn from_env() -> Self {
        let interval_ms: u64 = std::env::var("SHAKEDOWN_STATS_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1000)
            .max(1);

        let buffer_depth: usize = std::env::var("SHAKEDOWN_STATS_BUFFER_DEPTH")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|depth| *depth > 0)
            .unwrap_or(60)
            .min(86_400);

        Self {
            poll_interval: Duration::from_millis(interval_ms),
            buffer_depth,
        }
    }
}

pub(crate) struct ResourceStatsMonitor {
    join: JoinHandle<()>,
    rings: SharedResourceStatsRings,
}

impl ResourceStatsMonitor {
    pub(crate) fn spawn(
        docker: Docker,
        phase_receiver: watch::Receiver<(Phase, String)>,
        configuration: StatsConfig,
    ) -> (Self, SharedResourceStatsRings) {
        let rings: SharedResourceStatsRings = Arc::new(Mutex::new(HashMap::new()));
        let rings_for_task = Arc::clone(&rings);
        let join = tokio::spawn(run_resource_stats_loop(
            docker,
            rings_for_task,
            phase_receiver,
            configuration,
        ));
        let monitor = Self {
            join,
            rings: Arc::clone(&rings),
        };
        (monitor, rings)
    }

    pub(crate) async fn shutdown_and_drain(self, event_log: &mut crate::event::EventLog) {
        let ResourceStatsMonitor { join, rings } = self;
        join.abort();
        let _ = join.await;
        let mut guard = rings
            .lock()
            .expect("shakedown diagnostics: resource stats mutex poisoned");
        let mut collected: Vec<InfraEvent> = Vec::new();
        for ring in guard.values_mut() {
            collected.extend(ring.drain_ordered());
        }
        collected.sort_by_key(|event| event.timestamp_secs());
        for event in collected {
            event_log.append_infra(event);
        }
    }
}

async fn run_resource_stats_loop(
    docker: Docker,
    rings: SharedResourceStatsRings,
    phase_receiver: watch::Receiver<(Phase, String)>,
    configuration: StatsConfig,
) {
    let mut ticker = interval(configuration.poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let (phase, scenario_id) = phase_receiver.borrow().clone();
        let containers = match list_shakedown_running_containers(&docker).await {
            Ok(summary_list) => summary_list,
            Err(error) => {
                tracing::warn!(
                    target: "shakedown::harness",
                    error = %error,
                    "shakedown diagnostics: listing containers for stats failed; skipping sample"
                );
                continue;
            }
        };
        for summary in containers {
            let Some(container_id) = summary.id.clone() else {
                continue;
            };
            let display_name = container_display_name(&summary);
            match sample_container_stats(&docker, &container_id, &display_name, phase, &scenario_id)
                .await
            {
                Ok(Some(infra_event)) => {
                    let mut guard = rings
                        .lock()
                        .expect("shakedown diagnostics: resource stats mutex poisoned");
                    let ring = guard
                        .entry(container_id)
                        .or_insert_with(|| RingBuffer::new(configuration.buffer_depth));
                    ring.push(infra_event);
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!(
                        target: "shakedown::harness",
                        container_id = %container_id,
                        error = %error,
                        "shakedown diagnostics: stats sample failed"
                    );
                }
            }
        }
    }
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

async fn list_shakedown_running_containers(
    docker: &Docker,
) -> Result<Vec<bollard::models::ContainerSummary>, bollard::errors::Error> {
    let mut filters = HashMap::new();
    filters.insert("label".to_string(), vec![shakedown_docker_label_filter()]);
    filters.insert("status".to_string(), vec!["running".to_string()]);
    let options = ListContainersOptionsBuilder::default()
        .all(true)
        .filters(&filters)
        .build();
    docker.list_containers(Some(options)).await
}

async fn sample_container_stats(
    docker: &Docker,
    container_id: &str,
    display_name: &str,
    phase: Phase,
    scenario_id: &str,
) -> Result<Option<InfraEvent>, bollard::errors::Error> {
    let options = StatsOptionsBuilder::default().stream(false).build();
    let mut stream = docker.stats(container_id, Some(options));
    let Some(result) = stream.next().await else {
        return Ok(None);
    };
    let stats_response = match result {
        Ok(response) => response,
        Err(error) => return Err(error),
    };
    let timestamp = unix_timestamp_secs();
    let (net_rx, net_tx) = network_totals(stats_response.networks.as_ref());
    let mem_bytes = stats_response
        .memory_stats
        .as_ref()
        .and_then(|memory| memory.usage)
        .unwrap_or(0);
    let mem_limit = stats_response
        .memory_stats
        .as_ref()
        .and_then(|memory| memory.limit)
        .unwrap_or(0);
    let cpu_pct = cpu_usage_percent(&stats_response);
    Ok(Some(InfraEvent::ResourceSample {
        container: display_name.to_string(),
        phase,
        timestamp,
        scenario_id: scenario_id.to_string(),
        cpu_pct,
        mem_bytes,
        mem_limit,
        net_rx,
        net_tx,
    }))
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn network_totals(networks: Option<&HashMap<String, ContainerNetworkStats>>) -> (u64, u64) {
    let Some(map) = networks else {
        return (0, 0);
    };
    let mut rx_total = 0u64;
    let mut tx_total = 0u64;
    for iface in map.values() {
        rx_total += iface.rx_bytes.unwrap_or(0);
        tx_total += iface.tx_bytes.unwrap_or(0);
    }
    (rx_total, tx_total)
}

/// CPU % from delta(container CPU) / delta(system CPU), matching moby/docker CLI convention.
fn cpu_usage_percent(stats: &ContainerStatsResponse) -> f64 {
    let Some(cpu) = stats.cpu_stats.as_ref() else {
        return 0.0;
    };
    let Some(precpu) = stats.precpu_stats.as_ref() else {
        return 0.0;
    };
    let usage_now = cpu
        .cpu_usage
        .as_ref()
        .and_then(|usage| usage.total_usage)
        .unwrap_or(0);
    let usage_before = precpu
        .cpu_usage
        .as_ref()
        .and_then(|usage| usage.total_usage)
        .unwrap_or(0);
    let system_now = cpu.system_cpu_usage.unwrap_or(0);
    let system_before = precpu.system_cpu_usage.unwrap_or(0);
    let delta_usage = usage_now.saturating_sub(usage_before);
    let delta_system = system_now.saturating_sub(system_before);
    if delta_system == 0 {
        return 0.0;
    }
    let cpus = cpu
        .online_cpus
        .or_else(|| {
            cpu.cpu_usage
                .as_ref()
                .and_then(|usage| usage.percpu_usage.as_ref())
                .map(|percpu| percpu.len() as u32)
        })
        .filter(|count| *count > 0)
        .unwrap_or(1) as f64;
    (delta_usage as f64 / delta_system as f64) * cpus * 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use bollard::models::{
        ContainerCpuStats, ContainerCpuUsage, ContainerMemoryStats, ContainerStatsResponse,
    };

    #[test]
    fn cpu_percent_matches_moby_formula() {
        let stats = ContainerStatsResponse {
            cpu_stats: Some(ContainerCpuStats {
                cpu_usage: Some(ContainerCpuUsage {
                    total_usage: Some(3_000_000),
                    ..Default::default()
                }),
                system_cpu_usage: Some(10_000_000_000),
                online_cpus: Some(2),
                ..Default::default()
            }),
            precpu_stats: Some(ContainerCpuStats {
                cpu_usage: Some(ContainerCpuUsage {
                    total_usage: Some(1_000_000),
                    ..Default::default()
                }),
                system_cpu_usage: Some(9_000_000_000),
                ..Default::default()
            }),
            memory_stats: Some(ContainerMemoryStats {
                usage: Some(100),
                limit: Some(200),
                ..Default::default()
            }),
            ..Default::default()
        };
        let pct = cpu_usage_percent(&stats);
        let delta_cpu = 2_000_000.0_f64;
        let delta_sys = 1_000_000_000.0_f64;
        let expected = (delta_cpu / delta_sys) * 2.0 * 100.0;
        assert!((pct - expected).abs() < f64::EPSILON);
    }
}
