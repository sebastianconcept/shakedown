//! Diagnostics monitors for one [`super::run_scenario`] invocation.
//!
//! This module is useful when you add a Docker diagnostics stream or change how monitors drain into
//! the event log.
//! This module holds Docker event, stats, and log monitors for the run.
//! This module does not provision resources or run the scenario body.

use crate::error::ShakedownError;
use crate::event::EventLog;

use super::context::Context;
use super::phase::Phase;

#[cfg(all(
    any(
        feature = "kafka",
        feature = "rabbitmq",
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "redis",
        feature = "valkey",
        feature = "mongodb",
        feature = "nats",
        feature = "neo4j",
        feature = "elasticsearch",
        feature = "opensearch",
        feature = "qdrant",
        feature = "weaviate",
        feature = "influxdb",
        feature = "timescaledb",
        feature = "services",
    ),
    not(feature = "no-diagnostics"),
))]
mod enabled {
    use super::*;

    pub(crate) struct LifecycleDiagnostics {
        docker_events_monitor: Option<crate::diagnostics::DockerEventsMonitor>,
        resource_stats_monitor: Option<crate::diagnostics::ResourceStatsMonitor>,
        container_logs_monitor: Option<crate::diagnostics::ContainerLogsMonitor>,
        docker_phase_sender: Option<tokio::sync::watch::Sender<(Phase, String)>>,
    }

    impl LifecycleDiagnostics {
        pub(crate) async fn spawn(ctx: &mut Context) -> Self {
            let mut docker_events_monitor = None;
            let mut resource_stats_monitor = None;
            let mut container_logs_monitor = None;
            let mut docker_phase_sender = None;
            if let Some(diagnostics_handle) = crate::diagnostics::suite_diagnostics_handle().await {
                let (phase_tx, phase_rx) =
                    tokio::sync::watch::channel((Phase::Provision, ctx.scenario_id().to_string()));
                let docker = diagnostics_handle.docker().clone();
                docker_events_monitor = Some(crate::diagnostics::DockerEventsMonitor::spawn(
                    docker.clone(),
                    phase_rx.clone(),
                ));
                let (stats_monitor, rings_handle) = crate::diagnostics::ResourceStatsMonitor::spawn(
                    docker.clone(),
                    phase_rx.clone(),
                    crate::diagnostics::StatsConfig::from_env(),
                );
                resource_stats_monitor = Some(stats_monitor);
                container_logs_monitor = Some(crate::diagnostics::ContainerLogsMonitor::spawn(
                    docker, phase_rx,
                ));
                ctx.set_resource_stats_rings(Some(rings_handle));
                docker_phase_sender = Some(phase_tx);
            }
            Self {
                docker_events_monitor,
                resource_stats_monitor,
                container_logs_monitor,
                docker_phase_sender,
            }
        }

        pub(crate) fn push_phase(&self, ctx: &Context, phase: Phase) {
            if let Some(phase_sender) = self.docker_phase_sender.as_ref() {
                crate::diagnostics::push_lifecycle_phase(
                    phase_sender,
                    phase,
                    ctx.scenario_id().as_str(),
                );
            }
        }

        pub(crate) async fn merge_into(
            &mut self,
            event_log: &mut EventLog,
        ) -> Result<(), ShakedownError> {
            if let Some(monitor) = self.resource_stats_monitor.take() {
                monitor.shutdown_and_drain(event_log).await?;
            }
            if let Some(monitor) = self.container_logs_monitor.take() {
                monitor.shutdown_and_drain(event_log).await?;
            }
            if let Some(monitor) = self.docker_events_monitor.take() {
                monitor.shutdown_and_drain(event_log).await?;
            }
            Ok(())
        }
    }
}

#[cfg(not(all(
    any(
        feature = "kafka",
        feature = "rabbitmq",
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "redis",
        feature = "valkey",
        feature = "mongodb",
        feature = "nats",
        feature = "neo4j",
        feature = "elasticsearch",
        feature = "opensearch",
        feature = "qdrant",
        feature = "weaviate",
        feature = "influxdb",
        feature = "timescaledb",
        feature = "services",
    ),
    not(feature = "no-diagnostics"),
)))]
mod disabled {
    use super::*;

    pub(crate) struct LifecycleDiagnostics;

    impl LifecycleDiagnostics {
        pub(crate) async fn spawn(ctx: &mut Context) -> Self {
            let _ = ctx;
            Self
        }

        pub(crate) fn push_phase(&self, ctx: &Context, phase: Phase) {
            let _ = (self, ctx, phase);
        }

        pub(crate) async fn merge_into(
            &mut self,
            event_log: &mut EventLog,
        ) -> Result<(), ShakedownError> {
            let _ = event_log;
            Ok(())
        }
    }
}

#[cfg(all(
    any(
        feature = "kafka",
        feature = "rabbitmq",
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "redis",
        feature = "valkey",
        feature = "mongodb",
        feature = "nats",
        feature = "neo4j",
        feature = "elasticsearch",
        feature = "opensearch",
        feature = "qdrant",
        feature = "weaviate",
        feature = "influxdb",
        feature = "timescaledb",
        feature = "services",
    ),
    not(feature = "no-diagnostics"),
))]
pub(crate) use enabled::LifecycleDiagnostics;

#[cfg(not(all(
    any(
        feature = "kafka",
        feature = "rabbitmq",
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "redis",
        feature = "valkey",
        feature = "mongodb",
        feature = "nats",
        feature = "neo4j",
        feature = "elasticsearch",
        feature = "opensearch",
        feature = "qdrant",
        feature = "weaviate",
        feature = "influxdb",
        feature = "timescaledb",
        feature = "services",
    ),
    not(feature = "no-diagnostics"),
)))]
pub(crate) use disabled::LifecycleDiagnostics;
