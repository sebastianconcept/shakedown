//! [`RabbitClusterClusterControl`] — Docker node lifecycle + [`crate::cluster::rabbit_cluster::RabbitClusterOps`].
//!
//! Requires **`rabbitmq-cluster`**.

use std::collections::HashSet;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bollard::query_parameters::{InspectContainerOptionsBuilder, StopContainerOptionsBuilder};
use bollard::Docker;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

use crate::cluster::rabbit_cluster::RabbitClusterOps;
use crate::cluster::{ClusterControl, ClusterError, NodeStatus, CLUSTER_EVENT_SOURCE};
use crate::event::Event;
use crate::rabbit_cluster_resource::RabbitClusterSuiteSnapshot;

fn docker_connect() -> Result<Docker, ClusterError> {
    Docker::connect_with_defaults().map_err(|source| ClusterError::NetworkError { source })
}

async fn inspect_running_paused(
    docker: &Docker,
    container_id: &str,
) -> Result<(bool, bool), ClusterError> {
    let options = InspectContainerOptionsBuilder::default()
        .size(false)
        .build();
    let inspect = docker
        .inspect_container(container_id, Some(options))
        .await
        .map_err(|source| ClusterError::EngineOperation {
            message: format!("inspect container {container_id}"),
            source: Box::new(source),
        })?;
    let state = inspect
        .state
        .as_ref()
        .ok_or_else(|| ClusterError::EngineOperation {
            message: format!("inspect: missing state for {container_id}"),
            source: Box::new(std::io::Error::other("missing state")),
        })?;
    let running = state.running.unwrap_or(false);
    let paused = state.paused.unwrap_or(false);
    Ok((running, paused))
}

/// Suite-scoped control plane for a provisioned RabbitMQ Erlang cluster (`rabbitmq-cluster`).
pub struct RabbitClusterClusterControl {
    node_count: usize,
    container_ids: Vec<String>,
    hostnames: Vec<String>,
    management_host_ports: Vec<u16>,
    username: String,
    password: String,
    http_client: reqwest::Client,
    event_sender: Mutex<Option<SyncSender<Event>>>,
    disconnect_pause: Mutex<HashSet<usize>>,
}

impl RabbitClusterClusterControl {
    /// New control handle; fields must match the live suite snapshot from provisioning.
    pub(crate) fn new(snapshot: RabbitClusterSuiteSnapshot) -> Self {
        Self {
            node_count: snapshot.node_count,
            container_ids: snapshot.container_ids,
            hostnames: snapshot.hostnames,
            management_host_ports: snapshot.management_host_ports,
            username: snapshot.username,
            password: snapshot.password,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("reqwest client for RabbitMQ management API"),
            event_sender: Mutex::new(None),
            disconnect_pause: Mutex::new(HashSet::new()),
        }
    }

    /// Wire [`Event`] emission (same channel as [`crate::cluster::ClusterHandle::set_event_sender`]).
    pub fn set_event_sender(&self, sender: SyncSender<Event>) {
        if let Ok(mut guard) = self.event_sender.lock() {
            *guard = Some(sender);
        }
    }

    fn emit_cluster_row(&self, operation: &str, node_index: Option<usize>, detail: Option<String>) {
        let mut message = match node_index {
            Some(index) => format!("rabbitmq: {operation} node_index={index}"),
            None => format!("rabbitmq: {operation}"),
        };
        if let Some(extra) = detail {
            message.push(' ');
            message.push_str(&extra);
        }
        if let Ok(sender_guard) = self.event_sender.lock() {
            if let Some(sender) = sender_guard.as_ref() {
                let _ = sender.try_send(Event::info(CLUSTER_EVENT_SOURCE, message));
            }
        }
    }

    fn check_index(&self, index: usize) -> Result<(), ClusterError> {
        if index >= self.node_count {
            return Err(ClusterError::NodeIndexOutOfRange {
                index,
                node_count: self.node_count,
            });
        }
        Ok(())
    }

    fn leader_name_to_index(leader: &str, hostnames: &[String]) -> Result<usize, ClusterError> {
        let hostname = leader
            .split('@')
            .nth(1)
            .ok_or_else(|| ClusterError::EngineOperation {
                message: format!("unexpected leader node name (no @): {leader}"),
                source: Box::new(std::io::Error::other("leader format")),
            })?;
        hostnames
            .iter()
            .position(|candidate| candidate.as_str() == hostname)
            .ok_or_else(|| ClusterError::EngineOperation {
                message: format!("leader hostname {hostname} not in suite hostnames"),
                source: Box::new(std::io::Error::other("leader map")),
            })
    }

    /// Fetch leader **node name** (e.g. `rabbit@host`) from management API; does not emit events.
    async fn fetch_queue_leader_name(&self, queue_name: &str) -> Result<String, ClusterError> {
        let encoded = utf8_percent_encode(queue_name, NON_ALPHANUMERIC);
        let path = format!("/api/queues/%2F/{encoded}");
        let mut last_status = None::<reqwest::StatusCode>;
        for port in &self.management_host_ports {
            let url = format!("http://127.0.0.1:{port}{path}");
            let response = self
                .http_client
                .get(&url)
                .basic_auth(self.username.as_str(), Some(self.password.as_str()))
                .send()
                .await
                .map_err(|source| ClusterError::EngineOperation {
                    message: format!("management GET {url}"),
                    source: Box::new(source),
                })?;
            last_status = Some(response.status());
            if !response.status().is_success() {
                continue;
            }
            let value: serde_json::Value =
                response
                    .json()
                    .await
                    .map_err(|source| ClusterError::EngineOperation {
                        message: "queue JSON from management API".into(),
                        source: Box::new(source),
                    })?;
            let queue_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if queue_type != "quorum" {
                return Err(ClusterError::EngineOperation {
                    message: format!("queue {queue_name} is type {queue_type}, expected quorum"),
                    source: Box::new(std::io::Error::other("queue type")),
                });
            }
            let leader = value
                .get("leader")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ClusterError::EngineOperation {
                    message: format!("queue {queue_name} has no leader field"),
                    source: Box::new(std::io::Error::other("leader missing")),
                })?;
            return Ok(leader.to_string());
        }
        Err(ClusterError::EngineOperation {
            message: format!(
                "could not read queue {queue_name} from management API (last HTTP status: {last_status:?})"
            ),
            source: Box::new(std::io::Error::other("management")),
        })
    }

    async fn quorum_queue_leader_index_without_events(
        &self,
        queue_name: &str,
    ) -> Result<usize, ClusterError> {
        let leader_name = self.fetch_queue_leader_name(queue_name).await?;
        Self::leader_name_to_index(leader_name.as_str(), self.hostnames.as_slice())
    }
}

#[async_trait]
impl RabbitClusterOps for Arc<RabbitClusterClusterControl> {
    async fn quorum_queue_leader(&self, queue_name: &str) -> Result<usize, ClusterError> {
        let index = self
            .quorum_queue_leader_index_without_events(queue_name)
            .await?;
        self.emit_cluster_row(
            "quorum_queue_leader",
            Some(index),
            Some(format!("queue={queue_name}")),
        );
        Ok(index)
    }

    async fn await_queue_leader_election(
        &self,
        queue_name: &str,
        timeout: Duration,
    ) -> Result<usize, ClusterError> {
        let initial = self
            .quorum_queue_leader_index_without_events(queue_name)
            .await?;
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() > deadline {
                return Err(ClusterError::QuorumTimeout { timeout });
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            let current = match self
                .quorum_queue_leader_index_without_events(queue_name)
                .await
            {
                Ok(index) => index,
                Err(_) => continue,
            };
            if current != initial {
                self.emit_cluster_row(
                    "await_queue_leader_election",
                    Some(current),
                    Some(format!(
                        "queue={queue_name} previous_leader_node_index={initial}"
                    )),
                );
                return Ok(current);
            }
        }
    }
}

#[async_trait]
impl ClusterControl for Arc<RabbitClusterClusterControl> {
    async fn stop_node(&self, index: usize) -> Result<(), ClusterError> {
        self.check_index(index)?;
        let docker = docker_connect()?;
        let container_id = self.container_ids[index].as_str();
        let options = StopContainerOptionsBuilder::default().t(30).build();
        docker
            .stop_container(container_id, Some(options))
            .await
            .map_err(|source| ClusterError::DockerOperation { index, source })?;
        Ok(())
    }

    async fn start_node(&self, index: usize) -> Result<(), ClusterError> {
        self.check_index(index)?;
        let docker = docker_connect()?;
        let container_id = self.container_ids[index].as_str();
        docker
            .start_container(
                container_id,
                None::<bollard::query_parameters::StartContainerOptions>,
            )
            .await
            .map_err(|source| ClusterError::DockerOperation { index, source })?;
        Ok(())
    }

    async fn pause_node(&self, index: usize) -> Result<(), ClusterError> {
        self.check_index(index)?;
        let docker = docker_connect()?;
        let container_id = self.container_ids[index].as_str();
        docker
            .pause_container(container_id)
            .await
            .map_err(|source| ClusterError::DockerOperation { index, source })?;
        Ok(())
    }

    async fn unpause_node(&self, index: usize) -> Result<(), ClusterError> {
        self.check_index(index)?;
        let docker = docker_connect()?;
        let container_id = self.container_ids[index].as_str();
        docker
            .unpause_container(container_id)
            .await
            .map_err(|source| ClusterError::DockerOperation { index, source })?;
        if let Ok(mut guard) = self.disconnect_pause.lock() {
            guard.remove(&index);
        }
        Ok(())
    }

    async fn disconnect_node(&self, index: usize) -> Result<(), ClusterError> {
        self.pause_node(index).await?;
        if let Ok(mut guard) = self.disconnect_pause.lock() {
            guard.insert(index);
        }
        Ok(())
    }

    async fn reconnect_node(&self, index: usize) -> Result<(), ClusterError> {
        self.unpause_node(index).await
    }

    async fn node_status(&self, index: usize) -> Result<NodeStatus, ClusterError> {
        self.check_index(index)?;
        let docker = docker_connect()?;
        let container_id = self.container_ids[index].as_str();
        let (running, paused) = inspect_running_paused(&docker, container_id).await?;
        if let Ok(guard) = self.disconnect_pause.lock() {
            if guard.contains(&index) {
                return Ok(NodeStatus::Disconnected);
            }
        }
        if paused {
            return Ok(NodeStatus::Paused);
        }
        if !running {
            return Ok(NodeStatus::Stopped);
        }
        Ok(NodeStatus::Running)
    }

    async fn healthy_node_count(&self) -> Result<usize, ClusterError> {
        let mut count = 0usize;
        let docker = docker_connect()?;
        for index in 0..self.node_count {
            let container_id = self.container_ids[index].as_str();
            let (running, paused) = inspect_running_paused(&docker, container_id).await?;
            if running && !paused {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn reset_topology(&self) -> Result<(), ClusterError> {
        let docker = docker_connect()?;
        for index in 0..self.node_count {
            let container_id = self.container_ids[index].as_str();
            let (running, paused) = inspect_running_paused(&docker, container_id).await?;
            if !running {
                docker
                    .start_container(
                        container_id,
                        None::<bollard::query_parameters::StartContainerOptions>,
                    )
                    .await
                    .map_err(|source| ClusterError::DockerOperation { index, source })?;
            } else if paused {
                docker
                    .unpause_container(container_id)
                    .await
                    .map_err(|source| ClusterError::DockerOperation { index, source })?;
            }
        }
        if let Ok(mut guard) = self.disconnect_pause.lock() {
            guard.clear();
        }
        self.emit_cluster_row("reset_topology", None, None);

        let convergence_timeout = Duration::from_secs(90);
        let converge_deadline = Instant::now() + convergence_timeout;
        loop {
            if Instant::now() > converge_deadline {
                return Err(ClusterError::QuorumTimeout {
                    timeout: convergence_timeout,
                });
            }
            if self.healthy_node_count().await? == self.node_count {
                self.emit_cluster_row(
                    "reset_topology",
                    None,
                    Some(format!("healthy_node_count={}", self.node_count)),
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
    }

    fn node_count(&self) -> usize {
        self.node_count
    }
}
