//! [`RedisSentinelClusterControl`] — Redis node Docker lifecycle + [`crate::cluster::redis_sentinel::RedisSentinelOps`].
//!
//! Requires **`redis-sentinel`**.

use std::collections::HashSet;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bollard::query_parameters::{InspectContainerOptionsBuilder, StopContainerOptionsBuilder};
use bollard::Docker;

use super::resource::redis_sentinel_suite_snapshot;
use crate::cluster::redis_sentinel::RedisSentinelOps;
use crate::cluster::{ClusterControl, ClusterError, NodeStatus, CLUSTER_EVENT_SOURCE};
use crate::event::Event;

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

fn map_redis_err(error: redis::RedisError) -> ClusterError {
    ClusterError::EngineOperation {
        message: "redis sentinel client".into(),
        source: Box::new(error),
    }
}

/// Parse `SENTINEL MASTER <name>` flat array: alternating field name, value; locate `ip`.
fn master_ip_from_sentinel_master_fields(fields: &[String]) -> Result<String, ClusterError> {
    let mut index = 0usize;
    while index + 1 < fields.len() {
        if fields[index] == "ip" {
            return Ok(fields[index + 1].clone());
        }
        index += 2;
    }
    Err(ClusterError::EngineOperation {
        message: "SENTINEL MASTER reply missing ip field".into(),
        source: Box::new(std::io::Error::other("invalid sentinel master")),
    })
}

async fn sentinel_connection(
    snapshot: &super::resource::RedisSentinelSuiteSnapshot,
) -> Result<redis::aio::MultiplexedConnection, ClusterError> {
    let sentinel_port = snapshot
        .sentinel_host_ports
        .first()
        .copied()
        .ok_or_else(|| ClusterError::EngineOperation {
            message: "Redis Sentinel suite has no sentinel ports".into(),
            source: Box::new(std::io::Error::other("empty sentinel_host_ports")),
        })?;
    let url = format!(
        "redis://{}:{}/",
        snapshot.client_host.as_str(),
        sentinel_port
    );
    let client = redis::Client::open(url.as_str()).map_err(map_redis_err)?;
    client
        .get_multiplexed_async_connection()
        .await
        .map_err(map_redis_err)
}

/// Resolve primary node index using `SENTINEL MASTER` and bridge IP list from provisioning.
async fn current_master_index(
    snapshot: &super::resource::RedisSentinelSuiteSnapshot,
) -> Result<usize, ClusterError> {
    let mut connection = sentinel_connection(snapshot).await?;
    let fields: Vec<String> = redis::cmd("SENTINEL")
        .arg("MASTER")
        .arg(snapshot.master_name.as_str())
        .query_async(&mut connection)
        .await
        .map_err(map_redis_err)?;
    let ip = master_ip_from_sentinel_master_fields(&fields)?;
    snapshot
        .redis_bridge_ips
        .iter()
        .position(|bridge_ip| bridge_ip == ip.as_str())
        .ok_or_else(|| ClusterError::EngineOperation {
            message: format!("SENTINEL MASTER ip {ip} not found in suite bridge IPs"),
            source: Box::new(std::io::Error::other("unknown master ip")),
        })
}

/// Suite-scoped control plane for a provisioned Redis + Sentinel stack (`redis-sentinel`).
pub struct RedisSentinelClusterControl {
    node_count: usize,
    event_sender: Mutex<Option<SyncSender<Event>>>,
    disconnect_pause: Mutex<HashSet<usize>>,
    /// Primary index before [`RedisSentinelOps::trigger_failover`] for [`RedisSentinelOps::await_failover_complete`].
    master_before_failover: Mutex<Option<usize>>,
}

impl RedisSentinelClusterControl {
    /// New control handle; `node_count` must match the provisioned Redis node count.
    pub fn new(node_count: usize) -> Self {
        Self {
            node_count,
            event_sender: Mutex::new(None),
            disconnect_pause: Mutex::new(HashSet::new()),
            master_before_failover: Mutex::new(None),
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
            Some(index) => format!("redis-sentinel: {operation} node_index={index}"),
            None => format!("redis-sentinel: {operation}"),
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

    fn check_index(
        snapshot: &super::resource::RedisSentinelSuiteSnapshot,
        index: usize,
    ) -> Result<(), ClusterError> {
        if index >= snapshot.node_count {
            return Err(ClusterError::NodeIndexOutOfRange {
                index,
                node_count: snapshot.node_count,
            });
        }
        Ok(())
    }

    async fn snapshot_or_err(
        &self,
    ) -> Result<super::resource::RedisSentinelSuiteSnapshot, ClusterError> {
        redis_sentinel_suite_snapshot()
            .await
            .ok_or_else(|| ClusterError::EngineOperation {
                message: "Redis Sentinel suite is not running".into(),
                source: Box::new(std::io::Error::other("suite not provisioned")),
            })
    }
}

#[async_trait]
impl RedisSentinelOps for Arc<RedisSentinelClusterControl> {
    async fn trigger_failover(&self) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        let before = current_master_index(&snapshot).await?;
        let mut connection = sentinel_connection(&snapshot).await?;
        let _: String = redis::cmd("SENTINEL")
            .arg("FAILOVER")
            .arg(snapshot.master_name.as_str())
            .query_async(&mut connection)
            .await
            .map_err(map_redis_err)?;
        if let Ok(mut guard) = self.master_before_failover.lock() {
            *guard = Some(before);
        }
        self.emit_cluster_row(
            "trigger_failover",
            Some(before),
            Some(format!("master_index_before={before}")),
        );
        Ok(())
    }

    async fn current_master(&self) -> Result<usize, ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        let index = current_master_index(&snapshot).await?;
        self.emit_cluster_row(
            "current_master",
            Some(index),
            Some(format!("master_index={index}")),
        );
        Ok(index)
    }

    async fn await_failover_complete(&self, timeout: Duration) -> Result<usize, ClusterError> {
        let previous = self
            .master_before_failover
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
            .ok_or_else(|| ClusterError::EngineOperation {
                message: "call trigger_failover before await_failover_complete".into(),
                source: Box::new(std::io::Error::other("missing failover context")),
            })?;
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() > deadline {
                return Err(ClusterError::QuorumTimeout { timeout });
            }
            let snapshot = match self.snapshot_or_err().await {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            let current = match current_master_index(&snapshot).await {
                Ok(index) => index,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            if current != previous {
                self.emit_cluster_row(
                    "await_failover_complete",
                    Some(current),
                    Some(format!(
                        "new_master_index={current} previous_master_index={previous}"
                    )),
                );
                return Ok(current);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

#[async_trait]
impl ClusterControl for Arc<RedisSentinelClusterControl> {
    async fn stop_node(&self, index: usize) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        RedisSentinelClusterControl::check_index(&snapshot, index)?;
        let docker = docker_connect()?;
        let container_id = snapshot.redis_container_ids[index].as_str();
        let options = StopContainerOptionsBuilder::default().t(30).build();
        docker
            .stop_container(container_id, Some(options))
            .await
            .map_err(|source| ClusterError::DockerOperation { index, source })?;
        Ok(())
    }

    async fn start_node(&self, index: usize) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        RedisSentinelClusterControl::check_index(&snapshot, index)?;
        let docker = docker_connect()?;
        let container_id = snapshot.redis_container_ids[index].as_str();
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
        let snapshot = self.snapshot_or_err().await?;
        RedisSentinelClusterControl::check_index(&snapshot, index)?;
        let docker = docker_connect()?;
        let container_id = snapshot.redis_container_ids[index].as_str();
        docker
            .pause_container(container_id)
            .await
            .map_err(|source| ClusterError::DockerOperation { index, source })?;
        Ok(())
    }

    async fn unpause_node(&self, index: usize) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        RedisSentinelClusterControl::check_index(&snapshot, index)?;
        let docker = docker_connect()?;
        let container_id = snapshot.redis_container_ids[index].as_str();
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
        let snapshot = self.snapshot_or_err().await?;
        RedisSentinelClusterControl::check_index(&snapshot, index)?;
        let docker = docker_connect()?;
        let container_id = snapshot.redis_container_ids[index].as_str();
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
        let snapshot = self.snapshot_or_err().await?;
        let mut count = 0usize;
        let docker = docker_connect()?;
        for index in 0..snapshot.node_count {
            let container_id = snapshot.redis_container_ids[index].as_str();
            let (running, paused) = inspect_running_paused(&docker, container_id).await?;
            if running && !paused {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn reset_topology(&self) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        let docker = docker_connect()?;
        for index in 0..snapshot.node_count {
            let container_id = snapshot.redis_container_ids[index].as_str();
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
        let master_name = snapshot.master_name.clone();
        loop {
            if Instant::now() > converge_deadline {
                return Err(ClusterError::QuorumTimeout {
                    timeout: convergence_timeout,
                });
            }
            if self.healthy_node_count().await? == self.node_count {
                // CKQUORUM on any sentinel
                let host = snapshot.client_host.as_str();
                if let Some(port) = snapshot.sentinel_host_ports.first() {
                    let url = format!("redis://{host}:{port}/");
                    let client = redis::Client::open(url.as_str()).map_err(map_redis_err)?;
                    if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                        if redis::cmd("SENTINEL")
                            .arg("CKQUORUM")
                            .arg(master_name.as_str())
                            .query_async::<String>(&mut conn)
                            .await
                            .is_ok()
                        {
                            self.emit_cluster_row(
                                "reset_topology",
                                None,
                                Some(format!("healthy_node_count={}", self.node_count)),
                            );
                            return Ok(());
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
    }

    fn node_count(&self) -> usize {
        self.node_count
    }
}
