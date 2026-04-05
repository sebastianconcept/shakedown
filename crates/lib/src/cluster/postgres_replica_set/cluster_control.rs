//! [`PostgresReplicaSetClusterControl`] — Docker node lifecycle + [`crate::cluster::postgres_replica_set::PostgresReplicaSetOps`].
//!
//! Requires **`postgres-cluster`**.

use std::collections::HashSet;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bollard::query_parameters::{InspectContainerOptionsBuilder, StopContainerOptionsBuilder};
use bollard::Docker;
use tokio_postgres::NoTls;

use crate::cluster::postgres_replica_set::PostgresReplicaSetOps;
use crate::cluster::{ClusterControl, ClusterError, NodeStatus, CLUSTER_EVENT_SOURCE};
use crate::event::Event;

const POSTGRES_RS_NODE_COUNT: usize = 2;
const RESET_TOPOLOGY_TIMEOUT: Duration = Duration::from_secs(90);

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

fn map_pg_err(error: tokio_postgres::Error) -> ClusterError {
    ClusterError::EngineOperation {
        message: "tokio-postgres".into(),
        source: Box::new(std::io::Error::other(error.to_string())),
    }
}

/// Per-scenario control plane for [`crate::PostgresReplicaSetResource`] (primary + streaming replica).
pub struct PostgresReplicaSetClusterControl {
    client_host: String,
    primary_port: u16,
    replica_port: u16,
    username: String,
    password: String,
    primary_container_id: String,
    replica_container_id: String,
    event_sender: Mutex<Option<SyncSender<Event>>>,
    disconnect_pause: Mutex<HashSet<usize>>,
}

impl PostgresReplicaSetClusterControl {
    /// Build from provisioned [`crate::resources::ResourceHandle::metadata`] (`postgres_cluster` = `primary_replica`).
    pub fn from_handle_metadata(metadata: &serde_json::Value) -> Option<Self> {
        if metadata
            .get("postgres_cluster")
            .and_then(|value| value.as_str())
            != Some("primary_replica")
        {
            return None;
        }
        let client_host = metadata.get("host")?.as_str()?.to_string();
        let primary_port = u16::try_from(metadata.get("port")?.as_u64()?).ok()?;
        let replica_port = u16::try_from(metadata.get("replica_port")?.as_u64()?).ok()?;
        let username = metadata.get("username")?.as_str()?.to_string();
        let password = metadata.get("password")?.as_str()?.to_string();
        let primary_container_id = metadata.get("primary_container_id")?.as_str()?.to_string();
        let replica_container_id = metadata.get("replica_container_id")?.as_str()?.to_string();
        Some(Self {
            client_host,
            primary_port,
            replica_port,
            username,
            password,
            primary_container_id,
            replica_container_id,
            event_sender: Mutex::new(None),
            disconnect_pause: Mutex::new(HashSet::new()),
        })
    }

    /// Wire [`Event`] emission (same channel as [`crate::cluster::ClusterHandle::set_event_sender`]).
    pub fn set_event_sender(&self, sender: SyncSender<Event>) {
        if let Ok(mut guard) = self.event_sender.lock() {
            *guard = Some(sender);
        }
    }

    fn emit_cluster_row(&self, operation: &str, node_index: Option<usize>, detail: Option<String>) {
        let mut message = match node_index {
            Some(index) => format!("postgres: {operation} node_index={index}"),
            None => format!("postgres: {operation}"),
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
        if index >= POSTGRES_RS_NODE_COUNT {
            return Err(ClusterError::NodeIndexOutOfRange {
                index,
                node_count: POSTGRES_RS_NODE_COUNT,
            });
        }
        Ok(())
    }

    fn container_id_for_index(&self, index: usize) -> Result<&str, ClusterError> {
        self.check_index(index)?;
        Ok(match index {
            0 => self.primary_container_id.as_str(),
            1 => self.replica_container_id.as_str(),
            _ => unreachable!(),
        })
    }

    fn postgres_url(&self, index: usize, database: &str) -> Result<String, ClusterError> {
        self.check_index(index)?;
        let port = match index {
            0 => self.primary_port,
            1 => self.replica_port,
            _ => unreachable!(),
        };
        Ok(format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.username, self.password, self.client_host, port, database
        ))
    }

    async fn connect_postgres(
        &self,
        index: usize,
        database: &str,
    ) -> Result<tokio_postgres::Client, ClusterError> {
        let url = self.postgres_url(index, database)?;
        let (client, connection) = tokio_postgres::connect(url.as_str(), NoTls)
            .await
            .map_err(map_pg_err)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    async fn pg_is_in_recovery(&self, index: usize) -> Result<bool, ClusterError> {
        let client = self.connect_postgres(index, "postgres").await?;
        let row = client
            .query_one("SELECT pg_is_in_recovery() AS r", &[])
            .await
            .map_err(map_pg_err)?;
        Ok(row.get::<_, bool>("r"))
    }

    async fn primary_index_from_pg_state(&self) -> Result<usize, ClusterError> {
        let mut primary_index: Option<usize> = None;
        for index in 0..POSTGRES_RS_NODE_COUNT {
            let in_recovery = match self.pg_is_in_recovery(index).await {
                Ok(value) => value,
                Err(_) => continue,
            };
            if !in_recovery {
                if primary_index.is_some() {
                    return Err(ClusterError::EngineOperation {
                        message: "multiple nodes report not in recovery".into(),
                        source: Box::new(std::io::Error::other("split brain or transition")),
                    });
                }
                primary_index = Some(index);
            }
        }
        primary_index.ok_or_else(|| ClusterError::EngineOperation {
            message: "no primary found (all reachable nodes in recovery or unreachable)".into(),
            source: Box::new(std::io::Error::other("no primary")),
        })
    }
}

#[async_trait]
impl PostgresReplicaSetOps for Arc<PostgresReplicaSetClusterControl> {
    async fn promote_replica(&self, index: usize) -> Result<(), ClusterError> {
        self.check_index(index)?;
        if !self.pg_is_in_recovery(index).await? {
            return Err(ClusterError::EngineOperation {
                message: format!(
                    "promote_replica: node {index} is not in recovery (not a standby)"
                ),
                source: Box::new(std::io::Error::other("not a standby")),
            });
        }
        let client = self.connect_postgres(index, "postgres").await?;
        let _promoted: bool = client
            .query_one("SELECT pg_promote() AS promoted", &[])
            .await
            .map_err(map_pg_err)?
            .get("promoted");
        self.emit_cluster_row("promote_replica", Some(index), None);
        Ok(())
    }

    async fn current_primary(&self) -> Result<usize, ClusterError> {
        let resolved = self.primary_index_from_pg_state().await?;
        self.emit_cluster_row(
            "current_primary",
            Some(resolved),
            Some(format!("primary_index={resolved}")),
        );
        Ok(resolved)
    }
}

#[async_trait]
impl ClusterControl for Arc<PostgresReplicaSetClusterControl> {
    async fn stop_node(&self, index: usize) -> Result<(), ClusterError> {
        let docker = docker_connect()?;
        let container_id = self.container_id_for_index(index)?;
        let options = StopContainerOptionsBuilder::default().t(30).build();
        docker
            .stop_container(container_id, Some(options))
            .await
            .map_err(|source| ClusterError::DockerOperation { index, source })?;
        Ok(())
    }

    async fn start_node(&self, index: usize) -> Result<(), ClusterError> {
        let docker = docker_connect()?;
        let container_id = self.container_id_for_index(index)?;
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
        let docker = docker_connect()?;
        let container_id = self.container_id_for_index(index)?;
        docker
            .pause_container(container_id)
            .await
            .map_err(|source| ClusterError::DockerOperation { index, source })?;
        Ok(())
    }

    async fn unpause_node(&self, index: usize) -> Result<(), ClusterError> {
        let docker = docker_connect()?;
        let container_id = self.container_id_for_index(index)?;
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
        let container_id = self.container_id_for_index(index)?;
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
        let docker = docker_connect()?;
        let mut count = 0usize;
        for index in 0..POSTGRES_RS_NODE_COUNT {
            let container_id = self.container_id_for_index(index)?;
            let (running, paused) = inspect_running_paused(&docker, container_id).await?;
            if running && !paused {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn reset_topology(&self) -> Result<(), ClusterError> {
        let docker = docker_connect()?;
        for index in 0..POSTGRES_RS_NODE_COUNT {
            let container_id = self.container_id_for_index(index)?;
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
        self.emit_cluster_row(
            "reset_topology",
            None,
            Some("restarted_or_unpaused_all_nodes; awaiting single primary".into()),
        );
        let deadline = Instant::now() + RESET_TOPOLOGY_TIMEOUT;
        loop {
            if Instant::now() > deadline {
                return Err(ClusterError::QuorumTimeout {
                    timeout: RESET_TOPOLOGY_TIMEOUT,
                });
            }
            if let Ok(primary_index) = self.primary_index_from_pg_state().await {
                self.emit_cluster_row(
                    "reset_topology",
                    None,
                    Some(format!("converged primary_index={primary_index}")),
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
    }

    fn node_count(&self) -> usize {
        POSTGRES_RS_NODE_COUNT
    }
}
