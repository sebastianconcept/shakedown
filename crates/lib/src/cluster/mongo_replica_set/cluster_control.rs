//! [`MongoReplicaSetClusterControl`] — Docker node lifecycle + [`crate::cluster::mongo_replica_set::MongoClusterOps`].
//!
//! Requires **`mongodb-cluster`**.

use std::collections::HashSet;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bollard::query_parameters::{InspectContainerOptionsBuilder, StopContainerOptionsBuilder};
use bollard::Docker;
use mongodb::bson::{doc, Document};

use super::resource::mongo_replica_set_suite_snapshot;
use crate::cluster::mongo_replica_set::MongoClusterOps;
use crate::cluster::{ClusterControl, ClusterError, NodeStatus, CLUSTER_EVENT_SOURCE};
use crate::event::Event;

fn admin_uri_for_host_and_port(host: &str, port: u16) -> String {
    format!("mongodb://{host}:{port}/admin?directConnection=true")
}

async fn mongo_client(uri: &str) -> Result<mongodb::Client, mongodb::error::Error> {
    mongodb::Client::with_uri_str(uri).await
}

fn map_mongo_err(error: mongodb::error::Error) -> ClusterError {
    ClusterError::EngineOperation {
        message: "mongodb driver".into(),
        source: Box::new(error),
    }
}

fn primary_index_from_status(status: &Document) -> Result<usize, ClusterError> {
    let members = status
        .get_array("members")
        .map_err(|_| ClusterError::EngineOperation {
            message: "replSetGetStatus: missing members array".into(),
            source: Box::new(std::io::Error::other("invalid status document")),
        })?;
    for member in members {
        let Some(member_doc) = member.as_document() else {
            continue;
        };
        if member_doc.get_str("stateStr").unwrap_or("") != "PRIMARY" {
            continue;
        }
        let index =
            super::resource::replica_set_member_index_from_doc(member_doc).ok_or_else(|| {
                let name = member_doc.get_str("name").unwrap_or("?");
                ClusterError::EngineOperation {
                    message: format!("unrecognized member host in replSetGetStatus: {name}"),
                    source: Box::new(std::io::Error::other("parse member index")),
                }
            })?;
        return Ok(index);
    }
    Err(ClusterError::EngineOperation {
        message: "replSetGetStatus contained no PRIMARY member".into(),
        source: Box::new(std::io::Error::other("no primary")),
    })
}

async fn repl_set_status_from_any_node(
    snapshot: &super::resource::MongoReplicaSetSuiteSnapshot,
) -> Result<Document, ClusterError> {
    let host = snapshot.client_host.as_str();
    if snapshot.host_ports.is_empty() {
        return Err(ClusterError::EngineOperation {
            message: "replica set suite has no host ports".into(),
            source: Box::new(std::io::Error::other("empty host_ports")),
        });
    }
    let mut last_error: Option<mongodb::error::Error> = None;
    for port in &snapshot.host_ports {
        let uri = admin_uri_for_host_and_port(host, *port);
        let client = match mongo_client(uri.as_str()).await {
            Ok(client) => client,
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };
        let admin = client.database("admin");
        match admin.run_command(doc! { "replSetGetStatus": 1 }).await {
            Ok(document) => return Ok(document),
            Err(error) => last_error = Some(error),
        }
    }
    match last_error {
        Some(error) => Err(map_mongo_err(error)),
        None => Err(ClusterError::EngineOperation {
            message: "replSetGetStatus: no replica set member responded".into(),
            source: Box::new(std::io::Error::other("no host responded")),
        }),
    }
}

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

/// Suite-scoped control plane for a provisioned MongoDB replica set (`mongodb-cluster`).
pub struct MongoReplicaSetClusterControl {
    rs_node_count: usize,
    event_sender: Mutex<Option<SyncSender<Event>>>,
    /// Primary index captured by [`MongoReplicaSetClusterControl::step_down_primary`] for [`MongoReplicaSetClusterControl::await_new_primary`].
    await_primary_after_step_down: Mutex<Option<usize>>,
    /// Nodes paused via [`ClusterControl::disconnect_node`] (vs [`ClusterControl::pause_node`]).
    disconnect_pause: Mutex<HashSet<usize>>,
}

impl MongoReplicaSetClusterControl {
    /// New control handle; prefer storing one [`Arc`] per scenario and reusing it for
    /// [`crate::cluster::ClusterHandle`] + [`crate::lifecycle::Context::mongo_cluster`].
    ///
    /// `node_count` must match the provisioned replica set size (see suite snapshot).
    pub fn new(node_count: usize) -> Self {
        Self {
            rs_node_count: node_count,
            event_sender: Mutex::new(None),
            await_primary_after_step_down: Mutex::new(None),
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
            Some(index) => format!("mongo: {operation} node_index={index}"),
            None => format!("mongo: {operation}"),
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
        snapshot: &super::resource::MongoReplicaSetSuiteSnapshot,
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
    ) -> Result<super::resource::MongoReplicaSetSuiteSnapshot, ClusterError> {
        mongo_replica_set_suite_snapshot()
            .await
            .ok_or_else(|| ClusterError::EngineOperation {
                message: "MongoDB replica set suite is not running".into(),
                source: Box::new(std::io::Error::other("suite not provisioned")),
            })
    }
}

#[async_trait]
impl MongoClusterOps for Arc<MongoReplicaSetClusterControl> {
    async fn step_down_primary(&self) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        let primary_index =
            primary_index_from_status(&repl_set_status_from_any_node(&snapshot).await?)?;
        MongoReplicaSetClusterControl::check_index(&snapshot, primary_index)?;
        let port = snapshot.host_ports[primary_index];
        let uri = admin_uri_for_host_and_port(snapshot.client_host.as_str(), port);
        let client = mongo_client(uri.as_str()).await.map_err(map_mongo_err)?;
        let admin = client.database("admin");
        admin
            .run_command(doc! { "replSetStepDown": 60 })
            .await
            .map_err(map_mongo_err)?;
        if let Ok(mut guard) = self.await_primary_after_step_down.lock() {
            *guard = Some(primary_index);
        }
        self.emit_cluster_row("step_down_primary", Some(primary_index), None);
        Ok(())
    }

    async fn current_primary(&self) -> Result<usize, ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        let status = repl_set_status_from_any_node(&snapshot).await?;
        let index = primary_index_from_status(&status)?;
        self.emit_cluster_row(
            "current_primary",
            Some(index),
            Some(format!("primary_index={index}")),
        );
        Ok(index)
    }

    async fn await_new_primary(&self, timeout: Duration) -> Result<usize, ClusterError> {
        let previous = self
            .await_primary_after_step_down
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
            .ok_or_else(|| ClusterError::EngineOperation {
                message: "call step_down_primary before await_new_primary".into(),
                source: Box::new(std::io::Error::other("missing step-down context")),
            })?;
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() > deadline {
                return Err(ClusterError::QuorumTimeout { timeout });
            }
            let snapshot = self.snapshot_or_err().await?;
            let status = match repl_set_status_from_any_node(&snapshot).await {
                Ok(status_document) => status_document,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            let current = match primary_index_from_status(&status) {
                Ok(primary_index) => primary_index,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            if current != previous {
                self.emit_cluster_row(
                    "await_new_primary",
                    Some(current),
                    Some(format!("previous_primary_index={previous}")),
                );
                return Ok(current);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

#[async_trait]
impl ClusterControl for Arc<MongoReplicaSetClusterControl> {
    async fn stop_node(&self, index: usize) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        MongoReplicaSetClusterControl::check_index(&snapshot, index)?;
        let docker = docker_connect()?;
        let container_id = snapshot.container_ids[index].as_str();
        let options = StopContainerOptionsBuilder::default().t(30).build();
        docker
            .stop_container(container_id, Some(options))
            .await
            .map_err(|source| ClusterError::DockerOperation { index, source })?;
        Ok(())
    }

    async fn start_node(&self, index: usize) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        MongoReplicaSetClusterControl::check_index(&snapshot, index)?;
        let docker = docker_connect()?;
        let container_id = snapshot.container_ids[index].as_str();
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
        MongoReplicaSetClusterControl::check_index(&snapshot, index)?;
        let docker = docker_connect()?;
        let container_id = snapshot.container_ids[index].as_str();
        docker
            .pause_container(container_id)
            .await
            .map_err(|source| ClusterError::DockerOperation { index, source })?;
        Ok(())
    }

    async fn unpause_node(&self, index: usize) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        MongoReplicaSetClusterControl::check_index(&snapshot, index)?;
        let docker = docker_connect()?;
        let container_id = snapshot.container_ids[index].as_str();
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
        let snapshot = self.snapshot_or_err().await?;
        MongoReplicaSetClusterControl::check_index(&snapshot, index)?;
        #[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
        if snapshot.inter_node_mesh_active {
            if let Some(mesh) = super::inter_node::mongo_inter_node_mesh_snapshot().await {
                super::inter_node::mesh_set_edges_enabled_for_node(&mesh, index, false).await?;
                if let Ok(mut guard) = self.disconnect_pause.lock() {
                    guard.insert(index);
                }
                self.emit_cluster_row(
                    "disconnect_node",
                    Some(index),
                    Some("toxiproxy_mesh".to_string()),
                );
                return Ok(());
            }
        }
        self.pause_node(index).await?;
        if let Ok(mut guard) = self.disconnect_pause.lock() {
            guard.insert(index);
        }
        Ok(())
    }

    async fn reconnect_node(&self, index: usize) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        MongoReplicaSetClusterControl::check_index(&snapshot, index)?;
        #[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
        if snapshot.inter_node_mesh_active {
            if let Some(mesh) = super::inter_node::mongo_inter_node_mesh_snapshot().await {
                super::inter_node::mesh_set_edges_enabled_for_node(&mesh, index, true).await?;
                if let Ok(mut guard) = self.disconnect_pause.lock() {
                    guard.remove(&index);
                }
                self.emit_cluster_row(
                    "reconnect_node",
                    Some(index),
                    Some("toxiproxy_mesh".to_string()),
                );
                return Ok(());
            }
        }
        self.unpause_node(index).await
    }

    async fn node_status(&self, index: usize) -> Result<NodeStatus, ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        MongoReplicaSetClusterControl::check_index(&snapshot, index)?;
        let docker = docker_connect()?;
        let container_id = snapshot.container_ids[index].as_str();
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
            if let Ok(guard) = self.disconnect_pause.lock() {
                if guard.contains(&index) {
                    continue;
                }
            }
            let container_id = snapshot.container_ids[index].as_str();
            let (running, paused) = inspect_running_paused(&docker, container_id).await?;
            if running && !paused {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn reset_topology(&self) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        #[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
        if snapshot.inter_node_mesh_active {
            if let Some(mesh) = super::inter_node::mongo_inter_node_mesh_snapshot().await {
                super::inter_node::mesh_enable_all(&mesh).await?;
            }
        }
        let docker = docker_connect()?;
        for index in 0..snapshot.node_count {
            let container_id = snapshot.container_ids[index].as_str();
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
            Some(
                "restarted_or_unpaused_all_nodes; awaiting PRIMARY+SECONDARY convergence"
                    .to_string(),
            ),
        );
        let convergence_timeout = Duration::from_secs(90);
        let deadline = Instant::now() + convergence_timeout;
        loop {
            if Instant::now() > deadline {
                return Err(ClusterError::QuorumTimeout {
                    timeout: convergence_timeout,
                });
            }
            match repl_set_status_from_any_node(&snapshot).await {
                Ok(status) if members_healthy(&status, snapshot.node_count) => {
                    let primary_index = primary_index_from_status(&status)?;
                    self.emit_cluster_row(
                        "reset_topology",
                        None,
                        Some(format!("converged primary_index={primary_index}")),
                    );
                    return Ok(());
                }
                Ok(_) | Err(_) => {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
            }
        }
    }

    fn node_count(&self) -> usize {
        self.rs_node_count
    }

    async fn partition_nodes(&self, node_a: usize, node_b: usize) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        MongoReplicaSetClusterControl::check_index(&snapshot, node_a)?;
        MongoReplicaSetClusterControl::check_index(&snapshot, node_b)?;
        #[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
        if snapshot.inter_node_mesh_active {
            if let Some(mesh) = super::inter_node::mongo_inter_node_mesh_snapshot().await {
                super::inter_node::mesh_set_partition(&mesh, node_a, node_b, false).await?;
                self.emit_cluster_row(
                    "partition_nodes",
                    None,
                    Some(format!("node_a={node_a} node_b={node_b} toxiproxy_mesh")),
                );
                return Ok(());
            }
        }
        Err(ClusterError::FaultInjectionNotAvailable)
    }
}

fn members_healthy(status: &Document, node_count: usize) -> bool {
    let Some(members) = status.get_array("members").ok() else {
        return false;
    };
    if members.len() != node_count {
        return false;
    }
    let mut primary_count = 0usize;
    let mut secondary_count = 0usize;
    for member in members {
        let Some(member_doc) = member.as_document() else {
            continue;
        };
        let state = member_doc
            .get_str("stateStr")
            .unwrap_or("")
            .to_ascii_uppercase();
        match state.as_str() {
            "PRIMARY" => primary_count += 1,
            "SECONDARY" => secondary_count += 1,
            _ => {}
        }
    }
    primary_count == 1 && secondary_count == node_count.saturating_sub(1)
}
