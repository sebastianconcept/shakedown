//! [`KafkaClusterControl`] — Docker broker lifecycle + [`crate::cluster::kafka_cluster::KafkaClusterOps`].
//!
//! Requires **`kafka-cluster`**.

use std::collections::HashSet;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bollard::query_parameters::{InspectContainerOptionsBuilder, StopContainerOptionsBuilder};
use bollard::Docker;

use crate::cluster::kafka_cluster::KafkaClusterOps;
use crate::cluster::{ClusterControl, ClusterError, NodeStatus, CLUSTER_EVENT_SOURCE};
use crate::event::Event;
use crate::kafka_cluster_resource::kafka_cluster_suite_snapshot;

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

/// Map Kafka broker id from metadata to suite node index (`KAFKA_BROKER_ID` = index + 1).
fn broker_id_to_node_index(broker_id: i32, node_count: usize) -> Result<usize, ClusterError> {
    if broker_id <= 0 {
        return Err(ClusterError::EngineOperation {
            message: format!("partition has no elected leader yet (broker_id={broker_id})"),
            source: Box::new(std::io::Error::other("leader unknown")),
        });
    }
    let index =
        (broker_id as usize)
            .checked_sub(1)
            .ok_or_else(|| ClusterError::EngineOperation {
                message: format!("invalid broker id {broker_id}"),
                source: Box::new(std::io::Error::other("broker id")),
            })?;
    if index >= node_count {
        return Err(ClusterError::EngineOperation {
            message: format!(
                "leader broker id {broker_id} is not in this suite (node_count={node_count})"
            ),
            source: Box::new(std::io::Error::other("broker out of range")),
        });
    }
    Ok(index)
}

fn fetch_partition_leader_broker_id(
    bootstrap_servers: &str,
    topic: &str,
    partition: i32,
) -> Result<i32, ClusterError> {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, DefaultConsumerContext};

    let consumer: rdkafka::consumer::BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("client.id", "shakedown_kafka_partition_leader")
        .set("socket.timeout.ms", "10000")
        .create()
        .map_err(|source| ClusterError::EngineOperation {
            message: "rdkafka client for partition metadata".into(),
            source: Box::new(std::io::Error::other(source.to_string())),
        })?;

    let meta = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|source| ClusterError::EngineOperation {
            message: format!("fetch_metadata topic={topic}"),
            source: Box::new(std::io::Error::other(source.to_string())),
        })?;

    let topic_meta = meta
        .topics()
        .iter()
        .find(|meta_topic| meta_topic.name() == topic)
        .ok_or_else(|| ClusterError::EngineOperation {
            message: format!("topic not found in metadata: {topic}"),
            source: Box::new(std::io::Error::other("unknown topic")),
        })?;

    let partition_meta = topic_meta
        .partitions()
        .iter()
        .find(|partition_meta| partition_meta.id() == partition)
        .ok_or_else(|| ClusterError::EngineOperation {
            message: format!("partition {partition} not found for topic {topic}"),
            source: Box::new(std::io::Error::other("unknown partition")),
        })?;

    Ok(partition_meta.leader())
}

/// Suite-scoped control plane for a provisioned KRaft Kafka cluster (`kafka-cluster`).
pub struct KafkaClusterControl {
    node_count: usize,
    bootstrap_servers: String,
    event_sender: Mutex<Option<SyncSender<Event>>>,
    disconnect_pause: Mutex<HashSet<usize>>,
}

impl KafkaClusterControl {
    /// New control handle; `node_count` and `bootstrap_servers` must match the suite snapshot.
    pub fn new(node_count: usize, bootstrap_servers: String) -> Self {
        Self {
            node_count,
            bootstrap_servers,
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
            Some(index) => format!("kafka: {operation} node_index={index}"),
            None => format!("kafka: {operation}"),
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
        snapshot: &crate::kafka_cluster_resource::KafkaClusterSuiteSnapshot,
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
    ) -> Result<crate::kafka_cluster_resource::KafkaClusterSuiteSnapshot, ClusterError> {
        kafka_cluster_suite_snapshot()
            .await
            .ok_or_else(|| ClusterError::EngineOperation {
                message: "Kafka cluster suite is not running".into(),
                source: Box::new(std::io::Error::other("suite not provisioned")),
            })
    }
}

#[async_trait]
impl KafkaClusterOps for Arc<KafkaClusterControl> {
    async fn leader_for_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<usize, ClusterError> {
        let leader_id =
            fetch_partition_leader_broker_id(self.bootstrap_servers.as_str(), topic, partition)?;
        let index = broker_id_to_node_index(leader_id, self.node_count)?;
        self.emit_cluster_row(
            "leader_for_partition",
            Some(index),
            Some(format!(
                "topic={topic} partition={partition} leader_broker_id={leader_id}"
            )),
        );
        Ok(index)
    }

    async fn await_leader_election(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> Result<usize, ClusterError> {
        // Baseline is the first metadata read in this call. After `stop_node`, the broker id often
        // stays stale briefly; we poll until the reported leader id changes to a live broker.
        let initial =
            fetch_partition_leader_broker_id(self.bootstrap_servers.as_str(), topic, partition)?;
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() > deadline {
                return Err(ClusterError::QuorumTimeout { timeout });
            }
            let current = fetch_partition_leader_broker_id(
                self.bootstrap_servers.as_str(),
                topic,
                partition,
            )?;

            if initial < 0 {
                if current >= 0 {
                    let index = broker_id_to_node_index(current, self.node_count)?;
                    self.emit_cluster_row(
                        "await_leader_election",
                        Some(index),
                        Some(format!(
                            "topic={topic} partition={partition} leader_broker_id={current}"
                        )),
                    );
                    return Ok(index);
                }
            } else if current >= 0 && current != initial {
                let index = broker_id_to_node_index(current, self.node_count)?;
                self.emit_cluster_row(
                    "await_leader_election",
                    Some(index),
                    Some(format!(
                        "topic={topic} partition={partition} new_leader_broker_id={current} previous_broker_id={initial}"
                    )),
                );
                return Ok(index);
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

#[async_trait]
impl ClusterControl for Arc<KafkaClusterControl> {
    async fn stop_node(&self, index: usize) -> Result<(), ClusterError> {
        let snapshot = self.snapshot_or_err().await?;
        KafkaClusterClusterControl::check_index(&snapshot, index)?;
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
        KafkaClusterClusterControl::check_index(&snapshot, index)?;
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
        KafkaClusterClusterControl::check_index(&snapshot, index)?;
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
        KafkaClusterClusterControl::check_index(&snapshot, index)?;
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
        KafkaClusterClusterControl::check_index(&snapshot, index)?;
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
