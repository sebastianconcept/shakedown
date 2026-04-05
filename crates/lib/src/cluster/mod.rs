//! Cluster topology support — shared Docker networks, node control, and engine-specific ops.
//!
//! **Why:** some correctness and resilience scenarios require a real multi-node cluster — a
//! Kafka cluster for partition-leader failover, a MongoDB replica set for read-preference testing,
//! a Redis Sentinel group for automatic failover. Running a standalone container cannot reproduce
//! these failure modes. The cluster module provisions multi-node topologies on a shared Docker
//! network and exposes node-level control (stop, restart, promote, demote) through the
//! [`ClusterControl`] trait.
//!
//! **How:** register a cluster resource (e.g. `MongoReplicaSetResource`, `KafkaClusterResource`)
//! on [`crate::lifecycle::Context`] before calling [`crate::lifecycle::run_scenario`]. Access the
//! cluster handle inside the scenario body to drive node operations. Combine with the
//! `fault-injection` feature to inject network faults between specific nodes.
//!
//! Requires the **`cluster`** Cargo feature (pulls in [`bollard`] for Docker Engine API calls).
//!
//! **Layout:** each engine is a subdirectory (`kafka_cluster/`, `mongo_replica_set/`, …) with
//! `resource` + `cluster_control` modules.
//!
//! # Book reference
//!
//! Part V, *Chapters 19–23: Cluster Topologies* — covers the `ClusterControl` trait, engine-
//! specific cluster resources, node-level fault injection, and assertions for leader election and
//! failover behaviour.

pub mod network;

#[cfg(feature = "mongodb-cluster")]
pub mod mongo_replica_set;

#[cfg(feature = "kafka-cluster")]
pub mod kafka_cluster;

#[cfg(feature = "redis-sentinel")]
pub mod redis_sentinel;

#[cfg(feature = "postgres-cluster")]
pub mod postgres_replica_set;

#[cfg(feature = "rabbitmq-cluster")]
pub mod rabbit_cluster;

pub use network::{cluster_network_name, create_cluster_network, remove_cluster_network};

/// Bounded queue for [`ClusterHandle`] [`Event`] delivery into [`crate::lifecycle::run_scenario`].
#[cfg(any(
    feature = "mongodb-cluster",
    feature = "kafka-cluster",
    feature = "redis-sentinel",
    feature = "postgres-cluster",
    feature = "rabbitmq-cluster",
))]
pub const CLUSTER_EVENT_CHANNEL_CAPACITY: usize = 64;

use std::sync::mpsc::SyncSender;
use std::time::Duration;

use async_trait::async_trait;
use bollard::errors::Error as BollardError;
use displaydoc::Display;
use thiserror::Error;

use crate::event::Event;

/// `source` field on [`Event`] records emitted by [`ClusterHandle`].
pub const CLUSTER_EVENT_SOURCE: &str = "cluster";

/// Lifecycle state of a single cluster node from the control plane’s perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display)]
#[ignore_extra_doc_attributes]
pub enum NodeStatus {
    /// running
    Running,
    /// stopped
    Stopped,
    /// paused
    Paused,
    /// disconnected
    Disconnected,
}

/// Failure from cluster topology operations (node control, networks, engine-specific steps).
#[derive(Debug, Display, Error)]
#[ignore_extra_doc_attributes]
#[allow(missing_docs)]
pub enum ClusterError {
    /// node index {index} is out of range for cluster with {node_count} nodes
    NodeIndexOutOfRange { index: usize, node_count: usize },
    /// node {index} is already in state {current_status}
    NodeAlreadyInState {
        index: usize,
        current_status: NodeStatus,
    },
    /// Docker operation failed on node {index}
    DockerOperation {
        index: usize,
        #[source]
        source: BollardError,
    },
    /// cluster did not reach quorum within {timeout:?}
    QuorumTimeout { timeout: Duration },
    /// engine-specific operation failed: {message}
    EngineOperation {
        message: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// network management failed
    NetworkError {
        #[source]
        source: BollardError,
    },
    /// fault injection not available (enable `fault-injection` feature and register Toxiproxy)
    FaultInjectionNotAvailable,
}

/// Generic node-level control for clustered resources (MongoDB replica set, Kafka brokers, etc.).
#[async_trait]
pub trait ClusterControl: Send + Sync {
    /// Stop the node at `index` (Docker container stop / equivalent).
    async fn stop_node(&self, index: usize) -> Result<(), ClusterError>;
    /// Start or restart the node at `index`.
    async fn start_node(&self, index: usize) -> Result<(), ClusterError>;
    /// Pause (SIGSTOP / docker pause) the node at `index`.
    async fn pause_node(&self, index: usize) -> Result<(), ClusterError>;
    /// Unpause (SIGCONT / docker unpause) the node at `index`.
    async fn unpause_node(&self, index: usize) -> Result<(), ClusterError>;
    /// Disconnect the node at `index` from the cluster network.
    async fn disconnect_node(&self, index: usize) -> Result<(), ClusterError>;
    /// Reconnect the node at `index` to the cluster network.
    async fn reconnect_node(&self, index: usize) -> Result<(), ClusterError>;
    /// Return the current [`NodeStatus`] for the node at `index`.
    async fn node_status(&self, index: usize) -> Result<NodeStatus, ClusterError>;
    /// Number of nodes that are currently in a healthy / running state.
    async fn healthy_node_count(&self) -> Result<usize, ClusterError>;
    /// Bring all nodes back to a clean running state (undo all stop/pause/disconnect operations).
    async fn reset_topology(&self) -> Result<(), ClusterError>;
    /// Total number of nodes in this cluster (fixed for the lifetime of the handle).
    fn node_count(&self) -> usize;

    /// Disable the bidirectional proxy pair between two nodes (inter-node Toxiproxy mesh when enabled).
    async fn partition_nodes(&self, _node_a: usize, _node_b: usize) -> Result<(), ClusterError> {
        Err(ClusterError::FaultInjectionNotAvailable)
    }
}

/// Ergonomic wrapper around a [`ClusterControl`] implementation with optional [`Event`] emission.
pub struct ClusterHandle {
    control: Box<dyn ClusterControl>,
    event_sender: Option<SyncSender<Event>>,
    engine: String,
}

impl ClusterHandle {
    /// Build a handle for `engine` (used in event messages and diagnostics).
    pub fn new(
        engine: impl Into<String>,
        control: Box<dyn ClusterControl>,
        event_sender: Option<SyncSender<Event>>,
    ) -> Self {
        Self {
            engine: engine.into(),
            control,
            event_sender,
        }
    }

    /// Logical engine name (e.g. `mongo`, `kafka`).
    pub fn engine(&self) -> &str {
        self.engine.as_str()
    }

    /// Wire the sender half of an `mpsc` pair so cluster operations enqueue [`Event`] rows for the harness to drain.
    pub fn set_event_sender(&mut self, sender: SyncSender<Event>) {
        self.event_sender = Some(sender);
    }

    fn emit_cluster_event(&self, operation: &str, node_index: Option<usize>, detail: Option<&str>) {
        let mut message = match node_index {
            Some(index) => format!("{}: {} node_index={}", self.engine, operation, index),
            None => format!("{}: {}", self.engine, operation),
        };
        if let Some(extra) = detail {
            message.push(' ');
            message.push_str(extra);
        }
        if let Some(sender) = &self.event_sender {
            let _ = sender.try_send(Event::info(CLUSTER_EVENT_SOURCE, message));
        }
    }

    /// Number of nodes in this cluster (fixed for the lifetime of the handle).
    pub fn node_count(&self) -> usize {
        self.control.node_count()
    }

    /// See [`ClusterControl::stop_node`].
    pub async fn stop_node(&self, index: usize) -> Result<(), ClusterError> {
        let result = self.control.stop_node(index).await;
        if result.is_ok() {
            self.emit_cluster_event("stop_node", Some(index), None);
        }
        result
    }

    /// See [`ClusterControl::start_node`].
    pub async fn start_node(&self, index: usize) -> Result<(), ClusterError> {
        let result = self.control.start_node(index).await;
        if result.is_ok() {
            self.emit_cluster_event("start_node", Some(index), None);
        }
        result
    }

    /// See [`ClusterControl::pause_node`].
    pub async fn pause_node(&self, index: usize) -> Result<(), ClusterError> {
        let result = self.control.pause_node(index).await;
        if result.is_ok() {
            self.emit_cluster_event("pause_node", Some(index), None);
        }
        result
    }

    /// See [`ClusterControl::unpause_node`].
    pub async fn unpause_node(&self, index: usize) -> Result<(), ClusterError> {
        let result = self.control.unpause_node(index).await;
        if result.is_ok() {
            self.emit_cluster_event("unpause_node", Some(index), None);
        }
        result
    }

    /// See [`ClusterControl::disconnect_node`].
    pub async fn disconnect_node(&self, index: usize) -> Result<(), ClusterError> {
        let result = self.control.disconnect_node(index).await;
        if result.is_ok() {
            self.emit_cluster_event("disconnect_node", Some(index), None);
        }
        result
    }

    /// See [`ClusterControl::reconnect_node`].
    pub async fn reconnect_node(&self, index: usize) -> Result<(), ClusterError> {
        let result = self.control.reconnect_node(index).await;
        if result.is_ok() {
            self.emit_cluster_event("reconnect_node", Some(index), None);
        }
        result
    }

    /// See [`ClusterControl::node_status`].
    pub async fn node_status(&self, index: usize) -> Result<NodeStatus, ClusterError> {
        let result = self.control.node_status(index).await;
        if let Ok(status) = result.as_ref() {
            let detail = format!("status={status}");
            self.emit_cluster_event("node_status", Some(index), Some(&detail));
        }
        result
    }

    /// See [`ClusterControl::healthy_node_count`].
    pub async fn healthy_node_count(&self) -> Result<usize, ClusterError> {
        let result = self.control.healthy_node_count().await;
        if let Ok(count) = &result {
            let detail = format!("healthy_node_count={count}");
            self.emit_cluster_event("healthy_node_count", None, Some(&detail));
        }
        result
    }

    /// See [`ClusterControl::reset_topology`].
    pub async fn reset_topology(&self) -> Result<(), ClusterError> {
        let result = self.control.reset_topology().await;
        if result.is_ok() {
            self.emit_cluster_event("reset_topology", None, None);
        }
        result
    }

    /// See [`ClusterControl::partition_nodes`].
    pub async fn partition_nodes(&self, node_a: usize, node_b: usize) -> Result<(), ClusterError> {
        let result = self.control.partition_nodes(node_a, node_b).await;
        if result.is_ok() {
            self.emit_cluster_event(
                "partition_nodes",
                None,
                Some(&format!("node_a={node_a} node_b={node_b}")),
            );
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::sync_channel;

    use crate::event::EventLevel;
    use crate::lifecycle::Context;

    struct FakeCluster {
        node_count: usize,
    }

    impl FakeCluster {
        fn new(node_count: usize) -> Self {
            Self { node_count }
        }

        fn check_index(&self, index: usize) -> Result<(), ClusterError> {
            if index >= self.node_count {
                Err(ClusterError::NodeIndexOutOfRange {
                    index,
                    node_count: self.node_count,
                })
            } else {
                Ok(())
            }
        }
    }

    #[async_trait]
    impl ClusterControl for FakeCluster {
        async fn stop_node(&self, index: usize) -> Result<(), ClusterError> {
            self.check_index(index)
        }

        async fn start_node(&self, index: usize) -> Result<(), ClusterError> {
            self.check_index(index)
        }

        async fn pause_node(&self, index: usize) -> Result<(), ClusterError> {
            self.check_index(index)
        }

        async fn unpause_node(&self, index: usize) -> Result<(), ClusterError> {
            self.check_index(index)
        }

        async fn disconnect_node(&self, index: usize) -> Result<(), ClusterError> {
            self.check_index(index)
        }

        async fn reconnect_node(&self, index: usize) -> Result<(), ClusterError> {
            self.check_index(index)
        }

        async fn node_status(&self, index: usize) -> Result<NodeStatus, ClusterError> {
            self.check_index(index)?;
            Ok(NodeStatus::Running)
        }

        async fn healthy_node_count(&self) -> Result<usize, ClusterError> {
            Ok(self.node_count)
        }

        async fn reset_topology(&self) -> Result<(), ClusterError> {
            Ok(())
        }

        fn node_count(&self) -> usize {
            self.node_count
        }
    }

    struct FakeResourceKey;

    #[tokio::test]
    async fn cluster_handle_emits_event_on_successful_stop_node() {
        let (sender, receiver) = sync_channel::<Event>(8);
        let handle = ClusterHandle::new("test-engine", Box::new(FakeCluster::new(3)), Some(sender));

        handle.stop_node(1).await.expect("stop_node");

        let events: Vec<_> = receiver.try_iter().collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source, CLUSTER_EVENT_SOURCE);
        assert_eq!(events[0].level, EventLevel::Info);
        assert!(
            events[0].message.contains("test-engine")
                && events[0].message.contains("stop_node")
                && events[0].message.contains("node_index=1")
        );
    }

    #[tokio::test]
    async fn cluster_handle_skips_event_when_operation_fails() {
        let (sender, receiver) = sync_channel::<Event>(8);
        let handle = ClusterHandle::new("test-engine", Box::new(FakeCluster::new(2)), Some(sender));

        let err = handle.stop_node(9).await.expect_err("out of range");
        assert!(matches!(err, ClusterError::NodeIndexOutOfRange { .. }));
        assert_eq!(receiver.try_iter().count(), 0);
    }

    #[test]
    fn context_register_and_cluster_handle_by_type() {
        let mut context = Context::new();
        let cluster_handle = ClusterHandle::new("fake", Box::new(FakeCluster::new(5)), None);
        context.register_cluster_handle::<FakeResourceKey>(cluster_handle);

        let retrieved = context
            .cluster_handle::<FakeResourceKey>()
            .expect("registered");
        assert_eq!(retrieved.engine(), "fake");
        assert_eq!(retrieved.node_count(), 5);
    }
}
