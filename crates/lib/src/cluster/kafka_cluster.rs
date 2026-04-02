//! KRaft Kafka cluster marker, [`KafkaClusterOps`], and [`KafkaClusterHandle`].
//!
//! Requires **`kafka-cluster`** (implies `kafka` + `cluster`).

use std::time::Duration;

use async_trait::async_trait;

pub use crate::kafka_cluster_cluster_control::KafkaClusterClusterControl;

use crate::cluster::{ClusterControl, ClusterError};

/// Type marker for [`crate::lifecycle::Context::cluster_handle`] / [`crate::lifecycle::Context::register_cluster_handle`].
#[derive(Debug, Clone, Copy, Default)]
pub struct KafkaClusterMarker;

/// Engine-specific partition leadership helpers (metadata via librdkafka).
#[async_trait]
pub trait KafkaClusterOps: ClusterControl {
    /// Return the suite **node index** (`0..node_count`) of the broker that leads `partition` for `topic`.
    ///
    /// `topic` must be the **full Kafka topic name** (e.g. after [`crate::resources::KafkaBindings::topic_namespace`]).
    async fn leader_for_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<usize, ClusterError>;

    /// Poll metadata until the partition leader **broker id** differs from the first read in this call
    /// (stale metadata after `stop_node` usually keeps the old id briefly), or until a leader appears
    /// when the initial read had no leader.
    ///
    /// Prefer calling shortly after a topology change; a short pause after `stop_node` can keep the
    /// baseline read on the pre-failover leader while metadata catches up.
    async fn await_leader_election(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> Result<usize, ClusterError>;
}

/// Typed view of the suite Kafka cluster control (see [`crate::lifecycle::Context::kafka_cluster`]).
pub struct KafkaClusterHandle<'a> {
    inner: &'a std::sync::Arc<KafkaClusterClusterControl>,
}

impl<'a> KafkaClusterHandle<'a> {
    /// Wrap a shared control handle registered for [`KafkaClusterMarker`].
    pub fn new(inner: &'a std::sync::Arc<KafkaClusterClusterControl>) -> Self {
        Self { inner }
    }

    /// See [`KafkaClusterOps::leader_for_partition`].
    pub async fn leader_for_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<usize, ClusterError> {
        KafkaClusterOps::leader_for_partition(self.inner, topic, partition).await
    }

    /// See [`KafkaClusterOps::await_leader_election`].
    pub async fn await_leader_election(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> Result<usize, ClusterError> {
        KafkaClusterOps::await_leader_election(self.inner, topic, partition, timeout).await
    }
}
