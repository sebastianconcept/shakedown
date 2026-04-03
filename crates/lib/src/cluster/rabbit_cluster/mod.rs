//! RabbitMQ Erlang cluster — suite [`resource`], [`cluster_control`], marker, [`RabbitClusterOps`], [`RabbitClusterHandle`].
//!
//! Requires **`rabbitmq-cluster`** (implies `rabbitmq` + `cluster`).

pub(crate) mod cluster_control;
pub(crate) mod resource;

pub use cluster_control::RabbitClusterClusterControl;

use std::time::Duration;

use async_trait::async_trait;

use crate::cluster::{ClusterControl, ClusterError};

/// Type marker for [`crate::lifecycle::Context::cluster_handle`] / [`crate::lifecycle::Context::register_cluster_handle`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RabbitClusterMarker;

/// Quorum queue leadership via the RabbitMQ management HTTP API (per-node).
#[async_trait]
pub trait RabbitClusterOps: ClusterControl {
    /// Return the suite **node index** (`0..node_count`) whose broker is the **Raft leader** for the quorum queue.
    ///
    /// Uses `GET /api/queues/%2F/{queue}` on a reachable management endpoint (requires the
    /// **`rabbitmq-management`** plugin — the suite uses a **`-management`** image tag by default).
    async fn quorum_queue_leader(&self, queue_name: &str) -> Result<usize, ClusterError>;

    /// Poll until the quorum queue leader **node index** differs from the first successful read in this call,
    /// or until `timeout` elapses.
    async fn await_queue_leader_election(
        &self,
        queue_name: &str,
        timeout: Duration,
    ) -> Result<usize, ClusterError>;
}

/// Typed view of the suite RabbitMQ cluster control (see [`crate::lifecycle::Context::rabbit_cluster`]).
pub struct RabbitClusterHandle<'a> {
    inner: &'a std::sync::Arc<RabbitClusterClusterControl>,
}

impl<'a> RabbitClusterHandle<'a> {
    /// Wrap a shared control handle registered for [`RabbitClusterMarker`].
    pub fn new(inner: &'a std::sync::Arc<RabbitClusterClusterControl>) -> Self {
        Self { inner }
    }

    /// See [`RabbitClusterOps::quorum_queue_leader`].
    pub async fn quorum_queue_leader(&self, queue_name: &str) -> Result<usize, ClusterError> {
        RabbitClusterOps::quorum_queue_leader(self.inner, queue_name).await
    }

    /// See [`RabbitClusterOps::await_queue_leader_election`].
    pub async fn await_queue_leader_election(
        &self,
        queue_name: &str,
        timeout: Duration,
    ) -> Result<usize, ClusterError> {
        RabbitClusterOps::await_queue_leader_election(self.inner, queue_name, timeout).await
    }
}
