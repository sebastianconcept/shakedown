//! MongoDB replica set cluster marker, [`MongoClusterOps`], and [`MongoClusterHandle`].
//!
//! Requires **`mongodb-cluster`** (implies `cluster` + `mongodb`).

use std::time::Duration;

use async_trait::async_trait;

pub use crate::mongo_replica_set_cluster_control::MongoReplicaSetClusterControl;

use crate::cluster::{ClusterControl, ClusterError};

/// Type marker for [`crate::lifecycle::Context::cluster_handle`] / [`crate::lifecycle::Context::register_cluster_handle`].
#[derive(Debug, Clone, Copy, Default)]
pub struct MongoReplicaSetMarker;

/// Engine-specific replica set operations (primary elections, etc.).
#[async_trait]
pub trait MongoClusterOps: ClusterControl {
    /// Ask the current primary to step down so a new election can occur.
    async fn step_down_primary(&self) -> Result<(), ClusterError>;

    /// Return the member index (`mongo-{index}`) of the current primary.
    async fn current_primary(&self) -> Result<usize, ClusterError>;

    /// Wait until a **different** member becomes primary than the one recorded by
    /// [`MongoClusterOps::step_down_primary`], or return [`ClusterError::QuorumTimeout`].
    async fn await_new_primary(&self, timeout: Duration) -> Result<usize, ClusterError>;
}

/// Typed view of the suite’s MongoDB replica set control (see [`crate::lifecycle::Context::mongo_cluster`]).
pub struct MongoClusterHandle<'a> {
    inner: &'a std::sync::Arc<MongoReplicaSetClusterControl>,
}

impl<'a> MongoClusterHandle<'a> {
    /// Wrap a shared control handle registered for [`MongoReplicaSetMarker`].
    pub fn new(inner: &'a std::sync::Arc<MongoReplicaSetClusterControl>) -> Self {
        Self { inner }
    }

    /// See [`MongoClusterOps::step_down_primary`].
    pub async fn step_down_primary(&self) -> Result<(), ClusterError> {
        MongoClusterOps::step_down_primary(self.inner).await
    }

    /// See [`MongoClusterOps::current_primary`].
    pub async fn current_primary(&self) -> Result<usize, ClusterError> {
        MongoClusterOps::current_primary(self.inner).await
    }

    /// See [`MongoClusterOps::await_new_primary`].
    pub async fn await_new_primary(&self, timeout: Duration) -> Result<usize, ClusterError> {
        MongoClusterOps::await_new_primary(self.inner, timeout).await
    }
}
