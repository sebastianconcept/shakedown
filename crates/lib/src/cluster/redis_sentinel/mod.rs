//! Redis Sentinel — suite [`resource`], [`cluster_control`], marker, [`RedisSentinelOps`], [`RedisSentinelHandle`].
//!
//! Requires **`redis-sentinel`** (implies `redis` + `cluster`).

pub(crate) mod cluster_control;
pub(crate) mod resource;

pub use cluster_control::RedisSentinelClusterControl;

use std::time::Duration;

use async_trait::async_trait;

use crate::cluster::{ClusterControl, ClusterError};

/// Type marker for [`crate::lifecycle::Context::cluster_handle`] / [`crate::lifecycle::Context::register_cluster_handle`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RedisSentinelMarker;

/// Engine-specific Sentinel failover helpers (`SENTINEL FAILOVER`, master discovery).
#[async_trait]
pub trait RedisSentinelOps: ClusterControl {
    /// Ask Sentinels to start a failover for the monitored master (see `SENTINEL FAILOVER`).
    async fn trigger_failover(&self) -> Result<(), ClusterError>;

    /// Return the Redis **node index** (`0..node_count`) of the current primary, via `SENTINEL MASTER`.
    async fn current_master(&self) -> Result<usize, ClusterError>;

    /// Wait until the primary differs from the one recorded by [`RedisSentinelOps::trigger_failover`].
    async fn await_failover_complete(&self, timeout: Duration) -> Result<usize, ClusterError>;
}

/// Typed view of the suite Redis Sentinel control (see [`crate::lifecycle::Context::redis_sentinel_cluster`]).
pub struct RedisSentinelHandle<'a> {
    inner: &'a std::sync::Arc<RedisSentinelClusterControl>,
}

impl<'a> RedisSentinelHandle<'a> {
    /// Wrap a shared control handle registered for [`RedisSentinelMarker`].
    pub fn new(inner: &'a std::sync::Arc<RedisSentinelClusterControl>) -> Self {
        Self { inner }
    }

    /// See [`RedisSentinelOps::trigger_failover`].
    pub async fn trigger_failover(&self) -> Result<(), ClusterError> {
        RedisSentinelOps::trigger_failover(self.inner).await
    }

    /// See [`RedisSentinelOps::current_master`].
    pub async fn current_master(&self) -> Result<usize, ClusterError> {
        RedisSentinelOps::current_master(self.inner).await
    }

    /// See [`RedisSentinelOps::await_failover_complete`].
    pub async fn await_failover_complete(&self, timeout: Duration) -> Result<usize, ClusterError> {
        RedisSentinelOps::await_failover_complete(self.inner, timeout).await
    }
}
