//! PostgreSQL primary + replica marker, [`PostgresReplicaSetOps`], and [`PostgresReplicaSetHandle`].
//!
//! Requires **`postgres-cluster`** (`postgres` + `cluster`).

use async_trait::async_trait;

pub use crate::postgres_replica_set_cluster_control::PostgresReplicaSetClusterControl;

use crate::cluster::{ClusterControl, ClusterError};

/// Type marker for [`crate::lifecycle::Context::cluster_handle`] / [`crate::lifecycle::Context::register_cluster_handle`].
#[derive(Debug, Clone, Copy, Default)]
pub struct PostgresReplicaSetMarker;

/// Engine-specific promotion and primary discovery (`pg_promote`, `pg_is_in_recovery`).
#[async_trait]
pub trait PostgresReplicaSetOps: ClusterControl {
    /// Promote the standby at `index` to primary (`SELECT pg_promote` on that node).
    async fn promote_replica(&self, index: usize) -> Result<(), ClusterError>;

    /// Return the node index (`0` = initial primary port, `1` = initial replica port) whose
    /// `pg_is_in_recovery()` is `false`.
    async fn current_primary(&self) -> Result<usize, ClusterError>;
}

/// Typed view of per-scenario Postgres replica-set control (see [`crate::lifecycle::Context::postgres_replica_set_cluster`]).
pub struct PostgresReplicaSetHandle<'a> {
    inner: &'a std::sync::Arc<PostgresReplicaSetClusterControl>,
}

impl<'a> PostgresReplicaSetHandle<'a> {
    /// Wrap a shared control handle registered for [`PostgresReplicaSetMarker`].
    pub fn new(inner: &'a std::sync::Arc<PostgresReplicaSetClusterControl>) -> Self {
        Self { inner }
    }

    /// See [`PostgresReplicaSetOps::promote_replica`].
    pub async fn promote_replica(&self, index: usize) -> Result<(), ClusterError> {
        PostgresReplicaSetOps::promote_replica(self.inner, index).await
    }

    /// See [`PostgresReplicaSetOps::current_primary`].
    pub async fn current_primary(&self) -> Result<usize, ClusterError> {
        PostgresReplicaSetOps::current_primary(self.inner).await
    }
}
