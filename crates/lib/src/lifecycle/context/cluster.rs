//! Cluster-handle and topology-control accessors on [`super::Context`].
//!
//! This module is useful when a scenario looks up suite cluster control or registers a cluster
//! handle.
//! This module does not start or pause cluster nodes.

use super::Context;

impl Context {
    /// Register a [`crate::cluster::ClusterHandle`] for retrieval via [`Context::cluster_handle`].
    ///
    /// `T` is a zero-sized marker (often a private struct) used to disambiguate multiple cluster
    /// resources in one scenario.
    #[cfg(feature = "cluster")]
    pub fn register_cluster_handle<T: 'static>(&mut self, handle: crate::cluster::ClusterHandle) {
        self.cluster_handles
            .insert(std::any::TypeId::of::<T>(), handle);
    }

    /// Look up a cluster handle registered with [`Context::register_cluster_handle`].
    #[cfg(feature = "cluster")]
    pub fn cluster_handle<T: 'static>(&self) -> Option<&crate::cluster::ClusterHandle> {
        self.cluster_handles.get(&std::any::TypeId::of::<T>())
    }

    /// Mutable cluster handle for wiring [`crate::cluster::ClusterHandle::set_event_sender`] before the run phase.
    #[cfg(feature = "cluster")]
    pub fn cluster_handle_mut<T: 'static>(&mut self) -> Option<&mut crate::cluster::ClusterHandle> {
        self.cluster_handles.get_mut(&std::any::TypeId::of::<T>())
    }

    /// Start an open-model (or closed-model VU) load driver; see [`crate::load::LoadHandle`].
    #[cfg(feature = "load")]
    pub async fn start_load<E, F, Fut>(
        &mut self,
        load_name: &str,
        profile: crate::load::LoadProfile,
        work: F,
    ) -> Result<crate::load::LoadHandle, crate::load::LoadError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        crate::load::handle::start_load(self, load_name, profile, work).await
    }

    /// Used by [`run_scenario`] to wire [`Event`] merge for load phase transitions.
    #[cfg(feature = "load")]
    pub(crate) fn set_load_event_sender(
        &mut self,
        sender: Option<std::sync::mpsc::Sender<crate::event::Event>>,
    ) {
        self.load_event_sender = sender;
    }

    /// Sender cloned by [`crate::load::handle::start_load`] for structured load timeline rows.
    #[cfg(feature = "load")]
    pub(crate) fn load_event_sender(
        &self,
    ) -> Option<&std::sync::mpsc::Sender<crate::event::Event>> {
        self.load_event_sender.as_ref()
    }

    /// Used by [`run_scenario`] to merge [`crate::assert_eventually`] poll rows into [`crate::EventLog`].
    pub(crate) fn set_scenario_application_event_sender(
        &mut self,
        sender: Option<std::sync::mpsc::Sender<crate::event::Event>>,
    ) {
        self.scenario_application_event_sender = sender;
    }

    /// Sender for structured scenario-body events (run phase only), e.g. [`crate::assert_eventually`].
    pub(crate) fn scenario_application_event_sender(
        &self,
    ) -> Option<&std::sync::mpsc::Sender<crate::event::Event>> {
        self.scenario_application_event_sender.as_ref()
    }

    #[cfg(feature = "mongodb-cluster")]
    pub(crate) fn set_mongo_replica_set_cluster(
        &mut self,
        cluster: Option<
            std::sync::Arc<
                crate::cluster::mongo_replica_set::cluster_control::MongoReplicaSetClusterControl,
            >,
        >,
    ) {
        self.mongo_replica_set_cluster = cluster;
    }

    /// MongoDB replica set topology control ([`crate::cluster::mongo_replica_set::MongoClusterOps`]).
    #[cfg(feature = "mongodb-cluster")]
    pub fn mongo_cluster(
        &self,
    ) -> Option<crate::cluster::mongo_replica_set::MongoClusterHandle<'_>> {
        self.mongo_replica_set_cluster
            .as_ref()
            .map(crate::cluster::mongo_replica_set::MongoClusterHandle::new)
    }

    #[cfg(feature = "kafka-cluster")]
    pub(crate) fn set_kafka_cluster_control(
        &mut self,
        cluster: Option<
            std::sync::Arc<
                crate::cluster::kafka_cluster::cluster_control::KafkaClusterClusterControl,
            >,
        >,
    ) {
        self.kafka_cluster_control = cluster;
    }

    /// KRaft multi-broker Kafka partition leadership ([`crate::cluster::kafka_cluster::KafkaClusterOps`]).
    #[cfg(feature = "kafka-cluster")]
    pub fn kafka_cluster(&self) -> Option<crate::cluster::kafka_cluster::KafkaClusterHandle<'_>> {
        self.kafka_cluster_control
            .as_ref()
            .map(crate::cluster::kafka_cluster::KafkaClusterHandle::new)
    }

    #[cfg(feature = "redis-sentinel")]
    pub(crate) fn set_redis_sentinel_cluster(
        &mut self,
        cluster: Option<
            std::sync::Arc<
                crate::cluster::redis_sentinel::cluster_control::RedisSentinelClusterControl,
            >,
        >,
    ) {
        self.redis_sentinel_cluster = cluster;
    }

    /// Redis Sentinel failover and primary discovery ([`crate::cluster::redis_sentinel::RedisSentinelOps`]).
    #[cfg(feature = "redis-sentinel")]
    pub fn redis_sentinel_cluster(
        &self,
    ) -> Option<crate::cluster::redis_sentinel::RedisSentinelHandle<'_>> {
        self.redis_sentinel_cluster
            .as_ref()
            .map(crate::cluster::redis_sentinel::RedisSentinelHandle::new)
    }

    #[cfg(feature = "postgres-cluster")]
    pub(crate) fn set_postgres_replica_set_cluster(
        &mut self,
        cluster: Option<
            std::sync::Arc<
                crate::cluster::postgres_replica_set::cluster_control::PostgresReplicaSetClusterControl,
            >,
        >,
    ) {
        self.postgres_replica_set_cluster = cluster;
    }

    /// PostgreSQL replica-set promotion and primary discovery ([`crate::cluster::postgres_replica_set::PostgresReplicaSetOps`]).
    #[cfg(feature = "postgres-cluster")]
    pub fn postgres_replica_set_cluster(
        &self,
    ) -> Option<crate::cluster::postgres_replica_set::PostgresReplicaSetHandle<'_>> {
        self.postgres_replica_set_cluster
            .as_ref()
            .map(crate::cluster::postgres_replica_set::PostgresReplicaSetHandle::new)
    }

    #[cfg(feature = "rabbitmq-cluster")]
    pub(crate) fn set_rabbit_cluster_control(
        &mut self,
        cluster: Option<
            std::sync::Arc<
                crate::cluster::rabbit_cluster::cluster_control::RabbitClusterClusterControl,
            >,
        >,
    ) {
        self.rabbit_cluster_control = cluster;
    }

    /// RabbitMQ Erlang cluster quorum queue leadership ([`crate::cluster::rabbit_cluster::RabbitClusterOps`]).
    #[cfg(feature = "rabbitmq-cluster")]
    pub fn rabbit_cluster(
        &self,
    ) -> Option<crate::cluster::rabbit_cluster::RabbitClusterHandle<'_>> {
        self.rabbit_cluster_control
            .as_ref()
            .map(crate::cluster::rabbit_cluster::RabbitClusterHandle::new)
    }
}
