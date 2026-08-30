//! Fault-injection and diagnostics-ring accessors on [`super::Context`].
//!
//! This module is useful when a scenario or the harness must read or install a fault handle or stats
//! rings.
//! This module does not create Toxiproxy proxies.

use super::Context;

impl Context {
    /// Toxiproxy fault control when `fault-injection` is enabled and Toxiproxy was provisioned.
    #[cfg(feature = "fault-injection")]
    pub fn fault_handle(&self) -> Option<&crate::fault_injection::FaultHandle> {
        self.fault_handle.as_ref()
    }

    /// Mutable access for harness code and advanced scenario wiring.
    #[cfg(feature = "fault-injection")]
    pub fn fault_handle_mut(&mut self) -> Option<&mut crate::fault_injection::FaultHandle> {
        self.fault_handle.as_mut()
    }

    #[cfg(feature = "fault-injection")]
    pub(crate) fn set_fault_handle(
        &mut self,
        fault_handle: Option<crate::fault_injection::FaultHandle>,
    ) {
        self.fault_handle = fault_handle;
    }

    #[cfg(feature = "fault-injection")]
    pub(crate) fn clear_fault_handle(&mut self) {
        self.fault_handle = None;
        #[cfg(all(feature = "fault-injection", feature = "kafka"))]
        {
            self.toxiproxy_control_api_base_url = None;
            self.kafka_fault_proxy_pending = None;
            self.kafka_bindings_apply_deferred = false;
        }
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn set_toxiproxy_control_api_base_url(&mut self, url: Option<String>) {
        self.toxiproxy_control_api_base_url = url;
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn toxiproxy_control_api_base_url(&self) -> Option<&str> {
        self.toxiproxy_control_api_base_url.as_deref()
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn set_kafka_fault_proxy_pending(&mut self, pending: Option<(String, String)>) {
        self.kafka_fault_proxy_pending = pending;
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn take_kafka_fault_proxy_pending(&mut self) -> Option<(String, String)> {
        self.kafka_fault_proxy_pending.take()
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn set_kafka_bindings_apply_deferred(&mut self, deferred: bool) {
        self.kafka_bindings_apply_deferred = deferred;
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn kafka_bindings_apply_deferred(&self) -> bool {
        self.kafka_bindings_apply_deferred
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn clear_kafka_fault_injection_deferred_state(&mut self) {
        self.kafka_fault_proxy_pending = None;
        self.kafka_bindings_apply_deferred = false;
    }

    /// Snapshot handle for per-container resource stats rings (diagnostics).
    #[cfg(all(
        any(
            feature = "kafka",
            feature = "rabbitmq",
            feature = "postgres",
            feature = "mariadb",
            feature = "mysql",
            feature = "redis",
            feature = "valkey",
            feature = "mongodb",
            feature = "nats",
            feature = "neo4j",
            feature = "elasticsearch",
            feature = "opensearch",
            feature = "qdrant",
            feature = "weaviate",
            feature = "influxdb",
            feature = "timescaledb",
            feature = "services",
        ),
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    pub(crate) fn resource_stats_rings(
        &self,
    ) -> Option<&crate::diagnostics::SharedResourceStatsRings> {
        self.stats_rings.as_ref()
    }

    /// Install or clear the stats ring handle (used by [`run_scenario`] and tests).
    #[cfg(all(
        any(
            feature = "kafka",
            feature = "rabbitmq",
            feature = "postgres",
            feature = "mariadb",
            feature = "mysql",
            feature = "redis",
            feature = "valkey",
            feature = "mongodb",
            feature = "nats",
            feature = "neo4j",
            feature = "elasticsearch",
            feature = "opensearch",
            feature = "qdrant",
            feature = "weaviate",
            feature = "influxdb",
            feature = "timescaledb",
            feature = "services",
        ),
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    pub(crate) fn set_resource_stats_rings(
        &mut self,
        rings: Option<crate::diagnostics::SharedResourceStatsRings>,
    ) {
        self.stats_rings = rings;
    }

    /// Assert CPU and memory bounds using collected Docker stats for `container_name`.
    #[cfg(all(
        any(
            feature = "kafka",
            feature = "rabbitmq",
            feature = "postgres",
            feature = "mariadb",
            feature = "mysql",
            feature = "redis",
            feature = "valkey",
            feature = "mongodb",
            feature = "nats",
            feature = "neo4j",
            feature = "elasticsearch",
            feature = "opensearch",
            feature = "qdrant",
            feature = "weaviate",
            feature = "influxdb",
            feature = "timescaledb",
            feature = "services",
        ),
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    pub fn assert_resource<'a>(
        &'a self,
        container_name: &'a str,
    ) -> crate::diagnostics::ResourceAssertBuilder<'a> {
        crate::diagnostics::ResourceAssertBuilder::new(self, container_name)
    }
}
