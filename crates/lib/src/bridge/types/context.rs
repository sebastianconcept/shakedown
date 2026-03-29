//! Read-only context passed into [`super::super::HttpBridge::handle`].

use std::sync::Arc;

use super::resources::BridgeResources;

/// Read-only context passed into [`super::super::HttpBridge::handle`].
#[derive(Debug)]
pub struct BridgeContext {
    /// Active scenario identifier.
    pub scenario_id: String,
    /// Logical name of this bridge instance.
    pub bridge_name: String,
    /// Shared resource handles (Kafka, AMQP, …).
    pub resources: Arc<BridgeResources>,
}

impl BridgeContext {
    /// Construct a context for tests or harness wiring.
    pub fn new(
        scenario_id: impl Into<String>,
        bridge_name: impl Into<String>,
        resources: Arc<BridgeResources>,
    ) -> Self {
        Self {
            scenario_id: scenario_id.into(),
            bridge_name: bridge_name.into(),
            resources,
        }
    }
}
