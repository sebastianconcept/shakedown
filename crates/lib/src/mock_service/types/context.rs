//! Read-only context passed into [`super::super::HttpMockService::handle`].

use std::sync::Arc;

use super::resources::MockServiceResources;

/// Read-only context passed into [`super::super::HttpMockService::handle`].
#[derive(Debug)]
pub struct MockServiceContext {
    /// Active scenario identifier.
    pub scenario_id: String,
    /// Logical name of this mock service instance.
    pub mock_service_name: String,
    /// Shared resource handles (Kafka, AMQP, …).
    pub resources: Arc<MockServiceResources>,
}

impl MockServiceContext {
    /// Construct a context for tests or harness wiring.
    pub fn new(
        scenario_id: impl Into<String>,
        mock_service_name: impl Into<String>,
        resources: Arc<MockServiceResources>,
    ) -> Self {
        Self {
            scenario_id: scenario_id.into(),
            mock_service_name: mock_service_name.into(),
            resources,
        }
    }
}
