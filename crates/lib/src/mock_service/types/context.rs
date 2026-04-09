//! Read-only context passed into [`crate::mock_service::http::HttpMockServiceTrait::handle`].

use std::sync::Arc;

use super::resources::MockServiceResources;

/// Read-only context passed into [`crate::mock_service::http::HttpMockServiceTrait::handle`].
///
/// Most declarative stubs ignore it: the harness still supplies [`Self::scenario_id`] for logs and
/// diagnostics, [`Self::mock_service_name`] for disambiguation when multiple mocks run, and
/// [`Self::resources`] when a trait implementation must touch real infrastructure (produce a Kafka
/// message, query a seeded database, etc.) registered for the scenario. Use [`Self::resources`] only
/// when the mock’s behavior intentionally depends on those handles; otherwise prefer the
/// `resources = [...]` argument on [`#[shakedown::test]`](crate::test) for infrastructure and keep
/// the mock as pure HTTP.
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
