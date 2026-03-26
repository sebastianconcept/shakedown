//! # Shakedown — Orchestration library for shakedown testing
//!
//! **Shakedown** is a Rust framework for shakedown testing — proving systems are safe to
//! promote to production by running realistic scenarios that exercise the full stack.
//!
//! ## Overview
//!
//! This library provides:
//!
//! - **Scenario orchestration** — lifecycle phases: provision → readiness → seed → run → assert → teardown
//! - **Resource providers** — traits for integrating databases, message queues, and other infrastructure
//! - **Error classification** — structured errors distinguishing scenario vs infrastructure failures
//! - **Event logging hooks** — structured event collection for failure timelines
//!
//! ## Async Resource Trait (D-6)
//!
//! The [`Resource`] trait is now **async and scenario-aware**:
//!
//! ```rust,ignore
//! #[async_trait]
//! pub trait Resource: Send + Sync {
//!     async fn provision(&mut self, scenario_id: &ScenarioId) -> Result<Vec<ResourceHandle>, InfrastructureError>;
//!     async fn check_ready(&self) -> Result<(), InfrastructureError>;
//!     async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError>;
//!     async fn teardown(&mut self) -> Result<(), InfrastructureError>;
//! }
//! ```
//!
//! This enables real infrastructure (Kafka, Postgres, etc.) to be integrated while maintaining
//! a clean extension point for custom resources.
//!
//! ## Workspace layout
//!
//! ```text
//! shakedown/
//! ├── crates/
//! │   ├── lib/          # this crate: orchestration engine
//! │   ├── macros/       # procedural macros (shakedown-macros)
//! │   └── cli/          # CLI binary (shakedown-cli)
//! ```
//!
//! ## Extension points
//!
//! - **[`Resource`] trait** — implement for new infrastructure integrations (Kafka, Postgres, etc.)
//! - **[`EnvironmentBackend`] trait** — implement for alternative execution environments
//! - **`Context::register_builtin()` / `Context::register_extension()`** — declarative registration
//!
//! ## Milestone status
//!
//! - **Current**: `mvp-macros-system-test` — `#[shakedown::test]` / `#[shakedown::faulty_test]`, optional `resources = [...]`, trybuild UI tests in `shakedown-macros`
//! - **In progress**: `mvp-postgres-mariadb` — PostgreSQL `Resource` + `PostgresBindings` (`postgres` feature); MariaDB/MySQL/SQLite next
//!
//! ## See also
//!
//! Product docs (PRD, SPECS, TECH-STACK) live in the adopted documentation root; see the workspace
//! [`docs/README.md`](../../docs/README.md).
//!
//! ## Example
//!
//! ```ignore
//! use shakedown::Context;
//!
//! #[async_trait]
//! impl Resource for MyResource {
//!     async fn provision(&mut self, scenario_id: &ScenarioId) -> Result<Vec<ResourceHandle>, InfrastructureError> {
//!         // Provision my resource with scenario isolation
//!         Ok(vec![ResourceHandle { ... }])
//!     }
//!
//!     async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
//!         // Apply migrations, seed data, etc.
//!         Ok(())
//!     }
//!
//!     async fn check_ready(&self) -> Result<(), InfrastructureError> {
//!         // Verify deep readiness beyond TCP
//!         Ok(())
//!     }
//!
//!     async fn teardown(&mut self) -> Result<(), InfrastructureError> {
//!         // Clean up
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::test]
//! async fn test_my_scenario() {
//!     let mut ctx = Context::new();
//!     ctx.register_extension(MyResource);
//!
//!     run_scenario(&mut ctx, "my-scenario", |c| Box::pin(my_scenario_logic(c)), &mut event_log)
//!         .await
//!         .unwrap();
//! }
//! ```

/// Structured errors: scenario vs infrastructure vs internal framework failures.
pub mod error;
/// Structured events and in-memory logs for timelines and diagnostics.
pub mod event;
/// Scenario [`lifecycle::Context`], lifecycle phases, and [`lifecycle::run_scenario`].
pub mod lifecycle;
/// [`resources::Resource`] implementations, registration, and Kafka bindings.
pub mod resources;

#[doc(inline)]
pub use crate::error::{
    InfrastructureError, InternalError, ResourceHandle, Result, ScenarioError, ScenarioId,
};
pub use crate::event::{Event, EventLevel, EventLog, EventSnapshot};
/// Public types and functions from the lifecycle module.
pub use crate::lifecycle::{
    run_scenario, Context, EnvironmentBackend, LifecycleState, NoopEnvironmentBackend, ScenarioKind,
};

/// First-party resource implementations.
#[doc(inline)]
pub use crate::resources::{
    BuiltinResource, FakeResource, KafkaBindings, RegisteredResource, Resource,
};

#[cfg(feature = "postgres")]
pub use crate::resources::{PostgresBindings, PostgresResource};

#[cfg(feature = "kafka")]
pub use crate::resources::KafkaResource;

/// Procedural attributes for Tokio tests that run the full shakedown lifecycle.
///
/// Re-exported so callers depend only on `shakedown`. The macro crate does not depend on this
/// library (avoids cycles); generated code refers to `::shakedown::…` resolved at the call site.
pub use shakedown_macros::{faulty_test, test};

#[cfg(test)]
mod tests {
    use crate::error::{InfrastructureError, InternalError, ResourceHandle, ScenarioError};
    use crate::event::{EventLevel, EventLog};
    use crate::lifecycle::{Context, NoopEnvironmentBackend};
    use crate::resources::{
        BuiltinResource, FakeResource, KafkaBindings, RegisteredResource, Resource,
    };

    #[test]
    fn test_context_and_types() {
        // Given a new scenario context and noop environment backend
        let _ctx = Context::new();
        let _backend = NoopEnvironmentBackend {};
        // When we exercise the public surface (event log, Kafka bindings, scenario kind, resources)
        let mut event_log = EventLog::new();
        event_log.append_raw(EventLevel::Info, "test", "test message");
        let bindings = KafkaBindings::new("localhost:9092", "prefix_");
        use crate::lifecycle::ScenarioKind;
        let scenario_kind = ScenarioKind::Normal;
        let builtin = RegisteredResource::Builtin(BuiltinResource::Fake(FakeResource::default()));
        let extension = RegisteredResource::Extension(Box::new(FakeResource::default()));
        // Then core types behave as expected for a smoke test
        assert_eq!(event_log.len(), 1);
        assert_eq!(bindings.bootstrap_servers, "localhost:9092");
        assert_eq!(scenario_kind.to_string(), "Normal");
        assert!(matches!(builtin, RegisteredResource::Builtin(_)));
        assert!(matches!(extension, RegisteredResource::Extension(_)));
        assert_eq!(builtin, builtin);
        assert_ne!(builtin, extension);
    }

    #[test]
    fn test_error_types_are_send_sync() {
        fn check_send_sync<T: Send + Sync>(_: T) {}

        // Given one value of each Shakedown error category
        let _scenario_err = ScenarioError::Assertion("test".to_string());
        let _infra_err = InfrastructureError::Provision {
            resource: "test".to_string(),
            source: Box::new(std::io::Error::other("test")),
        };
        let _internal_err = InternalError::Bug("test".to_string());
        // When we require Send + Sync for each
        // Then the compiler accepts all three
        check_send_sync(_scenario_err);
        check_send_sync(_infra_err);
        check_send_sync(_internal_err);
    }

    #[test]
    fn test_resource_trait_is_send_sync() {
        // Given a trivial Resource implementation used only for trait-object checks
        use crate::resources::Resource;
        use async_trait::async_trait;

        struct TestResource;

        #[async_trait]
        impl Resource for TestResource {
            async fn provision(
                &mut self,
                _scenario_id: &crate::error::ScenarioId,
            ) -> std::result::Result<Vec<ResourceHandle>, InfrastructureError> {
                Ok(vec![ResourceHandle {
                    id: "test".to_string(),
                    connection_string: "://test".to_string(),
                    metadata: None,
                }])
            }

            async fn check_ready(&self) -> std::result::Result<(), InfrastructureError> {
                Ok(())
            }

            async fn apply_default_seed(&mut self) -> std::result::Result<(), InfrastructureError> {
                Ok(())
            }

            async fn teardown(&mut self) -> std::result::Result<(), InfrastructureError> {
                Ok(())
            }
        }

        // Given a minimal Resource implementation
        // When we store it behind Box<dyn Resource>
        let _boxed: Box<dyn Resource> = Box::new(TestResource);
        // Then it compiles and can be dropped
        drop(_boxed);
    }

    #[test]
    fn test_lifecycle_awaits_async_methods() {
        // Given a Resource whose async hooks yield so lifecycle truly awaits them
        use async_trait::async_trait;

        struct AsyncTestResource;

        #[async_trait]
        impl Resource for AsyncTestResource {
            async fn provision(
                &mut self,
                _scenario_id: &crate::error::ScenarioId,
            ) -> std::result::Result<Vec<ResourceHandle>, InfrastructureError> {
                tokio::task::yield_now().await;
                Ok(vec![ResourceHandle {
                    id: "async_test".to_string(),
                    connection_string: "://async_test".to_string(),
                    metadata: None,
                }])
            }

            async fn check_ready(&self) -> std::result::Result<(), InfrastructureError> {
                tokio::task::yield_now().await;
                Ok(())
            }

            async fn apply_default_seed(&mut self) -> std::result::Result<(), InfrastructureError> {
                Ok(())
            }

            async fn teardown(&mut self) -> std::result::Result<(), InfrastructureError> {
                Ok(())
            }
        }

        // Given a Resource whose async methods yield to the runtime
        // When we require it to implement Resource
        fn assert_resource<T: Resource>() {}
        // Then the compiler accepts AsyncTestResource
        assert_resource::<AsyncTestResource>();
    }

    #[test]
    fn test_context_scenario_kind() {
        // Given a fresh context defaulting to Normal
        let mut ctx = Context::new();
        assert_eq!(ctx.scenario_kind(), crate::lifecycle::ScenarioKind::Normal);
        // When we mark the scenario as Faulty
        ctx.set_scenario_kind(crate::lifecycle::ScenarioKind::Faulty);
        // Then the context reports Faulty
        assert_eq!(ctx.scenario_kind(), crate::lifecycle::ScenarioKind::Faulty);
    }
}
