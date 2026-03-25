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
//! - **Next**: infrastructure milestones (e.g. Postgres/MariaDB) per product roadmap
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

pub mod error;
pub mod event;
pub mod lifecycle;
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

#[cfg(feature = "kafka")]
pub use crate::resources::KafkaResource;

// Re-export proc macros so users can write #[shakedown::test] directly on their code.
// Note: shakedown-macros does NOT depend on shakedown (would be circular); it emits
// `::shakedown::*` paths in generated code, which are resolved at the call site.
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
        let _ctx = Context::new();
        let _backend = NoopEnvironmentBackend {};

        let mut event_log = EventLog::new();
        event_log.append_raw(EventLevel::Info, "test", "test message");
        assert_eq!(event_log.len(), 1);

        let bindings = KafkaBindings::new("localhost:9092", "prefix_");
        assert_eq!(bindings.bootstrap_servers, "localhost:9092");

        // ScenarioKind enum distinguishes Normal vs Faulty scenarios
        use crate::lifecycle::ScenarioKind;
        let scenario_kind = ScenarioKind::Normal;
        assert_eq!(scenario_kind.to_string(), "Normal");

        let builtin = RegisteredResource::Builtin(BuiltinResource::Fake(FakeResource::default()));
        let extension = RegisteredResource::Extension(Box::new(FakeResource::default()));

        assert!(matches!(builtin, RegisteredResource::Builtin(_)));
        assert!(matches!(extension, RegisteredResource::Extension(_)));

        assert_eq!(builtin, builtin);
        assert_ne!(builtin, extension);
    }

    #[test]
    fn test_error_types_are_send_sync() {
        fn check_send_sync<T: Send + Sync>(_: T) {}

        let _scenario_err = ScenarioError::Assertion("test".to_string());
        let _infra_err = InfrastructureError::Provision {
            resource: "test".to_string(),
            source: Box::new(std::io::Error::other("test")),
        };
        let _internal_err = InternalError::Bug("test".to_string());

        check_send_sync(_scenario_err);
        check_send_sync(_infra_err);
        check_send_sync(_internal_err);
    }

    #[test]
    fn test_resource_trait_is_send_sync() {
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

        let _boxed: Box<dyn Resource> = Box::new(TestResource);
        drop(_boxed);
    }

    #[test]
    fn test_lifecycle_awaits_async_methods() {
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

        fn assert_resource<T: Resource>() {}
        assert_resource::<AsyncTestResource>();
    }

    #[test]
    fn test_context_scenario_kind() {
        let mut ctx = Context::new();
        assert_eq!(ctx.scenario_kind(), crate::lifecycle::ScenarioKind::Normal);

        ctx.set_scenario_kind(crate::lifecycle::ScenarioKind::Faulty);
        assert_eq!(ctx.scenario_kind(), crate::lifecycle::ScenarioKind::Faulty);
    }
}
