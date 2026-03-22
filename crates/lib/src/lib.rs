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
//! - **ResourceProvider** trait — implement for new infrastructure integrations (Kafka, Postgres, etc.)
//! - **EnvironmentBackend** trait — implement for alternative execution environments
//! - **register_resource!** macro — register resources declaratively
//!
//! ## Milestone status
//!
//! - **Current**: `mvp-foundation` — orchestration skeleton, error types, extension seams
//! - **Next**: `mvp-kafka-vertical` — real resources (Kafka, Postgres), full API
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
//! # #[shakedown_macros::register_resource]
//! # struct FakeResource;
//! #
//! # impl shakedown::Resource for FakeResource {
//! #     fn provision(_ctx: &mut Context) -> Self { FakeResource }
//! #     fn apply_default_seed(&self) {}
//! #     fn check_ready(&self) -> bool { true }
//! #     fn teardown(self) {}
//! # }
//!
//! #[shakedown_macros::system_test]
//! async fn my_scenario(_ctx: Context) {
//!     // Scenario implementation
//! }
//! ```

pub mod error;
pub mod event;
pub mod lifecycle;
pub mod resources;

#[doc(inline)]
pub use crate::error::{InfrastructureError, InternalError, Result, ScenarioError, ShakedownError};
pub use crate::event::{Event, EventLevel, EventLog, EventSnapshot};
pub use crate::lifecycle::{
    run_scenario, Context, EnvironmentBackend, LifecycleState, NoopEnvironmentBackend, Phase,
};
pub use crate::resources::{
    CloneResource, FakeResource, Resource, ResourceBinding, ResourceConfig,
};

/// Returns the compile-time default greeting emitted by the `macros` crate.
pub fn greeting() -> &'static str {
    shakedown_macros::default_greeting!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greeting_matches_expected() {
        // Given the default greeting macro installed for this crate
        // When we read the greeting string
        let message = greeting();
        // Then it is the expected phrase
        assert_eq!(message, "Hello, shakedown!");
    }

    #[test]
    fn error_hierarchy_compiles() {
        // Verify all error types are accessible
        let _scenario: ScenarioError = ScenarioError::Assertion("test".to_string());
        let _infra: InfrastructureError = InfrastructureError::Provision {
            resource: "test".to_string(),
            source: Box::new(std::io::Error::other("test")),
        };
        let _internal: InternalError = InternalError::Bug("test".to_string());
    }

    #[test]
    fn test_context_and_types() {
        // Verify Context and other core types are accessible
        let mut ctx = Context::new();
        let _backend = NoopEnvironmentBackend {};

        // Verify Resource trait is accessible
        struct TestResource;
        impl Resource for TestResource {
            fn provision(_ctx: &mut Context) -> Self {
                TestResource
            }

            fn apply_default_seed(&self) {}

            fn check_ready(&self) -> bool {
                true
            }

            fn teardown(self) {}
        }

        let _resource = TestResource::provision(&mut ctx);

        // Verify EventLog is accessible
        let mut event_log = crate::EventLog::new();
        event_log.append_raw(EventLevel::Info, "test", "test message");
        assert_eq!(event_log.len(), 1);
    }
}
