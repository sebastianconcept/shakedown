//! Scenario lifecycle types and execution.
//!
//! Integration tests need a reproducible, phase-ordered harness that provisions real
//! infrastructure, seeds data, runs the scenario body, and tears everything down — even when
//! the scenario panics or an infrastructure step fails.
//!
//! Call [`run_scenario`] (or use `#[shakedown::test]`) passing a [`Context`] with
//! registered resources. The harness drives provision, readiness, seed, the scenario body,
//! and teardown, and classifies any failure as a [`crate::error::ScenarioError`] (your
//! logic or assertions) or an [`crate::error::InfrastructureError`] (provision or readiness).
//!
//! Outcome checks belong in the scenario body. After that body returns, the assert phase
//! records the outcome on the timeline — success, or the scenario error — and then teardown
//! runs. Infrastructure failures that return before the body stay infrastructure failures.
//!
//! ## Phase sequence
//!
//! 1. **Provision** — start containers or allocate logical namespaces
//! 2. **Readiness** — block until each resource reports healthy (no fixed sleeps)
//! 3. **Seed** — apply project-default data, then scenario-specific overrides
//! 4. **Run** — execute the scenario body with a [`Context`] reference
//! 5. **Assert** — record the scenario body's outcome on the timeline; the body already evaluated outcomes
//! 6. **Teardown** — stop containers and drop namespaces (always attempted)
//!
//! ## Event logging
//!
//! The lifecycle emits structured events during each phase. These events are collected in an
//! [`crate::event::EventLog`] that can be retrieved for debugging or failure analysis.
//!
//! # Example
//!
//! ```rust,no_run
//! use shakedown::{run_scenario, Context, EventLog};
//!
//! async fn my_scenario(ctx: &mut Context) {
//!     // Scenario implementation
//! }
//!
//! #[tokio::test]
//! async fn test_scenario_with_events() {
//!     let mut ctx = Context::new();
//!     let mut event_log = EventLog::new();
//!
//!     run_scenario(&mut ctx, "test-scenario", |c| Box::pin(my_scenario(c)), &mut event_log)
//!         .await
//!         .unwrap();
//!
//!     assert!(event_log
//!         .events()
//!         .any(|e| e.source == "scenario" && e.level == shakedown::EventLevel::Info));
//! }
//! ```

mod context;
mod environment;
mod phase;
mod run;

pub use context::{new_context_with_fake_resource, Context, DEFAULT_RESOURCE_BINDING_NAME};
pub use environment::{EnvironmentBackend, LifecycleState, NoopEnvironmentBackend, ScenarioKind};
pub use phase::Phase;
pub use run::{run_scenario, ScenarioRunOutcome};

#[cfg(test)]
mod tests;
