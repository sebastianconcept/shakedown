//! Scenario lifecycle types and execution.
//!
//! This module is the ordered harness that provisions infrastructure, seeds data, runs a
//! scenario body, and tears resources down. This module is useful when you change phase order,
//! failure classification, or the public [`Context`] / [`run_scenario`] surface.
//! This module classifies a failure as a scenario error or an infrastructure error so the caller
//! knows which side to fix.

#[cfg(any(
    feature = "mongodb-cluster",
    feature = "kafka-cluster",
    feature = "redis-sentinel",
    feature = "postgres-cluster",
    feature = "rabbitmq-cluster",
))]
mod cluster_registration;
mod context;
mod environment;
#[cfg(feature = "fault-injection")]
mod fault_proxy;
mod phase;
mod resource_bindings;
mod run;
mod run_diagnostics;
mod teardown;

#[cfg(test)]
mod tests;

pub use context::Context;
pub use environment::{
    EnvironmentBackend, LifecycleState, NoopEnvironmentBackend, ScenarioKind,
    DEFAULT_RESOURCE_BINDING_NAME,
};
pub use phase::Phase;
pub use run::{new_context_with_fake_resource, run_scenario, ScenarioRunOutcome};
