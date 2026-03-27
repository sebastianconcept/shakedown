//! Lifecycle phase labels shared by the harness and infrastructure event records.

use serde::{Deserialize, Serialize};

/// Ordered phase of [`crate::lifecycle::run_scenario`] (provision through teardown).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Provision resources
    Provision,
    /// Check resource readiness
    Readiness,
    /// Seed data into resources
    Seed,
    /// Run the scenario logic
    Run,
    /// Assert scenario outcomes
    Assert,
    /// Teardown resources
    Teardown,
}
