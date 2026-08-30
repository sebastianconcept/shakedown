//! Lifecycle phase labels shared by Shakedown and infrastructure event records.
//!
//! This module is useful when you add a lifecycle phase or stamp a new infra event with the active
//! step.
//! Infrastructure [`crate::event::InfraEvent`] records carry a [`Phase`] so logs, Docker events,
//! and resource samples sort and filter correctly in timelines.

use serde::{Deserialize, Serialize};

/// Ordered phase of [`crate::lifecycle::run_scenario`] (provision through teardown) so Shakedown can
/// correlate infrastructure events with the active lifecycle step.
///
/// Stamped on diagnostics output when available.
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
