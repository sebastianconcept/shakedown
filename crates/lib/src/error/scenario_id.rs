//! [`ScenarioId`] — identifier for one scenario run.

/// Identifier for one scenario run, carried through provision and teardown for isolation.
///
/// Why a newtype: distinguishes scenario ids from other strings (logs, resource names) and keeps
/// isolation keys (Kafka prefixes, DB names) in one place.
///
/// How: set by [`crate::lifecycle::run_scenario`] from the scenario name; read via
/// [`crate::lifecycle::Context::scenario_id`] inside the scenario body and in
/// [`crate::resources::Resource::provision`].
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ScenarioId(String);

impl std::fmt::Display for ScenarioId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ScenarioId {
    /// Construct an id from any string-like value (typically the Rust test function name).
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Empty id before the harness assigns a scenario name.
    pub fn empty() -> Self {
        Self(String::new())
    }

    /// View the id as `str` for formatting and isolation helpers.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
