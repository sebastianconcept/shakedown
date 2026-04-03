//! Shared load-facing types: profile / phase graph, saturation and runtime hints, and report assertion DTOs.
//!
//! - [`profile`] — [`LoadProfile`], [`Phase`], [`Rate`], worker and warmup rules.
//! - [`verdict`] — in-memory and JSON shapes for assertions and baseline comparison rows.

mod profile;
mod verdict;

pub(crate) use profile::summarize_load_profile;
pub use profile::{
    LoadProfile, LoadProfileBuilder, LoadRuntime, NamedPhase, Phase, Rate, RecoveryConfig,
    SaturationPolicy,
};
pub use verdict::{AssertionVerdict, ReportAssertion, ReportComparison, ReportComparisonMetric};
