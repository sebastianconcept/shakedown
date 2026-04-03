//! Open-model load profiles: phased arrival rates and worker settings (`load` feature).
//!
//! Load [`Phase`](types::Phase) values describe traffic shape (constant, ramp, spike, pause, and closed-model VU phases).
//! [`LoadProfile`](types::LoadProfile) is built with [`LoadProfile::open_model`](types::LoadProfile::open_model) and validated by [`LoadProfileBuilder::build`](types::LoadProfileBuilder::build).
//! [`LoadHandle`](handle::LoadHandle) schedules work from [`crate::lifecycle::Context::start_load`].
//! [`Phase::warmup`](types::Phase::warmup) runs as open-model traffic before steady-state phases; the driver records
//! `LoadMetricsAccumulator::set_warmup_end` when warmup completes.

pub(crate) mod error;
pub(crate) mod types;

mod assertions;
mod baseline;
pub(crate) mod fmt;
pub(crate) mod handle;
mod metrics;
mod report;
mod summary;

pub use assertions::LoadMetricsAssertBuilder;
pub use baseline::{
    compare_load_metrics_to_baseline, render_comparison_markdown, ComparisonResult, LoadBaseline,
    MetricComparison, Tolerance,
};
pub use error::LoadError;
pub use handle::LoadHandle;
pub use metrics::LoadMetrics;
pub use report::{LoadRunReport, ReportMetrics};
pub use summary::{render_summary, render_summary_with_comparison, render_summary_with_verdicts};
pub use types::{
    AssertionVerdict, LoadProfile, LoadProfileBuilder, LoadRuntime, NamedPhase, Phase, Rate,
    RecoveryConfig, ReportAssertion, ReportComparison, ReportComparisonMetric, SaturationPolicy,
};

pub(crate) use types::summarize_load_profile;
