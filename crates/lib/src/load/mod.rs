//! Load and soak testing: phased arrival rates, metrics, assertions, and baselines (`load` feature).
//!
//! **Why:** correctness tests prove a service behaves correctly for one request; load tests prove
//! it behaves correctly under sustained traffic. Shakedown reuses the same container lifecycle,
//! resource provisioning, and failure timeline for load scenarios so you do not need a separate
//! tool or separate infrastructure setup.
//!
//! **How:** build a [`types::LoadProfile`] with [`types::LoadProfile::open_model`], register it
//! via [`crate::lifecycle::Context::start_load`] to receive a [`handle::LoadHandle`], then drive
//! requests inside the load body. After the run, chain [`LoadMetricsAssertBuilder`] calls to
//! express SLO budgets (p99 latency, error rate, throughput). Persist baselines with
//! [`LoadBaseline`] for CI regression gating across PRs.
//!
//! [`types::Phase::warmup`] runs open-model traffic before steady-state phases and automatically
//! excludes cold-start latency from steady-state statistics.
//!
//! # Book reference
//!
//! Part VI, *Chapters 24–28: Load, Soak, and Resilience* — walks through building a
//! `LoadProfile`, interpreting `LoadRunReport` JSON artifacts, asserting SLOs with
//! `LoadMetricsAssertBuilder`, and comparing runs against committed baselines.

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
