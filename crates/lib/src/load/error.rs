//! Errors from load profile validation and the load driver (`LoadHandle`).
//!
//! These are distinct from [`crate::ShakedownError`]: they cover configuration mistakes
//! (invalid rates, warmup placement) and runtime driver outcomes (phase not found, saturation).

use std::time::Duration;

use displaydoc::Display;
use thiserror::Error;

/// Invalid [`super::types::LoadProfile`] construction, load driver failures, or phase lookup errors.
#[derive(Debug, Display, Error)]
pub enum LoadError {
    /// load profile must contain at least one phase
    NoPhases,
    /// workers must be greater than zero (got {workers})
    InvalidWorkers {
        /// Configured worker count.
        workers: usize,
    },
    /// max_workers must be >= workers (workers={workers}, max_workers={max_workers})
    MaxWorkersLessThanWorkers {
        /// Configured baseline workers.
        workers: usize,
        /// Configured worker ceiling.
        max_workers: usize,
    },
    /// open-model rate must be finite and strictly positive (got {rate})
    InvalidRate {
        /// Invalid rate value.
        rate: f64,
    },
    /// closed-model VU count must be greater than zero
    InvalidVuCount {
        /// Invalid zero count.
        count: usize,
    },
    /// closed-model VU ramp endpoints must be greater than zero (from={from}, to={to})
    InvalidVuRamp {
        /// Ramp start (virtual users).
        from: usize,
        /// Ramp end (virtual users).
        to: usize,
    },
    /// warmup phase duration must be greater than zero
    WarmupDurationInvalid,
    /// load profile may contain at most one warmup phase
    MultipleWarmups,
    /// warmup phase must be the first phase in the profile
    WarmupNotFirst,
    /// dedicated load runtime is not implemented yet (use LoadRuntime::Shared)
    UnsupportedRuntime,
    /// load phase is not implemented in this milestone: {description}
    UnsupportedPhase {
        /// Static description for diagnostics.
        description: &'static str,
    },
    /// no load phase matches name `{name}`
    PhaseNotFound {
        /// Requested phase label.
        name: String,
    },
    /// load driver stopped before the awaited phase started
    DriverStopped,
    /// open-model saturation policy was Error and all workers were busy
    SaturationExceeded,
    /// load driver failed
    DriverFailed {
        /// Underlying driver or runtime error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// load did not recover within {timeout:?} (rolling error rate stayed above threshold)
    RecoveryTimeout {
        /// Configured await timeout.
        timeout: Duration,
    },
}
