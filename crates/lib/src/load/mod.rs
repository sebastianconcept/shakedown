//! Open-model load profiles: phased arrival rates and worker settings (`load` feature, US-006+).
//!
//! Load [`Phase`] values describe traffic shape (constant, ramp, spike, pause, and closed-model VU phases).
//! [`LoadProfile`] is built with [`LoadProfile::open_model`] and validated by [`LoadProfileBuilder::build`].
//! [`LoadHandle`](handle::LoadHandle) schedules work from [`crate::lifecycle::Context::start_load`] (US-007).

pub(crate) mod handle;
mod metrics;

pub use handle::LoadHandle;
pub use metrics::LoadMetrics;

use std::time::Duration;

use displaydoc::Display;
use thiserror::Error;

/// Target iteration rate for open-model phases (iterations per second).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rate(f64);

impl Rate {
    /// Open-model arrival rate: `iterations_per_second` must be finite and strictly positive.
    pub fn per_second(iterations_per_second: f64) -> Self {
        Self(iterations_per_second)
    }

    /// Iterations per second (open-model phases).
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

/// One named or unnamed step in a [`LoadProfile`].
#[derive(Debug, Clone, PartialEq)]
pub struct NamedPhase {
    /// Optional label for `await_phase` and timeline correlation (US-007+).
    pub name: Option<String>,
    /// Phase definition.
    pub phase: Phase,
}

impl From<Phase> for NamedPhase {
    fn from(phase: Phase) -> Self {
        Self { name: None, phase }
    }
}

/// Traffic shape for a load profile (distinct from [`crate::phase::Phase`] lifecycle labels).
#[derive(Debug, Clone, PartialEq)]
pub enum Phase {
    /// Fixed open-model rate for a duration.
    Constant { duration: Duration, rate: Rate },
    /// Linear ramp between two open-model rates.
    Ramp {
        duration: Duration,
        from_rate: Rate,
        to_rate: Rate,
    },
    /// Heaviside-style burst at `peak_rate` for `duration`.
    Spike { duration: Duration, peak_rate: Rate },
    /// Zero traffic for `duration`.
    Pause { duration: Duration },
    /// Closed model: fixed virtual users for `duration`.
    ConstantVus { duration: Duration, count: usize },
    /// Closed model: ramp virtual users between `from` and `to` (inclusive endpoints, linear).
    RampVus {
        duration: Duration,
        from: usize,
        to: usize,
    },
}

impl Phase {
    /// Open model: hold `rate` for `duration`.
    pub fn constant(duration: Duration, rate: Rate) -> Self {
        Self::Constant { duration, rate }
    }

    /// Open model: ramp from `from_rate` to `to_rate` over `duration`.
    pub fn ramp(duration: Duration, from_rate: Rate, to_rate: Rate) -> Self {
        Self::Ramp {
            duration,
            from_rate,
            to_rate,
        }
    }

    /// Open model: spike at `peak_rate` for `duration`.
    pub fn spike(duration: Duration, peak_rate: Rate) -> Self {
        Self::Spike {
            duration,
            peak_rate,
        }
    }

    /// No scheduled iterations for `duration`.
    pub fn pause(duration: Duration) -> Self {
        Self::Pause { duration }
    }

    /// Closed model: `count` concurrent workers for `duration` (iteration starts when previous finishes).
    pub fn constant_vus(duration: Duration, count: usize) -> Self {
        Self::ConstantVus { duration, count }
    }

    /// Closed model: ramp concurrent workers from `from` to `to` over `duration`.
    pub fn ramp_vus(duration: Duration, from: usize, to: usize) -> Self {
        Self::RampVus { duration, from, to }
    }

    /// Attach an optional name for phase correlation (see [`NamedPhase`]).
    pub fn named(self, name: impl Into<String>) -> NamedPhase {
        NamedPhase {
            name: Some(name.into()),
            phase: self,
        }
    }

    fn validate(&self) -> Result<(), LoadError> {
        match self {
            Phase::Constant { rate, .. } => validate_rate(rate.as_f64()),
            Phase::Ramp {
                from_rate, to_rate, ..
            } => {
                validate_rate(from_rate.as_f64())?;
                validate_rate(to_rate.as_f64())
            }
            Phase::Spike { peak_rate, .. } => validate_rate(peak_rate.as_f64()),
            Phase::Pause { .. } => Ok(()),
            Phase::ConstantVus { count, .. } => {
                if *count == 0 {
                    Err(LoadError::InvalidVuCount { count: 0 })
                } else {
                    Ok(())
                }
            }
            Phase::RampVus { from, to, .. } => {
                if *from == 0 || *to == 0 {
                    Err(LoadError::InvalidVuRamp {
                        from: *from,
                        to: *to,
                    })
                } else {
                    Ok(())
                }
            }
        }
    }
}

fn validate_rate(rate: f64) -> Result<(), LoadError> {
    if rate.is_finite() && rate > 0.0 {
        Ok(())
    } else {
        Err(LoadError::InvalidRate { rate })
    }
}

/// When all workers are busy, how new iterations are handled (US-007+).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SaturationPolicy {
    /// Drop arrivals that cannot be assigned a worker immediately.
    #[default]
    Drop,
    /// Queue arrivals until a worker is free (bounded by harness; US-007).
    Queue,
    /// Treat saturation as an error (strict open-model).
    Error,
}

/// Tokio runtime affinity for the load driver (US-007+).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LoadRuntime {
    /// Use the scenario / test runtime.
    #[default]
    Shared,
    /// Dedicated threads for load workers.
    Dedicated {
        /// Thread count for the dedicated runtime.
        threads: usize,
    },
}

/// Validated load profile: phases, worker bounds, saturation, and runtime hint.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadProfile {
    phases: Vec<NamedPhase>,
    workers: usize,
    max_workers: usize,
    saturation_policy: SaturationPolicy,
    runtime: LoadRuntime,
}

impl LoadProfile {
    /// Start building an open-model profile (arrival-rate phases).
    pub fn open_model() -> LoadProfileBuilder {
        LoadProfileBuilder {
            phases: Vec::new(),
            workers: LoadProfileBuilder::DEFAULT_WORKERS,
            max_workers: LoadProfileBuilder::DEFAULT_MAX_WORKERS,
            saturation_policy: SaturationPolicy::default(),
            runtime: LoadRuntime::default(),
        }
    }

    /// Phases in order.
    pub fn phases(&self) -> &[NamedPhase] {
        &self.phases
    }

    /// Pre-allocated worker tasks (US-007).
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Upper bound on concurrent workers (US-007).
    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    /// Policy when no worker is available.
    pub fn saturation_policy(&self) -> SaturationPolicy {
        self.saturation_policy
    }

    /// Runtime preference for the load driver.
    pub fn runtime(&self) -> &LoadRuntime {
        &self.runtime
    }
}

/// Builder for [`LoadProfile`].
#[derive(Debug, Clone)]
pub struct LoadProfileBuilder {
    phases: Vec<NamedPhase>,
    workers: usize,
    max_workers: usize,
    saturation_policy: SaturationPolicy,
    runtime: LoadRuntime,
}

impl LoadProfileBuilder {
    const DEFAULT_WORKERS: usize = 1;
    const DEFAULT_MAX_WORKERS: usize = 1;

    /// Append a phase (unnamed unless you pass [`Phase::named`] / [`NamedPhase`]).
    pub fn phase(mut self, phase: impl Into<NamedPhase>) -> Self {
        self.phases.push(phase.into());
        self
    }

    /// Pre-allocated workers (must be > 0).
    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    /// Maximum concurrent workers (must be >= [`Self::workers`](LoadProfileBuilder::workers)).
    pub fn max_workers(mut self, max_workers: usize) -> Self {
        self.max_workers = max_workers;
        self
    }

    /// Behavior when all workers are busy (default: [`SaturationPolicy::Drop`]).
    pub fn on_saturation(mut self, policy: SaturationPolicy) -> Self {
        self.saturation_policy = policy;
        self
    }

    /// Hint for whether load runs on the shared runtime or a dedicated pool (US-007+).
    pub fn runtime(mut self, runtime: LoadRuntime) -> Self {
        self.runtime = runtime;
        self
    }

    /// Validate and produce a [`LoadProfile`].
    pub fn build(self) -> Result<LoadProfile, LoadError> {
        if self.phases.is_empty() {
            return Err(LoadError::NoPhases);
        }
        if self.workers == 0 {
            return Err(LoadError::InvalidWorkers {
                workers: self.workers,
            });
        }
        if self.max_workers < self.workers {
            return Err(LoadError::MaxWorkersLessThanWorkers {
                workers: self.workers,
                max_workers: self.max_workers,
            });
        }
        for named in &self.phases {
            named.phase.validate()?;
        }
        Ok(LoadProfile {
            phases: self.phases,
            workers: self.workers,
            max_workers: self.max_workers,
            saturation_policy: self.saturation_policy,
            runtime: self.runtime,
        })
    }
}

/// Invalid [`LoadProfile`] construction, load driver failures, or phase lookup errors.
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
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// load did not recover within {timeout:?} (rolling error rate stayed above threshold)
    RecoveryTimeout {
        /// Configured await timeout.
        timeout: Duration,
    },
}

/// Tunables for [`LoadHandle::await_recovery`](handle::LoadHandle::await_recovery) (US-009).
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Wall-clock budget for waiting until the rolling error rate is at or below [`Self::error_rate_threshold`].
    pub timeout: Duration,
    /// Target maximum error rate (failed / completed) over [`Self::rolling_window`], e.g. `0.0` for zero failures in-window.
    pub error_rate_threshold: f64,
    /// Rolling window for error-rate checks (and threshold-crossing alarms).
    pub rolling_window: Duration,
    /// Poll interval while waiting for recovery.
    pub poll_interval: Duration,
    /// Emit [`Event`] when rolling error rate crosses above this fraction (e.g. `0.10` for 10%).
    pub alert_threshold: f64,
}

impl RecoveryConfig {
    /// Default recovery wait: 1s rolling window, 50ms poll, 10% alert threshold crossings.
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            error_rate_threshold: 0.0,
            rolling_window: Duration::from_secs(1),
            poll_interval: Duration::from_millis(50),
            alert_threshold: 0.10,
        }
    }

    /// Set the recovery target error rate (default `0.0`).
    pub fn with_error_rate_threshold(mut self, error_rate_threshold: f64) -> Self {
        self.error_rate_threshold = error_rate_threshold;
        self
    }

    /// Set the rolling window used for error-rate checks (default 1s).
    pub fn with_rolling_window(mut self, rolling_window: Duration) -> Self {
        self.rolling_window = rolling_window;
        self
    }

    /// Set poll interval (default 50ms).
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Set alarm threshold for “error rate crossed above X%” events (default 10%).
    pub fn with_alert_threshold(mut self, alert_threshold: f64) -> Self {
        self.alert_threshold = alert_threshold;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_succeeds_for_valid_open_model_profile() {
        let profile = LoadProfile::open_model()
            .phase(Phase::ramp(
                Duration::from_secs(10),
                Rate::per_second(0.5),
                Rate::per_second(100.0),
            ))
            .phase(
                Phase::constant(Duration::from_secs(30), Rate::per_second(100.0)).named("baseline"),
            )
            .phase(Phase::constant(
                Duration::from_secs(60),
                Rate::per_second(100.0),
            ))
            .workers(50)
            .max_workers(200)
            .on_saturation(SaturationPolicy::Drop)
            .build()
            .expect("valid profile");

        assert_eq!(profile.phases().len(), 3);
        assert_eq!(profile.workers(), 50);
        assert_eq!(profile.max_workers(), 200);
        assert_eq!(profile.saturation_policy(), SaturationPolicy::Drop);
        assert!(matches!(profile.runtime(), LoadRuntime::Shared));
        assert_eq!(profile.phases()[1].name.as_deref(), Some("baseline"));
    }

    #[test]
    fn build_fails_without_phases() {
        let error = LoadProfile::open_model()
            .workers(2)
            .max_workers(2)
            .build()
            .expect_err("expected NoPhases");
        assert!(matches!(error, LoadError::NoPhases));
    }

    #[test]
    fn build_fails_when_workers_zero() {
        let error = LoadProfile::open_model()
            .phase(Phase::pause(Duration::from_secs(1)))
            .workers(0)
            .max_workers(1)
            .build()
            .expect_err("expected InvalidWorkers");
        assert!(matches!(error, LoadError::InvalidWorkers { workers: 0 }));
    }

    #[test]
    fn build_fails_when_max_workers_less_than_workers() {
        let error = LoadProfile::open_model()
            .phase(Phase::pause(Duration::from_secs(1)))
            .workers(10)
            .max_workers(5)
            .build()
            .expect_err("expected MaxWorkersLessThanWorkers");
        assert!(matches!(
            error,
            LoadError::MaxWorkersLessThanWorkers {
                workers: 10,
                max_workers: 5
            }
        ));
    }

    #[test]
    fn default_worker_bounds_are_one_and_valid() {
        let profile = LoadProfile::open_model()
            .phase(Phase::constant(
                Duration::from_secs(1),
                Rate::per_second(1.0),
            ))
            .build()
            .expect("valid");
        assert_eq!(profile.workers(), 1);
        assert_eq!(profile.max_workers(), 1);
    }

    #[test]
    fn saturation_queue_and_error_round_trip() {
        let queue = LoadProfile::open_model()
            .phase(Phase::pause(Duration::from_millis(1)))
            .on_saturation(SaturationPolicy::Queue)
            .build()
            .unwrap();
        assert_eq!(queue.saturation_policy(), SaturationPolicy::Queue);

        let error_policy = LoadProfile::open_model()
            .phase(Phase::pause(Duration::from_millis(1)))
            .on_saturation(SaturationPolicy::Error)
            .build()
            .unwrap();
        assert_eq!(error_policy.saturation_policy(), SaturationPolicy::Error);
    }

    #[test]
    fn closed_model_phases_validate_positive_vus() {
        let ok = LoadProfile::open_model()
            .phase(Phase::constant_vus(Duration::from_secs(1), 5))
            .phase(Phase::ramp_vus(Duration::from_secs(2), 2, 8))
            .build()
            .expect("valid VU phases");
        assert_eq!(ok.phases().len(), 2);

        let vu_zero = LoadProfile::open_model()
            .phase(Phase::constant_vus(Duration::from_secs(1), 0))
            .build()
            .expect_err("invalid vu");
        assert!(matches!(vu_zero, LoadError::InvalidVuCount { count: 0 }));

        let vu_ramp_zero = LoadProfile::open_model()
            .phase(Phase::ramp_vus(Duration::from_secs(1), 0, 5))
            .build()
            .expect_err("invalid ramp");
        assert!(matches!(
            vu_ramp_zero,
            LoadError::InvalidVuRamp { from: 0, to: 5 }
        ));
    }

    #[test]
    fn hdrhistogram_crate_is_linked_for_load_feature() {
        let mut histogram = hdrhistogram::Histogram::<u64>::new(3).expect("histogram");
        histogram.record(42).expect("record");
    }
}
