//! Traffic shape, worker bounds, and runtime hints for [`LoadProfile`].
//!
//! Phases describe open-model arrivals ([`Rate`], ramps, spikes) and closed-model virtual users.
//! [`LoadProfileBuilder::build`] enforces warmup-at-most-once-and-first and positive rates.

use std::time::Duration;

use crate::load::error::LoadError;

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
    /// Optional label for `await_phase` and timeline correlation.
    pub name: Option<String>,
    /// Phase definition.
    pub phase: Phase,
}

impl From<Phase> for NamedPhase {
    fn from(phase: Phase) -> Self {
        Self { name: None, phase }
    }
}

/// Traffic shape for a load profile (distinct from [`crate::lifecycle::Phase`] lifecycle labels).
#[derive(Debug, Clone, PartialEq)]
pub enum Phase {
    /// Open-model arrivals at `rate` for `duration` before steady-state phases; must be first and at most once in a profile.
    Warmup { duration: Duration, rate: Rate },
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
    /// Open model: warmup traffic at `rate` for `duration` (must be the first phase; at most one per profile).
    pub fn warmup(duration: Duration, rate: Rate) -> Self {
        Self::Warmup { duration, rate }
    }

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
            Phase::Warmup { duration, rate } => {
                if duration.is_zero() {
                    return Err(LoadError::WarmupDurationInvalid);
                }
                validate_rate(rate.as_f64())
            }
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

/// When all workers are busy, how new iterations are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SaturationPolicy {
    /// Drop arrivals that cannot be assigned a worker immediately.
    #[default]
    Drop,
    /// Queue arrivals until a worker is free (bounded by the harness queue).
    Queue,
    /// Treat saturation as an error (strict open-model).
    Error,
}

/// Tokio runtime affinity for the load driver.
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
#[cfg_attr(docsrs, doc(cfg(feature = "load")))]
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

    /// Pre-allocated worker tasks.
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Upper bound on concurrent workers.
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

    /// Hint for whether load runs on the shared runtime or a dedicated pool.
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
        let mut warmup_phase_count = 0usize;
        let mut first_warmup_index: Option<usize> = None;
        for (index, named) in self.phases.iter().enumerate() {
            if matches!(named.phase, Phase::Warmup { .. }) {
                warmup_phase_count += 1;
                if first_warmup_index.is_none() {
                    first_warmup_index = Some(index);
                }
            }
        }
        if warmup_phase_count > 1 {
            return Err(LoadError::MultipleWarmups);
        }
        if warmup_phase_count == 1 && first_warmup_index != Some(0) {
            return Err(LoadError::WarmupNotFirst);
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

/// Tunables for [`LoadHandle::await_recovery`](crate::load::LoadHandle::await_recovery).
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
    /// Emit [`crate::event::Event`] when rolling error rate crosses above this fraction (e.g. `0.10` for 10%).
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

/// Compact, stable summary of a [`LoadProfile`] for reports and baselines.
pub(crate) fn summarize_load_profile(profile: &LoadProfile) -> String {
    let mut parts = Vec::new();
    for (index, named) in profile.phases().iter().enumerate() {
        let name = if let Some(phase_name) = named.name.as_deref() {
            phase_name.to_string()
        } else {
            format!("#{index}")
        };
        let phase = match &named.phase {
            Phase::Warmup { duration, rate } => {
                format!("Warmup({duration:?}, {} it/s)", rate.as_f64())
            }
            Phase::Constant { duration, rate } => {
                format!("Constant({duration:?}, {} it/s)", rate.as_f64())
            }
            Phase::Ramp {
                duration,
                from_rate,
                to_rate,
            } => format!(
                "Ramp({duration:?}, {} → {} it/s)",
                from_rate.as_f64(),
                to_rate.as_f64()
            ),
            Phase::Spike {
                duration,
                peak_rate,
            } => format!("Spike({duration:?}, {} it/s)", peak_rate.as_f64()),
            Phase::Pause { duration } => format!("Pause({duration:?})"),
            Phase::ConstantVus { duration, count } => {
                format!("ConstantVus({duration:?}, {count} vu)")
            }
            Phase::RampVus { duration, from, to } => {
                format!("RampVus({duration:?}, {from} → {to} vu)")
            }
        };
        parts.push(format!("{name}: {phase}"));
    }
    parts.join(" | ")
}

#[cfg(test)]
mod tests;
