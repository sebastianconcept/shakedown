//! Driver-side recovery budget: how long a run may keep its rolling error rate above target.
//!
//! A profile opts in with [`LoadProfileBuilder::recovery`](crate::load::LoadProfileBuilder::recovery).
//! Without it the budget is never enforced and [`RecoveryWatch`] only carries the rolling window
//! and alert threshold used for threshold-crossing events.
//!
//! The driver task owns one [`RecoveryWatch`] for the whole run and passes it by `&mut` to the
//! phase runners, so the episode clock persists across phases without any lock.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::load::metrics::{LoadMetrics, LoadMetricsAccumulator};
use crate::load::RecoveryConfig;

/// Lower bound for how often the rolling error rate is re-evaluated.
const MINIMUM_EVALUATION_INTERVAL: Duration = Duration::from_millis(1);

/// What is left of the recovery budget at the moment [`RecoveryWatch::status`] was asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveryBudget {
    /// The budget is not enforced, or the rolling error rate is at or below the target.
    WithinTarget,
    /// The error rate is above target, with this much of the timeout left.
    Remaining(Duration),
    /// The error rate stayed above target for the whole timeout.
    Exhausted,
}

/// Tracks the current stretch of time the rolling error rate has been above target.
///
/// One continuous stretch is an episode. It starts when an evaluation finds the rate above
/// [`RecoveryConfig::error_rate_threshold`], ends when an evaluation finds it at or below, and is
/// [`RecoveryBudget::Exhausted`] once it lasts [`RecoveryConfig::timeout`]. Evaluations are
/// throttled to [`RecoveryConfig::poll_interval`] because reading the rolling window is far more
/// expensive than scheduling one arrival.
pub(super) struct RecoveryWatch {
    configuration: RecoveryConfig,
    enforced: bool,
    above_threshold_since: Option<Instant>,
    last_evaluated_at: Option<Instant>,
}

impl RecoveryWatch {
    /// Watch for a profile budget, or only supply alert parameters when `budget` is `None`.
    pub(super) fn new(budget: Option<&RecoveryConfig>) -> Self {
        match budget {
            Some(configuration) => Self {
                configuration: configuration.clone(),
                enforced: true,
                above_threshold_since: None,
                last_evaluated_at: None,
            },
            None => Self {
                configuration: RecoveryConfig::new(Duration::ZERO),
                enforced: false,
                above_threshold_since: None,
                last_evaluated_at: None,
            },
        }
    }

    /// Rolling window and alert threshold for threshold-crossing events.
    pub(super) fn configuration(&self) -> &RecoveryConfig {
        &self.configuration
    }

    /// Timeout reported in [`LoadError::RecoveryTimeout`](crate::load::LoadError::RecoveryTimeout).
    pub(super) fn timeout(&self) -> Duration {
        self.configuration.timeout
    }

    /// How long a runner may sleep before it should ask [`Self::status`] again.
    pub(super) fn poll_interval(&self) -> Duration {
        self.configuration
            .poll_interval
            .max(MINIMUM_EVALUATION_INTERVAL)
    }

    /// The budget left now, starting or ending an episode when the rolling rate has changed side.
    ///
    /// Re-reads the rolling error rate at most once per [`Self::poll_interval`], except that a
    /// budget about to be reported as exhausted is always confirmed against a fresh reading, so a
    /// recovery inside the last interval is never missed.
    pub(super) fn status(&mut self, metrics: &Arc<LoadMetricsAccumulator>) -> RecoveryBudget {
        if !self.enforced {
            return RecoveryBudget::WithinTarget;
        }
        let now = Instant::now();
        if self.evaluation_due(now) {
            self.evaluate(metrics, now);
        }
        match self.budget_at(now) {
            RecoveryBudget::Exhausted if self.last_evaluated_at != Some(now) => {
                self.evaluate(metrics, now);
                self.budget_at(now)
            }
            budget => budget,
        }
    }

    /// Stop `paused_for` from counting toward the open episode.
    ///
    /// A paused run completes no work, so the service gets no chance to recover; letting that time
    /// count would abort the run the moment it resumes.
    pub(super) fn credit_paused(&mut self, paused_for: Duration) {
        let now = Instant::now();
        if let Some(since) = self.above_threshold_since.as_mut() {
            *since = (*since + paused_for).min(now);
        }
    }

    fn evaluation_due(&self, now: Instant) -> bool {
        self.last_evaluated_at.is_none_or(|evaluated_at| {
            now.saturating_duration_since(evaluated_at) >= self.poll_interval()
        })
    }

    fn evaluate(&mut self, metrics: &Arc<LoadMetricsAccumulator>, now: Instant) {
        self.last_evaluated_at = Some(now);
        let rate = LoadMetrics::new(metrics.clone())
            .error_rate_in_window(self.configuration.rolling_window);
        if rate <= self.configuration.error_rate_threshold {
            self.above_threshold_since = None;
        } else if self.above_threshold_since.is_none() {
            self.above_threshold_since = Some(now);
        }
    }

    fn budget_at(&self, now: Instant) -> RecoveryBudget {
        let Some(since) = self.above_threshold_since else {
            return RecoveryBudget::WithinTarget;
        };
        let elapsed = now.saturating_duration_since(since);
        if elapsed >= self.configuration.timeout {
            RecoveryBudget::Exhausted
        } else {
            RecoveryBudget::Remaining(self.configuration.timeout - elapsed)
        }
    }
}

#[cfg(test)]
mod tests;
