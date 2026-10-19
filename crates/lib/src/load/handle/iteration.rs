//! Running one unit of load work and recording what happened.

use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::event::Event;
use crate::load::metrics::{LoadMetrics, LoadMetricsAccumulator};
use crate::load::RecoveryConfig;

use super::events::try_emit_load_event;

/// Which scheduler started an iteration, so failure rows can name it.
#[derive(Debug, Clone, Copy)]
pub(super) enum IterationOrigin {
    /// An open-model arrival.
    Arrival,
    /// One loop of a closed-model virtual user, by index.
    VirtualUser(usize),
}

/// Times each iteration, records its outcome, and raises the error-rate alert.
///
/// Shared by every spawned iteration of a run behind one `Arc`, so starting an iteration clones a
/// pointer instead of the metrics, event sink, and alert configuration separately.
pub(super) struct IterationRecorder {
    load_name: String,
    metrics: Arc<LoadMetricsAccumulator>,
    events: Option<std::sync::mpsc::Sender<Event>>,
    error_rate_alert_band: AtomicU8,
    alerting: RecoveryConfig,
}

impl IterationRecorder {
    /// Recorder for one run; `alerting` supplies the rolling window and alert threshold.
    pub(super) fn new(
        load_name: String,
        metrics: Arc<LoadMetricsAccumulator>,
        events: Option<std::sync::mpsc::Sender<Event>>,
        alerting: RecoveryConfig,
    ) -> Self {
        Self {
            load_name,
            metrics,
            events,
            error_rate_alert_band: AtomicU8::new(0),
            alerting,
        }
    }

    /// Name the run was started with.
    pub(super) fn load_name(&self) -> &str {
        &self.load_name
    }

    /// Counters shared with the [`LoadHandle`](crate::load::LoadHandle).
    pub(super) fn metrics(&self) -> &Arc<LoadMetricsAccumulator> {
        &self.metrics
    }

    /// Timeline sink for this run, when the scenario provided one.
    pub(super) fn events(&self) -> Option<&std::sync::mpsc::Sender<Event>> {
        self.events.as_ref()
    }

    /// Run `work` once and record its latency and outcome.
    ///
    /// A failure also adds a timeline row naming `origin`. Afterwards the rolling error rate is
    /// compared with the alert threshold, emitting a row only when it crosses upward.
    pub(super) async fn run<E, F, Fut>(&self, work: &F, origin: IterationOrigin)
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: std::fmt::Display,
    {
        let start = Instant::now();
        let result = work().await;
        let completed_at = Instant::now();
        let latency = start.elapsed();
        match result {
            Ok(()) => self.metrics.record_success(completed_at, latency),
            Err(error) => {
                self.metrics.record_failure(completed_at, latency);
                let message = match origin {
                    IterationOrigin::Arrival => format!("iteration failed: {error}"),
                    IterationOrigin::VirtualUser(index) => {
                        format!("vu {index} iteration failed: {error}")
                    }
                };
                try_emit_load_event(self.events(), &self.load_name, message);
            }
        }
        self.emit_error_rate_alert_on_upward_crossing();
    }

    fn emit_error_rate_alert_on_upward_crossing(&self) {
        let snapshot = LoadMetrics::new(self.metrics.clone());
        if snapshot.successful_operations() + snapshot.failed_operations() == 0 {
            return;
        }
        let rate = snapshot.error_rate_in_window(self.alerting.rolling_window);
        let alert = self.alerting.alert_threshold;
        let above = rate > alert;
        let previously_above = self.error_rate_alert_band.load(Ordering::Acquire) == 1;
        if above && !previously_above {
            self.error_rate_alert_band.store(1, Ordering::Release);
            try_emit_load_event(
                self.events(),
                &self.load_name,
                format!(
                    "error rate crossed above {:.0}% (rolling {:?})",
                    alert * 100.0,
                    self.alerting.rolling_window
                ),
            );
        } else if !above && previously_above {
            self.error_rate_alert_band.store(0, Ordering::Release);
        }
    }
}
