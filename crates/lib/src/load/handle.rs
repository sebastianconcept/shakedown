//! [`LoadHandle`] and [`Context::start_load`](crate::lifecycle::Context::start_load): the public
//! face of a running load.
//!
//! The run itself lives in submodules by job: `driver` walks the phases, `open_model` and
//! `closed_model` schedule work, `iteration` records each outcome, and `recovery` enforces the
//! profile's error-rate budget.

mod closed_model;
mod driver;
mod events;
mod iteration;
mod open_model;
mod recovery;

use std::future::Future;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use tokio::sync::{watch, Semaphore};

use crate::event::Event;
use crate::lifecycle::Context;

use super::metrics::{LoadMetrics, LoadMetricsAccumulator};
use super::{LoadError, LoadProfile, LoadRuntime, NamedPhase, RecoveryConfig};

use driver::{run_driver, DriverContext};
use events::try_emit_load_event;
use iteration::IterationRecorder;
use recovery::RecoveryWatch;

/// Open-model load driver handle: pause/resume, phase await, graceful [`LoadHandle::stop`].
pub struct LoadHandle {
    load_name: String,
    profile: Arc<LoadProfile>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    join_handle: Option<tokio::task::JoinHandle<Result<(), LoadError>>>,
    metrics: Arc<LoadMetricsAccumulator>,
    paused: Arc<AtomicBool>,
    phase_watch: watch::Receiver<Option<usize>>,
    /// Same channel as [`Context::start_load`] used for phase and recovery rows.
    event_sender: Option<std::sync::mpsc::Sender<Event>>,
}

impl LoadHandle {
    /// Pause scheduling new work: open-model arrivals and new virtual-user iterations (in-flight
    /// work continues). Time spent paused does not count toward a profile recovery budget.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    /// Resume after [`LoadHandle::pause`].
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Release);
    }

    /// Live metrics snapshot (same counters as after [`LoadHandle::stop`]).
    pub fn metrics(&self) -> LoadMetrics {
        LoadMetrics::new(self.metrics.clone())
    }

    /// Name passed to [`Context::start_load`].
    pub fn load_name(&self) -> &str {
        &self.load_name
    }

    /// Wait until the rolling error rate is at or below the configured threshold **after** at least
    /// one failed iteration has been recorded, or return [`LoadError::RecoveryTimeout`].
    ///
    /// Emits a structured [`Event`] with source `"load"` on success. Uses [`RecoveryConfig::new`]
    /// defaults; call [`Self::await_recovery_with_config`] for custom thresholds and windows.
    pub async fn await_recovery(&self, timeout: Duration) -> Result<Duration, LoadError> {
        self.await_recovery_with_config(RecoveryConfig::new(timeout))
            .await
    }

    /// Same as [`Self::await_recovery`] with explicit [`RecoveryConfig`].
    pub async fn await_recovery_with_config(
        &self,
        recovery_config: RecoveryConfig,
    ) -> Result<Duration, LoadError> {
        let deadline = tokio::time::Instant::now() + recovery_config.timeout;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(LoadError::RecoveryTimeout {
                    timeout: recovery_config.timeout,
                });
            }

            if *self.shutdown_rx.borrow() {
                return Err(LoadError::DriverStopped);
            }

            let snapshot = LoadMetrics::new(self.metrics.clone());
            let had_failure = snapshot.failed_operations() > 0;
            if !had_failure {
                tokio::time::sleep(recovery_config.poll_interval).await;
                continue;
            }

            let rate = snapshot.error_rate_in_window(recovery_config.rolling_window);
            if rate <= recovery_config.error_rate_threshold {
                let disruption = snapshot
                    .first_failure_instant()
                    .map(|start| StdInstant::now().saturating_duration_since(start))
                    .unwrap_or_default();
                let pct = recovery_config.error_rate_threshold * 100.0;
                try_emit_load_event(
                    &self.event_sender,
                    &self.load_name,
                    format!("error rate returned to {pct:.0}% after {disruption:?} disruption"),
                );
                return Ok(disruption);
            }

            tokio::time::sleep(recovery_config.poll_interval).await;
        }
    }

    /// Wait for the phase identified by `name_or_index` (name string or decimal index) to complete.
    pub async fn await_phase(&self, name_or_index: &str) -> Result<(), LoadError> {
        let index = resolve_phase_index(self.profile.phases(), name_or_index)?;
        let mut phase_rx = self.phase_watch.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        loop {
            if *shutdown_rx.borrow() {
                return Err(LoadError::DriverStopped);
            }
            if let Some(current) = *phase_rx.borrow() {
                if current >= index {
                    return Ok(());
                }
            }
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        return Err(LoadError::DriverStopped);
                    }
                }
                _ = phase_rx.changed() => {}
            }
        }
    }

    /// Wait for the driver to finish all phases, then return final [`LoadMetrics`].
    ///
    /// Does not short-circuit the profile: the work loop runs until each phase duration elapses.
    /// Dropping a handle without calling `stop` aborts the driver and sends shutdown (see [`Drop`]).
    pub async fn stop(mut self) -> Result<LoadMetrics, LoadError> {
        let join_result = match self.join_handle.take() {
            Some(handle) => handle.await,
            None => {
                return Err(LoadError::DriverFailed {
                    source: Box::new(std::io::Error::other("load driver join handle missing")),
                });
            }
        };
        let metrics_snapshot = LoadMetrics::new(self.metrics.clone());
        match join_result {
            Ok(Ok(())) => {
                if super::summary::load_summary_print_enabled() {
                    let use_color = std::io::stderr().is_terminal();
                    let text = super::summary::render_summary_styled(
                        &self.load_name,
                        &metrics_snapshot,
                        None,
                        None,
                        use_color,
                    );
                    eprintln!("{text}");
                }
                Ok(metrics_snapshot)
            }
            Ok(Err(load_error)) => Err(load_error),
            Err(join_error) => Err(LoadError::DriverFailed {
                source: Box::new(join_error),
            }),
        }
    }
}

impl Drop for LoadHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.join_handle.take() {
            tokio::spawn(async move {
                if let Err(e) = handle.await {
                    eprintln!("load driver task panicked during drop: {e}");
                }
            });
        }
    }
}

/// Start a load run on the shared scenario runtime.
///
/// Open-model phases schedule arrivals on the wall clock. A closed-model
/// [`Phase::ConstantVus`](super::Phase::ConstantVus) phase starts one loop per virtual user on
/// that same runtime: the user runs the work future to completion, then starts another, until the
/// phase duration elapses.
///
/// [`LoadRuntime::Dedicated`] is rejected with [`LoadError::UnsupportedRuntime`] before a handle
/// is returned and before any load timeline event is recorded.
///
/// A profile that opted in with
/// [`LoadProfileBuilder::recovery`](super::LoadProfileBuilder::recovery) is stopped by the driver
/// with [`LoadError::RecoveryTimeout`] when the rolling error rate stays above the budget's
/// threshold for its whole timeout. Without that call the driver never stops a run because of its
/// error rate.
pub async fn start_load<E, F, Fut>(
    context: &mut Context,
    load_name: &str,
    profile: LoadProfile,
    work: F,
) -> Result<LoadHandle, LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    match profile.runtime() {
        LoadRuntime::Shared => {}
        LoadRuntime::Dedicated { .. } => return Err(LoadError::UnsupportedRuntime),
    }

    let profile = Arc::new(profile);
    let metrics = Arc::new(LoadMetricsAccumulator::default());
    metrics.set_load_profile_summary(super::summarize_load_profile(&profile));
    let paused = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (phase_watch_tx, phase_watch_rx) = watch::channel(None::<usize>);

    let events = context.load_event_sender().cloned();
    let recovery = RecoveryWatch::new(profile.recovery());
    let recorder = Arc::new(IterationRecorder::new(
        load_name.to_string(),
        metrics.clone(),
        events.clone(),
        recovery.configuration().clone(),
    ));

    let join_handle = tokio::spawn(run_driver(
        DriverContext {
            profile: profile.clone(),
            work: Arc::new(work),
            recorder,
            shutdown_rx: shutdown_rx.clone(),
            paused: paused.clone(),
            semaphore: Arc::new(Semaphore::new(profile.max_workers())),
        },
        phase_watch_tx,
        recovery,
    ));

    Ok(LoadHandle {
        load_name: load_name.to_string(),
        profile,
        shutdown_tx,
        shutdown_rx,
        join_handle: Some(join_handle),
        metrics,
        paused,
        phase_watch: phase_watch_rx,
        event_sender: events,
    })
}

fn resolve_phase_index(phases: &[NamedPhase], name_or_index: &str) -> Result<usize, LoadError> {
    if let Ok(index) = name_or_index.parse::<usize>() {
        if index < phases.len() {
            Ok(index)
        } else {
            Err(LoadError::PhaseNotFound {
                name: name_or_index.to_string(),
            })
        }
    } else {
        phases
            .iter()
            .position(|named| named.name.as_deref() == Some(name_or_index))
            .ok_or_else(|| LoadError::PhaseNotFound {
                name: name_or_index.to_string(),
            })
    }
}
