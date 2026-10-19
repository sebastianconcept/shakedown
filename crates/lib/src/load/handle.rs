//! [`LoadHandle`] and [`Context::start_load`](crate::lifecycle::Context::start_load) driver:
//! open/closed-model scheduling, metrics accumulation, phase timeline events, and recovery signaling.

use std::future::Future;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant as StdInstant};

use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::event::Event;
use crate::lifecycle::Context;

use super::metrics::{LoadMetrics, LoadMetricsAccumulator};
use super::{
    LoadError, LoadProfile, LoadRuntime, NamedPhase, Phase, Rate, RecoveryConfig, SaturationPolicy,
};

const LOAD_EVENT_SOURCE: &str = "load";

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
    /// Pause scheduling new open-model arrivals (in-flight work continues). No-op for closed-model VU bursts.
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

    /// Block until phase `index` **or** a matching `name` has started (`"0"` is phase index 0).
    ///
    /// Accepts decimal phase index or the name set with [`Phase::named`](super::Phase::named).
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
/// [`RecoveryConfig::timeout`](super::RecoveryConfig::timeout) is enforced by the driver. After the
/// rolling error rate rises above [`RecoveryConfig::error_rate_threshold`](super::RecoveryConfig::error_rate_threshold),
/// the driver stops with [`LoadError::RecoveryTimeout`] if the rate stays above that threshold for
/// the whole timeout.
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

    let work = Arc::new(work);
    let profile = Arc::new(profile);
    let metrics = Arc::new(LoadMetricsAccumulator::default());
    metrics.set_load_profile_summary(super::summarize_load_profile(&profile));
    let paused = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (phase_watch_tx, phase_watch_rx) = watch::channel(None::<usize>);

    let events = context.load_event_sender().cloned();
    let load_name_owned = load_name.to_string();
    let error_rate_alert_band = Arc::new(AtomicU8::new(0));
    let recovery = RecoveryWatch::new(profile.recovery().clone());

    let join_handle = tokio::spawn(run_driver(
        load_name_owned.clone(),
        profile.clone(),
        work,
        metrics.clone(),
        events.clone(),
        shutdown_rx.clone(),
        paused.clone(),
        phase_watch_tx,
        error_rate_alert_band.clone(),
        recovery,
    ));

    Ok(LoadHandle {
        load_name: load_name_owned,
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

fn try_emit_load_event(
    events: &Option<std::sync::mpsc::Sender<Event>>,
    load_name: &str,
    message: impl Into<String>,
) {
    if let Some(sender) = events {
        let text = format!("{load_name}: {}", message.into());
        let _ = sender.send(Event::info(LOAD_EVENT_SOURCE, text));
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_driver<E, F, Fut>(
    load_name: String,
    profile: Arc<LoadProfile>,
    work: Arc<F>,
    metrics: Arc<LoadMetricsAccumulator>,
    events: Option<std::sync::mpsc::Sender<Event>>,
    shutdown_rx: watch::Receiver<bool>,
    paused: Arc<AtomicBool>,
    phase_watch_tx: watch::Sender<Option<usize>>,
    error_rate_alert_band: Arc<AtomicU8>,
    recovery: RecoveryWatch,
) -> Result<(), LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let semaphore = Arc::new(Semaphore::new(profile.max_workers()));
    let max_workers = profile.max_workers();

    for (phase_index, named) in profile.phases().iter().enumerate() {
        if *shutdown_rx.borrow() {
            break;
        }

        if !matches!(profile.runtime(), LoadRuntime::Shared) {
            return Err(LoadError::UnsupportedRuntime);
        }

        let phase_display = phase_label(named, phase_index);
        try_emit_load_event(
            &events,
            &load_name,
            format!("phase {phase_index} start ({phase_display})",),
        );
        phase_watch_tx
            .send(Some(phase_index))
            .map_err(|_| LoadError::DriverStopped)?;

        match &named.phase {
            Phase::Warmup { duration, rate } => {
                run_open_model_constant(
                    *duration,
                    *rate,
                    &load_name,
                    &profile,
                    &work,
                    &metrics,
                    &events,
                    &semaphore,
                    &shutdown_rx,
                    &paused,
                    phase_display.as_str(),
                    &error_rate_alert_band,
                    &recovery,
                )
                .await?;
                if !*shutdown_rx.borrow() {
                    metrics.set_warmup_end(StdInstant::now());
                }
            }
            Phase::Constant { duration, rate } => {
                run_open_model_constant(
                    *duration,
                    *rate,
                    &load_name,
                    &profile,
                    &work,
                    &metrics,
                    &events,
                    &semaphore,
                    &shutdown_rx,
                    &paused,
                    phase_display.as_str(),
                    &error_rate_alert_band,
                    &recovery,
                )
                .await?;
            }
            Phase::Ramp {
                duration,
                from_rate,
                to_rate,
            } => {
                run_open_model_ramp(
                    *duration,
                    *from_rate,
                    *to_rate,
                    &load_name,
                    &profile,
                    &work,
                    &metrics,
                    &events,
                    &semaphore,
                    &shutdown_rx,
                    &paused,
                    phase_display.as_str(),
                    &error_rate_alert_band,
                    &recovery,
                )
                .await?;
            }
            Phase::Spike {
                duration,
                peak_rate,
            } => {
                run_open_model_constant(
                    *duration,
                    *peak_rate,
                    &load_name,
                    &profile,
                    &work,
                    &metrics,
                    &events,
                    &semaphore,
                    &shutdown_rx,
                    &paused,
                    phase_display.as_str(),
                    &error_rate_alert_band,
                    &recovery,
                )
                .await?;
            }
            Phase::Pause { duration } => {
                try_emit_load_event(
                    &events,
                    &load_name,
                    format!("phase {phase_index} pause for {:?}", duration),
                );
                tokio::time::sleep(*duration).await;
            }
            Phase::ConstantVus { duration, count } => {
                run_closed_model_constant_vus(
                    *duration,
                    *count,
                    max_workers,
                    &load_name,
                    &work,
                    &metrics,
                    &events,
                    &shutdown_rx,
                    &paused,
                    phase_display.as_str(),
                    &error_rate_alert_band,
                    &recovery,
                )
                .await?;
            }
            Phase::RampVus { .. } => {
                return Err(LoadError::UnsupportedPhase {
                    description: "RampVus (use ConstantVus or wait for a later milestone)",
                });
            }
        }
    }

    Ok(())
}

fn phase_label(named: &NamedPhase, phase_index: usize) -> String {
    named
        .name
        .clone()
        .unwrap_or_else(|| format!("unnamed #{phase_index}"))
}

/// Returns `true` if the caller should stop the phase (shutdown while paused).
async fn wait_if_paused(paused: &Arc<AtomicBool>, shutdown_rx: &watch::Receiver<bool>) -> bool {
    while paused.load(Ordering::Acquire) {
        if *shutdown_rx.borrow() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[allow(clippy::too_many_arguments)]
async fn run_open_model_constant<E, F, Fut>(
    duration: Duration,
    rate: Rate,
    load_name: &str,
    profile: &LoadProfile,
    work: &Arc<F>,
    metrics: &Arc<LoadMetricsAccumulator>,
    events: &Option<std::sync::mpsc::Sender<Event>>,
    semaphore: &Arc<Semaphore>,
    shutdown_rx: &watch::Receiver<bool>,
    paused: &Arc<AtomicBool>,
    phase_label: &str,
    error_rate_alert_band: &Arc<AtomicU8>,
    recovery: &RecoveryWatch,
) -> Result<(), LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let phase_start = Instant::now();
    let end = phase_start + duration;
    let mut next_arrival = phase_start;
    let period = rate_period(rate);

    while next_arrival < end {
        if *shutdown_rx.borrow() || wait_if_paused(paused, shutdown_rx).await {
            break;
        }

        match recovery.status(metrics) {
            RecoveryBudget::Exhausted => {
                return Err(LoadError::RecoveryTimeout {
                    timeout: recovery.config.timeout,
                });
            }
            RecoveryBudget::Remaining(remaining) => {
                let budget_at = Instant::now() + remaining;
                if budget_at < next_arrival {
                    tokio::time::sleep_until(budget_at).await;
                    continue;
                }
            }
            RecoveryBudget::WithinTarget => {}
        }

        tokio::time::sleep_until(next_arrival).await;

        dispatch_open_model_arrival(
            profile,
            work,
            metrics,
            events,
            load_name,
            semaphore,
            shutdown_rx,
            phase_label,
            error_rate_alert_band,
            &recovery.config,
        )
        .await?;
        next_arrival += period;

        // Early exit when [`LoadHandle`] is dropped: `Drop` sends shutdown so we do not run the
        // full profile if the handle is abandoned without awaiting [`LoadHandle::stop`].
        if *shutdown_rx.borrow() {
            break;
        }
    }
    Ok(())
}

fn rate_period(rate: Rate) -> Duration {
    let r = rate.as_f64();
    Duration::from_secs_f64(1.0 / r)
}

/// Piecewise constant ramp (20 segments) for stable wall-clock scheduling.
#[allow(clippy::too_many_arguments)]
async fn run_open_model_ramp<E, F, Fut>(
    duration: Duration,
    from_rate: Rate,
    to_rate: Rate,
    load_name: &str,
    profile: &LoadProfile,
    work: &Arc<F>,
    metrics: &Arc<LoadMetricsAccumulator>,
    events: &Option<std::sync::mpsc::Sender<Event>>,
    semaphore: &Arc<Semaphore>,
    shutdown_rx: &watch::Receiver<bool>,
    paused: &Arc<AtomicBool>,
    phase_label: &str,
    error_rate_alert_band: &Arc<AtomicU8>,
    recovery: &RecoveryWatch,
) -> Result<(), LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    const STEPS: u32 = 20;
    let from = from_rate.as_f64();
    let to = to_rate.as_f64();
    let slice = duration / STEPS;
    for step in 0..STEPS {
        let t0 = step as f64 / f64::from(STEPS);
        let t1 = (step + 1) as f64 / f64::from(STEPS);
        let r0 = from + (to - from) * t0;
        let r1 = from + (to - from) * t1;
        let mid = (r0 + r1) / 2.0;
        run_open_model_constant(
            slice,
            Rate::per_second(mid),
            load_name,
            profile,
            work,
            metrics,
            events,
            semaphore,
            shutdown_rx,
            paused,
            phase_label,
            error_rate_alert_band,
            recovery,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_open_model_arrival<E, F, Fut>(
    profile: &LoadProfile,
    work: &Arc<F>,
    metrics: &Arc<LoadMetricsAccumulator>,
    events: &Option<std::sync::mpsc::Sender<Event>>,
    load_name: &str,
    semaphore: &Arc<Semaphore>,
    shutdown_rx: &watch::Receiver<bool>,
    phase_label: &str,
    error_rate_alert_band: &Arc<AtomicU8>,
    recovery_defaults: &RecoveryConfig,
) -> Result<(), LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    if *shutdown_rx.borrow() {
        return Ok(());
    }

    match profile.saturation_policy() {
        SaturationPolicy::Drop => match semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                spawn_work_unit(
                    work,
                    metrics,
                    permit,
                    events,
                    load_name,
                    error_rate_alert_band,
                    recovery_defaults,
                );
            }
            Err(_) => {
                metrics.record_dropped(std::time::Instant::now());
                try_emit_load_event(
                    events,
                    load_name,
                    format!(
                        "iteration dropped during phase '{phase_label}' (all workers busy, SaturationPolicy::Drop)"
                    ),
                );
            }
        },
        SaturationPolicy::Queue => {
            let permit =
                semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| LoadError::DriverFailed {
                        source: Box::new(std::io::Error::other("semaphore closed")),
                    })?;
            spawn_work_unit(
                work,
                metrics,
                permit,
                events,
                load_name,
                error_rate_alert_band,
                recovery_defaults,
            );
        }
        SaturationPolicy::Error => match semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                spawn_work_unit(
                    work,
                    metrics,
                    permit,
                    events,
                    load_name,
                    error_rate_alert_band,
                    recovery_defaults,
                );
            }
            Err(_) => {
                try_emit_load_event(
                    events,
                    load_name,
                    "saturation Error policy: all workers busy",
                );
                return Err(LoadError::SaturationExceeded);
            }
        },
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_work_unit<E, F, Fut>(
    work: &Arc<F>,
    metrics: &Arc<LoadMetricsAccumulator>,
    permit: OwnedSemaphorePermit,
    events: &Option<std::sync::mpsc::Sender<Event>>,
    load_name: &str,
    error_rate_alert_band: &Arc<AtomicU8>,
    recovery_defaults: &RecoveryConfig,
) where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let work = work.clone();
    let metrics = metrics.clone();
    let events = events.clone();
    let load_name = load_name.to_string();
    let band = error_rate_alert_band.clone();
    let recovery = recovery_defaults.clone();
    tokio::spawn(async move {
        let start = std::time::Instant::now();
        let result = work().await;
        let completed_at = std::time::Instant::now();
        let latency = start.elapsed();
        drop(permit);
        match result {
            Ok(()) => {
                metrics.record_success(completed_at, latency);
            }
            Err(error) => {
                metrics.record_failure(completed_at, latency);
                try_emit_load_event(&events, &load_name, format!("iteration failed: {error}"));
            }
        }
        maybe_emit_rolling_error_rate_threshold(&metrics, &events, &load_name, &band, &recovery);
    });
}

fn maybe_emit_rolling_error_rate_threshold(
    metrics: &Arc<LoadMetricsAccumulator>,
    events: &Option<std::sync::mpsc::Sender<Event>>,
    load_name: &str,
    band: &Arc<AtomicU8>,
    recovery_defaults: &RecoveryConfig,
) {
    let snapshot = LoadMetrics::new(metrics.clone());
    if snapshot.successful_operations() + snapshot.failed_operations() == 0 {
        return;
    }
    let rate = snapshot.error_rate_in_window(recovery_defaults.rolling_window);
    let alert = recovery_defaults.alert_threshold;
    let above = rate > alert;
    let prev_above = band.load(Ordering::Acquire) == 1;
    if above && !prev_above {
        band.store(1, Ordering::Release);
        try_emit_load_event(
            events,
            load_name,
            format!(
                "error rate crossed above {:.0}% (rolling {:?})",
                alert * 100.0,
                recovery_defaults.rolling_window
            ),
        );
    } else if !above && prev_above {
        band.store(0, Ordering::Release);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_closed_model_constant_vus<E, F, Fut>(
    duration: Duration,
    count: usize,
    max_workers: usize,
    load_name: &str,
    work: &Arc<F>,
    metrics: &Arc<LoadMetricsAccumulator>,
    events: &Option<std::sync::mpsc::Sender<Event>>,
    shutdown_rx: &watch::Receiver<bool>,
    paused: &Arc<AtomicBool>,
    _phase_label: &str,
    error_rate_alert_band: &Arc<AtomicU8>,
    recovery: &RecoveryWatch,
) -> Result<(), LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    try_emit_load_event(
        events,
        load_name,
        format!("ConstantVus: {count} virtual users (closed model; max_workers={max_workers} applies to open-model phases only)"),
    );

    let deadline = Instant::now() + duration;
    let stop_vus = Arc::new(AtomicBool::new(false));
    let mut join_handles = Vec::with_capacity(count);
    for virtual_user_index in 0..count {
        let work = work.clone();
        let metrics = metrics.clone();
        let events = events.clone();
        let load_name = load_name.to_string();
        let shutdown_rx = shutdown_rx.clone();
        let paused = paused.clone();
        let band = error_rate_alert_band.clone();
        let recovery_config = recovery.config.clone();
        let stop_vus = stop_vus.clone();
        join_handles.push(tokio::spawn(async move {
            loop {
                if stop_vus.load(Ordering::Acquire) || Instant::now() >= deadline {
                    break;
                }
                if *shutdown_rx.borrow() {
                    break;
                }
                while paused.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    if *shutdown_rx.borrow() || stop_vus.load(Ordering::Acquire) {
                        return;
                    }
                }
                let start = std::time::Instant::now();
                let result = work().await;
                let completed_at = std::time::Instant::now();
                let latency = start.elapsed();
                match result {
                    Ok(()) => metrics.record_success(completed_at, latency),
                    Err(error) => {
                        metrics.record_failure(completed_at, latency);
                        try_emit_load_event(
                            &events,
                            &load_name,
                            format!("vu {virtual_user_index} iteration failed: {error}"),
                        );
                    }
                }
                maybe_emit_rolling_error_rate_threshold(
                    &metrics,
                    &events,
                    &load_name,
                    &band,
                    &recovery_config,
                );
            }
        }));
    }

    let mut runners = std::pin::pin!(futures_util::future::join_all(join_handles));
    let mut finished = false;
    loop {
        let pause = match recovery.status(metrics) {
            RecoveryBudget::Exhausted => {
                stop_vus.store(true, Ordering::Release);
                break;
            }
            RecoveryBudget::Remaining(remaining) => remaining.min(recovery_poll(recovery)),
            RecoveryBudget::WithinTarget => recovery_poll(recovery),
        };
        tokio::select! {
            _ = &mut runners => {
                finished = true;
                break;
            }
            _ = tokio::time::sleep(pause) => {}
        }
    }
    if !finished {
        runners.await;
    }
    if stop_vus.load(Ordering::Acquire) {
        return Err(LoadError::RecoveryTimeout {
            timeout: recovery.config.timeout,
        });
    }
    Ok(())
}

/// Shared recovery budget for one load run.
///
/// `config` is the profile budget. `above_threshold_since` is the instant the rolling error rate
/// last rose above [`RecoveryConfig::error_rate_threshold`].
struct RecoveryWatch {
    config: RecoveryConfig,
    above_threshold_since: Mutex<Option<StdInstant>>,
}

impl RecoveryWatch {
    fn new(config: RecoveryConfig) -> Self {
        Self {
            config,
            above_threshold_since: Mutex::new(None),
        }
    }

    fn status(&self, metrics: &Arc<LoadMetricsAccumulator>) -> RecoveryBudget {
        let snapshot = LoadMetrics::new(metrics.clone());
        let completed = snapshot.successful_operations() + snapshot.failed_operations();
        let rate = if completed == 0 {
            0.0
        } else {
            snapshot.error_rate_in_window(self.config.rolling_window)
        };
        let mut above_since = self
            .above_threshold_since
            .lock()
            .expect("recovery episode clock");
        if rate <= self.config.error_rate_threshold {
            *above_since = None;
            return RecoveryBudget::WithinTarget;
        }
        let started = above_since.get_or_insert_with(StdInstant::now);
        let elapsed = started.elapsed();
        if elapsed >= self.config.timeout {
            RecoveryBudget::Exhausted
        } else {
            RecoveryBudget::Remaining(self.config.timeout.saturating_sub(elapsed))
        }
    }
}

/// How much of the profile recovery timeout is left while the error rate is above target.
enum RecoveryBudget {
    /// Rolling error rate is at or below the recovery target.
    WithinTarget,
    /// Error rate is above target, with this much of the timeout left.
    Remaining(Duration),
    /// Error rate stayed above target for the whole timeout.
    Exhausted,
}

fn recovery_poll(recovery: &RecoveryWatch) -> Duration {
    if recovery.config.poll_interval.is_zero() {
        Duration::from_millis(1)
    } else {
        recovery.config.poll_interval
    }
}
