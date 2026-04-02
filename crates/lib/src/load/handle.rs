//! [`LoadHandle`] and [`Context::start_load`](crate::lifecycle::Context::start_load) driver (US-007).

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::event::Event;
use crate::lifecycle::Context;

use super::metrics::{LoadMetrics, LoadMetricsAccumulator};
use super::{LoadError, LoadProfile, LoadRuntime, NamedPhase, Phase, Rate, SaturationPolicy};

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
                    message: "load driver join handle missing".into(),
                });
            }
        };
        let metrics_snapshot = LoadMetrics::new(self.metrics.clone());
        match join_result {
            Ok(Ok(())) => Ok(metrics_snapshot),
            Ok(Err(load_error)) => Err(load_error),
            Err(join_error) => Err(LoadError::DriverFailed {
                message: join_error.to_string(),
            }),
        }
    }
}

impl Drop for LoadHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.join_handle.take() {
            handle.abort();
        }
    }
}

/// Start a load run: open-model phases use wall-clock scheduling; closed-model VU phases run concurrent loops.
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
    if !matches!(profile.runtime(), LoadRuntime::Shared) {
        return Err(LoadError::UnsupportedRuntime);
    }

    let work = Arc::new(work);
    let profile = Arc::new(profile);
    let metrics = Arc::new(LoadMetricsAccumulator::default());
    let paused = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (phase_watch_tx, phase_watch_rx) = watch::channel(None::<usize>);

    let events = context.load_event_sender().cloned();
    let load_name_owned = load_name.to_string();

    let join_handle = tokio::spawn(run_driver(
        load_name_owned.clone(),
        profile.clone(),
        work,
        metrics.clone(),
        events,
        shutdown_rx.clone(),
        paused.clone(),
        phase_watch_tx,
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

        try_emit_load_event(
            &events,
            &load_name,
            format!(
                "phase {phase_index} start ({})",
                phase_label(named, phase_index)
            ),
        );
        phase_watch_tx
            .send(Some(phase_index))
            .map_err(|_| LoadError::DriverStopped)?;

        match &named.phase {
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
        if wait_if_paused(paused, shutdown_rx).await {
            break;
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
        )
        .await?;
    }
    Ok(())
}

async fn dispatch_open_model_arrival<E, F, Fut>(
    profile: &LoadProfile,
    work: &Arc<F>,
    metrics: &Arc<LoadMetricsAccumulator>,
    events: &Option<std::sync::mpsc::Sender<Event>>,
    load_name: &str,
    semaphore: &Arc<Semaphore>,
    shutdown_rx: &watch::Receiver<bool>,
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
                spawn_work_unit(work, metrics, permit, events, load_name);
            }
            Err(_) => {
                metrics.record_dropped(std::time::Instant::now());
                try_emit_load_event(
                    events,
                    load_name,
                    "iteration dropped (all workers busy, SaturationPolicy::Drop)",
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
                        message: "semaphore closed".into(),
                    })?;
            spawn_work_unit(work, metrics, permit, events, load_name);
        }
        SaturationPolicy::Error => match semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                spawn_work_unit(work, metrics, permit, events, load_name);
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

fn spawn_work_unit<E, F, Fut>(
    work: &Arc<F>,
    metrics: &Arc<LoadMetricsAccumulator>,
    permit: OwnedSemaphorePermit,
    events: &Option<std::sync::mpsc::Sender<Event>>,
    load_name: &str,
) where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let work = work.clone();
    let metrics = metrics.clone();
    let events = events.clone();
    let load_name = load_name.to_string();
    tokio::spawn(async move {
        let start = std::time::Instant::now();
        let result = work().await;
        let completed_at = std::time::Instant::now();
        let latency = start.elapsed();
        drop(permit);
        match result {
            Ok(()) => metrics.record_success(completed_at, latency),
            Err(error) => {
                metrics.record_failure(completed_at, latency);
                try_emit_load_event(&events, &load_name, format!("iteration failed: {error}"));
            }
        }
    });
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
    let mut join_handles = Vec::new();
    for virtual_user_index in 0..count {
        let work = work.clone();
        let metrics = metrics.clone();
        let events = events.clone();
        let load_name = load_name.to_string();
        let shutdown_rx = shutdown_rx.clone();
        let paused = paused.clone();
        join_handles.push(tokio::spawn(async move {
            loop {
                if Instant::now() >= deadline {
                    break;
                }
                if *shutdown_rx.borrow() {
                    break;
                }
                while paused.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    if *shutdown_rx.borrow() {
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
            }
        }));
    }

    for handle in join_handles {
        let _ = handle.await;
    }
    Ok(())
}
