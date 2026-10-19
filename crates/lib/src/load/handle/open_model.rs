//! Open-model scheduling: arrivals start on the wall clock whether or not earlier ones finished.

use std::future::Future;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant as StdInstant};

use tokio::sync::OwnedSemaphorePermit;
use tokio::time::Instant;

use crate::load::{LoadError, Rate, SaturationPolicy};

use super::driver::{wait_if_paused, DriverContext};
use super::events::try_emit_load_event;
use super::iteration::IterationOrigin;
use super::recovery::{RecoveryBudget, RecoveryWatch};

/// Start `rate` arrivals per second for `duration`, honoring pause, shutdown, and the recovery
/// budget.
pub(super) async fn run_constant<E, F, Fut>(
    context: &DriverContext<F>,
    recovery: &mut RecoveryWatch,
    duration: Duration,
    rate: Rate,
    phase_label: &str,
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
        if *context.shutdown_rx.borrow() {
            break;
        }
        let pause_started = context.paused.load(Ordering::Acquire).then(Instant::now);
        if wait_if_paused(&context.paused, &context.shutdown_rx).await {
            break;
        }
        if let Some(started) = pause_started {
            recovery.credit_paused(started.elapsed());
        }

        match recovery.status(context.recorder.metrics()) {
            RecoveryBudget::Exhausted => {
                return Err(LoadError::RecoveryTimeout {
                    timeout: recovery.timeout(),
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

        dispatch_arrival(context, recovery, phase_label).await?;
        next_arrival += period;

        // Early exit when [`LoadHandle`] is dropped: `Drop` sends shutdown so we do not run the
        // full profile if the handle is abandoned without awaiting [`LoadHandle::stop`].
        if *context.shutdown_rx.borrow() {
            break;
        }
    }
    Ok(())
}

fn rate_period(rate: Rate) -> Duration {
    Duration::from_secs_f64(1.0 / rate.as_f64())
}

/// Piecewise constant ramp (20 segments) for stable wall-clock scheduling.
pub(super) async fn run_ramp<E, F, Fut>(
    context: &DriverContext<F>,
    recovery: &mut RecoveryWatch,
    duration: Duration,
    from_rate: Rate,
    to_rate: Rate,
    phase_label: &str,
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
        let t0 = f64::from(step) / f64::from(STEPS);
        let t1 = f64::from(step + 1) / f64::from(STEPS);
        let r0 = from + (to - from) * t0;
        let r1 = from + (to - from) * t1;
        let mid = (r0 + r1) / 2.0;
        run_constant(context, recovery, slice, Rate::per_second(mid), phase_label).await?;
    }
    Ok(())
}

/// Start one arrival according to the profile's saturation policy.
async fn dispatch_arrival<E, F, Fut>(
    context: &DriverContext<F>,
    recovery: &mut RecoveryWatch,
    phase_label: &str,
) -> Result<(), LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    if *context.shutdown_rx.borrow() {
        return Ok(());
    }

    let recorder = &context.recorder;
    match context.profile.saturation_policy() {
        SaturationPolicy::Drop => match context.semaphore.clone().try_acquire_owned() {
            Ok(permit) => spawn_work_unit(context, permit),
            Err(_) => {
                recorder.metrics().record_dropped(StdInstant::now());
                try_emit_load_event(
                    recorder.events(),
                    recorder.load_name(),
                    format!(
                        "iteration dropped during phase '{phase_label}' (all workers busy, SaturationPolicy::Drop)"
                    ),
                );
            }
        },
        SaturationPolicy::Queue => {
            if let Some(permit) = acquire_queued_permit(context, recovery).await? {
                spawn_work_unit(context, permit);
            }
        }
        SaturationPolicy::Error => match context.semaphore.clone().try_acquire_owned() {
            Ok(permit) => spawn_work_unit(context, permit),
            Err(_) => {
                try_emit_load_event(
                    recorder.events(),
                    recorder.load_name(),
                    "saturation Error policy: all workers busy",
                );
                return Err(LoadError::SaturationExceeded);
            }
        },
    }
    Ok(())
}

/// Wait for a free worker under [`SaturationPolicy::Queue`] without going deaf to the run.
///
/// Wakes every poll interval to honor shutdown (`Ok(None)`) and the recovery budget, because
/// workers stuck on slow failing calls would otherwise block the scheduler past the timeout.
async fn acquire_queued_permit<E, F, Fut>(
    context: &DriverContext<F>,
    recovery: &mut RecoveryWatch,
) -> Result<Option<OwnedSemaphorePermit>, LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let mut acquire = std::pin::pin!(context.semaphore.clone().acquire_owned());
    loop {
        tokio::select! {
            permit = &mut acquire => {
                return permit.map(Some).map_err(|_| LoadError::DriverFailed {
                    source: Box::new(std::io::Error::other("semaphore closed")),
                });
            }
            _ = tokio::time::sleep(recovery.poll_interval()) => {
                if *context.shutdown_rx.borrow() {
                    return Ok(None);
                }
                if recovery.status(context.recorder.metrics()) == RecoveryBudget::Exhausted {
                    return Err(LoadError::RecoveryTimeout { timeout: recovery.timeout() });
                }
            }
        }
    }
}

/// Run one iteration on its own task, holding `permit` until it has been recorded.
fn spawn_work_unit<E, F, Fut>(context: &DriverContext<F>, permit: OwnedSemaphorePermit)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let work = context.work.clone();
    let recorder = context.recorder.clone();
    tokio::spawn(async move {
        let _permit = permit;
        recorder.run(work.as_ref(), IterationOrigin::Arrival).await;
    });
}
