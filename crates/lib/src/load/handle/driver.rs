//! The task that walks a profile's phases and hands each one to its scheduler.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use tokio::sync::{watch, Semaphore};

use crate::load::{LoadError, LoadProfile, NamedPhase, Phase};

use super::events::try_emit_load_event;
use super::iteration::IterationRecorder;
use super::recovery::RecoveryWatch;
use super::{closed_model, open_model};

/// Everything the phase schedulers of one run share.
pub(super) struct DriverContext<F> {
    pub(super) profile: Arc<LoadProfile>,
    pub(super) work: Arc<F>,
    pub(super) recorder: Arc<IterationRecorder>,
    pub(super) shutdown_rx: watch::Receiver<bool>,
    pub(super) paused: Arc<AtomicBool>,
    pub(super) semaphore: Arc<Semaphore>,
}

/// Run every phase in order, stopping early on shutdown or the first scheduler error.
///
/// `phase_watch_tx` announces each phase index as it starts. `recovery` is owned here so its
/// episode clock spans phases.
pub(super) async fn run_driver<E, F, Fut>(
    context: DriverContext<F>,
    phase_watch_tx: watch::Sender<Option<usize>>,
    mut recovery: RecoveryWatch,
) -> Result<(), LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    for (phase_index, named) in context.profile.phases().iter().enumerate() {
        if *context.shutdown_rx.borrow() {
            break;
        }

        let phase_display = phase_label(named, phase_index);
        try_emit_load_event(
            context.recorder.events(),
            context.recorder.load_name(),
            format!("phase {phase_index} start ({phase_display})"),
        );
        phase_watch_tx
            .send(Some(phase_index))
            .map_err(|_| LoadError::DriverStopped)?;

        run_phase(
            &context,
            &mut recovery,
            &named.phase,
            phase_index,
            &phase_display,
        )
        .await?;
    }

    Ok(())
}

/// Hand one phase to the scheduler that runs its traffic model.
async fn run_phase<E, F, Fut>(
    context: &DriverContext<F>,
    recovery: &mut RecoveryWatch,
    phase: &Phase,
    phase_index: usize,
    phase_display: &str,
) -> Result<(), LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    match phase {
        Phase::Warmup { duration, rate } | Phase::Constant { duration, rate } => {
            open_model::run_constant(context, recovery, *duration, *rate, phase_display).await?;
            if matches!(phase, Phase::Warmup { .. }) && !*context.shutdown_rx.borrow() {
                context.recorder.metrics().set_warmup_end(StdInstant::now());
            }
            Ok(())
        }
        Phase::Spike {
            duration,
            peak_rate,
        } => {
            open_model::run_constant(context, recovery, *duration, *peak_rate, phase_display).await
        }
        Phase::Ramp {
            duration,
            from_rate,
            to_rate,
        } => {
            open_model::run_ramp(
                context,
                recovery,
                *duration,
                *from_rate,
                *to_rate,
                phase_display,
            )
            .await
        }
        Phase::Pause { duration } => {
            try_emit_load_event(
                context.recorder.events(),
                context.recorder.load_name(),
                format!("phase {phase_index} pause for {duration:?}"),
            );
            tokio::time::sleep(*duration).await;
            Ok(())
        }
        Phase::ConstantVus { duration, count } => {
            closed_model::run_constant_virtual_users(context, recovery, *duration, *count).await
        }
        Phase::RampVus { .. } => Err(LoadError::UnsupportedPhase {
            description: "RampVus (use ConstantVus or wait for a later milestone)",
        }),
    }
}

fn phase_label(named: &NamedPhase, phase_index: usize) -> String {
    named
        .name
        .clone()
        .unwrap_or_else(|| format!("unnamed #{phase_index}"))
}

/// Block while the run is paused. Returns `true` if shutdown arrived meanwhile and the caller
/// should stop its phase.
pub(super) async fn wait_if_paused(
    paused: &AtomicBool,
    shutdown_rx: &watch::Receiver<bool>,
) -> bool {
    while paused.load(Ordering::Acquire) {
        if *shutdown_rx.borrow() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}
