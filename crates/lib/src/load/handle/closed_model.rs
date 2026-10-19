//! Closed-model scheduling: a fixed set of virtual users, each sending again as soon as its last
//! iteration finishes.
//!
//! Virtual users run on the shared runtime and ignore `workers`, `max_workers`, and the saturation
//! policy: concurrency is the user count.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::load::LoadError;

use super::driver::DriverContext;
use super::events::try_emit_load_event;
use super::iteration::{IterationOrigin, IterationRecorder};
use super::recovery::{RecoveryBudget, RecoveryWatch};

/// How long virtual users may finish an in-flight iteration after the run decided to stop them
/// (recovery budget exhausted, or a user panicked) before their tasks are aborted.
const VIRTUAL_USER_STOP_GRACE: Duration = Duration::from_secs(5);

/// Run `count` virtual users for `duration`.
///
/// Returns when every user has stopped. Fails with [`LoadError::RecoveryTimeout`] if the recovery
/// budget ran out, or [`LoadError::DriverFailed`] if a user task panicked; in both cases the
/// remaining users are asked to stop and, after [`VIRTUAL_USER_STOP_GRACE`], aborted.
pub(super) async fn run_constant_virtual_users<E, F, Fut>(
    context: &DriverContext<F>,
    recovery: &mut RecoveryWatch,
    duration: Duration,
    count: usize,
) -> Result<(), LoadError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    try_emit_load_event(
        context.recorder.events(),
        context.recorder.load_name(),
        format!(
            "ConstantVus: {count} virtual users (closed model; workers, max_workers and \
             saturation policy do not apply)"
        ),
    );

    let deadline = Instant::now() + duration;
    let stop_virtual_users = Arc::new(AtomicBool::new(false));
    let mut virtual_users = JoinSet::new();
    for virtual_user_index in 0..count {
        virtual_users.spawn(run_virtual_user(
            virtual_user_index,
            context.work.clone(),
            context.recorder.clone(),
            context.shutdown_rx.clone(),
            context.paused.clone(),
            stop_virtual_users.clone(),
            deadline,
        ));
    }

    let mut outcome = Ok(());
    let mut abort_at: Option<Instant> = None;
    let mut last_tick = Instant::now();
    loop {
        let now = Instant::now();
        if context.paused.load(Ordering::Acquire) {
            recovery.credit_paused(now.saturating_duration_since(last_tick));
        }
        last_tick = now;

        let wake_at = match abort_at {
            Some(abort_at) => abort_at,
            None => match recovery.status(context.recorder.metrics()) {
                RecoveryBudget::Exhausted => {
                    stop_virtual_users.store(true, Ordering::Release);
                    outcome = Err(LoadError::RecoveryTimeout {
                        timeout: recovery.timeout(),
                    });
                    abort_at = Some(now + VIRTUAL_USER_STOP_GRACE);
                    continue;
                }
                RecoveryBudget::Remaining(remaining) => {
                    now + remaining.min(recovery.poll_interval())
                }
                RecoveryBudget::WithinTarget => now + recovery.poll_interval(),
            },
        };

        tokio::select! {
            joined = virtual_users.join_next() => match joined {
                None => break,
                Some(Ok(())) => {}
                Some(Err(join_error)) => {
                    if outcome.is_ok() {
                        outcome = Err(LoadError::DriverFailed {
                            source: Box::new(join_error),
                        });
                    }
                    stop_virtual_users.store(true, Ordering::Release);
                    abort_at.get_or_insert(Instant::now() + VIRTUAL_USER_STOP_GRACE);
                }
            },
            _ = tokio::time::sleep_until(wake_at) => {
                if abort_at.is_some_and(|abort_at| Instant::now() >= abort_at) {
                    virtual_users.abort_all();
                    while virtual_users.join_next().await.is_some() {}
                    break;
                }
            }
        }
    }
    outcome
}

/// One virtual user: run the work to completion, record it, and start again until `deadline`,
/// shutdown, or `stop_virtual_users`.
async fn run_virtual_user<E, F, Fut>(
    virtual_user_index: usize,
    work: Arc<F>,
    recorder: Arc<IterationRecorder>,
    shutdown_rx: watch::Receiver<bool>,
    paused: Arc<AtomicBool>,
    stop_virtual_users: Arc<AtomicBool>,
    deadline: Instant,
) where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    loop {
        if stop_virtual_users.load(Ordering::Acquire)
            || Instant::now() >= deadline
            || *shutdown_rx.borrow()
        {
            return;
        }
        while paused.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if *shutdown_rx.borrow() || stop_virtual_users.load(Ordering::Acquire) {
                return;
            }
        }
        recorder
            .run(
                work.as_ref(),
                IterationOrigin::VirtualUser(virtual_user_index),
            )
            .await;
    }
}
