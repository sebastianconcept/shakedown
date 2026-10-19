//! Closed-model scheduling: a fixed set of virtual users, each sending again as soon as its last
//! iteration finishes.
//!
//! Virtual users run on the shared runtime and ignore `workers`, `max_workers`, and the saturation
//! policy: concurrency is the user count.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::load::LoadError;

use super::driver::DriverContext;
use super::events::try_emit_load_event;
use super::recovery::{RecoveryBudget, RecoveryWatch};
use super::virtual_user::run_virtual_user;

/// How long virtual users may finish an in-flight iteration once they have been told to stop
/// before their tasks are aborted.
///
/// The clock starts when the phase duration elapses, shutdown arrives, the recovery budget runs
/// out, or a user panics, whichever comes first.
const VIRTUAL_USER_STOP_GRACE: Duration = Duration::from_secs(5);

/// Run `count` virtual users for `duration`.
///
/// Returns when every user has stopped. A user mid-iteration when the phase ends is given
/// [`VIRTUAL_USER_STOP_GRACE`] to finish, then aborted (its iteration is not recorded), so work
/// that never completes cannot hold the phase open. Fails with [`LoadError::RecoveryTimeout`] if
/// the recovery budget ran out, or [`LoadError::DriverFailed`] if a user task panicked; in both
/// cases the remaining users are asked to stop and, after the same grace, aborted.
///
/// Time the run spends paused is credited back to the recovery budget when the pause is
/// observed to end. A pause is noticed on the next scheduler tick (at most one
/// [`RecoveryWatch::poll_interval`] later), so up to one interval of each pause is still charged.
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
    let mut pause_started_at: Option<Instant> = None;
    loop {
        let now = Instant::now();
        let paused = context.paused.load(Ordering::Acquire);
        match (paused, pause_started_at) {
            (true, None) => pause_started_at = Some(now),
            (false, Some(started_at)) => {
                recovery.credit_paused(now.saturating_duration_since(started_at));
                pause_started_at = None;
            }
            _ => {}
        }

        if abort_at.is_none() {
            if now >= deadline {
                abort_at = Some(deadline + VIRTUAL_USER_STOP_GRACE);
            } else if *context.shutdown_rx.borrow() {
                abort_at = Some(now + VIRTUAL_USER_STOP_GRACE);
            }
        }

        let wake_at = match abort_at {
            Some(abort_at) => abort_at,
            // A paused run completes no work, so its error rate cannot move: skip the budget.
            None if paused => (now + recovery.poll_interval()).min(deadline),
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
                    (now + remaining.min(recovery.poll_interval())).min(deadline)
                }
                RecoveryBudget::WithinTarget => (now + recovery.poll_interval()).min(deadline),
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
                    try_emit_load_event(
                        context.recorder.events(),
                        context.recorder.load_name(),
                        format!(
                            "ConstantVus: aborting {} virtual users still running {:?} after \
                             being told to stop",
                            virtual_users.len(),
                            VIRTUAL_USER_STOP_GRACE
                        ),
                    );
                    virtual_users.abort_all();
                    while virtual_users.join_next().await.is_some() {}
                    break;
                }
            }
        }
    }
    outcome
}
