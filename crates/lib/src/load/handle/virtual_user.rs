//! One closed-model virtual user: a loop that runs the work, records it, and starts again.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

use super::iteration::{IterationOrigin, IterationRecorder};

/// Run the work to completion, record it, and start again until `deadline`, shutdown, or
/// `stop_virtual_users`.
///
/// The stop conditions are only checked between iterations, so an iteration already running is
/// never interrupted from here; the scheduler aborts the task if it outlives its grace period.
pub(super) async fn run_virtual_user<E, F, Fut>(
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
