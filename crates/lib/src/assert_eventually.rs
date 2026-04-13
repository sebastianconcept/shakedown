//! Fixed-interval polling assertion helper ([`assert_eventually`]) for asynchronous conditions.

use std::time::{Duration, Instant};

use tokio::time::sleep;

use crate::error::{AssertionError, InternalError, ScenarioError, ShakedownError};
use crate::event::Event;
use crate::lifecycle::Context;

/// Poll `check` until it returns `Ok(())` or `timeout` elapses.
///
/// Uses fixed-interval polling ([`sleep`] after each failed attempt): the interval stays constant
/// so tests get predictable spacing and simple timing reasoning; this helper does not implement
/// exponential backoff.
///
/// The closure must be [`Fn`] — use `Arc<Mutex<_>>` inside the async block if you need
/// mutable state across attempts.
///
/// While [`run_scenario`] is executing the scenario body (run phase), failed attempts are appended
/// to the [`crate::EventLog`] when `context` has a run-phase event sender — pass the same
/// [`Context`] you receive in your scenario so polling attempts show up on the failure timeline
/// alongside other scenario events.
///
/// # Errors
///
/// Returns [`AssertionError::EventuallyTimeout`] when the deadline is exceeded, including the last
/// error from `check`. Returns [`InternalError::InvalidConfiguration`] when `poll_interval` is zero.
pub async fn assert_eventually<F, Fut>(
    description: &str,
    timeout: Duration,
    poll_interval: Duration,
    context: &Context,
    check: F,
) -> Result<(), ShakedownError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<(), ShakedownError>>,
{
    if poll_interval.is_zero() {
        return Err(ShakedownError::Internal(
            InternalError::InvalidConfiguration(
                "assert_eventually: poll_interval must be greater than zero".to_string(),
            ),
        ));
    }

    let start = Instant::now();
    let mut attempts: u32 = 0;

    loop {
        attempts = attempts.saturating_add(1);
        let last_error_message: String;
        match check().await {
            Ok(()) => {
                if attempts > 1 {
                    push_scenario_event(
                        context,
                        Event::info(
                            "assert_eventually",
                            format!(
                                "(phase: run) '{description}' succeeded after {attempts} attempts"
                            ),
                        ),
                    );
                }
                return Ok(());
            }
            Err(error) => {
                let message = error.to_string();
                last_error_message = message.clone();
                push_scenario_event(
                    context,
                    Event::debug(
                        "assert_eventually",
                        format!(
                            "(phase: run) poll attempt {attempts} for '{description}': {message}"
                        ),
                    ),
                );
            }
        }

        let elapsed = start.elapsed();
        if elapsed >= timeout {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::EventuallyTimeout {
                    description: description.to_string(),
                    timeout,
                    elapsed,
                    attempts,
                    last_error: last_error_message,
                },
            )));
        }

        let remaining = timeout.saturating_sub(elapsed);
        let sleep_for = poll_interval.min(remaining);
        sleep(sleep_for).await;
    }
}

fn push_scenario_event(context: &Context, event: Event) {
    if let Some(sender) = context.scenario_application_event_sender() {
        let _unused = sender.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::assert_eventually;
    use crate::error::{AssertionError, ScenarioError, ShakedownError};
    use crate::lifecycle::Context;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn succeeds_immediately() {
        // Given a context and a check that always succeeds
        let context = Context::new();
        // When we poll until success
        assert_eventually(
            "always ok",
            Duration::from_secs(10),
            Duration::from_millis(10),
            &context,
            || async { Ok(()) },
        )
        .await
        // Then the helper returns Ok on the first attempt
        .unwrap();
    }

    #[tokio::test]
    async fn succeeds_after_n_polls() {
        // Given a check that fails twice then succeeds
        let context = Context::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_for_closure = Arc::clone(&counter);
        // When we poll with enough timeout for three attempts
        assert_eventually(
            "third time lucky",
            Duration::from_secs(10),
            Duration::from_millis(5),
            &context,
            move || {
                let counter_for_async = Arc::clone(&counter_for_closure);
                async move {
                    let next = counter_for_async.fetch_add(1, Ordering::SeqCst) + 1;
                    if next >= 3 {
                        Ok(())
                    } else {
                        Err(ShakedownError::Scenario(ScenarioError::Assertion(
                            AssertionError::Unspecified {
                                detail: format!("not yet ({next})"),
                            },
                        )))
                    }
                }
            },
        )
        .await
        .unwrap();
        // Then we observed exactly three poll attempts
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn timeout_reports_fields() {
        // Given a check that never succeeds within the deadline
        let context = Context::new();
        // When assert_eventually exhausts the timeout
        let error = assert_eventually(
            "never ready",
            Duration::from_millis(80),
            Duration::from_millis(25),
            &context,
            || async {
                Err(ShakedownError::Scenario(ScenarioError::Assertion(
                    AssertionError::Unspecified {
                        detail: "still false".to_string(),
                    },
                )))
            },
        )
        .await
        .unwrap_err();

        // Then the error is EventuallyTimeout with populated fields
        match error {
            ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::EventuallyTimeout {
                    description,
                    timeout,
                    attempts,
                    last_error,
                    ..
                },
            )) => {
                assert_eq!(description, "never ready");
                assert_eq!(timeout, Duration::from_millis(80));
                assert!(attempts >= 2);
                assert!(
                    last_error.contains("still false"),
                    "last_error={last_error:?}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn zero_poll_interval_is_rejected() {
        // Given a zero poll interval (invalid configuration)
        let context = Context::new();
        // When we call assert_eventually
        let error = assert_eventually(
            "bad interval",
            Duration::from_secs(1),
            Duration::ZERO,
            &context,
            || async { Ok(()) },
        )
        .await
        .unwrap_err();
        // Then we get an internal invalid-configuration error
        assert!(matches!(error, ShakedownError::Internal(_)));
    }
}
