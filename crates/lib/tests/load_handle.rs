//! Integration tests for open-model [`shakedown::LoadHandle`]: scheduling, metrics, phase events, and recovery waits.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use shakedown::{
    run_scenario, Context, EventLog, LoadError, LoadPhase, LoadProfile, LoadRuntime, Rate,
    RecoveryConfig, SaturationPolicy,
};

#[tokio::test]
async fn open_model_ten_per_second_for_five_seconds_within_tolerance() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "load-open-model-10rps-5s",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_secs(5),
                        Rate::per_second(10.0),
                    ))
                    .workers(20)
                    .max_workers(20)
                    .build()
                    .expect("valid profile");

                let handle = scenario_context
                    .start_load("baseline", profile, || async { Ok::<(), &str>(()) })
                    .await
                    .expect("start_load");

                let metrics = handle.stop().await.expect("stop");
                let total = metrics.total_iterations();
                assert!(
                    (40..=60).contains(&total),
                    "expected ~50 total iterations (±20%), got {total}"
                );
                assert_eq!(metrics.dropped_iterations(), 0);
                assert_eq!(metrics.successful_operations(), total);
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

#[tokio::test]
async fn load_phase_events_merge_into_event_log_via_run_scenario() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "load-phase-events",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(
                        LoadPhase::constant(Duration::from_millis(150), Rate::per_second(80.0))
                            .named("burst"),
                    )
                    .workers(16)
                    .max_workers(16)
                    .build()
                    .expect("profile");

                let handle = scenario_context
                    .start_load("orders", profile, || async { Ok::<(), &str>(()) })
                    .await
                    .expect("start_load");

                handle.await_phase("burst").await.expect("await_phase");

                handle.stop().await.expect("stop");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");

    assert!(
        event_log.events().any(|entry| entry.source == "load"),
        "expected at least one load-sourced event in the merged log"
    );
}

#[tokio::test]
async fn await_recovery_resolves_after_error_burst_stops() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "load-await-recovery",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_secs(3),
                        Rate::per_second(200.0),
                    ))
                    .workers(4)
                    .max_workers(4)
                    .build()
                    .expect("profile");

                let fail_until = Arc::new(AtomicU64::new(8));
                let handle = scenario_context
                    .start_load("burst", profile, move || {
                        let fail_until = fail_until.clone();
                        async move {
                            let n = fail_until.fetch_sub(1, Ordering::SeqCst);
                            if n > 0 {
                                Err("injected failure")
                            } else {
                                Ok::<(), &str>(())
                            }
                        }
                    })
                    .await
                    .expect("start_load");

                let recovered = handle
                    .await_recovery_with_config(
                        RecoveryConfig::new(Duration::from_secs(5))
                            .with_rolling_window(Duration::from_millis(400))
                            .with_poll_interval(Duration::from_millis(20)),
                    )
                    .await
                    .expect("await_recovery");
                assert!(
                    !recovered.is_zero(),
                    "expected non-zero disruption span, got {recovered:?}"
                );

                handle.stop().await.expect("stop");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");

    assert!(event_log.events().any(|entry| {
        entry.source == "load"
            && entry.message.contains("error rate returned to")
            && entry.message.contains("disruption")
    }));
}

#[tokio::test]
async fn driver_stops_when_profile_recovery_timeout_elapses() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    let timeout = Duration::from_millis(200);

    run_scenario(
        &mut context,
        "load-recovery-timeout",
        move |scenario_context| {
            Box::pin(async move {
                // Given an open-model profile with a 200ms recovery budget and work that always fails
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_secs(4),
                        Rate::per_second(80.0),
                    ))
                    .workers(16)
                    .max_workers(16)
                    .recovery(
                        RecoveryConfig::new(timeout)
                            .with_error_rate_threshold(0.0)
                            .with_rolling_window(Duration::from_secs(1))
                            .with_poll_interval(Duration::from_millis(20)),
                    )
                    .build()
                    .expect("profile");

                // When the load runs
                let started = std::time::Instant::now();
                let handle = scenario_context
                    .start_load("failing", profile, || async { Err::<(), &str>("down") })
                    .await
                    .expect("start_load");
                let error = handle.stop().await.expect_err("recovery budget");
                let elapsed = started.elapsed();

                // Then the driver stops with the profile timeout long before the 4s phase ends
                assert!(
                    matches!(error, LoadError::RecoveryTimeout { timeout: got } if got == timeout),
                    "expected the profile recovery timeout, got {error}"
                );
                assert!(
                    elapsed < Duration::from_millis(2500),
                    "driver ignored the recovery timeout and kept running ({elapsed:?})"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

#[tokio::test]
async fn open_model_phase_shorter_than_recovery_timeout_completes() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "load-recovery-timeout-not-yet",
        |scenario_context| {
            Box::pin(async move {
                // Given a 5s recovery budget and a phase that ends after 250ms of constant failures
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_millis(250),
                        Rate::per_second(40.0),
                    ))
                    .workers(8)
                    .max_workers(8)
                    .recovery(RecoveryConfig::new(Duration::from_secs(5)))
                    .build()
                    .expect("profile");

                // When the load runs
                let handle = scenario_context
                    .start_load("short-errors", profile, || async {
                        Err::<(), &str>("blip")
                    })
                    .await
                    .expect("start_load");
                let metrics = handle.stop().await.expect("phase ends before timeout");

                // Then the run completes and the failures were still recorded
                assert!(
                    metrics.failed_operations() > 0,
                    "open-model iterations should still be recorded"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

#[tokio::test]
async fn run_without_a_recovery_budget_is_never_stopped_by_its_error_rate() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "load-no-recovery-budget",
        |scenario_context| {
            Box::pin(async move {
                // Given a profile that never opted in to a recovery budget and work that always fails
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_millis(600),
                        Rate::per_second(50.0),
                    ))
                    .workers(8)
                    .max_workers(8)
                    .build()
                    .expect("profile");

                // When the load runs through its whole phase
                let handle = scenario_context
                    .start_load("always-failing", profile, || async {
                        Err::<(), &str>("down")
                    })
                    .await
                    .expect("start_load");
                let result = handle.stop().await;

                // Then the run completes instead of failing on error rate
                let metrics = result.expect("no budget means no recovery timeout");
                assert_eq!(metrics.successful_operations(), 0);
                assert!(metrics.failed_operations() > 0);
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

#[tokio::test]
async fn open_model_queue_blocked_on_stuck_workers_still_honors_recovery_timeout() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    let timeout = Duration::from_millis(200);

    run_scenario(
        &mut context,
        "load-recovery-timeout-queue-blocked",
        move |scenario_context| {
            Box::pin(async move {
                // Given a single queued worker and work that fails three times, then hangs
                let calls = Arc::new(AtomicU64::new(0));
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_secs(5),
                        Rate::per_second(50.0),
                    ))
                    .workers(1)
                    .max_workers(1)
                    .on_saturation(SaturationPolicy::Queue)
                    .recovery(
                        RecoveryConfig::new(timeout).with_poll_interval(Duration::from_millis(20)),
                    )
                    .build()
                    .expect("profile");

                // When the scheduler blocks waiting for the hung worker
                let started = std::time::Instant::now();
                let handle = scenario_context
                    .start_load("queue-stuck", profile, move || {
                        let calls = calls.clone();
                        async move {
                            if calls.fetch_add(1, Ordering::SeqCst) < 3 {
                                Err("down")
                            } else {
                                tokio::time::sleep(Duration::from_secs(30)).await;
                                Ok(())
                            }
                        }
                    })
                    .await
                    .expect("start_load");
                let error = handle.stop().await.expect_err("recovery budget");

                // Then the budget still stops the run while the scheduler is waiting
                assert!(
                    matches!(error, LoadError::RecoveryTimeout { .. }),
                    "{error}"
                );
                assert!(
                    started.elapsed() < Duration::from_secs(3),
                    "scheduler stayed blocked on the queue ({:?})",
                    started.elapsed()
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

#[tokio::test]
async fn pausing_does_not_consume_the_recovery_budget() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "load-recovery-timeout-paused",
        |scenario_context| {
            Box::pin(async move {
                // Given a 400ms budget, work that always fails, and a 1s rolling window
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_secs(5),
                        Rate::per_second(100.0),
                    ))
                    .workers(8)
                    .max_workers(8)
                    .recovery(
                        RecoveryConfig::new(Duration::from_millis(400))
                            .with_poll_interval(Duration::from_millis(20)),
                    )
                    .build()
                    .expect("profile");
                let started = std::time::Instant::now();
                let handle = scenario_context
                    .start_load("paused", profile, || async { Err::<(), &str>("down") })
                    .await
                    .expect("start_load");

                // When the run is paused for 600ms, which is longer than the whole budget
                tokio::time::sleep(Duration::from_millis(150)).await;
                handle.pause();
                tokio::time::sleep(Duration::from_millis(600)).await;
                handle.resume();
                let error = handle.stop().await.expect_err("recovery budget");

                // Then the pause was not charged: the budget ran out only after the remaining ~250ms
                assert!(
                    matches!(error, LoadError::RecoveryTimeout { .. }),
                    "{error}"
                );
                assert!(
                    started.elapsed() >= Duration::from_millis(900),
                    "pause was charged to the budget ({:?})",
                    started.elapsed()
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

#[tokio::test]
async fn closed_model_stops_when_profile_recovery_timeout_elapses() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    let timeout = Duration::from_millis(200);

    run_scenario(
        &mut context,
        "load-closed-model-recovery-timeout",
        move |scenario_context| {
            Box::pin(async move {
                // Given four virtual users for 4s with a 200ms budget and work that always fails
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant_vus(Duration::from_secs(4), 4))
                    .recovery(
                        RecoveryConfig::new(timeout).with_poll_interval(Duration::from_millis(20)),
                    )
                    .build()
                    .expect("profile");

                // When the load runs
                let started = std::time::Instant::now();
                let handle = scenario_context
                    .start_load("vus-failing", profile, || async {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        Err::<(), &str>("down")
                    })
                    .await
                    .expect("start_load");
                let error = handle.stop().await.expect_err("recovery budget");

                // Then the users are stopped with the profile timeout long before 4s
                assert!(
                    matches!(error, LoadError::RecoveryTimeout { timeout: got } if got == timeout),
                    "expected the profile recovery timeout, got {error}"
                );
                assert!(
                    started.elapsed() < Duration::from_millis(2500),
                    "virtual users ignored the recovery timeout ({:?})",
                    started.elapsed()
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

fn explode() -> Result<(), &'static str> {
    panic!("virtual user exploded")
}

#[tokio::test]
async fn closed_model_panicking_virtual_user_fails_the_run() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "load-closed-model-panic",
        |scenario_context| {
            Box::pin(async move {
                // Given virtual users whose work panics
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant_vus(Duration::from_secs(2), 2))
                    .build()
                    .expect("profile");

                // When the load runs
                let handle = scenario_context
                    .start_load("vus-panicking", profile, || async { explode() })
                    .await
                    .expect("start_load");
                let result = handle.stop().await;

                // Then the panic is reported instead of being swallowed as a clean run
                assert!(
                    matches!(result, Err(LoadError::DriverFailed { .. })),
                    "expected DriverFailed, got {result:?}"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

#[tokio::test]
async fn closed_model_shared_runtime_repeats_vu_iterations() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    let vu_count = 4usize;

    run_scenario(
        &mut context,
        "load-closed-model-vus",
        move |scenario_context| {
            Box::pin(async move {
                // Given four virtual users for 500ms whose work takes 15ms
                let iterations = Arc::new(AtomicU64::new(0));
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant_vus(
                        Duration::from_millis(500),
                        vu_count,
                    ))
                    .workers(1)
                    .max_workers(1)
                    .build()
                    .expect("profile");
                let iterations_for_work = iterations.clone();

                // When the load runs to completion
                let handle = scenario_context
                    .start_load("vus", profile, move || {
                        let iterations_for_work = iterations_for_work.clone();
                        async move {
                            iterations_for_work.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(15)).await;
                            Ok::<(), &str>(())
                        }
                    })
                    .await
                    .expect("start_load");
                let metrics = handle.stop().await.expect("closed model finished");

                // Then every iteration was recorded and each user sent more than once
                let recorded = metrics.successful_operations();
                let observed = iterations.load(Ordering::SeqCst);
                assert_eq!(recorded, observed);
                assert!(
                    recorded >= u64::try_from(vu_count).expect("vu count") * 2,
                    "each virtual user should send, wait, and send again (recorded {recorded})"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");

    assert!(
        event_log.events().any(|entry| entry.source == "load"),
        "a closed-model run that executed should record load timeline rows"
    );
}

#[tokio::test]
async fn dedicated_runtime_closed_model_fails_before_timeline() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "load-closed-model-unsupported-runtime",
        |scenario_context| {
            Box::pin(async move {
                // Given a closed-model profile that asks for a dedicated runtime
                let iterations = Arc::new(AtomicU64::new(0));
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant_vus(Duration::from_secs(2), 2))
                    .runtime(LoadRuntime::Dedicated { threads: 2 })
                    .build()
                    .expect("profile");
                let iterations_for_work = iterations.clone();

                // When the load is started
                let result = scenario_context
                    .start_load("vus", profile, move || {
                        let iterations_for_work = iterations_for_work.clone();
                        async move {
                            iterations_for_work.fetch_add(1, Ordering::SeqCst);
                            Ok::<(), &str>(())
                        }
                    })
                    .await;
                tokio::time::sleep(Duration::from_millis(40)).await;

                // Then it is refused and no virtual user ever ran
                assert!(
                    matches!(result, Err(LoadError::UnsupportedRuntime)),
                    "expected UnsupportedRuntime"
                );
                assert_eq!(
                    iterations.load(Ordering::SeqCst),
                    0,
                    "skipped closed-model run must not execute virtual users"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");

    assert!(
        event_log.events().all(|entry| entry.source != "load"),
        "unsupported runtime must not record load timeline rows"
    );
}
