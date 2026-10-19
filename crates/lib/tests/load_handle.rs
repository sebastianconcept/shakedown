//! Integration tests for open-model [`shakedown::LoadHandle`]: scheduling, metrics, phase events, and recovery waits.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use shakedown::{
    run_scenario, Context, EventLog, LoadError, LoadPhase, LoadProfile, LoadRuntime, Rate,
    RecoveryConfig,
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

                let started = std::time::Instant::now();
                let handle = scenario_context
                    .start_load("failing", profile, || async { Err::<(), &str>("down") })
                    .await
                    .expect("start_load");
                let error = handle.stop().await.expect_err("recovery budget");
                let elapsed = started.elapsed();
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
async fn open_model_completes_when_failures_end_before_recovery_timeout() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "load-recovery-timeout-not-yet",
        |scenario_context| {
            Box::pin(async move {
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
                let handle = scenario_context
                    .start_load("short-errors", profile, || async {
                        Err::<(), &str>("blip")
                    })
                    .await
                    .expect("start_load");
                let metrics = handle.stop().await.expect("phase ends before timeout");
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
async fn closed_model_shared_runtime_repeats_vu_iterations() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    let vu_count = 4usize;

    run_scenario(
        &mut context,
        "load-closed-model-vus",
        move |scenario_context| {
            Box::pin(async move {
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
                let iterations = Arc::new(AtomicU64::new(0));
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant_vus(Duration::from_secs(2), 2))
                    .runtime(LoadRuntime::Dedicated { threads: 2 })
                    .build()
                    .expect("profile");
                let iterations_for_work = iterations.clone();
                match scenario_context
                    .start_load("vus", profile, move || {
                        let iterations_for_work = iterations_for_work.clone();
                        async move {
                            iterations_for_work.fetch_add(1, Ordering::SeqCst);
                            Ok::<(), &str>(())
                        }
                    })
                    .await
                {
                    Err(LoadError::UnsupportedRuntime) => {}
                    Err(other) => panic!("expected UnsupportedRuntime, got {other}"),
                    Ok(handle) => {
                        let metrics = handle.stop().await;
                        panic!("dedicated runtime returned a handle ({metrics:?})");
                    }
                }
                tokio::time::sleep(Duration::from_millis(40)).await;
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
