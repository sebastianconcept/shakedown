//! Integration tests for open-model [`shakedown::LoadHandle`]: scheduling, metrics, phase events, and recovery waits.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use shakedown::{run_scenario, Context, EventLog, LoadPhase, LoadProfile, Rate, RecoveryConfig};

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
