//! US-008: failure timeline integration for performance assertions and end-to-end load stack tests.

use std::fs;
use std::time::Duration;

use shakedown::error::{PerformanceAssertionDetails, ShakedownError};
use shakedown::timeline::{
    emit_failure_timeline_for_shakedown_error, render_failure_timeline,
    resolve_target_timelines_dir, sanitize_scenario_id_for_filename, FailureTimelineOptions,
    PerformanceAssertionFailureJson,
};
use shakedown::{
    render_comparison_markdown, render_summary, run_scenario, ComparisonResult, Context, EventLog,
    LoadBaseline, LoadPhase, LoadProfile, LoadRunReport, MetricComparison, Rate, Tolerance,
};

#[tokio::test]
async fn run_scenario_timeline_json_includes_performance_assertion_on_failure() {
    let scenario_label = "us008_timeline_perf_fail";
    let path = resolve_target_timelines_dir().join(format!(
        "{}.json",
        sanitize_scenario_id_for_filename(scenario_label)
    ));
    let _ = fs::remove_file(&path);

    let mut context = Context::new();
    let mut event_log = EventLog::new();
    let error = run_scenario(
        &mut context,
        scenario_label,
        |_ctx: &mut Context| {
            Box::pin(async {
                Err::<(), ShakedownError>(ShakedownError::PerformanceAssertionFailed(Box::new(
                    PerformanceAssertionDetails {
                        load_name: "checkout".to_string(),
                        metric: "p99".to_string(),
                        measured: "120ms".to_string(),
                        threshold: "50ms".to_string(),
                        phase: LoadPhase::constant(Duration::from_secs(1), Rate::per_second(1.0)),
                        comparison_table: Some("|x|y|\n|-|-|\n|a|b|".to_string()),
                    },
                )))
            })
        },
        &mut event_log,
    )
    .await
    .expect_err("scenario must fail");

    assert!(
        matches!(error, ShakedownError::PerformanceAssertionFailed(_)),
        "expected PerformanceAssertionFailed, got {error:?}"
    );

    let json_bytes = fs::read(&path).expect("timeline JSON should be written under target/");
    let parsed: serde_json::Value = serde_json::from_slice(&json_bytes).expect("valid json");
    assert_eq!(
        parsed["scenario"]["verdict"].as_str(),
        Some("performance_assertion_failed")
    );
    let block = parsed
        .get("performance_assertion_failure")
        .expect("performance_assertion_failure key");
    assert_eq!(block["load_name"].as_str(), Some("checkout"));
    assert_eq!(block["metric"].as_str(), Some("p99"));
    assert_eq!(block["measured"].as_str(), Some("120ms"));
    assert_eq!(block["threshold"].as_str(), Some("50ms"));
}

#[test]
fn performance_assertion_renders_in_failure_timeline_string() {
    let log = EventLog::new();
    let err = ShakedownError::PerformanceAssertionFailed(Box::new(PerformanceAssertionDetails {
        load_name: "orders".to_string(),
        metric: "throughput".to_string(),
        measured: "10/s".to_string(),
        threshold: "> 50/s".to_string(),
        phase: LoadPhase::constant(Duration::from_secs(2), Rate::per_second(20.0)),
        comparison_table: None,
    }));
    emit_failure_timeline_for_shakedown_error(&log, "us008_emit_smoke", &err);

    let mut options = FailureTimelineOptions::new("us008_render");
    options.failure_summary = Some(err.to_string());
    options.verdict = "performance_assertion_failed".to_string();
    options.performance_assertion_failure = Some(PerformanceAssertionFailureJson {
        load_name: "orders".to_string(),
        metric: "throughput".to_string(),
        measured: "10/s".to_string(),
        threshold: "> 50/s".to_string(),
        load_phase: "Constant".to_string(),
        comparison_table: None,
    });
    let rendered = render_failure_timeline(&log, &options);
    assert!(rendered.contains("--- PERFORMANCE ASSERTION ---"));
    assert!(rendered.contains("orders"));
    assert!(rendered.contains("throughput"));
}

#[tokio::test]
async fn warmup_excludes_iterations_from_steady_state_totals_end_to_end() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    run_scenario(
        &mut context,
        "us008_warmup_e2e",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::warmup(
                        Duration::from_millis(250),
                        Rate::per_second(80.0),
                    ))
                    .phase(LoadPhase::constant(
                        Duration::from_millis(400),
                        Rate::per_second(80.0),
                    ))
                    .workers(8)
                    .max_workers(8)
                    .build()
                    .expect("profile");

                let handle = scenario_context
                    .start_load("warmup_lane", profile, || async {
                        tokio::time::sleep(Duration::from_micros(200)).await;
                        Ok::<(), &str>(())
                    })
                    .await
                    .expect("start_load");

                let metrics = handle.stop().await.expect("stop");
                let steady_total = metrics.total_iterations();
                let full_total = metrics.including_warmup().total_iterations();
                assert!(
                    full_total > steady_total,
                    "including_warmup must count strictly more iterations than steady-state view when warmup ran (full={full_total}, steady={steady_total})"
                );
                Ok::<(), ShakedownError>(())
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

#[tokio::test]
async fn load_profile_rejects_warmup_not_first() {
    let result = LoadProfile::open_model()
        .phase(LoadPhase::constant(
            Duration::from_millis(50),
            Rate::per_second(10.0),
        ))
        .phase(LoadPhase::warmup(
            Duration::from_millis(50),
            Rate::per_second(10.0),
        ))
        .workers(2)
        .max_workers(2)
        .build();
    assert!(result.is_err(), "warmup not first must be rejected");
}

#[tokio::test]
async fn performance_assertion_pass_and_fail_via_run_scenario_result() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    run_scenario(
        &mut context,
        "us008_assert_pass",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_millis(200),
                        Rate::per_second(40.0),
                    ))
                    .workers(4)
                    .max_workers(4)
                    .build()
                    .expect("profile");
                let handle = scenario_context
                    .start_load("p", profile, || async {
                        tokio::time::sleep(Duration::from_micros(100)).await;
                        Ok::<(), &str>(())
                    })
                    .await
                    .expect("start");
                let metrics = handle.stop().await.expect("stop");
                metrics
                    .assert("p")
                    .p99_under(Duration::from_secs(2))
                    .expect("loose bound should pass");
                Ok::<(), ShakedownError>(())
            })
        },
        &mut event_log,
    )
    .await
    .expect("pass");

    let mut context = Context::new();
    let mut event_log = EventLog::new();
    let fail = run_scenario(
        &mut context,
        "us008_assert_fail",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_millis(200),
                        Rate::per_second(40.0),
                    ))
                    .workers(4)
                    .max_workers(4)
                    .build()
                    .expect("profile");
                let handle = scenario_context
                    .start_load("p", profile, || async {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        Ok::<(), &str>(())
                    })
                    .await
                    .expect("start");
                let metrics = handle.stop().await.expect("stop");
                metrics
                    .assert("p")
                    .p99_under(Duration::from_nanos(1))
                    .map(|_| ())?;
                Ok::<(), ShakedownError>(())
            })
        },
        &mut event_log,
    )
    .await
    .expect_err("must fail");

    assert!(
        matches!(fail, ShakedownError::PerformanceAssertionFailed(_)),
        "{fail:?}"
    );
}

#[tokio::test]
async fn render_summary_contains_core_fields() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    run_scenario(
        &mut context,
        "us008_summary",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_millis(120),
                        Rate::per_second(30.0),
                    ))
                    .workers(2)
                    .max_workers(2)
                    .build()
                    .expect("profile");
                let handle = scenario_context
                    .start_load("summary_load", profile, || async { Ok::<(), &str>(()) })
                    .await
                    .expect("start");
                let metrics = handle.stop().await.expect("stop");
                let text = render_summary("summary_load", &metrics);
                assert!(text.contains("summary_load") || text.contains("Load Summary"));
                assert!(text.to_lowercase().contains("p50"));
                assert!(text.to_lowercase().contains("p99"));
                Ok::<(), ShakedownError>(())
            })
        },
        &mut event_log,
    )
    .await
    .expect("run");
}

#[tokio::test]
async fn load_run_report_round_trip() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    run_scenario(
        &mut context,
        "us008_report_rt",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_millis(150),
                        Rate::per_second(25.0),
                    ))
                    .workers(2)
                    .max_workers(2)
                    .build()
                    .expect("profile");
                let handle = scenario_context
                    .start_load("rep", profile, || async {
                        tokio::time::sleep(Duration::from_micros(50)).await;
                        Ok::<(), &str>(())
                    })
                    .await
                    .expect("start");
                let metrics = handle.stop().await.expect("stop");
                let path = metrics
                    .write_report("rep", "us008_report_rt")
                    .expect("write report");
                let bytes = fs::read(&path).expect("read report");
                let decoded: LoadRunReport = serde_json::from_slice(&bytes).expect("decode");
                assert_eq!(decoded.scenario_id, "us008_report_rt");
                assert_eq!(decoded.load_name, "rep");
                assert!(!decoded.histogram_v2_base64.is_empty());
                Ok::<(), ShakedownError>(())
            })
        },
        &mut event_log,
    )
    .await
    .expect("run");
}

#[tokio::test]
async fn baseline_save_load_round_trip() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    run_scenario(
        &mut context,
        "us008_baseline_rt",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_millis(120),
                        Rate::per_second(20.0),
                    ))
                    .workers(2)
                    .max_workers(2)
                    .build()
                    .expect("profile");
                let handle = scenario_context
                    .start_load("base", profile, || async {
                        tokio::time::sleep(Duration::from_micros(80)).await;
                        Ok::<(), &str>(())
                    })
                    .await
                    .expect("start");
                let metrics = handle.stop().await.expect("stop");
                metrics
                    .save_baseline("golden", "us008_baseline_rt")
                    .expect("save");
                let loaded = LoadBaseline::load("us008_baseline_rt", "golden").expect("load");
                assert_eq!(loaded.baseline_name, "golden");
                assert_eq!(loaded.scenario_id, "us008_baseline_rt");
                assert!(!loaded.histogram_v2_base64.is_empty());
                Ok::<(), ShakedownError>(())
            })
        },
        &mut event_log,
    )
    .await
    .expect("run");
}

#[tokio::test]
async fn regression_detection_within_and_beyond_tolerance() {
    let mut context = Context::new();
    let mut event_log = EventLog::new();
    run_scenario(
        &mut context,
        "us008_regression_setup",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_millis(200),
                        Rate::per_second(40.0),
                    ))
                    .workers(4)
                    .max_workers(4)
                    .build()
                    .expect("profile");
                let handle = scenario_context
                    .start_load("reg", profile, || async {
                        tokio::time::sleep(Duration::from_micros(150)).await;
                        Ok::<(), &str>(())
                    })
                    .await
                    .expect("start");
                let metrics = handle.stop().await.expect("stop");
                metrics
                    .save_baseline("line", "us008_regression_setup")
                    .expect("save baseline");
                Ok::<(), ShakedownError>(())
            })
        },
        &mut event_log,
    )
    .await
    .expect("setup run");

    let mut context = Context::new();
    let mut event_log = EventLog::new();
    run_scenario(
        &mut context,
        "us008_regression_pass",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_millis(200),
                        Rate::per_second(40.0),
                    ))
                    .workers(4)
                    .max_workers(4)
                    .build()
                    .expect("profile");
                let handle = scenario_context
                    .start_load("reg", profile, || async {
                        tokio::time::sleep(Duration::from_micros(150)).await;
                        Ok::<(), &str>(())
                    })
                    .await
                    .expect("start");
                let metrics = handle.stop().await.expect("stop");
                metrics
                    .assert("reg")
                    .no_regression_from("line", "us008_regression_setup", Tolerance::percent(0.25))
                    .expect("same-shape run should stay within 25% tolerance");
                Ok::<(), ShakedownError>(())
            })
        },
        &mut event_log,
    )
    .await
    .expect("within tolerance");

    let mut context = Context::new();
    let mut event_log = EventLog::new();
    let slow = run_scenario(
        &mut context,
        "us008_regression_fail",
        |scenario_context| {
            Box::pin(async move {
                let profile = LoadProfile::open_model()
                    .phase(LoadPhase::constant(
                        Duration::from_millis(200),
                        Rate::per_second(40.0),
                    ))
                    .workers(4)
                    .max_workers(4)
                    .build()
                    .expect("profile");
                let handle = scenario_context
                    .start_load("reg", profile, || async {
                        tokio::time::sleep(Duration::from_millis(12)).await;
                        Ok::<(), &str>(())
                    })
                    .await
                    .expect("start");
                let metrics = handle.stop().await.expect("stop");
                metrics
                    .assert("reg")
                    .no_regression_from("line", "us008_regression_setup", Tolerance::percent(0.05))
                    .map(|_| ())?;
                Ok::<(), ShakedownError>(())
            })
        },
        &mut event_log,
    )
    .await
    .expect_err("slow run must regress under 5% tolerance");

    assert!(
        matches!(slow, ShakedownError::PerformanceAssertionFailed(_)),
        "{slow:?}"
    );
}

#[test]
fn markdown_comparison_table_renders_headers() {
    let sample = ComparisonResult {
        baseline_name: "line".to_string(),
        tolerance: Tolerance::percent(0.1),
        metrics: vec![MetricComparison {
            name: "p99".to_string(),
            baseline_value: "1ms".to_string(),
            current_value: "50ms".to_string(),
            change_pct: 49.0,
            threshold_pct: 0.1,
            passed: false,
            limit_description: "≤ baseline × 1.10".to_string(),
        }],
        passed: false,
    };
    let md = render_comparison_markdown(&sample);
    assert!(md.contains("p99") || md.contains('|'));
}
