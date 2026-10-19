use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::load::metrics::LoadMetricsAccumulator;
use crate::load::RecoveryConfig;

use super::{RecoveryBudget, RecoveryWatch};

/// Metrics whose epoch is a few milliseconds old; a window read within the first millisecond of
/// a run is empty by construction.
fn fresh_metrics() -> Arc<LoadMetricsAccumulator> {
    let metrics = Arc::new(LoadMetricsAccumulator::default());
    std::thread::sleep(Duration::from_millis(5));
    metrics
}

fn record_failures(metrics: &Arc<LoadMetricsAccumulator>, count: usize) {
    for _ in 0..count {
        metrics.record_failure(Instant::now(), Duration::from_millis(1));
    }
}

fn record_successes(metrics: &Arc<LoadMetricsAccumulator>, count: usize) {
    for _ in 0..count {
        metrics.record_success(Instant::now(), Duration::from_millis(1));
    }
}

fn remaining(budget: RecoveryBudget) -> Duration {
    match budget {
        RecoveryBudget::Remaining(left) => left,
        other => panic!("expected an open episode, got {other:?}"),
    }
}

#[test]
fn unenforced_budget_never_reports_an_episode() {
    // Given a watch with no profile budget and a run that only fails
    let metrics = fresh_metrics();
    record_failures(&metrics, 10);
    let mut watch = RecoveryWatch::new(None);

    // When the status is read
    let budget = watch.status(&metrics);

    // Then the run is within target
    assert_eq!(budget, RecoveryBudget::WithinTarget);
}

#[test]
fn error_rate_above_target_opens_an_episode_with_the_whole_timeout_left() {
    // Given a budget of 400ms and a run whose every iteration failed
    let timeout = Duration::from_millis(400);
    let metrics = fresh_metrics();
    record_failures(&metrics, 5);
    let mut watch = RecoveryWatch::new(Some(&RecoveryConfig::new(timeout)));

    // When the status is read for the first time
    let budget = watch.status(&metrics);

    // Then an episode has just started and nearly all of the timeout remains
    assert!(remaining(budget) > Duration::from_millis(350));
}

#[test]
fn episode_is_exhausted_once_it_lasts_the_timeout() {
    // Given an open episode on a 60ms budget
    let metrics = fresh_metrics();
    record_failures(&metrics, 5);
    let configuration =
        RecoveryConfig::new(Duration::from_millis(60)).with_poll_interval(Duration::from_millis(5));
    let mut watch = RecoveryWatch::new(Some(&configuration));
    remaining(watch.status(&metrics));

    // When the errors keep arriving past the timeout
    std::thread::sleep(Duration::from_millis(80));
    record_failures(&metrics, 5);
    let budget = watch.status(&metrics);

    // Then the budget is exhausted
    assert_eq!(budget, RecoveryBudget::Exhausted);
}

#[test]
fn episode_restarts_with_a_full_budget_after_the_rate_returns_to_target() {
    // Given an open episode on a 600ms budget with a 100ms rolling window (one metrics bucket)
    let metrics = fresh_metrics();
    record_failures(&metrics, 5);
    let configuration = RecoveryConfig::new(Duration::from_millis(600))
        .with_rolling_window(Duration::from_millis(100))
        .with_poll_interval(Duration::from_millis(5));
    let mut watch = RecoveryWatch::new(Some(&configuration));
    remaining(watch.status(&metrics));

    // When the window empties, then errors start again and the next poll is due
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(watch.status(&metrics), RecoveryBudget::WithinTarget);
    record_failures(&metrics, 5);
    std::thread::sleep(Duration::from_millis(10));
    let budget = watch.status(&metrics);

    // Then the new episode has a full budget instead of continuing the old one
    assert!(remaining(budget) > Duration::from_millis(500));
}

#[test]
fn error_rate_at_or_below_a_non_zero_target_never_opens_an_episode() {
    // Given a 50% target and a run where one iteration in four failed
    let metrics = fresh_metrics();
    record_failures(&metrics, 1);
    record_successes(&metrics, 3);
    let configuration =
        RecoveryConfig::new(Duration::from_millis(50)).with_error_rate_threshold(0.5);
    let mut watch = RecoveryWatch::new(Some(&configuration));

    // When the status is read after longer than the timeout
    watch.status(&metrics);
    std::thread::sleep(Duration::from_millis(80));
    let budget = watch.status(&metrics);

    // Then the run stays within target
    assert_eq!(budget, RecoveryBudget::WithinTarget);
}

#[test]
fn paused_time_is_credited_back_to_the_open_episode() {
    // Given an open episode on a 300ms budget
    let metrics = fresh_metrics();
    record_failures(&metrics, 5);
    let configuration = RecoveryConfig::new(Duration::from_millis(300))
        .with_poll_interval(Duration::from_millis(5));
    let mut watch = RecoveryWatch::new(Some(&configuration));
    remaining(watch.status(&metrics));

    // When the run is paused for 150ms and that time is credited
    std::thread::sleep(Duration::from_millis(150));
    watch.credit_paused(Duration::from_millis(150));
    let budget = watch.status(&metrics);

    // Then the pause did not eat into the budget
    assert!(remaining(budget) > Duration::from_millis(250));
}

#[test]
fn exhaustion_is_confirmed_against_a_fresh_reading_between_polls() {
    // Given an episode on a 50ms budget polled only every 10s, with a 100ms rolling window
    let metrics = fresh_metrics();
    record_failures(&metrics, 5);
    let configuration = RecoveryConfig::new(Duration::from_millis(50))
        .with_rolling_window(Duration::from_millis(100))
        .with_poll_interval(Duration::from_secs(10));
    let mut watch = RecoveryWatch::new(Some(&configuration));
    remaining(watch.status(&metrics));

    // When the errors stop and the timeout passes before the next scheduled poll
    std::thread::sleep(Duration::from_millis(250));
    let budget = watch.status(&metrics);

    // Then the stale episode is not reported as exhausted
    assert_eq!(budget, RecoveryBudget::WithinTarget);
}
