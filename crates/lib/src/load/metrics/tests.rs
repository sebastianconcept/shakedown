//! Unit tests for bucketed histograms, warmup exclusion, and concurrent recording.

use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use super::{LoadMetrics, LoadMetricsAccumulator};

#[test]
fn set_warmup_end_records_wall_time() {
    let accumulator = LoadMetricsAccumulator::default();
    let instant = Instant::now();
    accumulator.set_warmup_end(instant);
    assert_eq!(accumulator.warmup_end_snapshot(), Some(instant));
}

#[test]
fn total_is_sum_of_outcomes() {
    let accumulator = Arc::new(LoadMetricsAccumulator::default());
    accumulator.record_success(Instant::now(), Duration::from_millis(1));
    accumulator.record_success(Instant::now(), Duration::from_millis(1));
    accumulator.record_failure(Instant::now(), Duration::from_millis(1));
    accumulator.record_dropped(Instant::now());
    assert_eq!(accumulator.total_iterations(), 4);
    let snapshot = LoadMetrics::new(accumulator);
    assert_eq!(snapshot.total_iterations(), 4);
}

#[test]
fn percentile_tracks_injected_latencies() {
    let epoch = Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for index in 0..100u64 {
        let completion = epoch + Duration::from_millis(index);
        accumulator.record_success(completion, Duration::from_micros(1000 + index * 100));
    }
    let metrics = LoadMetrics::new(accumulator);
    assert!(metrics.p50() > Duration::ZERO);
    assert!(metrics.p99() >= metrics.p50());
    assert!(metrics.max_latency() >= metrics.min_latency());
}

#[test]
fn windowed_queries_use_buckets() {
    let epoch = Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    // Bucket 0: 0..100ms
    accumulator.record_success(epoch + Duration::from_millis(10), Duration::from_millis(2));
    accumulator.record_failure(epoch + Duration::from_millis(20), Duration::from_millis(8));
    // Bucket 1: 100..200ms
    accumulator.record_success(
        epoch + Duration::from_millis(150),
        Duration::from_millis(50),
    );

    let metrics = LoadMetrics::new(accumulator);
    let w0 = epoch..(epoch + Duration::from_millis(99));
    assert!((metrics.error_rate_during(&w0) - 0.5).abs() < f64::EPSILON);
    let w1 = epoch..(epoch + Duration::from_millis(200));
    assert!((metrics.error_rate_during(&w1) - 1.0 / 3.0).abs() < 0.01);

    let throughput_window = epoch + Duration::from_millis(100)..epoch + Duration::from_millis(200);
    assert!((metrics.throughput_during(&throughput_window) - 10.0).abs() < 1.0); // 1 op / 0.1s

    let p99_early = metrics.p99_during(&(epoch..(epoch + Duration::from_millis(90))));
    let p99_late = metrics
        .p99_during(&(epoch + Duration::from_millis(100)..epoch + Duration::from_millis(200)));
    assert!(p99_late > p99_early || p99_early.is_zero());
}

#[test]
fn p99_after_excludes_earlier_buckets() {
    let epoch = Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    accumulator.record_success(epoch + Duration::from_millis(10), Duration::from_nanos(100));
    accumulator.record_success(
        epoch + Duration::from_millis(250),
        Duration::from_millis(10),
    );
    let metrics = LoadMetrics::new(accumulator);
    let after = epoch + Duration::from_millis(200);
    assert!(metrics.p99_after(after) >= Duration::from_millis(10));
}

#[test]
fn recovery_and_burst_track_fail_success_sequence() {
    let epoch = Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    let t0 = epoch + Duration::from_millis(10);
    let t1 = epoch + Duration::from_millis(20);
    let t2 = epoch + Duration::from_millis(30);
    accumulator.record_failure(t0, Duration::from_micros(100));
    accumulator.record_failure(t1, Duration::from_micros(100));
    accumulator.record_success(t2, Duration::from_micros(50));
    let metrics = LoadMetrics::new(accumulator);
    assert_eq!(
        metrics.error_burst_duration(),
        Some(Duration::from_millis(10))
    );
    assert_eq!(metrics.recovery_time(), Some(Duration::from_millis(10)));
}

#[test]
fn concurrent_record_safety() {
    let accumulator = Arc::new(LoadMetricsAccumulator::default());
    let barrier = Arc::new(Barrier::new(32));
    let mut handles = Vec::new();
    for _ in 0..32 {
        let accumulator = accumulator.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..1000 {
                accumulator.record_success(Instant::now(), Duration::from_nanos(500));
            }
        }));
    }
    for handle in handles {
        handle.join().expect("thread");
    }
    let metrics = LoadMetrics::new(accumulator);
    assert_eq!(metrics.successful_operations(), 32_000);
    assert_eq!(metrics.total_iterations(), 32_000);
}

/// Warmup traffic in bucket 0; steady in bucket 1+; `warmup_end` at bucket boundary.
#[test]
fn warmup_exclusion_uses_post_warmup_buckets_only() {
    // Given high-latency samples in bucket 0 and low-latency in bucket 1 with warmup_end at 100ms
    let epoch = Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    // Bucket 0 (0..100ms): high latency "warmup" completions
    for _ in 0..5 {
        accumulator.record_success(epoch + Duration::from_millis(10), Duration::from_millis(90));
    }
    // Bucket 1 (100..200ms): low latency steady state
    for _ in 0..10 {
        accumulator.record_success(epoch + Duration::from_millis(150), Duration::from_millis(1));
    }
    let warmup_end = epoch + Duration::from_millis(100);
    accumulator.set_warmup_end(warmup_end);
    // Ensure wall `Instant::now()` is past `warmup_end` so steady-state throughput uses a positive window.
    thread::sleep(Duration::from_millis(120));

    // When we query default (steady-state) vs including_warmup
    let steady = LoadMetrics::new(accumulator.clone());
    // Then steady counts and percentiles reflect only post-warmup traffic
    assert_eq!(steady.successful_operations(), 10);
    assert_eq!(steady.failed_operations(), 0);
    assert_eq!(steady.dropped_iterations(), 0);
    assert_eq!(steady.total_iterations(), 10);
    assert_eq!(steady.error_rate(), 0.0);
    assert!(
        steady.p99() < Duration::from_millis(50),
        "steady p99 should reflect ~1ms samples, got {:?}",
        steady.p99()
    );
    let upper = Duration::from_micros(1100);
    assert!(
        steady.min_latency() <= upper && steady.min_latency() >= Duration::from_micros(900),
        "steady min_latency ~1ms, got {:?}",
        steady.min_latency()
    );
    assert!(
        steady.max_latency() <= upper && steady.max_latency() >= Duration::from_micros(900),
        "steady max_latency ~1ms, got {:?}",
        steady.max_latency()
    );

    // And full-run view includes warmup samples in totals and tail latency
    let full = steady.including_warmup();
    assert_eq!(full.successful_operations(), 15);
    assert_eq!(full.total_iterations(), 15);
    assert!(
        full.p99() >= Duration::from_millis(80),
        "including_warmup p99 should reflect 90ms samples, got {:?}",
        full.p99()
    );
}

#[test]
fn warmup_exclusion_error_rate_uses_post_warmup_completions_only() {
    // Given failures only in the warmup bucket and successes after warmup_end
    let epoch = Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for _ in 0..4 {
        accumulator.record_failure(epoch + Duration::from_millis(20), Duration::from_millis(1));
    }
    for _ in 0..6 {
        accumulator.record_success(epoch + Duration::from_millis(150), Duration::from_millis(1));
    }
    accumulator.set_warmup_end(epoch + Duration::from_millis(100));
    // When we read steady-state error rate vs full run
    let steady = LoadMetrics::new(accumulator);
    // Then steady error rate ignores warmup failures
    assert_eq!(steady.error_rate(), 0.0);
    assert_eq!(steady.including_warmup().error_rate(), 4.0 / 10.0);
}

#[test]
fn warmup_exclusion_counts_drops_in_steady_buckets_only() {
    // Given drops in warmup and one success after warmup_end
    let epoch = Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    accumulator.record_dropped(epoch + Duration::from_millis(10));
    accumulator.record_dropped(epoch + Duration::from_millis(20));
    accumulator.record_success(
        epoch + Duration::from_millis(150),
        Duration::from_nanos(100),
    );
    accumulator.set_warmup_end(epoch + Duration::from_millis(100));
    // When we read steady vs full totals
    let steady = LoadMetrics::new(accumulator);
    // Then steady excludes warmup drops
    assert_eq!(steady.dropped_iterations(), 0);
    assert_eq!(steady.total_iterations(), 1);
    assert_eq!(steady.including_warmup().dropped_iterations(), 2);
    assert_eq!(steady.including_warmup().total_iterations(), 3);
}

#[test]
fn without_warmup_end_aggregate_path_matches_global_histogram() {
    // Given no warmup_end recorded
    let epoch = Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    accumulator.record_success(epoch + Duration::from_millis(10), Duration::from_millis(5));
    accumulator.record_success(epoch + Duration::from_millis(20), Duration::from_millis(5));
    let metrics = LoadMetrics::new(accumulator.clone());
    // When we compare default view to including_warmup
    let including = metrics.including_warmup();
    // Then aggregates are identical
    assert_eq!(metrics.p50(), including.p50());
    assert_eq!(
        metrics.successful_operations(),
        including.successful_operations()
    );
}

#[test]
fn throughput_steady_matches_completed_over_elapsed_since_warmup_end() {
    // Given steady completions after warmup_end and a short sleep for wall time
    let epoch = Instant::now();
    let accumulator = Arc::new(LoadMetricsAccumulator::new_with_epoch(epoch));
    for _ in 0..10 {
        accumulator.record_success(epoch + Duration::from_millis(150), Duration::from_micros(1));
    }
    let warmup_end = epoch + Duration::from_millis(100);
    accumulator.set_warmup_end(warmup_end);
    thread::sleep(Duration::from_millis(150));
    // When we read throughput()
    let metrics = LoadMetrics::new(accumulator);
    let elapsed = Instant::now().saturating_duration_since(warmup_end);
    // Then it approximates completed / elapsed since warmup_end
    assert!(
        !elapsed.is_zero(),
        "wall clock must be past warmup_end for throughput denominator"
    );
    let expected = 10.0 / elapsed.as_secs_f64();
    assert!(
        (metrics.throughput() - expected).abs() < expected * 0.35,
        "throughput {:?} expected ~{} (elapsed {:?})",
        metrics.throughput(),
        expected,
        elapsed
    );
}
