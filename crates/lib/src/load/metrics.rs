//! Live [`LoadMetrics`] snapshots: atomic counters, time-bucketed HdrHistograms (100ms), windowed queries.

use std::cmp;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use super::{ReportAssertion, ReportComparison};

/// 100ms buckets for windowed percentile merges (same grid the load driver uses for steady-state reconstruction).
const BUCKET_MS: u128 = 100;

/// One time bucket: latency histogram plus per-bucket outcome counters (windowed error rate / throughput).
#[derive(Debug)]
struct TimeBucket {
    latency_ns: Mutex<Histogram<u64>>,
    successful: AtomicU64,
    failed: AtomicU64,
    dropped: AtomicU64,
}

impl Default for TimeBucket {
    fn default() -> Self {
        Self {
            latency_ns: Mutex::new(new_latency_histogram()),
            successful: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }
}

fn new_latency_histogram() -> Histogram<u64> {
    // 1 ns .. 60 s, 3 significant figures — operation latency in nanoseconds.
    Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("latency histogram bounds")
}

/// Single read pass over buckets from `min_index`: merged histogram + outcome sums.
#[derive(Debug)]
struct MergedBucketsSnapshot {
    histogram: Histogram<u64>,
    histogram_non_empty: bool,
    successful: u64,
    failed: u64,
    dropped: u64,
}

/// Tracks longest failure burst and last recovery interval (topology / disruption metrics).
#[derive(Debug, Clone, Default)]
struct BurstState {
    /// First failure in the current consecutive failure run.
    first_failure_in_burst: Option<Instant>,
    /// Last failure in the current consecutive failure run.
    last_failure_in_burst: Option<Instant>,
    longest_burst: Option<Duration>,
    last_recovery: Option<Duration>,
}

impl BurstState {
    fn on_failure(&mut self, now: Instant) {
        if self.first_failure_in_burst.is_none() {
            self.first_failure_in_burst = Some(now);
        }
        self.last_failure_in_burst = Some(now);
    }

    fn on_success(&mut self, now: Instant) {
        if let Some(last_fail) = self.last_failure_in_burst {
            self.last_recovery = Some(now.duration_since(last_fail));
        }
        if let (Some(first), Some(last)) = (self.first_failure_in_burst, self.last_failure_in_burst)
        {
            let burst = last.duration_since(first);
            self.longest_burst = Some(match self.longest_burst {
                Some(previous) => cmp::max(previous, burst),
                None => burst,
            });
        }
        self.first_failure_in_burst = None;
        self.last_failure_in_burst = None;
    }
}

/// Accumulator updated by the load driver and worker tasks: atomics + bucketed histograms.
#[derive(Debug)]
pub struct LoadMetricsAccumulator {
    epoch: Instant,
    /// Wall time after the warmup phase completes (`None` if no warmup or load stopped early).
    warmup_end: Mutex<Option<Instant>>,
    successful_operations: AtomicU64,
    failed_operations: AtomicU64,
    dropped_iterations: AtomicU64,
    /// Wall time of the first failed iteration in this run (feeds recovery and disruption-duration metrics).
    first_failure_instant: Mutex<Option<Instant>>,
    /// Combined latency for all completed iterations (success + failure) — global percentiles.
    global_latency_ns: Mutex<Histogram<u64>>,
    min_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
    buckets: RwLock<Vec<TimeBucket>>,
    burst: Mutex<BurstState>,
    /// Set when a load run starts from [`super::handle::start_load`](super::handle::start_load).
    load_profile_summary: Mutex<Option<String>>,
    /// Optional assertion rows to embed in JSON reports.
    report_assertions: Mutex<Option<Vec<ReportAssertion>>>,
    /// Optional baseline comparison block for JSON reports.
    report_comparison: Mutex<Option<ReportComparison>>,
}

impl Default for LoadMetricsAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadMetricsAccumulator {
    /// Wall-clock origin for bucket indices and windowed queries (set when the load run starts).
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
            warmup_end: Mutex::new(None),
            successful_operations: AtomicU64::new(0),
            failed_operations: AtomicU64::new(0),
            dropped_iterations: AtomicU64::new(0),
            first_failure_instant: Mutex::new(None),
            global_latency_ns: Mutex::new(new_latency_histogram()),
            min_latency_ns: AtomicU64::new(u64::MAX),
            max_latency_ns: AtomicU64::new(0),
            buckets: RwLock::new(Vec::new()),
            burst: Mutex::new(BurstState::default()),
            load_profile_summary: Mutex::new(None),
            report_assertions: Mutex::new(None),
            report_comparison: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_epoch(epoch: Instant) -> Self {
        Self {
            epoch,
            warmup_end: Mutex::new(None),
            successful_operations: AtomicU64::new(0),
            failed_operations: AtomicU64::new(0),
            dropped_iterations: AtomicU64::new(0),
            first_failure_instant: Mutex::new(None),
            global_latency_ns: Mutex::new(new_latency_histogram()),
            min_latency_ns: AtomicU64::new(u64::MAX),
            max_latency_ns: AtomicU64::new(0),
            buckets: RwLock::new(Vec::new()),
            burst: Mutex::new(BurstState::default()),
            load_profile_summary: Mutex::new(None),
            report_assertions: Mutex::new(None),
            report_comparison: Mutex::new(None),
        }
    }

    pub(crate) fn set_load_profile_summary(&self, summary: String) {
        let mut guard = self
            .load_profile_summary
            .lock()
            .expect("load_profile_summary");
        *guard = Some(summary);
    }

    pub(crate) fn load_profile_summary(&self) -> Option<String> {
        self.load_profile_summary
            .lock()
            .expect("load_profile_summary")
            .clone()
    }

    pub(crate) fn set_report_assertions(&self, assertions: Option<Vec<ReportAssertion>>) {
        let mut guard = self.report_assertions.lock().expect("report_assertions");
        *guard = assertions;
    }

    pub(crate) fn set_report_comparison(&self, comparison: Option<ReportComparison>) {
        let mut guard = self.report_comparison.lock().expect("report_comparison");
        *guard = comparison;
    }

    pub(crate) fn report_assertions_snapshot(&self) -> Option<Vec<ReportAssertion>> {
        self.report_assertions
            .lock()
            .expect("report_assertions")
            .clone()
    }

    pub(crate) fn report_comparison_snapshot(&self) -> Option<ReportComparison> {
        self.report_comparison
            .lock()
            .expect("report_comparison")
            .clone()
    }

    pub(crate) fn wall_clock_origin(&self) -> Instant {
        self.epoch
    }

    /// Mark the end of the warmup phase for steady-state metric queries.
    pub(crate) fn set_warmup_end(&self, instant: Instant) {
        let mut guard = self.warmup_end.lock().expect("warmup_end");
        *guard = Some(instant);
    }

    #[cfg(test)]
    pub(crate) fn warmup_end_snapshot(&self) -> Option<Instant> {
        *self.warmup_end.lock().expect("warmup_end")
    }

    fn ensure_bucket(&self, index: usize) {
        let mut guard = self.buckets.write().expect("bucket vec");
        while guard.len() <= index {
            guard.push(TimeBucket::default());
        }
    }

    pub(crate) fn bucket_index(&self, at: Instant) -> usize {
        let millis = at.saturating_duration_since(self.epoch).as_millis();
        (millis / BUCKET_MS) as usize
    }

    fn update_min_max_ns(&self, latency_ns: u64) {
        update_atomic_min(&self.min_latency_ns, latency_ns);
        update_atomic_max(&self.max_latency_ns, latency_ns);
    }

    /// Record a successful iteration completed at `completed_at` with measured `latency`.
    pub(crate) fn record_success(&self, completed_at: Instant, latency: Duration) {
        self.record_completion(completed_at, latency, CompletionKind::Success);
    }

    /// Record a failed iteration completed at `completed_at` with measured `latency`.
    pub(crate) fn record_failure(&self, completed_at: Instant, latency: Duration) {
        self.record_completion(completed_at, latency, CompletionKind::Failure);
    }

    fn record_completion(&self, completed_at: Instant, latency: Duration, kind: CompletionKind) {
        let latency_ns = latency.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.successful_operations.fetch_add(
            u64::from(kind == CompletionKind::Success),
            Ordering::Relaxed,
        );
        self.failed_operations.fetch_add(
            u64::from(kind == CompletionKind::Failure),
            Ordering::Relaxed,
        );

        {
            let mut global = self.global_latency_ns.lock().expect("global histogram");
            let _ = global.record(latency_ns);
        }
        self.update_min_max_ns(latency_ns);

        let index = self.bucket_index(completed_at);
        self.ensure_bucket(index);
        let buckets = self.buckets.read().expect("buckets read");
        if let Some(bucket) = buckets.get(index) {
            {
                let mut histogram = bucket.latency_ns.lock().expect("bucket histogram");
                let _ = histogram.record(latency_ns);
            }
            match kind {
                CompletionKind::Success => {
                    bucket.successful.fetch_add(1, Ordering::Relaxed);
                }
                CompletionKind::Failure => {
                    bucket.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        match kind {
            CompletionKind::Success => {
                let mut burst = self.burst.lock().expect("burst");
                burst.on_success(completed_at);
            }
            CompletionKind::Failure => {
                {
                    let mut first = self.first_failure_instant.lock().expect("first_failure");
                    if first.is_none() {
                        *first = Some(completed_at);
                    }
                }
                let mut burst = self.burst.lock().expect("burst");
                burst.on_failure(completed_at);
            }
        }
    }

    /// Drop at scheduling time (no worker latency).
    pub(crate) fn record_dropped(&self, at: Instant) {
        self.dropped_iterations.fetch_add(1, Ordering::Relaxed);
        let index = self.bucket_index(at);
        self.ensure_bucket(index);
        let buckets = self.buckets.read().expect("buckets read");
        if let Some(bucket) = buckets.get(index) {
            bucket.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Sum of scheduled arrivals: completed (success + failure) plus dropped before execution.
    pub fn total_iterations(&self) -> u64 {
        self.successful_operations.load(Ordering::Relaxed)
            + self.failed_operations.load(Ordering::Relaxed)
            + self.dropped_iterations.load(Ordering::Relaxed)
    }

    pub fn successful_operations(&self) -> u64 {
        self.successful_operations.load(Ordering::Relaxed)
    }

    pub fn failed_operations(&self) -> u64 {
        self.failed_operations.load(Ordering::Relaxed)
    }

    pub fn dropped_iterations(&self) -> u64 {
        self.dropped_iterations.load(Ordering::Relaxed)
    }

    fn completed_operations(&self) -> u64 {
        self.successful_operations() + self.failed_operations()
    }

    fn epoch(&self) -> Instant {
        self.epoch
    }

    fn merge_window_histogram(&self, range: &Range<Instant>) -> Option<Histogram<u64>> {
        let bucket_range = self.bucket_range_for_window(range);
        if bucket_range.is_empty() {
            return None;
        }
        let buckets = self.buckets.read().expect("buckets read");
        let mut merged = new_latency_histogram();
        let mut any = false;
        for index in bucket_range {
            if let Some(bucket) = buckets.get(index) {
                let guard = bucket.latency_ns.lock().expect("bucket hist");
                if !guard.is_empty() {
                    merged.add(&*guard).expect("merge histograms");
                    any = true;
                }
            }
        }
        if any {
            Some(merged)
        } else {
            None
        }
    }

    fn merge_from_bucket_index(&self, min_index: usize) -> Option<Histogram<u64>> {
        let merged_counts = self.merge_histogram_and_counts_from_bucket_index(min_index);
        if merged_counts.histogram_non_empty {
            Some(merged_counts.histogram)
        } else {
            None
        }
    }

    /// Merge latency histogram and sum outcome counters from `min_index` through the last bucket.
    fn merge_histogram_and_counts_from_bucket_index(
        &self,
        min_index: usize,
    ) -> MergedBucketsSnapshot {
        let buckets = self.buckets.read().expect("buckets read");
        let mut merged = new_latency_histogram();
        let mut histogram_non_empty = false;
        let mut successful = 0u64;
        let mut failed = 0u64;
        let mut dropped = 0u64;
        for bucket in buckets.iter().skip(min_index) {
            successful += bucket.successful.load(Ordering::Relaxed);
            failed += bucket.failed.load(Ordering::Relaxed);
            dropped += bucket.dropped.load(Ordering::Relaxed);
            let guard = bucket.latency_ns.lock().expect("bucket hist");
            if !guard.is_empty() {
                merged.add(&*guard).expect("merge histograms");
                histogram_non_empty = true;
            }
        }
        MergedBucketsSnapshot {
            histogram: merged,
            histogram_non_empty,
            successful,
            failed,
            dropped,
        }
    }

    /// First bucket index included in steady-state queries when warmup ended at `warmup_end`.
    pub(crate) fn steady_bucket_start_index(&self, force_full_run: bool) -> usize {
        if force_full_run {
            return 0;
        }
        match *self.warmup_end.lock().expect("warmup_end") {
            None => 0,
            Some(warmup_end) => self.bucket_index(warmup_end),
        }
    }

    /// `true` when aggregate queries should use global histogram + atomics (no warmup exclusion).
    pub(crate) fn use_global_aggregate_path(&self, force_full_run: bool) -> bool {
        force_full_run || self.warmup_end.lock().expect("warmup_end").is_none()
    }

    /// Wall-clock start of steady-state window for throughput.
    pub(crate) fn steady_state_throughput_origin(&self, force_full_run: bool) -> Instant {
        if force_full_run {
            return self.epoch;
        }
        match *self.warmup_end.lock().expect("warmup_end") {
            Some(warmup_end) => warmup_end,
            None => self.epoch,
        }
    }

    /// Inclusive `start`, exclusive `end` in wall time; aligns to 100ms buckets that overlap the window.
    fn bucket_range_for_window(&self, window: &Range<Instant>) -> Range<usize> {
        if window.end <= window.start {
            return 0..0;
        }
        let start_ms = window
            .start
            .saturating_duration_since(self.epoch)
            .as_millis();
        let end_ms = window.end.saturating_duration_since(self.epoch).as_millis();
        if end_ms <= start_ms {
            return 0..0;
        }
        let i_min = (start_ms / BUCKET_MS) as usize;
        let i_max = ((end_ms.saturating_sub(1)) / BUCKET_MS) as usize;
        i_min..(i_max.saturating_add(1))
    }

    fn window_totals(&self, window: &Range<Instant>) -> (u64, u64, u64) {
        let range = self.bucket_range_for_window(window);
        if range.is_empty() {
            return (0, 0, 0);
        }
        let buckets = self.buckets.read().expect("buckets read");
        let mut success = 0u64;
        let mut fail = 0u64;
        let mut drop = 0u64;
        for index in range {
            if let Some(bucket) = buckets.get(index) {
                success += bucket.successful.load(Ordering::Relaxed);
                fail += bucket.failed.load(Ordering::Relaxed);
                drop += bucket.dropped.load(Ordering::Relaxed);
            }
        }
        (success, fail, drop)
    }

    fn burst_snapshot(&self) -> BurstState {
        self.burst.lock().expect("burst").clone()
    }

    /// Instant of the first failed iteration in this load run, if any.
    pub(crate) fn first_failure_instant(&self) -> Option<Instant> {
        *self.first_failure_instant.lock().expect("first_failure")
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CompletionKind {
    Success,
    Failure,
}

fn update_atomic_min(cell: &AtomicU64, value: u64) {
    let mut current = cell.load(Ordering::Relaxed);
    while value < current {
        match cell.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(c) => current = c,
        }
    }
}

fn update_atomic_max(cell: &AtomicU64, value: u64) {
    let mut current = cell.load(Ordering::Relaxed);
    while value > current {
        match cell.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(c) => current = c,
        }
    }
}

fn duration_from_histogram_quantile(histogram: &Histogram<u64>, quantile: f64) -> Duration {
    if histogram.is_empty() {
        return Duration::ZERO;
    }
    let nanoseconds = histogram.value_at_quantile(quantile);
    Duration::from_nanos(nanoseconds)
}

/// Snapshot of load metrics (live or final): counters, HdrHistogram percentiles, windowed queries.
#[derive(Debug, Clone)]
pub struct LoadMetrics {
    inner: Arc<LoadMetricsAccumulator>,
    /// When `true`, aggregate queries use the full run from [`LoadMetricsAccumulator::epoch`], ignoring warmup exclusion.
    force_full_run: bool,
}

impl LoadMetrics {
    pub(crate) fn new(inner: Arc<LoadMetricsAccumulator>) -> Self {
        Self {
            inner,
            force_full_run: false,
        }
    }

    pub(crate) fn accumulator(&self) -> &Arc<LoadMetricsAccumulator> {
        &self.inner
    }

    /// Metrics view that includes warmup iterations in aggregates (same underlying run as `self`).
    pub fn including_warmup(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            force_full_run: true,
        }
    }

    pub(crate) fn clone_latency_histogram_for_report(&self) -> Histogram<u64> {
        if self.inner.use_global_aggregate_path(self.force_full_run) {
            return self
                .inner
                .global_latency_ns
                .lock()
                .expect("global_latency_ns")
                .clone();
        }
        self.merged_steady_buckets().histogram
    }

    /// Write a JSON run report to `target/shakedown/reports/{scenario_id}/{load_name}.json`.
    pub fn write_report(
        &self,
        load_name: impl AsRef<str>,
        scenario_id: impl AsRef<str>,
    ) -> Result<std::path::PathBuf, crate::ShakedownError> {
        super::report::write_report(self, load_name.as_ref(), scenario_id.as_ref())
    }

    /// Write a named baseline JSON to `target/shakedown/baselines/{scenario_id}/{name}.json`.
    pub fn save_baseline(
        &self,
        name: impl AsRef<str>,
        scenario_id: impl AsRef<str>,
    ) -> Result<std::path::PathBuf, crate::ShakedownError> {
        super::baseline::save_baseline(self, name.as_ref(), scenario_id.as_ref())
    }

    /// Attach assertion rows that [`Self::write_report`] will serialize.
    pub fn set_report_assertions(&self, assertions: Vec<ReportAssertion>) {
        self.inner.set_report_assertions(Some(assertions));
    }

    /// Attach a baseline comparison block for [`Self::write_report`].
    pub fn set_report_comparison(&self, comparison: ReportComparison) {
        self.inner.set_report_comparison(Some(comparison));
    }

    fn merged_steady_buckets(&self) -> MergedBucketsSnapshot {
        let min_index = self.inner.steady_bucket_start_index(self.force_full_run);
        self.inner
            .merge_histogram_and_counts_from_bucket_index(min_index)
    }

    /// All arrivals: successes, failures, and drops (before a worker ran the iteration).
    pub fn total_iterations(&self) -> u64 {
        if self.inner.use_global_aggregate_path(self.force_full_run) {
            return self.inner.total_iterations();
        }
        let merged = self.merged_steady_buckets();
        merged.successful + merged.failed + merged.dropped
    }

    pub fn successful_operations(&self) -> u64 {
        if self.inner.use_global_aggregate_path(self.force_full_run) {
            return self.inner.successful_operations();
        }
        self.merged_steady_buckets().successful
    }

    pub fn failed_operations(&self) -> u64 {
        if self.inner.use_global_aggregate_path(self.force_full_run) {
            return self.inner.failed_operations();
        }
        self.merged_steady_buckets().failed
    }

    pub fn dropped_iterations(&self) -> u64 {
        if self.inner.use_global_aggregate_path(self.force_full_run) {
            return self.inner.dropped_iterations();
        }
        self.merged_steady_buckets().dropped
    }

    /// Failed completed iterations / completed iterations (success + failure). Excludes drops.
    pub fn error_rate(&self) -> f64 {
        if self.inner.use_global_aggregate_path(self.force_full_run) {
            let completed = self.inner.completed_operations();
            if completed == 0 {
                return 0.0;
            }
            return self.inner.failed_operations() as f64 / completed as f64;
        }
        let merged = self.merged_steady_buckets();
        let completed = merged.successful + merged.failed;
        if completed == 0 {
            return 0.0;
        }
        merged.failed as f64 / completed as f64
    }

    /// Completed iterations per second since the load accumulator was created (wall clock).
    ///
    /// When a warmup phase completed, uses elapsed wall time since [`LoadMetricsAccumulator::set_warmup_end`].
    pub fn throughput(&self) -> f64 {
        if self.inner.use_global_aggregate_path(self.force_full_run) {
            let elapsed = self.inner.epoch().elapsed();
            if elapsed.is_zero() {
                return 0.0;
            }
            return self.inner.completed_operations() as f64 / elapsed.as_secs_f64();
        }
        let merged = self.merged_steady_buckets();
        let completed = merged.successful + merged.failed;
        let origin = self
            .inner
            .steady_state_throughput_origin(self.force_full_run);
        let elapsed = Instant::now().saturating_duration_since(origin);
        if elapsed.is_zero() {
            return 0.0;
        }
        completed as f64 / elapsed.as_secs_f64()
    }

    pub fn p50(&self) -> Duration {
        self.percentile(0.5)
    }

    pub fn p90(&self) -> Duration {
        self.percentile(0.90)
    }

    pub fn p95(&self) -> Duration {
        self.percentile(0.95)
    }

    pub fn p99(&self) -> Duration {
        self.percentile(0.99)
    }

    fn percentile(&self, quantile: f64) -> Duration {
        if self.inner.use_global_aggregate_path(self.force_full_run) {
            let guard = self
                .inner
                .global_latency_ns
                .lock()
                .expect("global histogram");
            return duration_from_histogram_quantile(&guard, quantile);
        }
        let merged = self.merged_steady_buckets();
        if !merged.histogram_non_empty {
            return Duration::ZERO;
        }
        duration_from_histogram_quantile(&merged.histogram, quantile)
    }

    pub fn min_latency(&self) -> Duration {
        if self.inner.use_global_aggregate_path(self.force_full_run) {
            let min_ns = self.inner.min_latency_ns.load(Ordering::Relaxed);
            if min_ns == u64::MAX {
                return Duration::ZERO;
            }
            return Duration::from_nanos(min_ns);
        }
        let merged = self.merged_steady_buckets();
        if !merged.histogram_non_empty {
            return Duration::ZERO;
        }
        let min_ns = merged.histogram.min();
        Duration::from_nanos(min_ns)
    }

    pub fn max_latency(&self) -> Duration {
        if self.inner.use_global_aggregate_path(self.force_full_run) {
            return Duration::from_nanos(self.inner.max_latency_ns.load(Ordering::Relaxed));
        }
        let merged = self.merged_steady_buckets();
        if !merged.histogram_non_empty {
            return Duration::ZERO;
        }
        Duration::from_nanos(merged.histogram.max())
    }

    /// Failed / completed in the window (bucket-aligned overlap).
    pub fn error_rate_during(&self, window: &Range<Instant>) -> f64 {
        let (success, fail, _drop) = self.inner.window_totals(window);
        let completed = success + fail;
        if completed == 0 {
            return 0.0;
        }
        fail as f64 / completed as f64
    }

    pub fn p99_during(&self, window: &Range<Instant>) -> Duration {
        match self.inner.merge_window_histogram(window) {
            Some(histogram) => duration_from_histogram_quantile(&histogram, 0.99),
            None => Duration::ZERO,
        }
    }

    /// p99 over all iterations completed at or after `instant` (bucket-aligned).
    pub fn p99_after(&self, instant: Instant) -> Duration {
        let index = self.inner.bucket_index(instant);
        match self.inner.merge_from_bucket_index(index) {
            Some(histogram) => duration_from_histogram_quantile(&histogram, 0.99),
            None => Duration::ZERO,
        }
    }

    /// Completed iterations per second over the window duration (not bucket-aligned rate).
    pub fn throughput_during(&self, window: &Range<Instant>) -> f64 {
        let duration = window.end.saturating_duration_since(window.start);
        if duration.is_zero() {
            return 0.0;
        }
        let (success, fail, _drop) = self.inner.window_totals(window);
        let completed = success + fail;
        completed as f64 / duration.as_secs_f64()
    }

    /// Duration from the last recorded failure to the first subsequent success (most recent recovery).
    pub fn recovery_time(&self) -> Option<Duration> {
        self.inner.burst_snapshot().last_recovery
    }

    /// Longest span from first to last failure within a consecutive failure burst (then cleared by a success).
    pub fn error_burst_duration(&self) -> Option<Duration> {
        self.inner.burst_snapshot().longest_burst
    }

    /// Failed / completed in the last `window` of wall time (relative to [`Instant::now()`]).
    pub fn error_rate_in_window(&self, window: Duration) -> f64 {
        let now = Instant::now();
        let epoch = self.inner.epoch();
        let start = now.checked_sub(window).unwrap_or(epoch);
        let range_start = if start < epoch { epoch } else { start };
        self.error_rate_during(&(range_start..now))
    }

    /// First failure instant in this load run (for disruption span / [`LoadHandle::await_recovery`](crate::load::LoadHandle::await_recovery)).
    pub fn first_failure_instant(&self) -> Option<Instant> {
        self.inner.first_failure_instant()
    }
}

#[cfg(test)]
mod tests;
