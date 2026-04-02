//! Live counters for load runs (US-007). HdrHistogram and windowed queries land in US-008.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Accumulator updated by the load driver and worker tasks (atomic counters).
#[derive(Debug, Default)]
pub struct LoadMetricsAccumulator {
    successful_operations: AtomicU64,
    failed_operations: AtomicU64,
    dropped_iterations: AtomicU64,
}

impl LoadMetricsAccumulator {
    pub(crate) fn record_success(&self) {
        self.successful_operations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_failure(&self) {
        self.failed_operations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dropped(&self) {
        self.dropped_iterations.fetch_add(1, Ordering::Relaxed);
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
}

/// Snapshot of load counters after [`super::LoadHandle::stop`](crate::load::LoadHandle::stop) or from a live handle.
#[derive(Debug, Clone)]
pub struct LoadMetrics {
    inner: Arc<LoadMetricsAccumulator>,
}

impl LoadMetrics {
    pub(crate) fn new(inner: Arc<LoadMetricsAccumulator>) -> Self {
        Self { inner }
    }

    /// All arrivals: successes, failures, and drops (before a worker ran the iteration).
    pub fn total_iterations(&self) -> u64 {
        self.inner.total_iterations()
    }

    pub fn successful_operations(&self) -> u64 {
        self.inner.successful_operations()
    }

    pub fn failed_operations(&self) -> u64 {
        self.inner.failed_operations()
    }

    pub fn dropped_iterations(&self) -> u64 {
        self.inner.dropped_iterations()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_is_sum_of_outcomes() {
        let accumulator = Arc::new(LoadMetricsAccumulator::default());
        accumulator.record_success();
        accumulator.record_success();
        accumulator.record_failure();
        accumulator.record_dropped();
        assert_eq!(accumulator.total_iterations(), 4);
        let snapshot = LoadMetrics::new(accumulator);
        assert_eq!(snapshot.total_iterations(), 4);
    }
}
