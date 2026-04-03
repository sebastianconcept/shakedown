//! Bounded FIFO ring buffer for per-container resource samples.
//!
//! [`ResourceStatsMonitor`](super::ResourceStatsMonitor) keeps one [`RingBuffer`] per container so
//! repeated stats polling does not grow memory unbounded over a long scenario.

use std::collections::VecDeque;

/// Bounded FIFO queue holding the most recent `capacity` items of type `T`.
///
/// Exists so diagnostics can retain a sliding window of values (e.g. [`crate::event::InfraEvent::ResourceSample`])
/// with fixed upper bound: when the queue is full, [`RingBuffer::push`] drops the oldest entry.
/// That keeps memory predictable during long scenarios while still exposing recent peaks and trends for
/// assertions and failure timelines.
///
/// One ring is stored per Docker container id in the diagnostics stats map; at shutdown the harness
/// drains rings in order and merges them into the [`crate::event::EventLog`]. Assertions read the same
/// rings via [`crate::lifecycle::Context::resource_stats_rings`] during the run/assert phases.
#[derive(Debug)]
pub(crate) struct RingBuffer<T> {
    inner: VecDeque<T>,
    capacity: usize,
}

impl<T> RingBuffer<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            inner: VecDeque::new(),
            capacity,
        }
    }

    pub(crate) fn push(&mut self, item: T) {
        if self.inner.len() == self.capacity {
            self.inner.pop_front();
        }
        self.inner.push_back(item);
    }

    /// Drain in chronological order (oldest first).
    pub(crate) fn drain_ordered(&mut self) -> Vec<T> {
        self.inner.drain(..).collect()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.inner.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drops_oldest_when_full() {
        let mut ring = RingBuffer::new(2);
        ring.push(1u8);
        ring.push(2u8);
        ring.push(3u8);
        let drained: Vec<_> = ring.drain_ordered();
        assert_eq!(drained, vec![2, 3]);
    }
}
