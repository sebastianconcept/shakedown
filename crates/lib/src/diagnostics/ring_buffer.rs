//! Bounded FIFO ring buffer for per-container resource samples.

use std::collections::VecDeque;

/// Fixed-capacity queue: oldest entries are dropped when full.
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
