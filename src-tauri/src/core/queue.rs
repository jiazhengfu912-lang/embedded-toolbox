use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub trait ByteSize {
    fn byte_size(&self) -> usize;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    Reject,
    DropOldest,
    ClearToLowWater,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PushOutcome {
    pub dropped_items: usize,
    pub dropped_bytes: usize,
}

struct Inner<T> {
    items: VecDeque<T>,
    bytes: usize,
    closed: bool,
}

pub struct ByteQueue<T: ByteSize> {
    inner: Mutex<Inner<T>>,
    ready: Condvar,
    max_bytes: usize,
    max_items: usize,
    low_water_bytes: usize,
    bytes: AtomicUsize,
}

impl<T: ByteSize> ByteQueue<T> {
    pub fn new(max_bytes: usize, max_items: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                items: VecDeque::new(),
                bytes: 0,
                closed: false,
            }),
            ready: Condvar::new(),
            max_bytes,
            max_items,
            low_water_bytes: max_bytes / 2,
            bytes: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, item: T, policy: OverflowPolicy) -> Result<PushOutcome, T> {
        let item_bytes = item.byte_size();
        let mut inner = self.inner.lock();
        if inner.closed || item_bytes > self.max_bytes {
            return Err(item);
        }
        let mut outcome = PushOutcome::default();
        let would_overflow = |inner: &Inner<T>| {
            inner.bytes + item_bytes > self.max_bytes || inner.items.len() + 1 > self.max_items
        };
        if would_overflow(&inner) {
            match policy {
                OverflowPolicy::Reject => return Err(item),
                OverflowPolicy::DropOldest => {
                    while would_overflow(&inner) {
                        let Some(old) = inner.items.pop_front() else {
                            break;
                        };
                        let old_bytes = old.byte_size();
                        inner.bytes = inner.bytes.saturating_sub(old_bytes);
                        outcome.dropped_items += 1;
                        outcome.dropped_bytes += old_bytes;
                    }
                }
                OverflowPolicy::ClearToLowWater => {
                    while inner.bytes > self.low_water_bytes
                        || inner.items.len() >= self.max_items / 2
                    {
                        let Some(old) = inner.items.pop_front() else {
                            break;
                        };
                        let old_bytes = old.byte_size();
                        inner.bytes = inner.bytes.saturating_sub(old_bytes);
                        outcome.dropped_items += 1;
                        outcome.dropped_bytes += old_bytes;
                    }
                }
            }
        }
        inner.bytes += item_bytes;
        inner.items.push_back(item);
        self.bytes.store(inner.bytes, Ordering::Relaxed);
        self.ready.notify_one();
        Ok(outcome)
    }

    pub fn pop_timeout(&self, timeout: Duration) -> Option<T> {
        let deadline = Instant::now() + timeout;
        let mut inner = self.inner.lock();
        loop {
            if let Some(item) = inner.items.pop_front() {
                inner.bytes = inner.bytes.saturating_sub(item.byte_size());
                self.bytes.store(inner.bytes, Ordering::Relaxed);
                return Some(item);
            }
            if inner.closed {
                return None;
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            self.ready.wait_for(&mut inner, deadline - now);
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        let mut inner = self.inner.lock();
        let item = inner.items.pop_front()?;
        inner.bytes = inner.bytes.saturating_sub(item.byte_size());
        self.bytes.store(inner.bytes, Ordering::Relaxed);
        Some(item)
    }

    pub fn close(&self) {
        let mut inner = self.inner.lock();
        inner.closed = true;
        self.ready.notify_all();
    }

    pub fn depth_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn depth_items(&self) -> usize {
        self.inner.lock().items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct Item(usize);
    impl ByteSize for Item {
        fn byte_size(&self) -> usize {
            self.0
        }
    }

    #[test]
    fn consumers_have_explicit_overflow_policy() {
        let q = ByteQueue::new(10, 3);
        q.push(Item(4), OverflowPolicy::Reject).unwrap();
        q.push(Item(4), OverflowPolicy::Reject).unwrap();
        assert!(q.push(Item(4), OverflowPolicy::Reject).is_err());
        let outcome = q.push(Item(4), OverflowPolicy::DropOldest).unwrap();
        assert_eq!(outcome.dropped_items, 1);
        assert_eq!(q.depth_bytes(), 8);
    }
}
