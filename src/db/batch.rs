use crate::db::config::WriterConfig;
use std::time::Instant;

/// Key used for sharding + dynamic table selection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchKey {
    pub exchange: String,
    pub stream: String,
    pub symbol: String,
}

#[derive(Debug, Clone)]
pub struct Batch<T> {
    pub key: BatchKey,
    pub enqueued_at: Instant,
    pub rows: Vec<T>,

    /// Flush threshold (runtime-tunable)
    pub flush_rows: usize,

    /// Max time to wait before flushing (runtime-tunable)
    pub flush_interval_ms: u64,

    /// Safety cap: prevents unbounded growth when DB is down
    pub hard_cap_rows: usize,

    /// Maximum rows per db insert
    pub chunk_rows: usize,
}

impl<T> Batch<T> {
    pub fn new(key: BatchKey, rows: Vec<T>, cfg: &WriterConfig) -> Self {
        let flush_rows = cfg.batch_size.max(1);
        let hard_cap_rows = cfg.hard_batch_size.max(1);
        let flush_interval_ms = cfg.flush_interval_ms;
        let chunk_rows = cfg.chunk_rows.max(1);

        let mut s = Self {
            key,
            enqueued_at: Instant::now(),
            rows,
            flush_rows,
            flush_interval_ms,
            hard_cap_rows,
            chunk_rows,
        };

        // Ensure we respect cap even if rows is pre-filled
        s.enforce_cap();
        s
    }

    pub fn set_flush_rows(&mut self, flush_rows: usize) {
        self.flush_rows = flush_rows.max(1);
        // No dropping here: changing flush policy should not discard buffered rows.
        // If flush_rows shrinks, caller can call should_flush() and flush immediately.
    }

    pub fn set_flush_interval_ms(&mut self, flush_interval_ms: u64) {
        self.flush_interval_ms = flush_interval_ms;
        // Same: don’t drop. This just changes when should_flush() becomes true.
    }

    /// Optional: allow changing cap at runtime (safe; drops only if now over cap)
    pub fn set_hard_cap_rows(&mut self, hard_cap_rows: usize) {
        self.hard_cap_rows = hard_cap_rows.max(1);
        self.enforce_cap();
    }

    pub fn extend(&mut self, rows: Vec<T>) {
        self.rows.extend(rows);
    }

    fn enforce_cap(&mut self) {
        if self.rows.len() > self.hard_cap_rows {
            let excess = self.rows.len() - self.hard_cap_rows;
            self.rows.drain(0..excess); // drop oldest overflow only
        }
    }

    /// Flush decision uses internal knobs (no args).
    pub fn should_flush(&self) -> bool {
        if self.rows.is_empty() {
            return false;
        }

        let flush_due = (self.enqueued_at.elapsed().as_millis() as u64) >= self.flush_interval_ms;

        self.rows.len() >= self.flush_rows || flush_due
    }

    /// Move buffered rows out (empties the batch) and resets timer.
    pub fn take_rows(&mut self) -> Vec<T> {
        self.enqueued_at = Instant::now();
        std::mem::take(&mut self.rows)
    }

    /// Call when you clear manually on success (alternative to take_rows()).
    pub fn reset_timer(&mut self) {
        self.enqueued_at = Instant::now();
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

