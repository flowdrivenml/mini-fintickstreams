use std::{collections::HashMap, sync::Arc};

use crate::app::dependencies::AppDeps;
use crate::app::stream_types::{StreamId, StreamSpec, StreamStatus};
use crate::error::{AppError, AppResult};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Runtime handle for a running stream task (WS or HTTP poll).
#[derive(Debug)]
pub struct StreamHandle {
    pub spec: StreamSpec,
    pub status: StreamStatus,
    /// Cancels this specific stream.
    pub cancel: CancellationToken,
    /// Join handle for the task running this stream loop.
    pub task: Option<JoinHandle<()>>,
    /// Runtime knobs (controller side).
    pub knobs: tokio::sync::watch::Sender<StreamKnobs>,
    /// Optional: task that reacts to knob changes (batch tuning, etc.)
    pub knobs_tasks: Vec<JoinHandle<()>>,
}

/// Runtime-configurable behavior flags for the running stream.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StreamKnobs {
    pub disable_db_writes: bool,
    pub disable_redis_publishes: bool,
    // NEW
    pub flush_rows: usize, // when to flush buffered rows
    pub flush_interval_ms: u64,
    pub chunk_rows: usize,    // rows per SQL INSERT chunk
    pub hard_cap_rows: usize, // memory safety cap
}

impl Default for StreamKnobs {
    fn default() -> Self {
        Self {
            disable_db_writes: false,
            disable_redis_publishes: false,
            flush_rows: 1000,
            flush_interval_ms: 1000,
            chunk_rows: 1000,
            hard_cap_rows: 5000,
        }
    }
}

impl StreamKnobs {
    pub fn from_deps(deps: Arc<AppDeps>) -> Self {
        let mut ob = StreamKnobs::default();

        // If db exists: read from db.cfg.writer, otherwise use your defaults
        ob.flush_rows = deps
            .db
            .as_ref()
            .map(|db| db.cfg.as_ref().writer.batch_size)
            .unwrap_or(1000);

        ob.hard_cap_rows = deps
            .db
            .as_ref()
            .map(|db| db.cfg.as_ref().writer.hard_batch_size)
            .unwrap_or(10000);

        ob.flush_interval_ms = deps
            .db
            .as_ref()
            .map(|db| db.cfg.as_ref().writer.flush_interval_ms)
            .unwrap_or(50);

        ob.chunk_rows = deps
            .db
            .as_ref()
            .map(|db| db.cfg.as_ref().writer.chunk_rows)
            .unwrap_or(5000);

        ob
    }
}

impl StreamHandle {
    pub fn new(
        spec: StreamSpec,
        status: StreamStatus,
        cancel: CancellationToken,
        task: JoinHandle<()>,
        knobs: tokio::sync::watch::Sender<StreamKnobs>,
        knobs_tasks: Vec<JoinHandle<()>>,
    ) -> Self {
        Self {
            spec,
            status,
            cancel,
            task: Some(task),
            knobs,
            knobs_tasks,
        }
    }
}

impl StreamHandle {
    pub fn disable_db_writes(&self) {
        let _ = self.knobs.send_modify(|k| k.disable_db_writes = true);
    }

    pub fn enable_db_writes(&self) {
        let _ = self.knobs.send_modify(|k| k.disable_db_writes = false);
    }

    pub fn set_db_writes_enabled(&self, enabled: bool) {
        let _ = self.knobs.send_modify(|k| k.disable_db_writes = !enabled);
    }

    pub fn disable_redis_publishes(&self) {
        let _ = self.knobs.send_modify(|k| k.disable_redis_publishes = true);
    }

    pub fn enable_redis_publishes(&self) {
        let _ = self
            .knobs
            .send_modify(|k| k.disable_redis_publishes = false);
    }

    pub fn set_redis_publishes_enabled(&self, enabled: bool) {
        let _ = self
            .knobs
            .send_modify(|k| k.disable_redis_publishes = !enabled);
    }

    // ---------------------------
    // NEW: batching knob setters
    // ---------------------------

    /// Set number of rows required to trigger a flush.
    pub fn set_flush_rows(&self, rows: usize) {
        let rows = rows.max(1);
        let _ = self.knobs.send_modify(|k| k.flush_rows = rows);
    }

    /// Set maximum time (ms) before a batch is flushed.
    pub fn set_flush_interval_ms(&self, interval_ms: u64) {
        let _ = self
            .knobs
            .send_modify(|k| k.flush_interval_ms = interval_ms);
    }

    /// Set hard cap on buffered rows (drop-oldest policy).
    pub fn set_hard_cap_rows(&self, rows: usize) {
        let rows = rows.max(1);
        let _ = self.knobs.send_modify(|k| k.hard_cap_rows = rows);
    }

    /// Set number of rows per INSERT chunk.
    pub fn set_chunk_rows(&self, rows: usize) {
        let rows = rows.max(1);
        let _ = self.knobs.send_modify(|k| k.chunk_rows = rows);
    }

    // ---------------------------
    // Convenience: set all batch knobs at once
    // ---------------------------

    pub fn set_batch_knobs(
        &self,
        flush_rows: usize,
        flush_interval_ms: u64,
        hard_cap_rows: usize,
        chunk_rows: usize,
    ) {
        let flush_rows = flush_rows.max(1);
        let hard_cap_rows = hard_cap_rows.max(1);

        let _ = self.knobs.send_modify(|k| {
            k.flush_rows = flush_rows;
            k.flush_interval_ms = flush_interval_ms;
            k.hard_cap_rows = hard_cap_rows;
            k.chunk_rows = chunk_rows;
        });
    }

    // ---------------------------
    // Existing helpers
    // ---------------------------

    pub fn set_knobs(&self, knobs: StreamKnobs) {
        let _ = self.knobs.send(knobs);
    }

    pub fn knobs_snapshot(&self) -> StreamKnobs {
        *self.knobs.borrow()
    }
}

#[derive(Debug, Default)]
struct AppStateInner {
    streams: HashMap<StreamId, StreamHandle>,
}

/// Shared runtime mutable state.
///
/// - `shutdown`: cancels the whole app (propagate to streams).
/// - `inner.streams`: registry of currently running stream tasks.
#[derive(Clone, Debug)]
pub struct AppState {
    pub shutdown: CancellationToken,
    inner: Arc<RwLock<AppStateInner>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            shutdown: CancellationToken::new(),
            inner: Arc::new(RwLock::new(AppStateInner::default())),
        }
    }

    // --------------------------------------------------
    // Registry ops (used by control plane)
    // --------------------------------------------------

    pub async fn contains(&self, id: &StreamId) -> bool {
        self.inner.read().await.streams.contains_key(id)
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.streams.len()
    }

    pub async fn list(&self) -> Vec<(StreamId, StreamStatus, StreamSpec)> {
        let inner = self.inner.read().await;
        inner
            .streams
            .iter()
            .map(|(id, h)| (id.clone(), h.status.clone(), h.spec.clone()))
            .collect()
    }

    /// Insert a newly spawned stream handle.
    /// Returns Err if the stream id already exists.
    pub async fn insert(&self, id: StreamId, handle: StreamHandle) -> AppResult<()> {
        let mut inner = self.inner.write().await;
        if inner.streams.contains_key(&id) {
            return Err(AppError::StreamAlreadyExists(id.to_string()));
        }
        inner.streams.insert(id, handle);
        Ok(())
    }

    /// NOTE: This does NOT cancel or await the task. Use `stop_and_remove` for graceful shutdown.
    pub async fn remove(&self, id: &StreamId) -> Option<StreamHandle> {
        self.inner.write().await.streams.remove(id)
    }

    pub async fn stop(&self, id: &StreamId) -> AppResult<bool> {
        // 1) mark stopping + signal cancel
        let task = {
            let mut inner = self.inner.write().await;
            let h = match inner.streams.get_mut(id) {
                Some(h) => h,
                None => return Ok(false),
            };
            h.status = StreamStatus::Stopping;
            h.cancel.cancel();
            h.task.take()
        };

        // 2) await outside the lock
        if let Some(jh) = task {
            match jh.await {
                Ok(()) => {}
                Err(e) => {
                    self.set_status(
                        id,
                        StreamStatus::Failed {
                            last_error: e.to_string(),
                        },
                    )
                    .await;
                    return Err(e.into()); // if you have JoinError -> AppError
                }
            }
        }

        // 3) mark stopped
        self.set_status(id, StreamStatus::Stopped).await;
        Ok(true)
    }

    pub async fn stop_and_remove(&self, id: &StreamId) -> AppResult<bool> {
        let existed = self.stop(id).await?;
        if existed {
            self.remove(id).await;
        }
        Ok(existed)
    }

    /// Update status for an existing stream.
    pub async fn set_status(&self, id: &StreamId, status: StreamStatus) {
        let mut inner = self.inner.write().await;
        if let Some(h) = inner.streams.get_mut(id) {
            h.status = status;
        }
    }

    // --------------------------------------------------
    // Shutdown helpers
    // --------------------------------------------------

    /// Cancel all streams (does not await join handles).
    pub async fn cancel_all_streams(&self) {
        let inner = self.inner.read().await;
        for (_, h) in inner.streams.iter() {
            h.cancel.cancel();
        }
    }

    /// Cancel app shutdown token (call once on graceful shutdown).
    pub fn trigger_shutdown(&self) {
        self.shutdown.cancel();
    }
}

// ---------------------------
// Helpers
// ---------------------------

impl AppState {
    pub async fn with_handle<R>(
        &self,
        id: &StreamId,
        f: impl FnOnce(&StreamHandle) -> R,
    ) -> Option<R> {
        let inner = self.inner.read().await;
        inner.streams.get(id).map(f)
    }
    pub async fn is_stream_cancelled(&self, id: &StreamId) -> bool {
        let inner = self.inner.read().await;
        inner
            .streams
            .get(id)
            .map(|h| h.cancel.is_cancelled())
            .unwrap_or(false)
    }
}
// --------------------------------------------------
// Knobs helpers (by StreamId)
// --------------------------------------------------

impl AppState {
    // --------------------------------------------------
    // Knobs helpers (by StreamId)
    // --------------------------------------------------

    /// Set all knobs for a stream. Returns Ok(false) if stream not found.
    pub async fn set_knobs(
        &self,
        id: &StreamId,
        knobs: crate::app::state::StreamKnobs,
    ) -> AppResult<bool> {
        let inner = self.inner.read().await;
        let Some(h) = inner.streams.get(id) else {
            return Ok(false);
        };
        h.set_knobs(knobs);
        Ok(true)
    }

    /// Snapshot current knobs. Returns None if stream not found.
    pub async fn knobs_snapshot(&self, id: &StreamId) -> Option<crate::app::state::StreamKnobs> {
        self.with_handle(id, |h| h.knobs_snapshot()).await
    }

    // ---------------------------
    // DB writes
    // ---------------------------

    pub async fn set_db_writes_enabled(&self, id: &StreamId, enabled: bool) -> AppResult<bool> {
        let inner = self.inner.read().await;
        let Some(h) = inner.streams.get(id) else {
            return Ok(false);
        };
        h.set_db_writes_enabled(enabled);
        Ok(true)
    }

    pub async fn disable_db_writes(&self, id: &StreamId) -> AppResult<bool> {
        self.set_db_writes_enabled(id, false).await
    }

    pub async fn enable_db_writes(&self, id: &StreamId) -> AppResult<bool> {
        self.set_db_writes_enabled(id, true).await
    }

    // ---------------------------
    // Redis publishes
    // ---------------------------

    pub async fn set_redis_publishes_enabled(
        &self,
        id: &StreamId,
        enabled: bool,
    ) -> AppResult<bool> {
        let inner = self.inner.read().await;
        let Some(h) = inner.streams.get(id) else {
            return Ok(false);
        };
        h.set_redis_publishes_enabled(enabled);
        Ok(true)
    }

    pub async fn disable_redis_publishes(&self, id: &StreamId) -> AppResult<bool> {
        self.set_redis_publishes_enabled(id, false).await
    }

    pub async fn enable_redis_publishes(&self, id: &StreamId) -> AppResult<bool> {
        self.set_redis_publishes_enabled(id, true).await
    }

    // ---------------------------
    // Batching knobs
    // ---------------------------

    pub async fn set_flush_rows(&self, id: &StreamId, rows: usize) -> AppResult<bool> {
        let inner = self.inner.read().await;
        let Some(h) = inner.streams.get(id) else {
            return Ok(false);
        };
        h.set_flush_rows(rows);
        Ok(true)
    }

    pub async fn set_flush_interval_ms(&self, id: &StreamId, interval_ms: u64) -> AppResult<bool> {
        let inner = self.inner.read().await;
        let Some(h) = inner.streams.get(id) else {
            return Ok(false);
        };
        h.set_flush_interval_ms(interval_ms);
        Ok(true)
    }

    pub async fn set_hard_cap_rows(&self, id: &StreamId, rows: usize) -> AppResult<bool> {
        let inner = self.inner.read().await;
        let Some(h) = inner.streams.get(id) else {
            return Ok(false);
        };
        h.set_hard_cap_rows(rows);
        Ok(true)
    }

    pub async fn set_chunk_rows(&self, id: &StreamId, rows: usize) -> AppResult<bool> {
        let inner = self.inner.read().await;
        let Some(h) = inner.streams.get(id) else {
            return Ok(false);
        };
        h.set_chunk_rows(rows);
        Ok(true)
    }

    pub async fn set_batch_knobs(
        &self,
        id: &StreamId,
        flush_rows: usize,
        flush_interval_ms: u64,
        hard_cap_rows: usize,
        chunk_rows: usize,
    ) -> AppResult<bool> {
        let inner = self.inner.read().await;
        let Some(h) = inner.streams.get(id) else {
            return Ok(false);
        };
        h.set_batch_knobs(flush_rows, flush_interval_ms, hard_cap_rows, chunk_rows);
        Ok(true)
    }
}
