//! db/handler.rs
//!
//! Main DB handler: routes -> acquires pool conn -> writes batch -> updates metrics.
//!
//! IMPORTANT CHANGE (batching behavior):
//! - `write_batch()` now takes `&mut Batch<T>`
//! - It will ONLY write when:
//!     (a) batch.rows.len() >= writer.batch_size
//!     OR
//!     (b) flush_interval_ms has elapsed since batch.enqueued_at
//! - On success it clears the batch (keeps same Batch object reusable).
//!
//! This makes `batch_size` act like the “transporter threshold” with minimal changes.
//!
//! Caller usage pattern:
//!     batch.rows.push(row);
//!     db.write_batch(&mut batch).await?;

use crate::db::Batch;
use crate::db::config::WriterConfig;
use crate::db::metrics::DbMetrics;
use crate::db::pools::DbPools;
use crate::db::traits::BatchInsertRow;
use crate::error::{AppError, AppResult};
use sqlx::{Postgres, QueryBuilder};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Main DB handler: routes -> acquires pool conn -> writes batch -> updates metrics.
#[derive(Clone, Debug)]
pub struct DbHandler {
    pools: Arc<DbPools>,
    writer: WriterConfig,
    metrics: Arc<DbMetrics>,
    inflight: Arc<Semaphore>,
}

impl DbHandler {
    pub fn new(pools: Arc<DbPools>, writer: WriterConfig, metrics: Arc<DbMetrics>) -> Self {
        let inflight = Arc::new(Semaphore::new(writer.max_inflight_batches));
        Self {
            pools,
            writer,
            metrics,
            inflight,
        }
    }

    /// Write a batch using INSERT ... VALUES (...), (...), ...
    ///
    /// NEW batching behavior:
    /// - If batch is empty: returns Ok
    /// - If batch has fewer than batch_size rows AND flush_interval has NOT elapsed: returns Ok (keeps rows)
    /// - Otherwise: writes (in chunks of batch_size), then clears rows and resets enqueued_at
    pub async fn write_batch<T: BatchInsertRow>(&self, batch: &mut Batch<T>) -> AppResult<()> {
        if !batch.should_flush() {
            return Ok(());
        }

        // --- Backpressure: wait for a permit (queue wait time)
        let t0 = Instant::now();
        let permit = self
            .inflight
            .acquire()
            .await
            .map_err(|_| AppError::Shutdown)?;
        self.metrics.observe_queue_wait(t0.elapsed().as_secs_f64());

        // Approx inflight depth = max - available
        let depth =
            (self.writer.max_inflight_batches as i64) - (self.inflight.available_permits() as i64);
        self.metrics.set_queue_depth(depth);

        // --- Flush delay (how long it waited since enqueue)
        self.metrics
            .observe_flush_delay(batch.enqueued_at.elapsed().as_secs_f64());

        // --- Route shard + get pool
        let shard_id = self
            .pools
            .shard_id_for(&batch.key.exchange, &batch.key.stream, &batch.key.symbol)
            .await?;

        let pool = self.pools.pool_by_id(&shard_id).await?;

        // --- Pool max (from shard config) for health gauge
        let pool_max = self
            .pools
            .shards_snapshot()
            .await?
            .iter()
            .find(|s| s.id == shard_id)
            .map(|s| s.pool_max as i64)
            .unwrap_or(0);

        // --- Pool wait (explicit acquire to measure wait time)
        let acquire_t0 = Instant::now();
        let mut conn = pool.acquire().await.map_err(AppError::Sqlx)?;
        self.metrics
            .observe_pool_wait(acquire_t0.elapsed().as_secs_f64());

        // pool.size() includes idle+in-use; num_idle() is idle
        let size = pool.size() as i64;
        let idle = pool.num_idle() as i64;
        let in_use = (size - idle).max(0);
        self.metrics.set_pool_health(in_use, idle, pool_max);

        // --- Build & execute INSERT batches (chunked by batch_size)
        let write_t0 = Instant::now();

        // Table name is dynamic (depends on exchange). Compute once.
        let table_name = batch.rows[0].table(&batch.key.exchange);

        let mut total_written: u64 = 0;

        for chunk in batch.rows.chunks(batch.chunk_rows) {
            let mut qb: QueryBuilder<Postgres> = QueryBuilder::new("INSERT INTO ");
            qb.push("\"");
            qb.push(&table_name.replace('.', "\".\""));
            qb.push("\"");

            qb.push(" (");

            for (i, col) in T::COLUMNS.iter().enumerate() {
                if i > 0 {
                    qb.push(", ");
                }
                qb.push("\"");
                qb.push(*col);
                qb.push("\"");
            }
            qb.push(") ");

            qb.push_values(chunk.iter(), |mut b, row| {
                row.push_binds(&mut b);
            });

            // Execute without capturing `permit` in a closure
            let res = qb.build().execute(&mut *conn).await;
            if let Err(e) = res {
                self.metrics.inc_failed_batch();
                drop(permit); // release before returning
                return Err(AppError::Sqlx(e));
            }

            total_written += chunk.len() as u64;
        }

        // release permit (drop) after successful writes
        drop(permit);

        // Success metrics
        self.metrics
            .observe_write_latency(write_t0.elapsed().as_secs_f64());
        self.metrics.inc_batches_written();
        self.metrics.add_rows_written(total_written);
        self.metrics.observe_rows_per_batch(total_written as f64);

        // Clear batch after successful write and reset timer
        batch.rows.clear();
        batch.enqueued_at = Instant::now();

        Ok(())
    }

    /// Simple retry helper (linear backoff).
    ///
    /// Note: No `T: Clone` needed now because we don't consume the batch.
    /// We also only clear rows on success inside `write_batch()`.
    pub async fn write_batch_with_retry<T: BatchInsertRow>(
        &self,
        batch: &mut Batch<T>,
        retries: usize,
        backoff: Duration,
    ) -> AppResult<()> {
        let mut attempt = 0usize;

        loop {
            match self.write_batch(batch).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < retries => {
                    self.metrics.inc_retried_batch();
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
