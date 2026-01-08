use crate::error::AppResult;

#[cfg(feature = "metrics")]
use prometheus::{
    Gauge, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
};
/// Metrics handle for DB writer + pool health.
///
/// If you don't enable the `metrics` feature, this becomes a no-op stub with the same API.
#[derive(Clone, Debug)]
pub struct DbMetrics {
    #[cfg(feature = "metrics")]
    registry: Registry,

    // --- Writer throughput
    #[cfg(feature = "metrics")]
    pub rows_written_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub batches_written_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub rows_per_batch: Histogram,

    // --- Writer latency
    #[cfg(feature = "metrics")]
    pub write_latency_seconds: Histogram,
    #[cfg(feature = "metrics")]
    pub flush_delay_seconds: Histogram,

    // --- Backpressure
    #[cfg(feature = "metrics")]
    pub writer_queue_depth: IntGauge,
    #[cfg(feature = "metrics")]
    pub queue_wait_seconds: Histogram,
    #[cfg(feature = "metrics")]
    pub stalled_flush_seconds: Histogram,

    // --- Failures / retries / drops
    #[cfg(feature = "metrics")]
    pub failed_batches_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub retried_batches_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub rows_dropped_total: IntCounter,

    // --- Pool health (per-shard would be nicer later; start global)
    #[cfg(feature = "metrics")]
    pub pool_in_use: IntGauge,
    #[cfg(feature = "metrics")]
    pub pool_idle: IntGauge,
    #[cfg(feature = "metrics")]
    pub pool_max: IntGauge,
    #[cfg(feature = "metrics")]
    pub pool_wait_seconds: Histogram,

    // ---------------------------
    // ADDITIONS (health + clarity)
    // ---------------------------
    /// 0=green, 1=yellow, 2=red
    #[cfg(feature = "metrics")]
    pub db_health_state: IntGauge,

    /// Error counter by kind (sqlx/timeout/connect/shutdown/other)
    #[cfg(feature = "metrics")]
    pub db_errors_total: IntCounterVec,

    /// How many rows/batches we intended to write (enqueued), for loss/backlog visibility
    #[cfg(feature = "metrics")]
    pub rows_enqueued_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub batches_enqueued_total: IntCounter,

    /// Oldest pending batch age (seconds). Useful when writes stop and histograms stop updating.
    #[cfg(feature = "metrics")]
    pub oldest_batch_age_seconds: Gauge,

    // no-op fallback data (keeps struct non-empty without feature)
    #[cfg(not(feature = "metrics"))]
    _noop: (),
}

impl DbMetrics {
    /// Create metrics (registers them).
    pub fn new() -> AppResult<Self> {
        #[cfg(feature = "metrics")]
        {
            let registry = Registry::new();

            let rows_written_total =
                IntCounter::with_opts(Opts::new("db_rows_written_total", "Rows written total"))?;
            let batches_written_total = IntCounter::with_opts(Opts::new(
                "db_batches_written_total",
                "Batches written total",
            ))?;

            let rows_per_batch = Histogram::with_opts(HistogramOpts::new(
                "db_rows_per_batch",
                "Rows per batch distribution",
            ))?;

            let write_latency_seconds = Histogram::with_opts(HistogramOpts::new(
                "db_write_latency_seconds",
                "Batch write latency (seconds)",
            ))?;

            let flush_delay_seconds = Histogram::with_opts(HistogramOpts::new(
                "db_flush_delay_seconds",
                "Delay between enqueue and flush (seconds)",
            ))?;

            let writer_queue_depth = IntGauge::with_opts(Opts::new(
                "db_writer_queue_depth",
                "Writer inflight depth (approx queue depth)",
            ))?;

            let queue_wait_seconds = Histogram::with_opts(HistogramOpts::new(
                "db_queue_wait_seconds",
                "Time waiting for writer permit (seconds)",
            ))?;

            let stalled_flush_seconds = Histogram::with_opts(HistogramOpts::new(
                "db_stalled_flush_seconds",
                "Time spent stalled in flush path (seconds)",
            ))?;

            let failed_batches_total = IntCounter::with_opts(Opts::new(
                "db_failed_batches_total",
                "Failed batches total",
            ))?;

            let retried_batches_total = IntCounter::with_opts(Opts::new(
                "db_retried_batches_total",
                "Retried batches total",
            ))?;

            let rows_dropped_total =
                IntCounter::with_opts(Opts::new("db_rows_dropped_total", "Rows dropped total"))?;

            let pool_in_use =
                IntGauge::with_opts(Opts::new("db_pool_in_use", "Connections in use"))?;
            let pool_idle = IntGauge::with_opts(Opts::new("db_pool_idle", "Idle connections"))?;
            let pool_max = IntGauge::with_opts(Opts::new("db_pool_max", "Max pool size"))?;

            let pool_wait_seconds = Histogram::with_opts(HistogramOpts::new(
                "db_pool_wait_seconds",
                "Time waiting to acquire a DB connection (seconds)",
            ))?;

            // ---------------------------
            // ADDITIONS (create)
            // ---------------------------

            let db_health_state = IntGauge::with_opts(Opts::new(
                "db_health_state",
                "DB health state: 0=green, 1=yellow, 2=red",
            ))?;

            let db_errors_total = IntCounterVec::new(
                Opts::new("db_errors_total", "DB errors total by kind"),
                &["kind"],
            )?;

            let rows_enqueued_total = IntCounter::with_opts(Opts::new(
                "db_rows_enqueued_total",
                "Rows enqueued for writing (intended) total",
            ))?;

            let batches_enqueued_total = IntCounter::with_opts(Opts::new(
                "db_batches_enqueued_total",
                "Batches enqueued for writing (intended) total",
            ))?;

            let oldest_batch_age_seconds = Gauge::with_opts(Opts::new(
                "db_oldest_batch_age_seconds",
                "Oldest pending batch age in seconds",
            ))?;

            // Register everything
            registry.register(Box::new(rows_written_total.clone()))?;
            registry.register(Box::new(batches_written_total.clone()))?;
            registry.register(Box::new(rows_per_batch.clone()))?;
            registry.register(Box::new(write_latency_seconds.clone()))?;
            registry.register(Box::new(flush_delay_seconds.clone()))?;
            registry.register(Box::new(writer_queue_depth.clone()))?;
            registry.register(Box::new(queue_wait_seconds.clone()))?;
            registry.register(Box::new(stalled_flush_seconds.clone()))?;
            registry.register(Box::new(failed_batches_total.clone()))?;
            registry.register(Box::new(retried_batches_total.clone()))?;
            registry.register(Box::new(rows_dropped_total.clone()))?;
            registry.register(Box::new(pool_in_use.clone()))?;
            registry.register(Box::new(pool_idle.clone()))?;
            registry.register(Box::new(pool_max.clone()))?;
            registry.register(Box::new(pool_wait_seconds.clone()))?;

            // ---------------------------
            // ADDITIONS (register)
            // ---------------------------
            registry.register(Box::new(db_health_state.clone()))?;
            registry.register(Box::new(db_errors_total.clone()))?;
            registry.register(Box::new(rows_enqueued_total.clone()))?;
            registry.register(Box::new(batches_enqueued_total.clone()))?;
            registry.register(Box::new(oldest_batch_age_seconds.clone()))?;

            Ok(Self {
                registry,
                rows_written_total,
                batches_written_total,
                rows_per_batch,
                write_latency_seconds,
                flush_delay_seconds,
                writer_queue_depth,
                queue_wait_seconds,
                stalled_flush_seconds,
                failed_batches_total,
                retried_batches_total,
                rows_dropped_total,
                pool_in_use,
                pool_idle,
                pool_max,
                pool_wait_seconds,

                // additions
                db_health_state,
                db_errors_total,
                rows_enqueued_total,
                batches_enqueued_total,
                oldest_batch_age_seconds,
            })
        }

        #[cfg(not(feature = "metrics"))]
        {
            Ok(Self { _noop: () })
        }
    }

    /// Encode metrics to Prometheus text format.
    #[cfg(feature = "metrics")]
    pub fn encode_text(&self) -> AppResult<String> {
        use prometheus::{Encoder, TextEncoder};
        let mf = self.registry.gather();
        let mut buf = Vec::new();
        TextEncoder::new().encode(&mf, &mut buf)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    #[cfg(not(feature = "metrics"))]
    pub fn encode_text(&self) -> AppResult<String> {
        Err(AppError::InvalidConfig(
            "metrics feature is disabled".into(),
        ))
    }

    // --- No-op helpers (so handler code can call these unconditionally)

    #[inline]
    pub fn set_queue_depth(&self, _depth: i64) {
        #[cfg(feature = "metrics")]
        self.writer_queue_depth.set(_depth);
    }

    #[inline]
    pub fn observe_queue_wait(&self, _secs: f64) {
        #[cfg(feature = "metrics")]
        self.queue_wait_seconds.observe(_secs);
    }

    #[inline]
    pub fn observe_flush_delay(&self, _secs: f64) {
        #[cfg(feature = "metrics")]
        self.flush_delay_seconds.observe(_secs);
    }

    #[inline]
    pub fn observe_write_latency(&self, _secs: f64) {
        #[cfg(feature = "metrics")]
        self.write_latency_seconds.observe(_secs);
    }

    #[inline]
    pub fn inc_batches_written(&self) {
        #[cfg(feature = "metrics")]
        self.batches_written_total.inc();
    }

    #[inline]
    pub fn add_rows_written(&self, _n: u64) {
        #[cfg(feature = "metrics")]
        self.rows_written_total.inc_by(_n);
    }

    #[inline]
    pub fn observe_rows_per_batch(&self, _rows: f64) {
        #[cfg(feature = "metrics")]
        self.rows_per_batch.observe(_rows);
    }

    #[inline]
    pub fn inc_failed_batch(&self) {
        #[cfg(feature = "metrics")]
        self.failed_batches_total.inc();
    }

    #[inline]
    pub fn inc_retried_batch(&self) {
        #[cfg(feature = "metrics")]
        self.retried_batches_total.inc();
    }

    #[inline]
    pub fn add_rows_dropped(&self, _n: u64) {
        #[cfg(feature = "metrics")]
        self.rows_dropped_total.inc_by(_n);
    }

    #[inline]
    pub fn observe_pool_wait(&self, _secs: f64) {
        #[cfg(feature = "metrics")]
        self.pool_wait_seconds.observe(_secs);
    }

    #[inline]
    pub fn set_pool_health(&self, _in_use: i64, _idle: i64, _max: i64) {
        #[cfg(feature = "metrics")]
        {
            self.pool_in_use.set(_in_use);
            self.pool_idle.set(_idle);
            self.pool_max.set(_max);
        }
    }

    /// 0=green, 1=yellow, 2=red
    #[inline]
    pub fn set_health_state(&self, _state: i64) {
        #[cfg(feature = "metrics")]
        self.db_health_state.set(_state);
    }

    /// kind examples: "sqlx", "timeout", "connect", "shutdown", "other"
    #[inline]
    pub fn inc_db_error(&self, _kind: &'static str) {
        #[cfg(feature = "metrics")]
        self.db_errors_total.with_label_values(&[_kind]).inc();
    }

    #[inline]
    pub fn add_rows_enqueued(&self, _n: u64) {
        #[cfg(feature = "metrics")]
        self.rows_enqueued_total.inc_by(_n);
    }

    #[inline]
    pub fn inc_batches_enqueued(&self) {
        #[cfg(feature = "metrics")]
        self.batches_enqueued_total.inc();
    }

    #[inline]
    pub fn set_oldest_batch_age_seconds(&self, _secs: f64) {
        #[cfg(feature = "metrics")]
        self.oldest_batch_age_seconds.set(_secs);
    }
}
