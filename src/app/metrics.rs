use crate::error::{AppError, AppResult};

#[cfg(feature = "metrics")]
use prometheus::{IntCounter, IntGauge, Opts, Registry};

/// App-level / orchestrator metrics.
///
/// Scope:
/// - app identity & config
/// - readiness / health
/// - stream control plane
/// - limits derived from config
/// - runtime health guard (GREEN/RED + reasons)
///
/// NO per-stream labels.
/// NO high-cardinality labels.
#[derive(Clone, Debug)]
pub struct AppMetrics {
    #[cfg(feature = "metrics")]
    registry: Registry,

    // --------------------------------------------------
    // Lifecycle / readiness
    // --------------------------------------------------
    #[cfg(feature = "metrics")]
    pub app_ready: IntGauge,
    #[cfg(feature = "metrics")]
    pub app_health: IntGauge,

    // --------------------------------------------------
    // Runtime health (GREEN/RED) + reasons
    // --------------------------------------------------
    #[cfg(feature = "metrics")]
    pub runtime_health: IntGauge, // 1=GREEN, 0=RED

    // reason gauges (multi-hot; more than one can be 1)
    #[cfg(feature = "metrics")]
    pub runtime_red_rss: IntGauge,
    #[cfg(feature = "metrics")]
    pub runtime_red_avail_mem: IntGauge,
    #[cfg(feature = "metrics")]
    pub runtime_red_fd: IntGauge,
    #[cfg(feature = "metrics")]
    pub runtime_red_drift: IntGauge,
    #[cfg(feature = "metrics")]
    pub runtime_red_cpu: IntGauge,

    // state transition counters
    #[cfg(feature = "metrics")]
    pub runtime_to_red_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub runtime_to_green_total: IntCounter,

    // admission denials due to runtime RED
    #[cfg(feature = "metrics")]
    pub streams_add_denied_runtime_red_total: IntCounter,

    // --------------------------------------------------
    // Stream registry state
    // --------------------------------------------------
    #[cfg(feature = "metrics")]
    pub streams_active: IntGauge,
    #[cfg(feature = "metrics")]
    pub streams_limit: IntGauge,

    // --------------------------------------------------
    // Control-plane operations
    // --------------------------------------------------
    #[cfg(feature = "metrics")]
    pub streams_add_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub streams_remove_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub streams_update_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub streams_op_errors_total: IntCounter,

    // --------------------------------------------------
    // Config lifecycle
    // --------------------------------------------------
    #[cfg(feature = "metrics")]
    pub config_reload_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub config_reload_errors_total: IntCounter,

    #[cfg(feature = "metrics")]
    pub streams_add_denied_redis_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub streams_add_denied_db_total: IntCounter,

    // no-op fallback
    #[cfg(not(feature = "metrics"))]
    _noop: (),
}

impl AppMetrics {
    pub fn new(app_id: &str, env: &str, config_version: u32, max_streams: u32) -> AppResult<Self> {
        #[cfg(feature = "metrics")]
        {
            let registry = Registry::new();

            // --------------------------------------------------
            // app_info (const labels)
            // --------------------------------------------------
            let app_info = IntGauge::with_opts(
                Opts::new("app_info", "Static app identity info")
                    .const_label("app_id", app_id)
                    .const_label("env", env)
                    .const_label("config_version", config_version.to_string()),
            )?;
            app_info.set(1);
            registry.register(Box::new(app_info))?;

            // --------------------------------------------------
            // Lifecycle
            // --------------------------------------------------
            let app_ready = IntGauge::with_opts(Opts::new(
                "app_ready",
                "Whether the app is ready to serve traffic (0/1)",
            ))?;

            let app_health =
                IntGauge::with_opts(Opts::new("app_health", "Overall app health (0/1)"))?;

            // --------------------------------------------------
            // Runtime health (GREEN/RED) + reasons
            // --------------------------------------------------
            let runtime_health = IntGauge::with_opts(Opts::new(
                "runtime_health",
                "Runtime health state (1=GREEN, 0=RED)",
            ))?;
            runtime_health.set(1); // default assume green at boot

            let runtime_red_rss = IntGauge::with_opts(Opts::new(
                "runtime_health_red_rss",
                "RED reason: process RSS exceeded threshold (0/1)",
            ))?;
            let runtime_red_avail_mem = IntGauge::with_opts(Opts::new(
                "runtime_health_red_avail_mem",
                "RED reason: system available memory below threshold (0/1)",
            ))?;
            let runtime_red_fd = IntGauge::with_opts(Opts::new(
                "runtime_health_red_fd",
                "RED reason: file descriptor usage exceeded threshold (0/1)",
            ))?;
            let runtime_red_drift = IntGauge::with_opts(Opts::new(
                "runtime_health_red_drift",
                "RED reason: Tokio scheduler/tick drift exceeded threshold (0/1)",
            ))?;
            let runtime_red_cpu = IntGauge::with_opts(Opts::new(
                "runtime_health_red_cpu",
                "RED reason: sustained CPU usage exceeded threshold (0/1)",
            ))?;

            let runtime_to_red_total = IntCounter::with_opts(Opts::new(
                "runtime_health_to_red_total",
                "Total transitions of runtime health to RED",
            ))?;
            let runtime_to_green_total = IntCounter::with_opts(Opts::new(
                "runtime_health_to_green_total",
                "Total transitions of runtime health to GREEN",
            ))?;

            let streams_add_denied_runtime_red_total = IntCounter::with_opts(Opts::new(
                "streams_add_denied_runtime_red_total",
                "Total stream add operations denied due to runtime health RED",
            ))?;

            // --------------------------------------------------
            // Stream state
            // --------------------------------------------------
            let streams_active = IntGauge::with_opts(Opts::new(
                "streams_active",
                "Number of currently active streams",
            ))?;

            let streams_limit = IntGauge::with_opts(Opts::new(
                "streams_limit",
                "Configured maximum allowed active streams",
            ))?;
            streams_limit.set(max_streams as i64);

            // --------------------------------------------------
            // Control-plane ops
            // --------------------------------------------------
            let streams_add_total = IntCounter::with_opts(Opts::new(
                "streams_add_total",
                "Total stream add operations",
            ))?;

            let streams_remove_total = IntCounter::with_opts(Opts::new(
                "streams_remove_total",
                "Total stream remove operations",
            ))?;

            let streams_update_total = IntCounter::with_opts(Opts::new(
                "streams_update_total",
                "Total stream update operations",
            ))?;

            let streams_op_errors_total = IntCounter::with_opts(Opts::new(
                "streams_op_errors_total",
                "Total stream control-plane errors",
            ))?;

            // --------------------------------------------------
            // Config lifecycle
            // --------------------------------------------------
            let config_reload_total = IntCounter::with_opts(Opts::new(
                "config_reload_total",
                "Total config reload attempts",
            ))?;

            let config_reload_errors_total = IntCounter::with_opts(Opts::new(
                "config_reload_errors_total",
                "Total config reload failures",
            ))?;

            let streams_add_denied_redis_total = IntCounter::with_opts(Opts::new(
                "streams_add_denied_redis_total",
                "Total stream add operations denied due to redis not being able to assign new symbol",
            ))?;

            let streams_add_denied_db_total = IntCounter::with_opts(Opts::new(
                "streams_add_denied_db_total",
                "Total stream add operations denied due to db admission check",
            ))?;

            // --------------------------------------------------
            // Register gauges
            // --------------------------------------------------
            for g in [
                &app_ready,
                &app_health,
                &runtime_health,
                &runtime_red_rss,
                &runtime_red_avail_mem,
                &runtime_red_fd,
                &runtime_red_drift,
                &runtime_red_cpu,
                &streams_active,
                &streams_limit,
            ] {
                registry.register(Box::new(g.clone()))?;
            }

            // --------------------------------------------------
            // Register counters
            // --------------------------------------------------
            for c in [
                &runtime_to_red_total,
                &runtime_to_green_total,
                &streams_add_denied_runtime_red_total,
                &streams_add_total,
                &streams_remove_total,
                &streams_update_total,
                &streams_op_errors_total,
                &config_reload_total,
                &config_reload_errors_total,
                &streams_add_denied_redis_total,
                &streams_add_denied_db_total,
            ] {
                registry.register(Box::new(c.clone()))?;
            }

            Ok(Self {
                registry,
                app_ready,
                app_health,

                runtime_health,
                runtime_red_rss,
                runtime_red_avail_mem,
                runtime_red_fd,
                runtime_red_drift,
                runtime_red_cpu,
                runtime_to_red_total,
                runtime_to_green_total,
                streams_add_denied_runtime_red_total,

                streams_active,
                streams_limit,

                streams_add_total,
                streams_remove_total,
                streams_update_total,
                streams_op_errors_total,

                config_reload_total,
                config_reload_errors_total,
                streams_add_denied_redis_total,
                streams_add_denied_db_total,
            })
        }

        #[cfg(not(feature = "metrics"))]
        {
            Ok(Self { _noop: () })
        }
    }

    // --------------------------------------------------
    // Encoding
    // --------------------------------------------------
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

    // --------------------------------------------------
    // Helpers (safe to call unconditionally)
    // --------------------------------------------------
    #[inline]
    pub fn set_ready(&self, ready: bool) {
        #[cfg(feature = "metrics")]
        self.app_ready.set(ready as i64);
    }

    #[inline]
    pub fn set_health(&self, healthy: bool) {
        #[cfg(feature = "metrics")]
        self.app_health.set(healthy as i64);
    }

    // -------- runtime health --------

    /// Sets runtime health gauge: GREEN=1, RED=0.
    #[inline]
    pub fn set_runtime_health(&self, healthy: bool) {
        #[cfg(feature = "metrics")]
        self.runtime_health.set(healthy as i64);
    }

    /// Multi-hot reason gauges for why runtime is RED.
    /// You can set these every poll; caller decides which are true.
    #[inline]
    pub fn set_runtime_red_reasons(
        &self,
        rss: bool,
        avail_mem: bool,
        fd: bool,
        drift: bool,
        cpu: bool,
    ) {
        #[cfg(feature = "metrics")]
        {
            self.runtime_red_rss.set(rss as i64);
            self.runtime_red_avail_mem.set(avail_mem as i64);
            self.runtime_red_fd.set(fd as i64);
            self.runtime_red_drift.set(drift as i64);
            self.runtime_red_cpu.set(cpu as i64);
        }
    }

    #[inline]
    pub fn inc_runtime_to_red(&self) {
        #[cfg(feature = "metrics")]
        self.runtime_to_red_total.inc();
    }

    #[inline]
    pub fn inc_runtime_to_green(&self) {
        #[cfg(feature = "metrics")]
        self.runtime_to_green_total.inc();
    }

    #[inline]
    pub fn inc_stream_add_denied_runtime_red(&self) {
        #[cfg(feature = "metrics")]
        self.streams_add_denied_runtime_red_total.inc();
    }

    // -------- existing stream metrics --------

    #[inline]
    pub fn set_streams_active(&self, n: i64) {
        #[cfg(feature = "metrics")]
        self.streams_active.set(n);
    }

    #[inline]
    pub fn inc_stream_add(&self) {
        #[cfg(feature = "metrics")]
        self.streams_add_total.inc();
    }

    #[inline]
    pub fn inc_stream_remove(&self) {
        #[cfg(feature = "metrics")]
        self.streams_remove_total.inc();
    }

    #[inline]
    pub fn inc_stream_update(&self) {
        #[cfg(feature = "metrics")]
        self.streams_update_total.inc();
    }

    #[inline]
    pub fn inc_stream_error(&self) {
        #[cfg(feature = "metrics")]
        self.streams_op_errors_total.inc();
    }

    #[inline]
    pub fn inc_config_reload(&self) {
        #[cfg(feature = "metrics")]
        self.config_reload_total.inc();
    }

    #[inline]
    pub fn inc_config_reload_error(&self) {
        #[cfg(feature = "metrics")]
        self.config_reload_errors_total.inc();
    }

    #[inline]
    pub fn inc_stream_add_denied_redis(&self) {
        #[cfg(feature = "metrics")]
        self.streams_add_denied_redis_total.inc();
    }

    #[inline]
    pub fn inc_stream_add_denied_db(&self) {
        #[cfg(feature = "metrics")]
        self.streams_add_denied_db_total.inc();
    }
}
