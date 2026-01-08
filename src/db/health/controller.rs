use crate::db::config::HealthConfig;
use crate::db::health::types::{HealthState, HealthStatus};
use crate::db::metrics::DbMetrics;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// A very small rolling estimator:
/// - EWMA for "typical" value
/// - MAX since last tick for "spikes"
#[derive(Debug, Clone)]
struct EwmaMax {
    ewma: f64,
    max_since_tick: f64,
    initialized: bool,
    alpha: f64,
}

impl EwmaMax {
    fn new(alpha: f64) -> Self {
        Self {
            ewma: 0.0,
            max_since_tick: 0.0,
            initialized: false,
            alpha,
        }
    }

    fn observe(&mut self, x: f64) {
        if !self.initialized {
            self.ewma = x;
            self.initialized = true;
        } else {
            self.ewma = self.alpha * x + (1.0 - self.alpha) * self.ewma;
        }
        if x > self.max_since_tick {
            self.max_since_tick = x;
        }
    }

    fn take_max_and_reset(&mut self) -> f64 {
        let m = self.max_since_tick;
        self.max_since_tick = 0.0;
        m
    }

    fn ewma(&self) -> f64 {
        self.ewma
    }
}

/// Shared signals updated from write-path instrumentation.
#[derive(Debug)]
struct Signals {
    // milliseconds
    flush_delay_ms: EwmaMax,
    pool_wait_ms: EwmaMax,

    // queue depth is a gauge-like value; we still track max since tick
    queue_depth_max: i64,
}

impl Signals {
    fn new() -> Self {
        // alpha ~0.2 is responsive but not too twitchy
        Self {
            flush_delay_ms: EwmaMax::new(0.2),
            pool_wait_ms: EwmaMax::new(0.2),
            queue_depth_max: 0,
        }
    }

    fn reset_tick_max(&mut self) {
        self.flush_delay_ms.take_max_and_reset();
        self.pool_wait_ms.take_max_and_reset();
        self.queue_depth_max = 0;
    }
}

/// Call these from your write path (later) alongside existing `metrics.observe_*`.
/// This is intentionally cheap (Mutex + a few floats/ints).
#[derive(Clone, Debug)]
pub struct HealthRecorder {
    inner: Arc<Mutex<Signals>>,
}

impl HealthRecorder {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Signals::new())),
        }
    }

    /// secs -> stored as ms
    pub fn observe_flush_delay_secs(&self, secs: f64) {
        if let Ok(mut s) = self.inner.lock() {
            s.flush_delay_ms.observe(secs * 1000.0);
        }
    }

    /// secs -> stored as ms
    pub fn observe_pool_wait_secs(&self, secs: f64) {
        if let Ok(mut s) = self.inner.lock() {
            s.pool_wait_ms.observe(secs * 1000.0);
        }
    }

    pub fn set_queue_depth(&self, depth: i64) {
        if let Ok(mut s) = self.inner.lock() {
            if depth > s.queue_depth_max {
                s.queue_depth_max = depth;
            }
        }
    }

    /// Optional: if your batching layer knows the oldest pending batch age, set it here.
    pub fn set_oldest_batch_age_secs(&self, metrics: &DbMetrics, secs: f64) {
        metrics.set_oldest_batch_age_seconds(secs);
    }
}

/// Runs the health evaluation loop and publishes a HealthStatus.
/// Also updates Prometheus gauge `db_health_state`.
#[derive(Clone, Debug)]
pub struct DBHealthController {
    cfg: HealthConfig,
    recorder: HealthRecorder,
    metrics: Arc<DbMetrics>,
    tx: watch::Sender<HealthStatus>,
}

impl DBHealthController {
    pub fn new(cfg: HealthConfig, metrics: Arc<DbMetrics>) -> Self {
        let recorder = HealthRecorder::new();
        let initial = HealthStatus::new(&cfg, HealthState::Green, Vec::new());
        let (tx, _rx) = watch::channel(initial);
        Self {
            cfg,
            recorder,
            metrics,
            tx,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<HealthStatus> {
        self.tx.subscribe()
    }

    pub fn recorder(&self) -> HealthRecorder {
        self.recorder.clone()
    }

    pub fn current(&self) -> HealthStatus {
        self.tx.borrow().clone()
    }

    pub fn spawn_health_loop(
        self: Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(shutdown).await })
    }

    pub async fn run(&self, shutdown: tokio_util::sync::CancellationToken) {
        // If disabled, publish GREEN and exit.
        if !self.cfg.enabled {
            let st = HealthStatus::new(&self.cfg, HealthState::Green, Vec::new());
            self.metrics.set_health_state(st.state.as_i64());
            let _ = self.tx.send(st);
            return;
        }

        let eval_every = Duration::from_millis(self.cfg.evaluate_interval_ms);
        let hold_down = Duration::from_millis(self.cfg.hold_down_ms);

        let mut last_applied_state = HealthState::Green;
        let mut pending_state: Option<(HealthState, Instant, Vec<&'static str>)> = None;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("health loop shutting down");
                    break;
                }
                _ = tokio::time::sleep(eval_every) => {}
            }

            let snapshot = {
                let mut s = match self.recorder.inner.lock() {
                    Ok(g) => g,
                    Err(_) => continue, // poisoned; skip tick
                };

                // Evaluate based on current EWMA + max since tick
                let flush_ewma = s.flush_delay_ms.ewma();
                let flush_max = s.flush_delay_ms.take_max_and_reset();

                let pool_ewma = s.pool_wait_ms.ewma();
                let pool_max = s.pool_wait_ms.take_max_and_reset();

                let qmax = s.queue_depth_max;
                s.queue_depth_max = 0;

                (flush_ewma, flush_max, pool_ewma, pool_max, qmax)
            };

            let (flush_ewma, flush_max, pool_ewma, pool_max, qmax) = snapshot;

            let (raw_state, reasons) =
                self.evaluate_raw(flush_ewma, flush_max, pool_ewma, pool_max, qmax);

            // Hold-down to prevent flapping: require raw_state to persist for hold_down
            let now = Instant::now();
            let next_state = if raw_state == last_applied_state {
                pending_state = None;
                raw_state
            } else {
                match &mut pending_state {
                    None => {
                        pending_state = Some((raw_state, now, reasons.clone()));
                        last_applied_state
                    }
                    Some((st, since, _rsn)) if *st != raw_state => {
                        // state changed again; restart timer
                        *st = raw_state;
                        *since = now;
                        last_applied_state
                    }
                    Some((st, since, rsn)) => {
                        // same pending state; check timer
                        *rsn = reasons.clone();
                        if now.duration_since(*since) >= hold_down {
                            *st
                        } else {
                            last_applied_state
                        }
                    }
                }
            };

            if next_state != last_applied_state {
                last_applied_state = next_state;
            }

            let status = HealthStatus::new(&self.cfg, last_applied_state, reasons);
            self.metrics.set_health_state(status.state.as_i64());
            let _ = self.tx.send(status);
        }
    }

    fn evaluate_raw(
        &self,
        flush_ewma_ms: f64,
        flush_max_ms: f64,
        pool_ewma_ms: f64,
        pool_max_ms: f64,
        queue_depth_max: i64,
    ) -> (HealthState, Vec<&'static str>) {
        let t = &self.cfg.thresholds;

        let mut reasons = Vec::new();

        // Treat MAX as "spike detector" and EWMA as "sustained pressure"
        let flush_bad_red = flush_max_ms >= t.flush_delay_p95_ms_red as f64
            || flush_ewma_ms >= t.flush_delay_p95_ms_red as f64;
        let flush_bad_yellow = flush_max_ms >= t.flush_delay_p95_ms_yellow as f64
            || flush_ewma_ms >= t.flush_delay_p95_ms_yellow as f64;

        let pool_bad_red = pool_max_ms >= t.pool_wait_p95_ms_red as f64
            || pool_ewma_ms >= t.pool_wait_p95_ms_red as f64;
        let pool_bad_yellow = pool_max_ms >= t.pool_wait_p95_ms_yellow as f64
            || pool_ewma_ms >= t.pool_wait_p95_ms_yellow as f64;

        let queue_red = queue_depth_max >= t.writer_queue_depth_red;

        if flush_bad_red {
            reasons.push("flush_delay_red");
        } else if flush_bad_yellow {
            reasons.push("flush_delay_yellow");
        }

        if pool_bad_red {
            reasons.push("pool_wait_red");
        } else if pool_bad_yellow {
            reasons.push("pool_wait_yellow");
        }

        if queue_red {
            reasons.push("writer_queue_depth_red");
        }

        // State selection: RED beats YELLOW beats GREEN
        let state = if flush_bad_red || pool_bad_red || queue_red {
            HealthState::Red
        } else if flush_bad_yellow || pool_bad_yellow {
            HealthState::Yellow
        } else {
            HealthState::Green
        };

        (state, reasons)
    }

    #[inline]
    pub fn can_admit_new_stream(&self) -> Option<String> {
        let s = self.tx.borrow();

        if s.can_admit_new_stream {
            None
        } else if s.reasons.is_empty() {
            Some(format!("admission denied: state={:?}", s.state))
        } else {
            Some(format!(
                "admission denied: state={:?}, reasons={}",
                s.state,
                s.reasons.join(",")
            ))
        }
    }

    /// Returns the current health state (Green / Yellow / Red).
    #[inline]
    pub fn state(&self) -> HealthState {
        self.tx.borrow().state
    }

    /// Returns a full snapshot of the current health status.
    #[inline]
    pub fn status(&self) -> HealthStatus {
        self.tx.borrow().clone()
    }

    /// If admission is denied, returns a human-readable reason string.
    /// Otherwise returns None.
    #[inline]
    pub fn deny_reason(&self) -> Option<String> {
        let s = self.tx.borrow();
        if s.can_admit_new_stream {
            None
        } else if s.reasons.is_empty() {
            Some(format!("admission denied: state={:?}", s.state))
        } else {
            Some(format!(
                "admission denied: state={:?}, reasons={}",
                s.state,
                s.reasons.join(",")
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::config::{AdmissionPolicy, HealthConfig, HealthThresholds};
    use crate::db::health::types::HealthState;
    use crate::db::metrics::DbMetrics;

    use std::sync::Arc;
    use tokio::time::{Duration, sleep};

    fn make_test_cfg() -> HealthConfig {
        HealthConfig {
            enabled: true,
            evaluate_interval_ms: 50,
            hold_down_ms: 50,
            admission_policy: AdmissionPolicy::GreenOnly,
            thresholds: HealthThresholds {
                flush_delay_p95_ms_yellow: 200,
                flush_delay_p95_ms_red: 500,
                pool_wait_p95_ms_yellow: 10,
                pool_wait_p95_ms_red: 50,
                writer_queue_depth_red: 4,
            },
        }
    }

    #[tokio::test]
    async fn health_controller_can_admit_flips_to_denied_on_red_signal() {
        let mut cfg = make_test_cfg();
        cfg.evaluate_interval_ms = 50;
        cfg.hold_down_ms = 100;

        let metrics = Arc::new(DbMetrics::new().expect("metrics init failed"));

        // NOTE: Arc
        let controller = Arc::new(DBHealthController::new(cfg.clone(), metrics));

        // IMPORTANT: keep at least one receiver alive, otherwise watch::send() fails.
        let _rx = controller.subscribe();

        let recorder = controller.recorder();

        // Spawn using your helper (preferred)
        let token = tokio_util::sync::CancellationToken::new();
        let handle = controller.clone().spawn_health_loop(token);

        tokio::time::sleep(Duration::from_millis(cfg.evaluate_interval_ms * 2)).await;

        assert_eq!(controller.state(), HealthState::Green);
        assert!(controller.can_admit_new_stream().is_none());

        // Inject RED flush delay
        let inject_for = Duration::from_millis(cfg.hold_down_ms + cfg.evaluate_interval_ms * 4);
        let start = Instant::now();
        while start.elapsed() < inject_for {
            recorder.observe_flush_delay_secs(0.600);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tokio::time::sleep(Duration::from_millis(cfg.evaluate_interval_ms * 3)).await;

        assert_eq!(controller.state(), HealthState::Red);
        assert!(controller.can_admit_new_stream().is_some());

        handle.abort();
    }

    #[tokio::test]
    async fn health_controller_returns_to_green_when_signals_stop() {
        let mut cfg = make_test_cfg();
        cfg.hold_down_ms = 100;
        cfg.evaluate_interval_ms = 50;

        let metrics = Arc::new(DbMetrics::new().expect("metrics init failed"));

        // NOTE: Arc
        let controller = Arc::new(DBHealthController::new(cfg.clone(), metrics));

        let _rx = controller.subscribe();

        let recorder = controller.recorder();
        let token = tokio_util::sync::CancellationToken::new();
        let handle = controller.clone().spawn_health_loop(token);

        tokio::time::sleep(Duration::from_millis(cfg.evaluate_interval_ms * 2)).await;

        // Drive to RED
        let inject_for = Duration::from_millis(cfg.hold_down_ms + cfg.evaluate_interval_ms * 4);
        let start = Instant::now();
        while start.elapsed() < inject_for {
            recorder.observe_pool_wait_secs(0.100);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tokio::time::sleep(Duration::from_millis(cfg.evaluate_interval_ms * 3)).await;

        assert_eq!(controller.state(), HealthState::Red);

        // Decay EWMA with zeros
        let decay_for = Duration::from_millis(cfg.evaluate_interval_ms * 20);
        let start = Instant::now();
        while start.elapsed() < decay_for {
            recorder.observe_pool_wait_secs(0.0);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tokio::time::sleep(Duration::from_millis(cfg.evaluate_interval_ms * 3)).await;

        assert_eq!(controller.state(), HealthState::Green);
        assert!(controller.can_admit_new_stream().is_none());

        handle.abort();
    }
}
