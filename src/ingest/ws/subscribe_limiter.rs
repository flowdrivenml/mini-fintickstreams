// src/ingest/ws_limiters.rs
use crate::ingest::config::ExchangeConfig;
use crate::ingest::metrics::IngestMetrics;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

/// Shared config for a windowed "attempts" limiter (weight=1 per attempt).
#[derive(Debug, Clone)]
pub struct AttemptLimiterConfig {
    pub max_attempts: u32,
    pub window: Duration,
}

#[derive(Debug)]
struct State {
    window_start: Instant,
    used: u32,
}

impl State {
    fn new_now() -> Self {
        Self {
            window_start: Instant::now(),
            used: 0,
        }
    }
}

/// Limits WS SUBSCRIBE attempts: each attempt costs weight=1.
#[derive(Debug, Clone)]
pub struct SubscribeAttemptLimiter {
    cfg: AttemptLimiterConfig,
    state: Arc<Mutex<State>>,
    metrics: Option<Arc<IngestMetrics>>,
}

impl SubscribeAttemptLimiter {
    pub fn new(cfg: AttemptLimiterConfig, metrics: Option<Arc<IngestMetrics>>) -> Self {
        Self {
            cfg,
            state: Arc::new(Mutex::new(State::new_now())),
            metrics,
        }
    }

    /// Acquire permission for 1 subscribe attempt. Sleeps until window resets if exhausted.
    pub async fn acquire(&self) {
        loop {
            let start_wait = Instant::now();
            let mut st = self.state.lock().await;

            let elapsed = st.window_start.elapsed();
            if elapsed >= self.cfg.window {
                st.window_start = Instant::now();
                st.used = 0;
            }

            if st.used < self.cfg.max_attempts {
                st.used += 1;
                drop(st);

                if let Some(m) = &self.metrics {
                    m.inc_ws_subscribe_attempt();
                    let waited = start_wait.elapsed();
                    if waited.as_secs_f64() > 0.0 {
                        m.observe_ws_subscribe_wait(waited.as_secs_f64());
                    }
                }
                return;
            }

            // exhausted
            if let Some(m) = &self.metrics {
                m.inc_ws_subscribe_rate_limited();
            }

            let sleep_for = self.cfg.window.saturating_sub(elapsed);
            drop(st);
            tokio::time::sleep(sleep_for).await;
        }
    }

    /// Explicitly set used attempts (weight=1 per attempt).
    /// This is useful if some other code path already tried N times.
    pub async fn set_used_attempts(&self, used_attempts: u32) {
        let mut st = self.state.lock().await;

        let elapsed = st.window_start.elapsed();
        if elapsed >= self.cfg.window {
            st.window_start = Instant::now();
            st.used = 0;
        }

        st.used = used_attempts.min(self.cfg.max_attempts);
    }

    /// Current used attempts in the active window (auto-resets if window expired).
    pub async fn used_attempts(&self) -> u32 {
        let mut st = self.state.lock().await;

        let elapsed = st.window_start.elapsed();
        if elapsed >= self.cfg.window {
            st.window_start = Instant::now();
            st.used = 0;
        }

        st.used
    }

    /// Remaining attempts in the active window.
    pub async fn remaining_attempts(&self) -> u32 {
        let used = self.used_attempts().await;
        self.cfg.max_attempts.saturating_sub(used)
    }
}

/// Limits WS RECONNECT attempts: each attempt costs weight=1.
#[derive(Debug, Clone)]
pub struct ReconnectAttemptLimiter {
    cfg: AttemptLimiterConfig,
    state: Arc<Mutex<State>>,
    metrics: Option<Arc<IngestMetrics>>,
}

impl ReconnectAttemptLimiter {
    pub fn new(cfg: AttemptLimiterConfig, metrics: Option<Arc<IngestMetrics>>) -> Self {
        Self {
            cfg,
            state: Arc::new(Mutex::new(State::new_now())),
            metrics,
        }
    }

    /// Acquire permission for 1 reconnect attempt. Sleeps until window resets if exhausted.
    pub async fn acquire(&self) {
        loop {
            let start_wait = Instant::now();
            let mut st = self.state.lock().await;

            let elapsed = st.window_start.elapsed();
            if elapsed >= self.cfg.window {
                st.window_start = Instant::now();
                st.used = 0;
            }

            if st.used < self.cfg.max_attempts {
                st.used += 1;
                drop(st);

                if let Some(m) = &self.metrics {
                    m.inc_ws_reconnect_attempt();
                    let waited = start_wait.elapsed();
                    if waited.as_secs_f64() > 0.0 {
                        m.observe_ws_reconnect_wait(waited.as_secs_f64());
                    }
                }
                return;
            }

            // exhausted
            if let Some(m) = &self.metrics {
                m.inc_ws_reconnect_rate_limited();
            }

            let sleep_for = self.cfg.window.saturating_sub(elapsed);
            drop(st);
            tokio::time::sleep(sleep_for).await;
        }
    }

    /// Explicitly set used attempts (weight=1 per attempt).
    pub async fn set_used_attempts(&self, used_attempts: u32) {
        let mut st = self.state.lock().await;

        let elapsed = st.window_start.elapsed();
        if elapsed >= self.cfg.window {
            st.window_start = Instant::now();
            st.used = 0;
        }

        st.used = used_attempts.min(self.cfg.max_attempts);
    }

    pub async fn used_attempts(&self) -> u32 {
        let mut st = self.state.lock().await;

        let elapsed = st.window_start.elapsed();
        if elapsed >= self.cfg.window {
            st.window_start = Instant::now();
            st.used = 0;
        }

        st.used
    }

    /// Remaining attempts in the active window.
    pub async fn remaining_attempts(&self) -> u32 {
        let used = self.used_attempts().await;
        self.cfg.max_attempts.saturating_sub(used)
    }
}

/// Builder: subscribe limiter from ExchangeConfig.
pub fn build_ws_subscribe_limiter(
    cfg: &ExchangeConfig,
    metrics: Option<Arc<IngestMetrics>>,
) -> SubscribeAttemptLimiter {
    SubscribeAttemptLimiter::new(
        AttemptLimiterConfig {
            max_attempts: cfg.ws_subscribe_attempt_limit as u32,
            window: Duration::from_secs(cfg.ws_subscribe_attempts_reset_seconds),
        },
        metrics,
    )
}

/// Builder: reconnect limiter from ExchangeConfig.
pub fn build_ws_reconnect_limiter(
    cfg: &ExchangeConfig,
    metrics: Option<Arc<IngestMetrics>>,
) -> ReconnectAttemptLimiter {
    ReconnectAttemptLimiter::new(
        AttemptLimiterConfig {
            max_attempts: cfg.ws_reconnect_attempts_limit as u32,
            window: Duration::from_secs(cfg.ws_reconnect_attempts_reset_seconds),
        },
        metrics,
    )
}
