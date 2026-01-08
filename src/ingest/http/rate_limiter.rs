// src/ingest/http/rate_limiter.rs
use crate::app::config::AppConfig;
use crate::error::{AppError, AppResult};
use crate::ingest::config::{ExchangeConfig, ExchangeConfigs};
use crate::ingest::metrics::IngestMetrics;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub max_weight: u32,
    pub window: Duration,
}

#[derive(Debug, Clone)]
pub enum WeightSync {
    ManualOnly,
    UsedWeightHeader { header_key: String },
}

#[derive(Debug)]
struct WindowState {
    window_start: Instant,
    used_weight: u32,
}

#[derive(Debug, Clone)]
pub struct WeightedWindowLimiter {
    cfg: RateLimitConfig,
    sync: WeightSync,
    metrics: Option<Arc<IngestMetrics>>,
    pub state: Arc<Mutex<WindowState>>,
}

impl WeightedWindowLimiter {
    pub fn new(
        cfg: RateLimitConfig,
        sync: WeightSync,
        metrics: Option<Arc<IngestMetrics>>,
    ) -> Self {
        Self {
            cfg,
            sync,
            metrics,
            state: Arc::new(Mutex::new(WindowState {
                window_start: Instant::now(),
                used_weight: 0,
            })),
        }
    }

    /// Wait until at least `weight` is available (client-side fixed-window limiter).
    ///
    /// Records time spent waiting in `ingest_rate_limit_wait_seconds` (if metrics enabled).
    pub async fn acquire(&self, weight: u32) {
        loop {
            let mut st = self.state.lock().await;

            let elapsed = st.window_start.elapsed();
            if elapsed >= self.cfg.window {
                st.window_start = Instant::now();
                st.used_weight = 0;
            }

            if st.used_weight.saturating_add(weight) <= self.cfg.max_weight {
                st.used_weight += weight;
                return;
            }

            // Not enough budget — sleep until our window resets.
            let sleep_for = self.cfg.window.saturating_sub(elapsed);
            drop(st);

            let t0 = Instant::now();
            tokio::time::sleep(sleep_for).await;
            let waited = t0.elapsed().as_secs_f64();

            if let Some(m) = self.metrics.as_ref() {
                m.observe_rate_limit_wait(waited);
            }
        }
    }
    /// Current used weight in the active window (auto-resets if window expired).
    pub async fn used_weight(&self) -> u32 {
        let mut st = self.state.lock().await;

        let elapsed = st.window_start.elapsed();
        if elapsed >= self.cfg.window {
            st.window_start = Instant::now();
            st.used_weight = 0;
        }

        st.used_weight
    }

    /// Optional convenience: remaining budget in current window.
    pub async fn remaining_weight(&self) -> u32 {
        let used = self.used_weight().await;
        self.cfg.max_weight.saturating_sub(used)
    }
    /// Single entrypoint to sync used weight.
    ///
    /// - If `explicit_used` is Some(u32), set it directly (manual mode / caller knows value).
    /// - Else, if WeightSync::UsedWeightHeader is configured, try to parse it from `headers`.
    /// - Otherwise, do nothing.
    pub async fn set_used_weight(
        &self,
        headers: Option<&reqwest::header::HeaderMap>,
        explicit_used: Option<u32>,
    ) {
        let used_opt = if let Some(u) = explicit_used {
            Some(u)
        } else {
            match (&self.sync, headers) {
                (WeightSync::UsedWeightHeader { header_key }, Some(h)) => h
                    .get(header_key)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u32>().ok()),
                _ => None,
            }
        };

        if let Some(used) = used_opt {
            let mut st = self.state.lock().await;
            st.used_weight = used.min(self.cfg.max_weight);
        }
    }
}

fn build_limiter(
    cfg: &ExchangeConfig,
    metrics: Option<Arc<IngestMetrics>>,
) -> Option<WeightedWindowLimiter> {
    let max = cfg.max_weight? as u32;

    Some(WeightedWindowLimiter::new(
        RateLimitConfig {
            max_weight: max,
            window: Duration::from_secs(cfg.window),
        },
        WeightSync::UsedWeightHeader {
            header_key: cfg.api_weight_header_key.clone(),
        },
        metrics,
    ))
}

/// Central registry so production code never deals with per-exchange limiter instances.
#[derive(Debug, Clone)]
pub struct RateLimiterRegistry {
    pub binance_linear: Option<WeightedWindowLimiter>,
    pub hyperliquid_perp: Option<WeightedWindowLimiter>,
}

impl RateLimiterRegistry {
    /// Build from AppConfig toggles + loaded ExchangeConfigs.
    ///
    /// - If an exchange is disabled via toggles -> limiter is None.
    /// - If enabled but config missing -> returns an error (production-safe).
    pub fn new(app_cfg: &AppConfig, metrics: Option<Arc<IngestMetrics>>) -> AppResult<Self> {
        let exchange_cfgs = ExchangeConfigs::new(app_cfg)?;

        let binance_linear = if app_cfg.exchange_toggles.binance_linear {
            let cfg = exchange_cfgs.binance_linear.as_ref().ok_or_else(|| {
                AppError::InvalidConfig("binance_linear enabled but config missing".into())
            })?;
            build_limiter(cfg, metrics.clone())
        } else {
            None
        };

        let hyperliquid_perp = if app_cfg.exchange_toggles.hyperliquid_perp {
            let cfg = exchange_cfgs.hyperliquid_perp.as_ref().ok_or_else(|| {
                AppError::InvalidConfig("hyperliquid_perp enabled but config missing".into())
            })?;
            build_limiter(cfg, metrics.clone())
        } else {
            None
        };

        Ok(Self {
            binance_linear,
            hyperliquid_perp,
        })
    }

    /// Acquire weight for a specific exchange (no-op if limiter not configured).
    pub async fn acquire(&self, exchange: &str, weight: u32) -> AppResult<()> {
        if let Some(l) = self.get(exchange)? {
            l.acquire(weight).await;
        }
        Ok(())
    }

    /// Sync used weight for a specific exchange (no-op if limiter not configured).
    pub async fn set_used_weight(
        &self,
        exchange: &str,
        headers: Option<&reqwest::header::HeaderMap>,
        explicit_used: Option<u32>,
    ) -> AppResult<()> {
        if let Some(l) = self.get(exchange)? {
            l.set_used_weight(headers, explicit_used).await;
        }
        Ok(())
    }

    /// Internal: fetch limiter by exchange key.
    pub fn get(&self, exchange: &str) -> AppResult<Option<&WeightedWindowLimiter>> {
        Ok(match exchange {
            "binance_linear" => self.binance_linear.as_ref(),
            "hyperliquid_perp" => self.hyperliquid_perp.as_ref(),
            _ => {
                return Err(AppError::InvalidConfig(format!(
                    "Unknown exchange key '{}' for rate limiter registry",
                    exchange
                )));
            }
        })
    }

    /// Get current used weight for an exchange (None if limiter not configured).
    pub async fn get_used_weight(&self, exchange: &str) -> AppResult<Option<u32>> {
        if let Some(l) = self.get(exchange)? {
            Ok(Some(l.used_weight().await))
        } else {
            Ok(None)
        }
    }

    /// Optional: remaining budget for an exchange (None if limiter not configured).
    pub async fn get_remaining_weight(&self, exchange: &str) -> AppResult<Option<u32>> {
        if let Some(l) = self.get(exchange)? {
            Ok(Some(l.remaining_weight().await))
        } else {
            Ok(None)
        }
    }
}
