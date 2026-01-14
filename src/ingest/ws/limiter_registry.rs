// src/ingest/ws_limiter_registry.rs
use crate::{
    app::config::AppConfig,
    error::{AppError, AppResult},
    ingest::{
        config::ExchangeConfigs,
        metrics::IngestMetrics,
        ws::{
            ReconnectAttemptLimiter, SubscribeAttemptLimiter, build_ws_reconnect_limiter,
            build_ws_subscribe_limiter,
        },
    },
};
use std::sync::Arc;

/// Central registry so production code never deals with per-exchange WS limiter instances.
#[derive(Debug, Clone)]
pub struct WsLimiterRegistry {
    pub binance_linear_subscribe: SubscribeAttemptLimiter,
    pub binance_linear_reconnect: ReconnectAttemptLimiter,

    pub hyperliquid_perp_subscribe: SubscribeAttemptLimiter,
    pub hyperliquid_perp_reconnect: ReconnectAttemptLimiter,
}

impl WsLimiterRegistry {
    pub fn new(
        app_cfg: &AppConfig,
        exchange_cfgs: &ExchangeConfigs,
        metrics: Option<Arc<IngestMetrics>>,
    ) -> AppResult<Self> {
        // let exchange_cfgs = ExchangeConfigs::new(app_cfg, false, 0)?;

        let binance_cfg = if app_cfg.exchange_toggles.binance_linear {
            exchange_cfgs.binance_linear.as_ref().ok_or_else(|| {
                AppError::InvalidConfig("binance_linear enabled but config missing".into())
            })?
        } else {
            return Err(AppError::InvalidConfig(
                "WsLimiterRegistry expects binance_linear enabled (or adjust registry to Option fields)".into(),
            ));
        };

        let hyper_cfg = if app_cfg.exchange_toggles.hyperliquid_perp {
            exchange_cfgs.hyperliquid_perp.as_ref().ok_or_else(|| {
                AppError::InvalidConfig("hyperliquid_perp enabled but config missing".into())
            })?
        } else {
            return Err(AppError::InvalidConfig(
                "WsLimiterRegistry expects hyperliquid_perp enabled (or adjust registry to Option fields)".into(),
            ));
        };

        Ok(Self {
            binance_linear_subscribe: build_ws_subscribe_limiter(binance_cfg, metrics.clone()),
            binance_linear_reconnect: build_ws_reconnect_limiter(binance_cfg, metrics.clone()),
            hyperliquid_perp_subscribe: build_ws_subscribe_limiter(hyper_cfg, metrics.clone()),
            hyperliquid_perp_reconnect: build_ws_reconnect_limiter(hyper_cfg, metrics.clone()),
        })
    }

    pub async fn acquire_subscribe(&self, exchange: &str) -> AppResult<()> {
        self.get_subscribe(exchange)?.acquire().await;
        Ok(())
    }

    pub async fn acquire_reconnect(&self, exchange: &str) -> AppResult<()> {
        self.get_reconnect(exchange)?.acquire().await;
        Ok(())
    }

    pub async fn set_used_subscribe_attempts(&self, exchange: &str, used: u32) -> AppResult<()> {
        self.get_subscribe(exchange)?.set_used_attempts(used).await;
        Ok(())
    }

    pub async fn set_used_reconnect_attempts(&self, exchange: &str, used: u32) -> AppResult<()> {
        self.get_reconnect(exchange)?.set_used_attempts(used).await;
        Ok(())
    }

    fn get_subscribe(&self, exchange: &str) -> AppResult<&SubscribeAttemptLimiter> {
        Ok(match exchange {
            "binance_linear" => &self.binance_linear_subscribe,
            "hyperliquid_perp" => &self.hyperliquid_perp_subscribe,
            _ => {
                return Err(AppError::InvalidConfig(format!(
                    "Unknown exchange key '{}' for ws limiter registry (subscribe)",
                    exchange
                )));
            }
        })
    }

    fn get_reconnect(&self, exchange: &str) -> AppResult<&ReconnectAttemptLimiter> {
        Ok(match exchange {
            "binance_linear" => &self.binance_linear_reconnect,
            "hyperliquid_perp" => &self.hyperliquid_perp_reconnect,
            _ => {
                return Err(AppError::InvalidConfig(format!(
                    "Unknown exchange key '{}' for ws limiter registry (reconnect)",
                    exchange
                )));
            }
        })
    }
}

impl WsLimiterRegistry {
    pub async fn get_used_subscribe_attempts(&self, exchange: &str) -> AppResult<u32> {
        Ok(self.get_subscribe(exchange)?.used_attempts().await)
    }

    pub async fn get_used_reconnect_attempts(&self, exchange: &str) -> AppResult<u32> {
        Ok(self.get_reconnect(exchange)?.used_attempts().await)
    }

    // Optional: remaining (often more useful for admission decisions)
    pub async fn get_remaining_subscribe_attempts(&self, exchange: &str) -> AppResult<u32> {
        Ok(self.get_subscribe(exchange)?.remaining_attempts().await)
    }

    pub async fn get_remaining_reconnect_attempts(&self, exchange: &str) -> AppResult<u32> {
        Ok(self.get_reconnect(exchange)?.remaining_attempts().await)
    }
}
