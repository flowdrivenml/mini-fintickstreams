use crate::app::config::AppConfig;
use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;

// -----------------------------
// Root config
// -----------------------------
#[derive(Debug, Deserialize, Clone)]
pub struct ExchangeConfig {
    pub timezone: String,
    pub exchange: String,
    pub margin: String,
    pub api_pool: String,

    // REST
    pub api_base_url: String,

    pub max_weight: Option<u64>,
    pub window: u64,
    pub api_weight_header_key: String,

    // WebSocket
    pub ws_base_url: String,
    pub ws_connection_timeout_seconds: u64,
    pub ws_max_streams_per_connection: u64,

    pub ws_heartbeat_type: Option<String>,

    pub ws_heartbeat_timeout_seconds: Option<u64>,

    // Sometimes "" (string), sometimes { method="ping" } (table)
    pub ws_heartbeat_frame: Option<StringOrTable>,

    pub ws_reconnect_attempts_limit: u64,
    pub ws_reconnect_attempts_reset_seconds: u64,

    pub ws_subscribe_attempt_limit: u64,
    pub ws_subscribe_attempts_reset_seconds: u64,

    // Message templates differ a lot between exchanges
    pub ws_subscribe_msg: Option<TableValue>,
    pub ws_unsubscribe_msg: Option<TableValue>,

    // Dynamic keyed tables: [api.*] and [ws.*]
    #[serde(default)]
    pub api: BTreeMap<String, ApiEndpoint>,
    #[serde(default)]
    pub ws: BTreeMap<String, WsStream>,
}

// -----------------------------
// API endpoint table entries
// -----------------------------
#[derive(Debug, Deserialize, Clone)]
pub struct ApiEndpoint {
    pub endpoint: String,
    pub weight: u64,
    #[serde(default)]
    pub params: Option<TableValue>,
    pub interval_seconds: u64,
    pub method: String,
}

// -----------------------------
// WS stream table entries
// -----------------------------
#[derive(Debug, Deserialize, Clone)]
pub struct WsStream {
    // Binance: string "<symbol>@aggTrade"
    // Hyperliquid: table { type="trades", coin="<coin>" }
    pub stream_title: Option<String>,
    pub coin: Option<String>,
    pub subscription_type: Option<String>,
}

// -----------------------------
// Helpers for polymorphic TOML values
// -----------------------------

/// A TOML "table-like object" or more generally any TOML value.
pub type TableValue = toml::Value;

/// Used for fields that can be either "..." or { ... }.
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum StringOrTable {
    String(String),
    Table(TableValue),
}

// -----------------------------
// Loader
// -----------------------------
pub fn load_exchange_config(name: &str) -> AppResult<ExchangeConfig> {
    let path = match name {
        "binance_linear" => "src/config/binance_linear.toml",
        "hyperliquid_perp" => "src/config/hyperliquid_perp.toml",
        _ => {
            return Err(AppError::ConfigIo(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unknown exchange config: {name}"),
            )));
        }
    };

    let toml_str = fs::read_to_string(path).map_err(AppError::ConfigIo)?;
    let cfg = toml::from_str::<ExchangeConfig>(&toml_str).map_err(AppError::ConfigToml)?;

    Ok(cfg)
}

#[derive(Debug)]
pub struct ExchangeConfigs {
    pub binance_linear: Option<ExchangeConfig>,
    pub hyperliquid_perp: Option<ExchangeConfig>,
}

impl ExchangeConfigs {
    pub fn new(app_cfg: &AppConfig) -> AppResult<ExchangeConfigs> {
        let mut exchanges = ExchangeConfigs {
            binance_linear: None,
            hyperliquid_perp: None,
        };

        if app_cfg.exchange_toggles.binance_linear {
            exchanges.binance_linear = Some(load_exchange_config("binance_linear")?);
        }

        if app_cfg.exchange_toggles.hyperliquid_perp {
            exchanges.hyperliquid_perp = Some(load_exchange_config("hyperliquid_perp")?);
        }

        Ok(exchanges)
    }
}

use crate::app::stream_types::ExchangeId;

impl ExchangeConfigs {
    pub fn get(&self, exchange: ExchangeId) -> Option<&ExchangeConfig> {
        match exchange {
            ExchangeId::BinanceLinear => self.binance_linear.as_ref(),
            ExchangeId::HyperliquidPerp => self.hyperliquid_perp.as_ref(),
        }
    }

    pub fn get_mut(&mut self, exchange: ExchangeId) -> Option<&mut ExchangeConfig> {
        match exchange {
            ExchangeId::BinanceLinear => self.binance_linear.as_mut(),
            ExchangeId::HyperliquidPerp => self.hyperliquid_perp.as_mut(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::load_exchange_config;

    #[test]
    fn print_exchange_configs() {
        let binance =
            load_exchange_config("binance_linear").expect("failed to load binance_linear config");

        let hyper = load_exchange_config("hyperliquid_perp")
            .expect("failed to load hyperliquid_perp config");

        println!("\n=== BINANCE LINEAR CONFIG ===");
        println!("{:#?}", binance);
        println!(
            "exchange={} ws.depth_update={:?}",
            binance.exchange,
            binance.ws.get("depth_update")
        );

        println!("\n=== HYPERLIQUID PERP CONFIG ===");
        println!("{:#?}", hyper);
        println!(
            "exchange={} ws.depth_update={:?}",
            hyper.exchange,
            hyper.ws.get("depth_update")
        );
    }
}
