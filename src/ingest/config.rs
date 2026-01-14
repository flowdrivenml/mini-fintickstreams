use crate::app::config::AppConfig;
use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::{io::ErrorKind, path::Path};
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
pub fn load_exchange_config(name: &str, from_env: bool, version: u32) -> AppResult<ExchangeConfig> {
    // local (dev) paths
    const LOCAL_BINANCE: &str = "src/config/binance_linear.toml";
    const LOCAL_HL: &str = "src/config/hyperliquid_perp.toml";

    // k8s default mount paths
    const K8S_BINANCE: &str = "/etc/mini-fintickstreams/binance_linear.toml";
    const K8S_HL: &str = "/etc/mini-fintickstreams/hyperliquid_perp.toml";

    let (local_path, k8s_path, env_key_suffix) = match name {
        "binance_linear" => (LOCAL_BINANCE, K8S_BINANCE, "BINANCE_LINEAR"),
        "hyperliquid_perp" => (LOCAL_HL, K8S_HL, "HYPERLIQUID_PERP"),
        _ => {
            return Err(AppError::InvalidConfig(format!(
                "\n❌ UNKNOWN EXCHANGE CONFIG\n\
                 ├─ requested: `{}`\n\
                 ├─ allowed: `binance_linear`, `hyperliquid_perp`\n\
                 └─ fix: pass a supported exchange name\n",
                name
            )));
        }
    };

    let env_key = format!("MINI_FINTICKSTREAMS_EXCHANGE_CONFIG_PATH_{version}_{env_key_suffix}");

    let (path, source): (String, &'static str) = if from_env {
        match std::env::var(&env_key) {
            Ok(p) => (p, "env var"),
            Err(std::env::VarError::NotPresent) => {
                (k8s_path.to_string(), "default fallback (env var not set)")
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ EXCHANGE config path env var is not valid unicode\n\
                     ├─ exchange: `{}`\n\
                     ├─ env var: `{}`\n\
                     └─ fix: set it to a valid UTF-8 path, e.g.\n\
                        export {}={}\n",
                    name, env_key, env_key, k8s_path
                )));
            }
        }
    } else {
        (local_path.to_string(), "local default (from_env=false)")
    };

    let p = Path::new(&path);

    // Fail fast with explicit reasons before trying to read
    match std::fs::metadata(p) {
        Ok(meta) => {
            if !meta.is_file() {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ EXCHANGE config path exists but is NOT a file\n\
                     ├─ exchange: `{}`\n\
                     ├─ path: `{}`\n\
                     ├─ source: {}\n\
                     └─ fix: point to a TOML file (not a directory)\n",
                    name,
                    p.display(),
                    source
                )));
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return Err(AppError::InvalidConfig(format!(
                "\n❌ EXCHANGE CONFIG FILE NOT FOUND\n\
                 ├─ exchange: `{}`\n\
                 ├─ tried path: `{}`\n\
                 ├─ source: {}\n\
                 ├─ env var (if enabled): `{}`\n\
                 ├─ k8s default fallback: `{}`\n\
                 ├─ local default path: `{}`\n\
                 └─ fix: create the file OR set env var:\n\
                    export {}=/absolute/path/to/{}.toml\n",
                name,
                p.display(),
                source,
                env_key,
                k8s_path,
                local_path,
                env_key,
                name
            )));
        }
        Err(e) if e.kind() == ErrorKind::PermissionDenied => {
            return Err(AppError::InvalidConfig(format!(
                "\n❌ EXCHANGE config file exists but permission was denied\n\
                 ├─ exchange: `{}`\n\
                 ├─ path: `{}`\n\
                 ├─ source: {}\n\
                 └─ os error: {}\n",
                name,
                p.display(),
                source,
                e
            )));
        }
        Err(e) => {
            return Err(AppError::InvalidConfig(format!(
                "\n❌ Failed to stat EXCHANGE config file\n\
                 ├─ exchange: `{}`\n\
                 ├─ path: `{}`\n\
                 ├─ source: {}\n\
                 └─ os error: {}\n",
                name,
                p.display(),
                source,
                e
            )));
        }
    }

    // Read with detailed errors (in case it changes between metadata() and read_to_string())
    let toml_str = fs::read_to_string(p).map_err(|e| match e.kind() {
        ErrorKind::NotFound => AppError::InvalidConfig(format!(
            "\n❌ EXCHANGE config file disappeared while reading\n\
             ├─ exchange: `{}`\n\
             └─ path: `{}`\n",
            name,
            p.display()
        )),
        ErrorKind::PermissionDenied => AppError::InvalidConfig(format!(
            "\n❌ EXCHANGE config file is not readable (permission denied)\n\
             ├─ exchange: `{}`\n\
             ├─ path: `{}`\n\
             └─ os error: {}\n",
            name,
            p.display(),
            e
        )),
        _ => AppError::ConfigIo(e),
    })?;

    let cfg = toml::from_str::<ExchangeConfig>(&toml_str).map_err(AppError::ConfigToml)?;
    Ok(cfg)
}

#[derive(Debug)]
pub struct ExchangeConfigs {
    pub binance_linear: Option<ExchangeConfig>,
    pub hyperliquid_perp: Option<ExchangeConfig>,
}

impl ExchangeConfigs {
    pub fn new(app_cfg: &AppConfig, from_env: bool, version: u32) -> AppResult<ExchangeConfigs> {
        let mut exchanges = ExchangeConfigs {
            binance_linear: None,
            hyperliquid_perp: None,
        };

        if app_cfg.exchange_toggles.binance_linear {
            exchanges.binance_linear =
                Some(load_exchange_config("binance_linear", from_env, version)?);
        }

        if app_cfg.exchange_toggles.hyperliquid_perp {
            exchanges.hyperliquid_perp =
                Some(load_exchange_config("hyperliquid_perp", from_env, version)?);
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
        let binance = load_exchange_config("binance_linear", false, 0)
            .expect("failed to load binance_linear config");

        let hyper = load_exchange_config("hyperliquid_perp", false, 0)
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
