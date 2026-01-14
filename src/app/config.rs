use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub id: String,
    pub env: String,
    pub config_version: u32,

    pub db: DbConfig,
    pub redis: RedisConfig,

    pub scales: ScalesConfig,

    pub exchange_toggles: ExchangeToggles,
    pub streams: StreamsConfig,
    pub limits: LimitsConfig,
    pub logging: LoggingConfig,
    pub metrics: MetricsConfig,

    // NEW
    pub health: HealthConfig,
}

#[derive(Debug, Deserialize)]
pub struct DbConfig {
    pub enabled: bool,
    pub verify: bool,
}

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct ScalesConfig {
    pub price: i64,
    pub qty: i64,
    pub open_interest: i64,
    pub funding: i64,
}

#[derive(Debug, Deserialize)]
pub struct ExchangeToggles {
    pub binance_linear: bool,
    pub hyperliquid_perp: bool,
}

#[derive(Debug, Deserialize)]
pub struct StreamsConfig {
    // --- reconnect policy ---
    pub ws_reconnect_backoff_initial_ms: u64,
    pub ws_reconnect_backoff_max_ms: u64,
    pub ws_reconnect_trip_after_failures: u32,
    pub ws_reconnect_cooldown_seconds: u64,
}

#[derive(Debug, Deserialize)]
pub struct LimitsConfig {
    pub max_active_streams: u32,
    pub max_events_per_sec: u64,
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
}

#[derive(Debug, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
}

// ==================================================
// NEW: Health + Runtime health (GREEN/RED only)
// ==================================================

#[derive(Debug, Deserialize)]
pub struct HealthConfig {
    pub enabled: bool,
    pub runtime: RuntimeHealthConfig,
}

#[derive(Debug, Deserialize)]
pub struct RuntimeHealthConfig {
    pub enabled: bool,
    pub poll_interval_ms: u64,
    pub hold_down_ms: u64,

    // Memory (RAM)
    pub max_rss_mb_red: Option<u64>,
    pub min_avail_mem_mb_red: Option<u64>,

    // File descriptors
    pub fd_pct_red: Option<u64>, // 0..=100

    // Tokio overload proxy (scheduler latency / drift)
    pub tick_drift_ms_red: u64,
    pub drift_sustain_ticks: u32,

    // Optional CPU sustained overload
    pub cpu_pct_red: Option<u64>,     // 0..=100
    pub cpu_sustain_sec: Option<u64>, // seconds
}

fn validate_config(cfg: &AppConfig) -> AppResult<()> {
    // --------------------------------------------------
    // booleans already typed as bool (no try_into needed),
    // but leaving your style intact
    // --------------------------------------------------
    let db_enabled: bool = cfg
        .db
        .enabled
        .try_into()
        .map_err(|_| AppError::InvalidConfig("db.enabled must be a boolean".into()))?;

    let db_verify: bool = cfg
        .db
        .verify
        .try_into()
        .map_err(|_| AppError::InvalidConfig("db.verify must be a boolean".into()))?;

    if db_enabled && !db_verify {
        return Err(AppError::InvalidConfig(
            "db.verify must be true when db.enabled is true".into(),
        ));
    }

    let _redis_enabled: bool = cfg
        .redis
        .enabled
        .try_into()
        .map_err(|_| AppError::InvalidConfig("redis.enabled must be a boolean".into()))?;

    if cfg.id.is_empty() {
        return Err(AppError::MissingConfig("id"));
    }

    if cfg.config_version == 0 {
        return Err(AppError::InvalidConfig(
            "config_version must be >= 1".into(),
        ));
    }

    if cfg.limits.max_active_streams == 0 {
        return Err(AppError::InvalidConfig(
            "max_active_streams must be > 0".into(),
        ));
    }

    if cfg.limits.max_events_per_sec == 0 {
        return Err(AppError::InvalidConfig(
            "max_events_per_sec must be > 0".into(),
        ));
    }

    // --------------------------------------------------
    // Fixed-point scale validation
    // --------------------------------------------------
    let scales = &cfg.scales;

    for (name, value) in [
        ("price", scales.price),
        ("qty", scales.qty),
        ("open_interest", scales.open_interest),
        ("funding", scales.funding),
    ] {
        if value <= 0 {
            return Err(AppError::InvalidConfig(format!(
                "scale '{name}' must be > 0"
            )));
        }

        if !is_power_of_ten(value) {
            return Err(AppError::InvalidConfig(format!(
                "scale '{name}' must be a power of 10 (got {value})"
            )));
        }
    }

    // --------------------------------------------------
    // Streams reconnect policy validation
    // --------------------------------------------------
    let s = &cfg.streams;

    if s.ws_reconnect_backoff_initial_ms == 0 {
        return Err(AppError::InvalidConfig(
            "streams.ws_reconnect_backoff_initial_ms must be > 0".into(),
        ));
    }

    // cap should be >= initial (otherwise backoff logic gets weird)
    if s.ws_reconnect_backoff_max_ms < s.ws_reconnect_backoff_initial_ms {
        return Err(AppError::InvalidConfig(format!(
            "streams.ws_reconnect_backoff_max_ms ({}) must be >= streams.ws_reconnect_backoff_initial_ms ({})",
            s.ws_reconnect_backoff_max_ms, s.ws_reconnect_backoff_initial_ms
        )));
    }

    // keep the cap within a sane bound to avoid "stuck for hours" misconfigs
    // (adjust if you have a use-case for longer)
    if s.ws_reconnect_backoff_max_ms > 10 * 60 * 1000 {
        return Err(AppError::InvalidConfig(
            "streams.ws_reconnect_backoff_max_ms must be <= 600000 (10 minutes)".into(),
        ));
    }

    if s.ws_reconnect_trip_after_failures == 0 {
        return Err(AppError::InvalidConfig(
            "streams.ws_reconnect_trip_after_failures must be > 0".into(),
        ));
    }

    // prevent "trip on every tiny blip" misconfig
    if s.ws_reconnect_trip_after_failures > 10_000 {
        return Err(AppError::InvalidConfig(
            "streams.ws_reconnect_trip_after_failures must be <= 10000".into(),
        ));
    }

    if s.ws_reconnect_cooldown_seconds == 0 {
        return Err(AppError::InvalidConfig(
            "streams.ws_reconnect_cooldown_seconds must be > 0".into(),
        ));
    }

    // avoid accidentally setting like 1 day and wondering why it's dead
    if s.ws_reconnect_cooldown_seconds > 60 * 60 {
        return Err(AppError::InvalidConfig(
            "streams.ws_reconnect_cooldown_seconds must be <= 3600 (1 hour)".into(),
        ));
    }

    // --------------------------------------------------
    // NEW: Health runtime validation (GREEN/RED)
    // --------------------------------------------------
    validate_health_config(&cfg.health)?;

    Ok(())
}

fn validate_health_config(health: &HealthConfig) -> AppResult<()> {
    let _enabled: bool = health
        .enabled
        .try_into()
        .map_err(|_| AppError::InvalidConfig("health.enabled must be a boolean".into()))?;

    // If health is disabled, we still validate structure lightly (optional).
    // You can choose to return Ok(()) here if you want to skip all checks.
    let rt = &health.runtime;

    let _rt_enabled: bool = rt
        .enabled
        .try_into()
        .map_err(|_| AppError::InvalidConfig("health.runtime.enabled must be a boolean".into()))?;

    if rt.poll_interval_ms == 0 {
        return Err(AppError::InvalidConfig(
            "health.runtime.poll_interval_ms must be > 0".into(),
        ));
    }

    if rt.hold_down_ms == 0 {
        return Err(AppError::InvalidConfig(
            "health.runtime.hold_down_ms must be > 0".into(),
        ));
    }

    // At least one red trigger should exist, otherwise runtime health can never flip red
    let has_any_trigger = rt.max_rss_mb_red.is_some()
        || rt.min_avail_mem_mb_red.is_some()
        || rt.fd_pct_red.is_some()
        || rt.cpu_pct_red.is_some()
        || rt.tick_drift_ms_red > 0;

    if !has_any_trigger {
        return Err(AppError::InvalidConfig(
            "health.runtime must define at least one red threshold".into(),
        ));
    }

    // Memory thresholds (if present)
    if let Some(rss) = rt.max_rss_mb_red {
        if rss == 0 {
            return Err(AppError::InvalidConfig(
                "health.runtime.max_rss_mb_red must be > 0".into(),
            ));
        }
    }

    if let Some(avail) = rt.min_avail_mem_mb_red {
        if avail == 0 {
            return Err(AppError::InvalidConfig(
                "health.runtime.min_avail_mem_mb_red must be > 0".into(),
            ));
        }
    }

    // FD percentage (if present)
    if let Some(fd) = rt.fd_pct_red {
        if fd > 100 {
            return Err(AppError::InvalidConfig(
                "health.runtime.fd_pct_red must be between 0 and 100".into(),
            ));
        }
        if fd == 0 {
            return Err(AppError::InvalidConfig(
                "health.runtime.fd_pct_red must be > 0 (or omit it)".into(),
            ));
        }
    }

    // Drift (required in this model)
    if rt.tick_drift_ms_red == 0 {
        return Err(AppError::InvalidConfig(
            "health.runtime.tick_drift_ms_red must be > 0".into(),
        ));
    }

    if rt.drift_sustain_ticks == 0 {
        return Err(AppError::InvalidConfig(
            "health.runtime.drift_sustain_ticks must be > 0".into(),
        ));
    }

    // CPU sustained (optional but must be consistent)
    match (rt.cpu_pct_red, rt.cpu_sustain_sec) {
        (Some(pct), Some(sec)) => {
            if pct > 100 || pct == 0 {
                return Err(AppError::InvalidConfig(
                    "health.runtime.cpu_pct_red must be between 1 and 100".into(),
                ));
            }
            if sec == 0 {
                return Err(AppError::InvalidConfig(
                    "health.runtime.cpu_sustain_sec must be > 0".into(),
                ));
            }
        }
        (None, None) => {}
        _ => {
            return Err(AppError::InvalidConfig(
                "health.runtime.cpu_pct_red and cpu_sustain_sec must be set together (or both omitted)"
                    .into(),
            ));
        }
    }

    Ok(())
}

fn is_power_of_ten(mut v: i64) -> bool {
    if v <= 0 {
        return false;
    }
    while v % 10 == 0 {
        v /= 10;
    }
    v == 1
}

use std::{io::ErrorKind, path::Path};

const APP_CONFIG_PATH: &str = "src/config/app.toml";

pub fn load_app_config(from_env: bool, version: u32) -> AppResult<AppConfig> {
    const DEFAULT_K8S_PATH: &str = "/etc/mini-fintickstreams/app.toml";

    let key = format!("MINI_FINTICKSTREAMS_APP_CONFIG_PATH_{version}");

    let (path, source): (String, &'static str) = if from_env {
        match std::env::var(&key) {
            Ok(p) => (p, "env var"),
            Err(std::env::VarError::NotPresent) => (
                DEFAULT_K8S_PATH.to_string(),
                "default fallback (env var not set)",
            ),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ APP config path env var is not valid unicode\n\
                     ├─ env var: `{}`\n\
                     └─ fix: set it to a valid UTF-8 path, e.g.\n\
                        export {}={}\n",
                    key, key, DEFAULT_K8S_PATH
                )));
            }
        }
    } else {
        (
            APP_CONFIG_PATH.to_string(),
            "local default (from_env=false)",
        )
    };

    let p = Path::new(&path);

    // Fail fast with explicit reasons before trying to read
    match std::fs::metadata(p) {
        Ok(meta) => {
            if !meta.is_file() {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ APP config path exists but is NOT a file\n\
                     ├─ path: `{}`\n\
                     ├─ source: {}\n\
                     └─ fix: point to a TOML file (not a directory)\n",
                    p.display(),
                    source
                )));
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return Err(AppError::InvalidConfig(format!(
                "\n❌ APP CONFIG FILE NOT FOUND\n\
                 ├─ tried path: `{}`\n\
                 ├─ source: {}\n\
                 ├─ env var (if enabled): `{}`\n\
                 ├─ k8s default fallback: `{}`\n\
                 ├─ local default path: `{}`\n\
                 └─ fix: create the file OR set env var:\n\
                    export {}=/absolute/path/to/app.toml\n",
                p.display(),
                source,
                key,
                DEFAULT_K8S_PATH,
                APP_CONFIG_PATH,
                key
            )));
        }
        Err(e) if e.kind() == ErrorKind::PermissionDenied => {
            return Err(AppError::InvalidConfig(format!(
                "\n❌ APP config file exists but permission was denied\n\
                 ├─ path: `{}`\n\
                 ├─ source: {}\n\
                 └─ os error: {}\n",
                p.display(),
                source,
                e
            )));
        }
        Err(e) => {
            return Err(AppError::InvalidConfig(format!(
                "\n❌ Failed to stat APP config file\n\
                 ├─ path: `{}`\n\
                 ├─ source: {}\n\
                 └─ os error: {}\n",
                p.display(),
                source,
                e
            )));
        }
    }

    // Read with better errors if something changes between metadata() and read_to_string()
    let contents = std::fs::read_to_string(p).map_err(|e| match e.kind() {
        ErrorKind::NotFound => AppError::InvalidConfig(format!(
            "\n❌ APP config file disappeared while reading\n\
             └─ path: `{}`\n",
            p.display()
        )),
        ErrorKind::PermissionDenied => AppError::InvalidConfig(format!(
            "\n❌ APP config file is not readable (permission denied)\n\
             ├─ path: `{}`\n\
             └─ os error: {}\n",
            p.display(),
            e
        )),
        _ => AppError::ConfigIo(e), // preserves raw IO error
    })?;

    // Keep parse errors as your typed error (has great info already)
    let config: AppConfig = toml::from_str(&contents).map_err(AppError::ConfigToml)?;

    validate_config(&config)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_and_print_app_config() {
        let cfg = load_app_config(false, 0).expect("failed to load app config");

        println!("id = {}", cfg.id);
        println!("env = {}", cfg.env);
        println!("config_version = {}", cfg.config_version);

        println!(
            "exchanges: binance_linear={}, hyperliquid_perp={}",
            cfg.exchange_toggles.binance_linear, cfg.exchange_toggles.hyperliquid_perp
        );

        println!(
            "limits: max_active_streams={}, max_events_per_sec={}",
            cfg.limits.max_active_streams, cfg.limits.max_events_per_sec
        );

        println!("logging.level = {}", cfg.logging.level);
        println!("metrics.enabled = {}", cfg.metrics.enabled);

        println!("health.enabled = {}", cfg.health.enabled);
        println!("health.runtime.enabled = {}", cfg.health.runtime.enabled);
        println!(
            "health.runtime: poll_interval_ms={}, hold_down_ms={}",
            cfg.health.runtime.poll_interval_ms, cfg.health.runtime.hold_down_ms
        );
    }
}
