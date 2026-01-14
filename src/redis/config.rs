use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::io::ErrorKind;
use std::{collections::HashMap, fs, path::Path, path::PathBuf};

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    /// Runtime switch (not required in redis.toml).
    /// If omitted in TOML, defaults to true.
    #[serde(default = "default_true")]
    pub enabled: bool,

    pub mode: RedisMode,
    pub default_node: String,

    pub nodes: HashMap<String, String>,
    /// Optional env var override per node key (e.g. a -> "REDIS_NODE_A")
    #[serde(default)]
    pub nodes_env: HashMap<String, String>,

    pub connection: ConnectionConfig,

    pub capacity: CapacityConfig,
    pub failover: FailoverConfig,
    pub streams: StreamsConfig,
    pub retention: RetentionConfig,

    /// DOCUMENTATION ONLY for this producer; consumers will use these names.
    /// Keep it here so people don't forget the agreed naming.
    pub groups: GroupsConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ConnectionConfig {
    pub connect_timeout_ms: u64,
    pub command_timeout_ms: u64,
    pub keepalive_sec: u64,
    pub tcp_nodelay: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisMode {
    Single,
    Pool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CapacityConfig {
    pub poll_interval_sec: u64,
    pub max_memory_pct: u8,
    pub max_pending: u64,

    /// Rolling p99 command latency threshold (ms). App-defined.
    pub max_p99_cmd_ms: u64,
    pub redis_publish_latency_window: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FailoverConfig {
    pub on_saturated: SaturationPolicy,
    pub on_down: DownPolicy,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SaturationPolicy {
    StopAssigningNew,
    ErrorNew,
    SpilloverToOtherNode, // future (only if you ever allow multi-node new-assign placement)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownPolicy {
    DisableRedisTemporarily,
    PauseAndRetry, // future
                   // historical (do not use in producer-only, no-reroute design):
                   // RerouteNewOnly,
                   // RerouteAll,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamsConfig {
    pub key_format: String,

    pub publish_trades: bool,
    pub publish_depth: bool,
    pub publish_liquidations: bool,
    pub publish_funding: bool,
    pub publish_open_interest: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetentionConfig {
    pub maxlen: u64,
    pub approx: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GroupsConfig {
    pub feature_builder: String,

    /// Optional; may be commented out in redis.toml.
    #[serde(default)]
    pub ml_infer: Option<String>,
}

impl RedisConfig {
    pub fn load_from_file(path: impl AsRef<Path>) -> AppResult<Self> {
        let path = path.as_ref();

        // 1) Read file (with path + operation context)
        let raw = fs::read_to_string(path).map_err(|e| AppError::ConfigIoCtx {
            operation: "read_to_string",
            path: path.to_path_buf(),
            source: e,
        })?;

        // 2) Parse TOML (with path context)
        let cfg: Self = toml::from_str(&raw).map_err(|e| {
            AppError::InvalidConfig(format!(
                "\n❌ Failed to parse Redis config TOML\n\
                 ├─ path: `{}`\n\
                 └─ error: {}\n",
                path.display(),
                e
            ))
        })?;

        // 3) Validate (with path context)
        cfg.validate().map_err(|e| {
            AppError::InvalidConfig(format!(
                "\n❌ Redis config failed validation\n\
                 ├─ path: `{}`\n\
                 └─ error: {}\n",
                path.display(),
                e
            ))
        })?;

        Ok(cfg)
    }

    /// Primary path: `config/redis.toml`
    /// Fallback path: `src/config/redis.toml`
    pub fn load_default() -> AppResult<Self> {
        const PRIMARY: &str = "config/redis.toml";
        const FALLBACK: &str = "src/config/redis.toml";

        let primary_attempt = Self::load_from_file(PRIMARY);
        match primary_attempt {
            Ok(cfg) => return Ok(cfg),
            Err(primary_err) => {
                let fallback_attempt = Self::load_from_file(FALLBACK);
                match fallback_attempt {
                    Ok(cfg) => Ok(cfg),
                    Err(fallback_err) => Err(AppError::InvalidConfig(format!(
                        "\n❌ Failed to load Redis config from default locations\n\
                         ├─ primary: `{}`\n\
                         │  └─ error: {}\n\
                         ├─ fallback: `{}`\n\
                         │  └─ error: {}\n\
                         └─ fix: create one of these files, or point the app to the correct path\n",
                        PRIMARY, primary_err, FALLBACK, fallback_err
                    ))),
                }
            }
        }
    }

    /// Kubernetes-aware loader
    ///
    /// - if `from_env == false`: loads from repo (`src/config/redis.toml`)
    /// - if `from_env == true`: uses `MINI_FINTICKSTREAMS_REDIS_CONFIG_PATH_{version}`
    ///   and falls back to `/etc/mini-fintickstreams/redis.toml`
    pub fn load(from_env: bool, version: u32) -> AppResult<Self> {
        const DEFAULT_K8S_PATH: &str = "/etc/mini-fintickstreams/redis.toml";

        if !from_env {
            return Self::load_default();
        }

        let key = format!("MINI_FINTICKSTREAMS_REDIS_CONFIG_PATH_{version}");

        let (path, source): (String, &'static str) = match std::env::var(&key) {
            Ok(p) => (p, "env var"),
            Err(std::env::VarError::NotPresent) => (
                DEFAULT_K8S_PATH.to_string(),
                "default fallback (env var not set)",
            ),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ REDIS config path env var is not valid unicode\n\
                 ├─ env var: `{}`\n\
                 └─ fix: set it to a valid UTF-8 path, e.g.\n\
                    export {}={}\n",
                    key, key, DEFAULT_K8S_PATH
                )));
            }
        };

        let p = Path::new(&path);

        // Fail fast with explicit diagnostics
        match std::fs::metadata(p) {
            Ok(meta) => {
                if !meta.is_file() {
                    return Err(AppError::InvalidConfig(format!(
                        "\n❌ REDIS config path exists but is NOT a file\n\
                     ├─ path: `{}`\n\
                     ├─ source: {} (`{}`)\n\
                     └─ fix: point `{}` to a TOML file (not a directory)\n",
                        p.display(),
                        source,
                        key,
                        key
                    )));
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ REDIS CONFIG FILE NOT FOUND\n\
                 ├─ tried path: `{}`\n\
                 ├─ source: {} (`{}`)\n\
                 ├─ k8s default fallback: `{}`\n\
                 └─ fix: create the file OR set env var:\n\
                    export {}=/absolute/path/to/redis.toml\n",
                    p.display(),
                    source,
                    key,
                    DEFAULT_K8S_PATH,
                    key
                )));
            }
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ REDIS config file exists but permission was denied\n\
                 ├─ path: `{}`\n\
                 ├─ source: {} (`{}`)\n\
                 └─ os error: {}\n",
                    p.display(),
                    source,
                    key,
                    e
                )));
            }
            Err(e) => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ Failed to stat REDIS config file\n\
                 ├─ path: `{}`\n\
                 ├─ source: {} (`{}`)\n\
                 └─ os error: {}\n",
                    p.display(),
                    source,
                    key,
                    e
                )));
            }
        }

        // Delegate to your real loader (which returns ConfigIo / ConfigToml)
        Self::load_from_file(&path).map_err(|e| {
            AppError::InvalidConfig(format!(
                "\n❌ Failed to load REDIS config\n\
             ├─ path: `{}`\n\
             ├─ source: {} (`{}`)\n\
             └─ error: {}\n",
                p.display(),
                source,
                key,
                e
            ))
        })
    }

    pub fn validate(&self) -> AppResult<()> {
        // If disabled, keep validation light (parseable, but don’t block startup)
        if !self.enabled {
            return Ok(());
        }

        // mode + default node sanity
        if self.default_node.trim().is_empty() {
            return Err(AppError::InvalidConfig(
                "redis.toml: default_node must not be empty".into(),
            ));
        }

        if self.nodes.is_empty() {
            return Err(AppError::InvalidConfig(
                "redis.toml: [nodes] must contain at least one node".into(),
            ));
        }

        if !self.nodes.contains_key(self.default_node.trim()) {
            return Err(AppError::InvalidConfig(format!(
                "redis.toml: default_node '{}' not found in [nodes]",
                self.default_node
            )));
        }

        // node URI sanity
        for (name, uri) in &self.nodes {
            let u = uri.trim();
            if u.is_empty() {
                return Err(AppError::InvalidConfig(format!(
                    "redis.toml: node '{name}' URI must not be empty"
                )));
            }
            if !u.starts_with("redis://") && !u.starts_with("rediss://") {
                return Err(AppError::InvalidConfig(format!(
                    "redis.toml: node '{name}' URI must start with redis:// or rediss://"
                )));
            }
        }

        // connection
        if self.connection.connect_timeout_ms == 0 {
            return Err(AppError::InvalidConfig(
                "redis.toml: connection.connect_timeout_ms must be > 0".into(),
            ));
        }
        if self.connection.command_timeout_ms == 0 {
            return Err(AppError::InvalidConfig(
                "redis.toml: connection.command_timeout_ms must be > 0".into(),
            ));
        }
        if self.connection.keepalive_sec == 0 {
            return Err(AppError::InvalidConfig(
                "redis.toml: connection.keepalive_sec must be > 0".into(),
            ));
        }

        // capacity
        if self.capacity.poll_interval_sec == 0 {
            return Err(AppError::InvalidConfig(
                "redis.toml: capacity.poll_interval_sec must be > 0".into(),
            ));
        }
        if self.capacity.max_memory_pct == 0 || self.capacity.max_memory_pct > 100 {
            return Err(AppError::InvalidConfig(
                "redis.toml: capacity.max_memory_pct must be in 1..=100".into(),
            ));
        }
        if self.capacity.max_pending == 0 {
            return Err(AppError::InvalidConfig(
                "redis.toml: capacity.max_pending must be > 0".into(),
            ));
        }
        if self.capacity.max_p99_cmd_ms == 0 {
            return Err(AppError::InvalidConfig(
                "redis.toml: capacity.max_p99_cmd_ms must be > 0".into(),
            ));
        }

        if self.capacity.redis_publish_latency_window < 100 {
            return Err(AppError::InvalidConfig(
                "redis.toml: capacity.redis_publish_latency_window must be >= 100".into(),
            ));
        }

        // streams
        let fmt = self.streams.key_format.trim();
        if fmt.is_empty() {
            return Err(AppError::InvalidConfig(
                "redis.toml: streams.key_format must not be empty".into(),
            ));
        }
        // your current format is stream:{exchange}:{symbol}:{kind}
        if !fmt.contains("{exchange}") || !fmt.contains("{symbol}") || !fmt.contains("{kind}") {
            return Err(AppError::InvalidConfig(
                "redis.toml: streams.key_format must include {exchange}, {symbol}, and {kind}"
                    .into(),
            ));
        }

        // retention
        if self.retention.maxlen == 0 {
            return Err(AppError::InvalidConfig(
                "redis.toml: retention.maxlen must be > 0".into(),
            ));
        }

        // groups (documentation-only, but still validate basic sanity)
        if self.groups.feature_builder.trim().is_empty() {
            return Err(AppError::InvalidConfig(
                "redis.toml: groups.feature_builder must not be empty".into(),
            ));
        }
        if let Some(v) = &self.groups.ml_infer {
            if v.trim().is_empty() {
                return Err(AppError::InvalidConfig(
                    "redis.toml: groups.ml_infer must not be empty if present".into(),
                ));
            }
        }

        // mode-specific constraints (still minimal)
        match self.mode {
            RedisMode::Single => {
                // ok: uses nodes map, always picks default_node
            }
            RedisMode::Pool => {
                if self.nodes.len() < 2 {
                    return Err(AppError::InvalidConfig(
                        "redis.toml: mode='pool' requires at least 2 nodes".into(),
                    ));
                }
            }
        }

        Ok(())
    }

    pub fn default_uri(&self, from_env: bool) -> AppResult<String> {
        let key = self.default_node.trim();

        if from_env {
            if let Some(env_key) = self.nodes_env.get(key) {
                match std::env::var(env_key) {
                    Ok(url) => return Ok(url),
                    Err(std::env::VarError::NotPresent) => {
                        // fall back to [nodes]
                    }
                    Err(e) => {
                        return Err(AppError::InvalidConfig(format!(
                            "redis.toml: env var '{env_key}' for default node '{key}' invalid: {e}"
                        )));
                    }
                }
            }
        }

        self.nodes.get(key).cloned().ok_or_else(|| {
            AppError::InvalidConfig(format!(
                "redis.toml: default_node '{}' not found in [nodes]",
                self.default_node
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_and_print_redis_config() {
        // Try loading the default redis.toml
        let cfg = RedisConfig::load_default().expect("failed to load redis.toml");

        // Print full config (pretty debug)
        println!("=== RedisConfig ===");
        println!("{:#?}", cfg);

        // Basic sanity prints
        println!("Redis enabled: {}", cfg.enabled);
        println!("Mode: {:?}", cfg.mode);
        println!("Default node: {}", cfg.default_node);

        let uri = cfg.default_uri(false).expect("default_node URI missing");

        println!("Default Redis URI: {}", uri);

        // Capacity guardrails
        println!(
            "Capacity: mem={}%, pending={}, p99_cmd_ms={}",
            cfg.capacity.max_memory_pct, cfg.capacity.max_pending, cfg.capacity.max_p99_cmd_ms,
        );

        // Streams
        println!("Stream key format: {}", cfg.streams.key_format);
        println!(
            "Publish: trades={}, depth={}, liquidations={}, funding={}, open_interest={}",
            cfg.streams.publish_trades,
            cfg.streams.publish_depth,
            cfg.streams.publish_liquidations,
            cfg.streams.publish_funding,
            cfg.streams.publish_open_interest,
        );

        // Groups (documentation-only, but still visible)
        println!("Feature builder group: {}", cfg.groups.feature_builder);
        println!("ML infer group: {:?}", cfg.groups.ml_infer);
    }
}
