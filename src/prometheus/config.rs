use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::io::ErrorKind;
use std::{collections::HashMap, fs, net::IpAddr, path::Path, path::PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct PrometheusConfig {
    pub bind_addr: String,
    pub port: u16,
    pub metrics_path: String,

    #[serde(default)]
    pub labels: HashMap<String, String>,

    pub export: ExportConfig,
    pub redis_poll: RedisPollConfig,

    #[serde(default)]
    pub targets: TargetsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExportConfig {
    pub export_build_info: bool,
    pub export_uptime: bool,

    pub export_ingest_rates: bool,
    pub export_process_rates: bool,
    pub export_end_to_end_delay: bool,

    pub export_stream_group_stats: bool,
    pub export_redis_errors: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisPollConfig {
    pub enabled: bool,
    pub interval_sec: u64,
    pub use_xinfo_groups: bool,
    pub use_xpending_summary: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TargetsConfig {
    #[serde(default)]
    pub redis_exporter: Vec<Target>,
    #[serde(default)]
    pub postgres_exporter: Vec<Target>,
    #[serde(default)]
    pub app: Vec<Target>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Target {
    pub name: String,
    pub url: String,
}

impl PrometheusConfig {
    pub fn load_from_file(path: impl AsRef<Path>) -> AppResult<Self> {
        let path = path.as_ref();

        // 1) Read file
        let raw = fs::read_to_string(path).map_err(|e| AppError::ConfigIoCtx {
            operation: "read_to_string",
            path: path.to_path_buf(),
            source: e,
        })?;

        // 2) Parse TOML with path context
        let cfg: Self = toml::from_str(&raw).map_err(|e| {
            AppError::InvalidConfig(format!(
                "\n❌ Failed to parse config TOML\n\
                 ├─ path: `{}`\n\
                 └─ error: {}\n",
                path.display(),
                e
            ))
        })?;

        // 3) Validate with path context
        cfg.validate().map_err(|e| {
            AppError::InvalidConfig(format!(
                "\n❌ Config failed validation\n\
                 ├─ path: `{}`\n\
                 └─ error: {}\n",
                path.display(),
                e
            ))
        })?;

        Ok(cfg)
    }

    pub fn load_default() -> AppResult<Self> {
        // Build default path explicitly
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let path: PathBuf = manifest_dir
            .join("src")
            .join("config")
            .join("prometheus.toml");

        // Load, but if it fails, wrap with a "default config" hint.
        Self::load_from_file(&path).map_err(|e| {
            AppError::InvalidConfig(format!(
                "\n❌ Failed to load default config\n\
                 ├─ default path: `{}`\n\
                 ├─ hint: ensure the file exists in the repo, or override via env/config selection\n\
                 └─ cause: {}\n",
                path.display(),
                e
            ))
        })
    }

    /// - if `from_env == false`: loads from repo (`src/config/prometheus.toml`)
    /// - if `from_env == true`: uses `MINI_FINTICKSTREAMS_PROMETHEUS_CONFIG_PATH_{version}`
    ///   and falls back to `/etc/mini-fintickstreams/prometheus.toml`
    pub fn load(from_env: bool, version: u32) -> AppResult<Self> {
        const DEFAULT_K8S_PATH: &str = "/etc/mini-fintickstreams/prometheus.toml";

        if !from_env {
            return Self::load_default();
        }

        let key = format!("MINI_FINTICKSTREAMS_PROMETHEUS_CONFIG_PATH_{version}");

        let (path, source): (String, &'static str) = match std::env::var(&key) {
            Ok(p) => (p, "env var"),
            Err(std::env::VarError::NotPresent) => (
                DEFAULT_K8S_PATH.to_string(),
                "default fallback (env var not set)",
            ),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ PROMETHEUS config path env var is not valid unicode\n\
                 ├─ env var: `{}`\n\
                 └─ fix: set it to a valid UTF-8 path, e.g.\n\
                    export {}={}\n",
                    key, key, DEFAULT_K8S_PATH
                )));
            }
        };

        let p = Path::new(&path);

        // Fail fast with explicit reasons before trying to read/parse
        match std::fs::metadata(p) {
            Ok(meta) => {
                if !meta.is_file() {
                    return Err(AppError::InvalidConfig(format!(
                        "\n❌ PROMETHEUS config path exists but is NOT a file\n\
                     ├─ path: `{}`\n\
                     ├─ source: {}\n\
                     └─ fix: point `{}` to a TOML file (not a directory)\n",
                        p.display(),
                        source,
                        key
                    )));
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ PROMETHEUS CONFIG FILE NOT FOUND\n\
                 ├─ tried path: `{}`\n\
                 ├─ source: {} (`{}`)\n\
                 ├─ k8s default fallback: `{}`\n\
                 └─ fix: create the file OR set env var:\n\
                    export {}=/absolute/path/to/prometheus.toml\n",
                    p.display(),
                    source,
                    key,
                    DEFAULT_K8S_PATH,
                    key
                )));
            }
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ PROMETHEUS config file exists but permission was denied\n\
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
                    "\n❌ Failed to stat PROMETHEUS config file\n\
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

        // Keep the existing parse path, but attach context if read fails inside load_from_file
        Self::load_from_file(&path).map_err(|e| {
            AppError::InvalidConfig(format!(
                "\n❌ Failed to load PROMETHEUS config\n\
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
        // bind_addr should be parseable IP (you can later extend to hostname)
        let _ip: IpAddr = self.bind_addr.parse().map_err(|e| {
            AppError::InvalidConfig(format!(
                "prometheus.toml: bind_addr '{}' is not a valid IP: {e}",
                self.bind_addr
            ))
        })?;

        if self.port == 0 {
            return Err(AppError::InvalidConfig(
                "prometheus.toml: port must be in 1..=65535".into(),
            ));
        }

        let p = self.metrics_path.trim();
        if p.is_empty() || !p.starts_with('/') {
            return Err(AppError::InvalidConfig(
                "prometheus.toml: metrics_path must start with '/'".into(),
            ));
        }

        // labels: keys should be non-empty
        for (k, v) in &self.labels {
            if k.trim().is_empty() {
                return Err(AppError::InvalidConfig(
                    "prometheus.toml: labels keys must not be empty".into(),
                ));
            }
            if v.trim().is_empty() {
                return Err(AppError::InvalidConfig(format!(
                    "prometheus.toml: label '{k}' value must not be empty"
                )));
            }
        }

        // redis_poll
        if self.redis_poll.enabled && self.redis_poll.interval_sec == 0 {
            return Err(AppError::InvalidConfig(
                "prometheus.toml: redis_poll.interval_sec must be > 0 when enabled".into(),
            ));
        }

        // targets: minimal URL sanity
        for t in self
            .targets
            .redis_exporter
            .iter()
            .chain(self.targets.postgres_exporter.iter())
            .chain(self.targets.app.iter())
        {
            if t.name.trim().is_empty() {
                return Err(AppError::InvalidConfig(
                    "prometheus.toml: targets.*.name must not be empty".into(),
                ));
            }
            let u = t.url.trim();
            if u.is_empty() || !(u.starts_with("http://") || u.starts_with("https://")) {
                return Err(AppError::InvalidConfig(format!(
                    "prometheus.toml: target '{}' url must start with http:// or https://",
                    t.name
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn load_from_file_parses_and_validates_prometheus_toml() {
        let cfg = PrometheusConfig::load_default()
            .unwrap_or_else(|e| panic!("failed to load prometheus.toml: {e}"));

        // --- minimal sanity assertions ---
        assert!(!cfg.bind_addr.is_empty());
        assert!(cfg.port > 0);
        assert!(cfg.metrics_path.starts_with('/'));

        // labels must be non-empty (validate already enforces this)
        for (k, v) in &cfg.labels {
            assert!(!k.trim().is_empty());
            assert!(!v.trim().is_empty());
        }

        // redis_poll sanity
        if cfg.redis_poll.enabled {
            assert!(cfg.redis_poll.interval_sec > 0);
        }

        // targets sanity (even if empty)
        for t in cfg
            .targets
            .redis_exporter
            .iter()
            .chain(cfg.targets.postgres_exporter.iter())
            .chain(cfg.targets.app.iter())
        {
            assert!(!t.name.trim().is_empty());
            assert!(
                t.url.starts_with("http://") || t.url.starts_with("https://"),
                "invalid target url: {}",
                t.url
            );
        }
    }
}
