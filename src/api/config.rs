use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::io::ErrorKind;
use std::{fs, net::IpAddr, path::Path};

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    pub bind_addr: String,
    pub port: u16,
}

impl ApiConfig {
    pub fn load_from_file(path: impl AsRef<std::path::Path>) -> AppResult<Self> {
        let path = path.as_ref();

        let raw = std::fs::read_to_string(path).map_err(|e| AppError::ConfigIoCtx {
            operation: "read_to_string",
            path: path.to_path_buf(),
            source: e,
        })?;

        let cfg: Self = toml::from_str(&raw).map_err(AppError::from)?; // or map_err(AppError::ConfigToml)
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn load(from_env: bool, version: u32) -> AppResult<Self> {
        if !from_env {
            return Self::load_default();
        }

        const DEFAULT_K8S_PATH_API: &str = "/etc/mini-fintickstreams/api.toml";
        let key = format!("MINI_FINTICKSTREAMS_API_CONFIG_PATH_{version}");

        // Track where the path came from for better diagnostics
        let (path, source): (String, &'static str) = match std::env::var(&key) {
            Ok(p) => (p, "env var"),
            Err(std::env::VarError::NotPresent) => (
                DEFAULT_K8S_PATH_API.to_string(),
                "default fallback (env var not set)",
            ),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ API config path env var is not valid unicode\n\
                 ├─ env var: `{}`\n\
                 └─ fix: set it to a valid UTF-8 path, e.g.\n\
                    export {}={}\n",
                    key, key, DEFAULT_K8S_PATH_API
                )));
            }
        };

        let p = Path::new(&path);

        // Make "file not found" scream with actionable info
        match std::fs::metadata(p) {
            Ok(meta) => {
                if !meta.is_file() {
                    return Err(AppError::InvalidConfig(format!(
                        "\n❌ API config path exists but is NOT a file\n\
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
                    "\n❌ API CONFIG FILE NOT FOUND\n\
                 ├─ tried path: `{}`\n\
                 ├─ source: {} (`{}`)\n\
                 ├─ default path: `{}`\n\
                 └─ fix: create the file OR set env var:\n\
                    export {}=/absolute/path/to/api.toml\n",
                    p.display(),
                    source,
                    key,
                    DEFAULT_K8S_PATH_API,
                    key
                )));
            }
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ API config file exists but permission was denied\n\
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
                // Preserve raw IO error, but wrap with context in message
                return Err(AppError::InvalidConfig(format!(
                    "\n❌ Failed to stat API config file\n\
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

        // Delegate actual reading/parsing (and keep your existing error mapping)
        Self::load_from_file(&path)
    }

    pub fn load_default() -> AppResult<Self> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("config")
            .join("api.toml");

        Self::load_from_file(path)
    }

    pub fn validate(&self) -> AppResult<()> {
        let _ip: IpAddr = self.bind_addr.parse().map_err(|e| {
            AppError::InvalidConfig(format!(
                "api.toml: bind_addr '{}' is not a valid IP: {e}",
                self.bind_addr
            ))
        })?;

        if self.port == 0 {
            return Err(AppError::InvalidConfig(
                "api.toml: port must be in 1..=65535".into(),
            ));
        }

        Ok(())
    }
}
