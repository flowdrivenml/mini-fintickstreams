use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::{fs, net::IpAddr, path::Path};

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    pub bind_addr: String,
    pub port: u16,
}

impl ApiConfig {
    pub fn load_from_file(path: impl AsRef<Path>) -> AppResult<Self> {
        let raw = fs::read_to_string(path)?;
        let cfg: Self = toml::from_str(&raw)?;
        cfg.validate()?;
        Ok(cfg)
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
