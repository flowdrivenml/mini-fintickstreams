use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::env;
use std::{collections::HashSet, fs, path::Path};

#[derive(Debug, Clone, Deserialize)]
pub struct TimescaleDbConfig {
    pub shards: Vec<ShardConfig>,
    pub writer: WriterConfig,
    pub health: HealthConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardConfig {
    pub id: String,
    /// Name of the environment variable that holds the DSN
    pub dsn_env: String,
    // Connection pool
    pub pool_min: u32,
    pub pool_max: u32,
    pub connect_timeout_ms: u64,
    pub idle_timeout_sec: u64,

    // Routing rules
    #[serde(default)]
    pub rules: Vec<ShardRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardRule {
    pub exchange: String,
    pub stream: String,
    pub symbol: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WriterConfig {
    pub batch_size: usize,
    pub hard_batch_size: usize,
    pub flush_interval_ms: u64,
    pub chunk_rows: usize, // max rows per insert
    pub max_inflight_batches: usize,
    pub use_copy: bool,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            hard_batch_size: 2000,
            flush_interval_ms: 50,
            chunk_rows: 500,
            max_inflight_batches: 4,
            use_copy: true,
        }
    }
}

impl TimescaleDbConfig {
    pub fn load() -> AppResult<Self> {
        let path = std::env::var("TIMESCALE_DB_CONFIG")
            .unwrap_or_else(|_| "src/config/timescale_db.toml".to_string());

        let raw = fs::read_to_string(&path)?;
        let cfg: Self = toml::from_str(&raw)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> AppResult<()> {
        // ---- Top-level checks
        if self.shards.is_empty() {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: must define at least one [[shards]]".into(),
            ));
        }

        // ---- Shards checks
        let mut seen_ids = HashSet::new();

        for (i, shard) in self.shards.iter().enumerate() {
            let prefix = format!("timescale_db.toml: shards[{i}]");

            if shard.id.trim().is_empty() {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: id must not be empty"
                )));
            }
            if !seen_ids.insert(shard.id.clone()) {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: duplicate shard id '{}'",
                    shard.id
                )));
            }

            // dsn_env checks
            if shard.dsn_env.trim().is_empty() {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: dsn_env must not be empty"
                )));
            }

            // Fail fast if the env var is missing
            let dsn = env::var(&shard.dsn_env).map_err(|_| {
                AppError::InvalidConfig(format!(
                    "{prefix}: environment variable '{}' is not set",
                    shard.dsn_env
                ))
            })?;

            // Lightweight sanity check; sqlx will do real parsing later.
            if !dsn.starts_with("postgres://") && !dsn.starts_with("postgresql://") {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: DSN from env var '{}' must start with postgres:// or postgresql://",
                    shard.dsn_env
                )));
            }

            if shard.pool_min == 0 {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: pool_min must be >= 1"
                )));
            }
            if shard.pool_max == 0 {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: pool_max must be >= 1"
                )));
            }
            if shard.pool_min > shard.pool_max {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: pool_min ({}) must be <= pool_max ({})",
                    shard.pool_min, shard.pool_max
                )));
            }
            if shard.connect_timeout_ms == 0 {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: connect_timeout_ms must be > 0"
                )));
            }
            if shard.idle_timeout_sec == 0 {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: idle_timeout_sec must be > 0"
                )));
            }

            if shard.rules.is_empty() {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: must define at least one [[shards.rules]]"
                )));
            }

            // Validate rules (allow "*" wildcard; otherwise require non-empty)
            for (r, rule) in shard.rules.iter().enumerate() {
                let rprefix = format!("{prefix}.rules[{r}]");
                validate_rule_field(&rprefix, "exchange", &rule.exchange)?;
                validate_rule_field(&rprefix, "stream", &rule.stream)?;
                validate_rule_field(&rprefix, "symbol", &rule.symbol)?;
            }
        }

        // ---- Writer checks
        if self.writer.batch_size == 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: writer.batch_size must be > 0".into(),
            ));
        }
        if self.writer.flush_interval_ms == 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: writer.flush_interval_ms must be > 0".into(),
            ));
        }
        if self.writer.max_inflight_batches == 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: writer.max_inflight_batches must be > 0".into(),
            ));
        }

        // ---- Health checks (minimal)
        let h = &self.health;
        if h.evaluate_interval_ms == 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: health.evaluate_interval_ms must be > 0".into(),
            ));
        }
        if h.hold_down_ms == 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: health.hold_down_ms must be > 0".into(),
            ));
        }

        let t = &h.thresholds;

        // monotonicity for yellow/red thresholds
        if t.flush_delay_p95_ms_yellow == 0 || t.flush_delay_p95_ms_red == 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: health.thresholds.flush_delay_* must be > 0".into(),
            ));
        }
        if t.flush_delay_p95_ms_yellow > t.flush_delay_p95_ms_red {
            return Err(AppError::InvalidConfig(
                    "timescale_db.toml: health.thresholds.flush_delay_p95_ms_yellow must be <= flush_delay_p95_ms_red"
                        .into(),
                ));
        }

        if t.pool_wait_p95_ms_yellow == 0 || t.pool_wait_p95_ms_red == 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: health.thresholds.pool_wait_* must be > 0".into(),
            ));
        }
        if t.pool_wait_p95_ms_yellow > t.pool_wait_p95_ms_red {
            return Err(AppError::InvalidConfig(
                    "timescale_db.toml: health.thresholds.pool_wait_p95_ms_yellow must be <= pool_wait_p95_ms_red"
                        .into(),
                ));
        }

        if t.writer_queue_depth_red <= 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: health.thresholds.writer_queue_depth_red must be > 0".into(),
            ));
        }

        // Safety: don't configure queue depth red above what the writer can actually represent.
        // (Your metrics set_queue_depth uses max_inflight_batches as the scale.)
        let max = self.writer.max_inflight_batches as i64;
        if t.writer_queue_depth_red > max {
            return Err(AppError::InvalidConfig(format!(
                "timescale_db.toml: health.thresholds.writer_queue_depth_red ({}) must be <= writer.max_inflight_batches ({})",
                t.writer_queue_depth_red, max
            )));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,

    #[serde(default = "default_eval_interval_ms")]
    pub evaluate_interval_ms: u64,

    #[serde(default = "default_hold_down_ms")]
    pub hold_down_ms: u64,

    #[serde(default)]
    pub admission_policy: AdmissionPolicy,

    pub thresholds: HealthThresholds,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionPolicy {
    GreenOnly,
    GreenOrYellow,
}

impl Default for AdmissionPolicy {
    fn default() -> Self {
        AdmissionPolicy::GreenOnly
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthThresholds {
    pub flush_delay_p95_ms_yellow: u64,
    pub flush_delay_p95_ms_red: u64,

    pub pool_wait_p95_ms_yellow: u64,
    pub pool_wait_p95_ms_red: u64,

    pub writer_queue_depth_red: i64,
}

fn default_true() -> bool {
    true
}
fn default_eval_interval_ms() -> u64 {
    1000
}
fn default_hold_down_ms() -> u64 {
    3000
}

fn validate_rule_field(prefix: &str, field: &str, value: &str) -> AppResult<()> {
    let v = value.trim();
    if v.is_empty() {
        return Err(AppError::InvalidConfig(format!(
            "{prefix}: {field} must not be empty (use \"*\" for wildcard)"
        )));
    }
    // Currently we allow "*" or exact match strings.
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::config::TimescaleDbConfig;

    #[test]
    fn load_timescale_config_and_print() {
        let cfg = TimescaleDbConfig::load().expect("failed to load timescale_db.toml");
        println!("Loaded TimescaleDB config:\n{:#?}", cfg);
        // Minimal sanity assertions so the test actually verifies something
        assert!(!cfg.shards.is_empty());
        assert!(cfg.writer.batch_size > 0);
    }
}
