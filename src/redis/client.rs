use crate::error::{AppError, AppResult};
use crate::redis::config::RedisConfig;
use crate::redis::health::poller::RedisProbe;
use crate::redis::manager::RedisStreamPublisher;

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{RedisResult, Value};
use std::time::Duration;
use tokio::time::timeout;

/// Async Redis client for the producer:
/// - owns a ConnectionManager (auto reconnect)
/// - enforces per-command timeouts
/// - provides primitives needed by:
///   - HealthPoller (ping, memory, pending)
///   - RedisManager (xadd publish)
///
/// No policy logic here.
#[derive(Clone, Debug)]
pub struct RedisClient {
    pub manager: ConnectionManager,
    command_timeout: Duration,
}

impl RedisClient {
    /// Connect using RedisConfig.default_node URI and connection timeouts.
    pub async fn connect_from_config(cfg: &RedisConfig) -> AppResult<Self> {
        let uri = cfg.default_uri()?; // you already have this helper
        let connect_timeout = Duration::from_millis(cfg.connection.connect_timeout_ms);
        let command_timeout = Duration::from_millis(cfg.connection.command_timeout_ms);

        let client = redis::Client::open(uri)
            .map_err(|e| AppError::InvalidConfig(format!("invalid redis uri '{uri}': {e}")))?;

        // Optional: ConnectionManager config (reconnect behavior).
        // Keep minimal for now.
        let mgr = timeout(connect_timeout, ConnectionManager::new(client))
            .await
            .map_err(|_| {
                AppError::RedisLogic(format!("redis connect timeout after {connect_timeout:?}"))
            })?
            .map_err(|e| AppError::RedisLogic(format!("redis connect error: {e}")))?;

        Ok(Self {
            manager: mgr,
            command_timeout,
        })
    }

    async fn with_timeout<T>(
        &self,
        fut: impl std::future::Future<Output = RedisResult<T>>,
    ) -> AppResult<T> {
        timeout(self.command_timeout, fut)
            .await
            .map_err(|_| {
                AppError::RedisLogic(format!(
                    "redis command timeout after {:?}",
                    self.command_timeout
                ))
            })?
            .map_err(|e| AppError::RedisLogic(format!("{e}")))
    }

    async fn cmd_string(&self, cmd: &redis::Cmd) -> AppResult<String> {
        self.with_timeout(async {
            let mut conn = self.manager.clone();
            cmd.query_async(&mut conn).await
        })
        .await
    }

    async fn cmd_value(&self, cmd: redis::Cmd) -> AppResult<Value> {
        self.with_timeout(async {
            let mut conn = self.manager.clone();
            cmd.query_async(&mut conn).await
        })
        .await
    }

    /// INFO MEMORY parsing helper.
    async fn info_memory_raw(&self) -> AppResult<String> {
        self.cmd_string(redis::cmd("INFO").arg("memory")).await
    }
}

// ------------------------------------------------------------
// RedisProbe implementation (health polling I/O)
// ------------------------------------------------------------
#[async_trait]
impl RedisProbe for RedisClient {
    async fn ping(&self) -> AppResult<()> {
        let cmd = redis::cmd("PING");
        let pong = self.cmd_string(&cmd).await?;
        if pong.trim() == "PONG" {
            Ok(())
        } else {
            Err(AppError::RedisLogic(format!("PING returned '{pong}'")))
        }
    }

    async fn memory_info(&self) -> AppResult<(u64, Option<u64>, Option<f64>)> {
        let raw = self.info_memory_raw().await?;
        let parsed = RedisMemoryInfo::parse(&raw);

        Ok((
            parsed.used_memory_bytes,
            parsed.maxmemory_bytes,
            parsed.used_memory_pct,
        ))
    }

    /// App-defined pending/backlog.
    ///
    /// For now, return 0 so the health gate can be built incrementally.
    /// Next step (later module): implement sampling / aggregate streams and compute this properly.
    async fn pending_total(&self) -> AppResult<u64> {
        Ok(0)
    }
}

// ------------------------------------------------------------
// RedisStreamPublisher implementation (XADD)
// ------------------------------------------------------------
#[async_trait]
impl RedisStreamPublisher for RedisClient {
    async fn xadd(
        &self,
        stream_key: &str,
        maxlen: u64,
        approx: bool,
        fields: &[(&str, &str)],
    ) -> AppResult<String> {
        // XADD key MAXLEN [~] maxlen * field value [field value ...]
        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream_key);

        cmd.arg("MAXLEN");
        if approx {
            cmd.arg("~");
        }
        cmd.arg(maxlen);

        cmd.arg("*");

        for (k, v) in fields {
            cmd.arg(k).arg(v);
        }

        self.cmd_string(&cmd).await
    }
}

// ------------------------------------------------------------
// INFO memory parser (small, self-contained)
// ------------------------------------------------------------
#[derive(Debug, Clone)]
struct RedisMemoryInfo {
    used_memory_bytes: u64,
    maxmemory_bytes: Option<u64>,
    used_memory_pct: Option<f64>,
}

impl RedisMemoryInfo {
    fn parse(info_memory: &str) -> Self {
        // INFO memory is "key:value" lines.
        // We care about:
        // - used_memory:<bytes>
        // - maxmemory:<bytes>  (0 means "not configured")
        let mut used: u64 = 0;
        let mut max: Option<u64> = None;

        for line in info_memory.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once(':') {
                let k = k.trim();
                let v = v.trim();

                match k {
                    "used_memory" => {
                        if let Ok(n) = v.parse::<u64>() {
                            used = n;
                        }
                    }
                    "maxmemory" => {
                        if let Ok(n) = v.parse::<u64>() {
                            if n > 0 {
                                max = Some(n);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        let pct = max.map(|m| (used as f64) * 100.0 / (m as f64));

        Self {
            used_memory_bytes: used,
            maxmemory_bytes: max,
            used_memory_pct: pct,
        }
    }
}
