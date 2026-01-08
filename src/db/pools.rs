//! db_pools.rs
//!
//! Sharded TimescaleDB pool manager.
//! - Builds one sqlx Pool<Postgres> per shard.
//! - Routes (exchange, stream, symbol) -> shard via rules.
//! - Routing uses "most specific match wins" (fewer '*') to avoid catch-all rules stealing traffic.
//! - Public API stays simple: `pool_for(exchange, stream, symbol)`.

use crate::db::config::{ShardConfig, ShardRule, TimescaleDbConfig};
use crate::error::{AppError, AppResult};

use sqlx::{
    Pool, Postgres,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{collections::HashMap, env, str::FromStr, time::Duration};
use tokio::sync::RwLock;
use tokio::time::timeout;

#[derive(Debug)]
pub struct DbPools {
    pools_by_id: RwLock<HashMap<String, Pool<Postgres>>>,
    shards: RwLock<Vec<ShardConfig>>,
}

impl DbPools {
    /// Build pools for every configured shard (and optionally check connectivity).
    pub async fn new(cfg: TimescaleDbConfig, verify: bool) -> AppResult<Self> {
        let this = Self {
            pools_by_id: RwLock::new(HashMap::with_capacity(cfg.shards.len())),
            shards: RwLock::new(Vec::with_capacity(cfg.shards.len())),
        };

        for shard in cfg.shards {
            this.add_pool(shard, verify).await?;
        }

        Ok(this)
    }

    /// Add (or replace) a shard pool at runtime.
    /// - If `shard.id` already exists, the pool is replaced.
    /// - Routing rules are replaced for that shard id.
    pub async fn add_pool(&self, shard: ShardConfig, verify: bool) -> AppResult<()> {
        // Build pool first (no locks held during await-heavy work).
        let pool = build_pool(&shard).await?;

        if verify {
            sqlx::query("SELECT 1")
                .execute(&pool)
                .await
                .map_err(AppError::Sqlx)?;
        }

        // Update pool map
        {
            let mut pools = self.pools_by_id.write().await;
            pools.insert(shard.id.clone(), pool);
        }

        // Update shards list (replace existing shard config with same id)
        {
            let mut shards = self.shards.write().await;
            if let Some(pos) = shards.iter().position(|s| s.id == shard.id) {
                shards[pos] = shard;
            } else {
                shards.push(shard);
            }
        }

        Ok(())
    }

    /// Replace a shard pool at runtime (safe; builds new pool first, then swaps).
    pub async fn replace_pool(&self, shard: ShardConfig, verify: bool) -> AppResult<()> {
        // 1) Build new pool first (await-heavy work happens outside locks)
        let new_pool = build_pool(&shard).await?;

        if verify {
            sqlx::query("SELECT 1")
                .execute(&new_pool)
                .await
                .map_err(AppError::Sqlx)?;
        }

        // 2) Swap into the map quickly (short lock)
        let old_pool = {
            let mut pools = self.pools_by_id.write().await;
            pools.insert(shard.id.clone(), new_pool)
        };

        // 3) Update shard config (short lock)
        {
            let mut shards = self.shards.write().await;
            if let Some(pos) = shards.iter().position(|s| s.id == shard.id) {
                shards[pos] = shard;
            } else {
                shards.push(shard);
            }
        }

        // 4) Gracefully close old pool in the background (optional)
        if let Some(old) = old_pool {
            tokio::spawn(async move {
                old.close().await;
            });
        }

        Ok(())
    }

    /// Route by (exchange, stream, symbol) using shard rules.
    ///
    /// Matching:
    /// - `*` matches anything
    /// - by default matching is case-insensitive (see `field_matches`)
    ///
    /// Selection:
    /// - most specific match wins (specificity = number of non-"*" fields)
    /// - tie-breakers (deterministic):
    ///   1) higher specificity
    ///   2) earlier rule (within shard)
    ///   3) earlier shard (config order)
    pub async fn shard_id_for(
        &self,
        exchange: &str,
        stream: &str,
        symbol: &str,
    ) -> AppResult<String> {
        let shards = self.shards.read().await;
        let key = RouteKey {
            exchange,
            stream,
            symbol,
        };

        let mut best: Option<Best<'_>> = None;

        for (shard_idx, shard) in shards.iter().enumerate() {
            for (rule_idx, rule) in shard.rules.iter().enumerate() {
                if rule_matches(rule, &key) {
                    let cand = Best {
                        shard_id: shard.id.as_str(),
                        score: specificity(rule),
                        shard_idx,
                        rule_idx,
                    };

                    best = match best {
                        None => Some(cand),
                        Some(curr) => Some(if cand.better_than(curr) { cand } else { curr }),
                    };
                }
            }
        }

        best.map(|b| b.shard_id.to_string()).ok_or_else(|| {
            AppError::InvalidConfig(format!(
                "No shard routing rule matched exchange='{exchange}', stream='{stream}', symbol='{symbol}'"
            ))
        })
    }

    /// Get pool by shard id (cloned; Pool is cheap to clone).
    pub async fn pool_by_id(&self, shard_id: &str) -> AppResult<Pool<Postgres>> {
        let pools = self.pools_by_id.read().await;
        pools
            .get(shard_id)
            .cloned()
            .ok_or_else(|| AppError::InvalidConfig(format!("Unknown shard id '{shard_id}'")))
    }

    /// Route and return the pool (the only call most code should need).
    pub async fn pool_for(
        &self,
        exchange: &str,
        stream: &str,
        symbol: &str,
    ) -> AppResult<Pool<Postgres>> {
        let shard_id = self.shard_id_for(exchange, stream, symbol).await?;
        self.pool_by_id(&shard_id).await
    }

    /// For shutdown / stats.
    pub async fn all_pools(&self) -> Vec<(String, Pool<Postgres>)> {
        let pools = self.pools_by_id.read().await;
        pools.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub async fn shards_snapshot(&self) -> AppResult<Vec<ShardConfig>> {
        let shards = self.shards.read().await;
        Ok(shards.clone())
    }
}

/* ------------------------- routing (private) -------------------------- */

#[derive(Debug, Clone)]
struct RouteKey<'a> {
    exchange: &'a str,
    stream: &'a str,
    symbol: &'a str,
}

#[derive(Debug, Clone, Copy)]
struct Best<'a> {
    shard_id: &'a str,
    score: i32,
    shard_idx: usize,
    rule_idx: usize,
}

impl<'a> Best<'a> {
    fn better_than(self, other: Best<'a>) -> bool {
        if self.score != other.score {
            return self.score > other.score;
        }
        if self.rule_idx != other.rule_idx {
            return self.rule_idx < other.rule_idx; // earlier rule wins
        }
        self.shard_idx < other.shard_idx // earlier shard wins
    }
}

fn rule_matches(rule: &ShardRule, key: &RouteKey<'_>) -> bool {
    field_matches(&rule.exchange, key.exchange)
        && field_matches(&rule.stream, key.stream)
        && field_matches(&rule.symbol, key.symbol)
}

fn field_matches(rule_val: &str, actual: &str) -> bool {
    let rv = rule_val.trim();
    let av = actual.trim();
    rv == "*" || rv.eq_ignore_ascii_case(av)
}

fn specificity(rule: &ShardRule) -> i32 {
    (rule.exchange.trim() != "*") as i32
        + (rule.stream.trim() != "*") as i32
        + (rule.symbol.trim() != "*") as i32
}

/* ------------------------- pool build (private) ----------------------- */

async fn build_pool(shard: &ShardConfig) -> AppResult<Pool<Postgres>> {
    let connect_timeout = Duration::from_millis(shard.connect_timeout_ms);
    let idle_timeout = Duration::from_secs(shard.idle_timeout_sec);

    // Read DSN from env using explicit key from config
    let dsn = env::var(&shard.dsn_env).map_err(|_| {
        AppError::InvalidConfig(format!(
            "Missing environment variable '{}' for shard '{}'",
            shard.dsn_env, shard.id
        ))
    })?;

    let connect_opts = PgConnectOptions::from_str(&dsn).map_err(|e| {
        AppError::InvalidConfig(format!(
            "Invalid DSN in env var '{}' for shard '{}': {e}",
            shard.dsn_env, shard.id
        ))
    })?;

    let connect_fut = PgPoolOptions::new()
        .min_connections(shard.pool_min)
        .max_connections(shard.pool_max)
        .acquire_timeout(connect_timeout)
        .idle_timeout(idle_timeout)
        .connect_with(connect_opts);

    let pool = timeout(connect_timeout, connect_fut)
        .await
        .map_err(|_| {
            AppError::InvalidConfig(format!(
                "Timed out connecting to shard '{}' after {}ms",
                shard.id, shard.connect_timeout_ms
            ))
        })?
        .map_err(AppError::Sqlx)?;

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::config::{ShardConfig, ShardRule, TimescaleDbConfig};

    fn db_tests_enabled() -> bool {
        std::env::var("RUN_DB_TESTS").ok().as_deref() == Some("1")
    }

    fn override_dsn_if_present(cfg: &mut TimescaleDbConfig) {
        if let Ok(dsn) = std::env::var("TEST_DATABASE_URL") {
            println!("[test] Overriding DSN with TEST_DATABASE_URL");
            for shard in &mut cfg.shards {
                shard.dsn_env = dsn.clone();
            }
        }
    }

    fn make_runtime_shard_from(base: &ShardConfig, id: &str) -> ShardConfig {
        println!(
            "[test] Creating runtime shard '{id}' from base shard '{}'",
            base.id
        );

        let mut shard = base.clone();
        shard.id = id.to_string();

        shard.rules = vec![ShardRule {
            exchange: "*".to_string(),
            stream: "*".to_string(),
            symbol: "*".to_string(),
        }];

        shard
    }

    #[tokio::test]
    async fn dbpools_new_works_with_toml_config() {
        if !db_tests_enabled() {
            println!("[test] Skipping DB test (set RUN_DB_TESTS=1 to enable)");
            return;
        }

        println!("[test] Loading TimescaleDB config");
        let mut cfg = TimescaleDbConfig::load().expect("failed to load timescale_db.toml");
        override_dsn_if_present(&mut cfg);

        println!("[test] Loaded {} shard(s) from config", cfg.shards.len());

        println!("[test] Creating DbPools via DbPools::new()");
        let pools = DbPools::new(cfg.clone(), false)
            .await
            .expect("DbPools::new failed");

        let all = pools.all_pools().await;
        println!("[test] DbPools contains {} pool(s)", all.len());

        assert_eq!(all.len(), cfg.shards.len());

        let shard0 = &cfg.shards[0];
        println!("[test] Fetching pool by id '{}'", shard0.id);

        let pool0 = pools
            .pool_by_id(&shard0.id)
            .await
            .expect("pool_by_id failed");

        println!("[test] Executing SELECT 1 on pool '{}'", shard0.id);
        sqlx::query("SELECT 1")
            .execute(&pool0)
            .await
            .expect("SELECT 1 failed");

        println!("[test] DbPools::new() test OK");
    }

    #[tokio::test]
    async fn add_pool_runtime_works_starting_from_empty() {
        if !db_tests_enabled() {
            println!("[test] Skipping DB test (set RUN_DB_TESTS=1 to enable)");
            return;
        }

        println!("[test] Loading base config for runtime add test");
        let mut cfg = TimescaleDbConfig::load().expect("failed to load timescale_db.toml");
        override_dsn_if_present(&mut cfg);

        let base = cfg
            .shards
            .get(0)
            .expect("expected at least one shard")
            .clone();

        println!("[test] Creating EMPTY DbPools");
        let pools = DbPools {
            pools_by_id: RwLock::new(HashMap::new()),
            shards: RwLock::new(Vec::new()),
        };

        let runtime_id = "runtime_shard";
        let runtime_shard = make_runtime_shard_from(&base, runtime_id);

        println!("[test] Adding pool '{}' at runtime", runtime_id);
        pools
            .add_pool(runtime_shard.clone(), true)
            .await
            .expect("add_pool failed");

        println!("[test] Fetching runtime pool by id '{}'", runtime_id);
        let p = pools
            .pool_by_id(runtime_id)
            .await
            .expect("pool_by_id should find runtime_shard");

        println!("[test] Executing SELECT 1 on runtime pool");
        sqlx::query("SELECT 1")
            .execute(&p)
            .await
            .expect("SELECT 1 failed on runtime pool");

        let snapshot = pools
            .shards_snapshot()
            .await
            .expect("shards_snapshot failed");
        println!("[test] shards_snapshot len = {}", snapshot.len());

        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].id, runtime_id);

        println!("[test] Runtime add_pool() test OK");
    }

    #[tokio::test]
    async fn pool_for_routes_and_returns_pool() {
        if !db_tests_enabled() {
            println!("[test] Skipping DB test (set RUN_DB_TESTS=1 to enable)");
            return;
        }

        println!("[test] Loading TimescaleDB config for routing test");
        let mut cfg = TimescaleDbConfig::load().expect("failed to load timescale_db.toml");
        override_dsn_if_present(&mut cfg);

        println!("[test] Building DbPools with verification");
        let pools = DbPools::new(cfg.clone(), true)
            .await
            .expect("DbPools::new failed");

        println!("[test] Routing (binance, depth, btcusdt)");
        let pool = pools
            .pool_for("binance", "depth", "btcusdt")
            .await
            .expect("pool_for failed");

        println!("[test] Executing SELECT 1 via pool_for()");
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .expect("SELECT 1 failed via pool_for");

        let expected = cfg.shards[0].id.clone();
        let actual = pools
            .shard_id_for("binance", "trades", "btcusdt")
            .await
            .expect("shard_id_for failed");

        println!(
            "[test] shard_id_for returned '{}' (expected '{}')",
            actual, expected
        );

        assert_eq!(actual, expected);

        println!("[test] pool_for() routing test OK");
    }
    #[tokio::test]
    async fn add_pool_with_same_id_replaces_and_does_not_duplicate() {
        if !db_tests_enabled() {
            println!("[test] Skipping DB test (set RUN_DB_TESTS=1 to enable)");
            return;
        }

        println!("[test] Loading TimescaleDB config");
        let mut cfg = TimescaleDbConfig::load().expect("failed to load timescale_db.toml");
        override_dsn_if_present(&mut cfg);

        assert_eq!(
            cfg.shards.len(),
            1,
            "[test] expected config to have exactly 1 shard"
        );

        println!("[test] Building DbPools with verification");
        let pools = DbPools::new(cfg.clone(), true)
            .await
            .expect("DbPools::new failed");

        let shard0 = cfg.shards[0].clone();
        println!("[test] Initial shard id: {}", shard0.id);

        println!("[test] Sanity: SELECT 1 on initial pool");
        let p0 = pools
            .pool_by_id(&shard0.id)
            .await
            .expect("pool_by_id failed");
        sqlx::query("SELECT 1")
            .execute(&p0)
            .await
            .expect("SELECT 1 failed");

        println!("[test] Calling add_pool() again with SAME shard id (should replace)");
        pools
            .add_pool(shard0.clone(), true)
            .await
            .expect("add_pool(same id) failed");

        println!("[test] Verifying pool count is still 1");
        let all = pools.all_pools().await;
        println!(
            "[test] DbPools contains {} pool(s) after replace",
            all.len()
        );
        assert_eq!(
            all.len(),
            1,
            "expected exactly one pool after replacing same id"
        );

        println!("[test] Verifying shard snapshot count is still 1");
        let snap = pools
            .shards_snapshot()
            .await
            .expect("shards_snapshot failed");
        println!("[test] shards_snapshot contains {} shard(s)", snap.len());
        assert_eq!(
            snap.len(),
            1,
            "expected exactly one shard config after replacing same id"
        );
        assert_eq!(
            snap[0].id, shard0.id,
            "expected shard id to remain the same"
        );

        println!("[test] SELECT 1 again via pool_by_id after replace");
        let p1 = pools
            .pool_by_id(&shard0.id)
            .await
            .expect("pool_by_id failed after replace");
        sqlx::query("SELECT 1")
            .execute(&p1)
            .await
            .expect("SELECT 1 failed after replace");

        println!("[test] add_pool same-id replace test OK");
    }
}
