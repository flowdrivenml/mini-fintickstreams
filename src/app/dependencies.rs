use crate::app::config::AppConfig; // or wherever your config module lives
use crate::app::config::load_app_config;
use crate::app::ports::{DbWriter, RedisPublisher};
use crate::app::ports::{NoopDbWriter, NoopRedisPublisher, RealDbWriter, RealRedisPublisher};
use crate::db::DbHandler;
use crate::db::config::TimescaleDbConfig;
use crate::db::health::DBHealthController;
use crate::db::metrics::DbMetrics;
use crate::db::pools::DbPools;
use crate::error::AppError;
use crate::error::AppResult;
use crate::ingest::config::ExchangeConfigs;
use crate::ingest::http::api_client::ApiClient;
use crate::ingest::http::rate_limiter::RateLimiterRegistry;
use crate::ingest::instruments::loader::InstrumentSpecLoader;
use crate::ingest::metrics::IngestMetrics;
use crate::ingest::ws::limiter_registry::WsLimiterRegistry;
use crate::ingest::ws::ws_client::WsClient;
use crate::redis::client::RedisClient;
use crate::redis::config::RedisConfig;
use crate::redis::manager::RedisManager;
use crate::redis::metrics::RedisMetrics;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub type AppRedisManager = RedisManager<RedisClient>;

/// All Redis-related runtime deps bundled together.
#[derive(Debug, Clone)]
pub struct RedisDeps {
    pub cfg: Arc<RedisConfig>,
    pub metrics: Arc<RedisMetrics>,
    pub client: Arc<RedisClient>,
    pub manager: Arc<AppRedisManager>,
}

/// All DB-related runtime deps bundled together.
#[derive(Debug, Clone)]
pub struct DbDeps {
    pub cfg: Arc<TimescaleDbConfig>,
    pub pools: Arc<DbPools>,
    pub metrics: Arc<DbMetrics>,
    pub handler: Arc<DbHandler>,
    pub health: Arc<DBHealthController>,
}

#[derive(Debug)]
pub struct HealthLoopHandles {
    pub db: Option<JoinHandle<()>>,
    pub db_cancel: Option<CancellationToken>,
    pub redis: Option<JoinHandle<()>>,
    pub redis_cancel: Option<CancellationToken>,
}

impl Default for HealthLoopHandles {
    fn default() -> Self {
        Self {
            db: None,
            db_cancel: None,
            redis: None,
            redis_cancel: None,
        }
    }
}

#[derive(Debug)]
pub struct AppDeps {
    pub app_cfgs: Arc<AppConfig>,
    pub exchange_cfgs: Arc<ExchangeConfigs>,
    pub ingest_metrics: Option<Arc<IngestMetrics>>,

    // HTTP
    pub http_limiters: Option<Arc<RateLimiterRegistry>>,
    pub binance_linear_client: Option<Arc<ApiClient>>,
    pub hyperliquid_perp_client: Option<Arc<ApiClient>>,

    // WS
    pub ws_limiters: Option<Arc<WsLimiterRegistry>>,
    pub binance_linear_ws: Option<Arc<WsClient>>,
    pub hyperliquid_perp_ws: Option<Arc<WsClient>>,

    // DB (optional)
    pub db: Option<DbDeps>,

    // Redis (optional)
    pub redis: Option<RedisDeps>,

    // ✅ Always-present ports (real or noop)
    pub db_writer: Arc<dyn DbWriter>,
    pub redis_publisher: Arc<dyn RedisPublisher>,

    // ✅ Runtime gates (4 toggle methods operate on these)
    db_enabled: Arc<AtomicBool>,
    redis_enabled: Arc<AtomicBool>,

    // Instrument loader
    pub instruments_loader: Arc<InstrumentSpecLoader>,

    // Health loop handles
    health_loop_handles: HealthLoopHandles,
}

/// Newtype for runtime gates.

impl AppDeps {
    pub async fn new() -> AppResult<Self> {
        // --------------------------------------------------
        // Load core app config
        // --------------------------------------------------
        let app_cfgs = Arc::new(load_app_config()?);

        // --------------------------------------------------
        // Load exchange configs (depends on app config)
        // --------------------------------------------------
        let exchange_cfgs = Arc::new(ExchangeConfigs::new(&app_cfgs)?);

        // --------------------------------------------------
        // Optional ingest metrics
        // --------------------------------------------------
        let ingest_metrics = Some(Arc::new(IngestMetrics::new()?));

        // --------------------------------------------------
        // Redis (optional)
        // --------------------------------------------------
        let redis: Option<RedisDeps> = if app_cfgs.redis.enabled {
            let cfg = Arc::new(RedisConfig::load_default()?);
            Some(Self::bootstrap_redis(cfg).await?)
        } else {
            None
        };

        // --------------------------------------------------
        // DB (optional)
        // --------------------------------------------------
        let db: Option<DbDeps> = if app_cfgs.db.enabled {
            let cfg = Arc::new(TimescaleDbConfig::load()?);

            if app_cfgs.db.enabled && !app_cfgs.db.verify {
                return Err(AppError::InvalidConfig(
                    "db.verify must be true when db.enabled is true".into(),
                ));
            }

            Some(Self::bootstrap_db(cfg, app_cfgs.db.verify).await?)
        } else {
            None
        };

        // --------------------------------------------------
        // Build runtime gates (start from config)
        // --------------------------------------------------
        let db_enabled = Arc::new(AtomicBool::new(app_cfgs.db.enabled));
        let redis_enabled = Arc::new(AtomicBool::new(app_cfgs.redis.enabled));

        // --------------------------------------------------
        // Bootstrap ports (publisher / writer)
        // --------------------------------------------------
        let db_writer: Arc<dyn DbWriter> = match &db {
            Some(d) => Arc::new(RealDbWriter::new(
                Arc::clone(&db_enabled),
                Arc::clone(&d.handler),
            )),
            None => Arc::new(NoopDbWriter::new(Arc::clone(&db_enabled))),
        };

        let redis_publisher: Arc<dyn RedisPublisher> = match &redis {
            Some(r) => Arc::new(RealRedisPublisher::new(
                Arc::clone(&redis_enabled),
                Arc::clone(&r.manager),
            )),
            None => Arc::new(NoopRedisPublisher::new(Arc::clone(&redis_enabled))),
        };
        // --------------------------------------------------
        // HTTP clients
        // --------------------------------------------------
        let (http_limiters, binance_linear_client, hyperliquid_perp_client) =
            Self::bootstrap_http(&app_cfgs, &exchange_cfgs, ingest_metrics.clone())?;

        // --------------------------------------------------
        // WS clients
        // --------------------------------------------------
        let (ws_limiters, binance_linear_ws, hyperliquid_perp_ws) =
            Self::bootstrap_ws(&app_cfgs, &exchange_cfgs, ingest_metrics.clone())?;

        // --------------------------------------------------
        // Instrument loader (use the already-built exchange_cfgs!)
        // --------------------------------------------------
        let excfg = ExchangeConfigs::new(&app_cfgs)?;
        let instruments_loader = Arc::new(InstrumentSpecLoader::new(
            excfg,
            http_limiters.clone(),
            ingest_metrics.clone(),
        )?);

        let health_loop_handles = HealthLoopHandles::default();

        Ok(Self {
            app_cfgs,
            exchange_cfgs,
            ingest_metrics,

            http_limiters,
            binance_linear_client,
            hyperliquid_perp_client,

            ws_limiters,
            binance_linear_ws,
            hyperliquid_perp_ws,

            db,
            redis,

            db_writer,
            redis_publisher,

            db_enabled,
            redis_enabled,

            instruments_loader,

            health_loop_handles,
        })
    }

    pub async fn bootstrap_redis(cfg: Arc<RedisConfig>) -> AppResult<RedisDeps> {
        // 1) Metrics
        let metrics = Arc::new(RedisMetrics::new()?);

        // 2) Connect client
        let client = Arc::new(RedisClient::connect_from_config(&cfg).await?);

        // 3) Manager (prefer Arc metrics if your manager can accept it;
        //    keeping your existing signature here).
        let manager = Arc::new(RedisManager::new(
            (*cfg).clone(),
            Arc::clone(&client),
            (*metrics).clone(),
        )?);

        Ok(RedisDeps {
            cfg,
            metrics,
            client,
            manager,
        })
    }

    pub async fn bootstrap_db(cfg: Arc<TimescaleDbConfig>, verify: bool) -> AppResult<DbDeps> {
        // 1) Build pools
        let pools = Arc::new(DbPools::new((*cfg).clone(), verify).await?);

        // 2) Build metrics
        let metrics = Arc::new(DbMetrics::new()?);

        // 3) Build handler
        let handler = Arc::new(DbHandler::new(
            Arc::clone(&pools),
            cfg.writer.clone(),
            Arc::clone(&metrics),
        ));

        let health_configs = cfg.clone().as_ref().health.clone();
        let health = Arc::new(DBHealthController::new(
            health_configs,
            Arc::clone(&metrics),
        ));

        Ok(DbDeps {
            cfg,
            pools,
            metrics,
            handler,
            health,
        })
    }

    pub fn bootstrap_http(
        app_cfg: &AppConfig,
        exchange_cfgs: &ExchangeConfigs,
        ingest_metrics: Option<Arc<IngestMetrics>>,
    ) -> AppResult<(
        Option<Arc<RateLimiterRegistry>>,
        Option<Arc<ApiClient>>,
        Option<Arc<ApiClient>>,
    )> {
        // Create limiters only if any HTTP exchange is enabled
        let any_http =
            app_cfg.exchange_toggles.binance_linear || app_cfg.exchange_toggles.hyperliquid_perp;

        let http_limiters = if any_http {
            Some(Arc::new(RateLimiterRegistry::new(
                app_cfg,
                ingest_metrics.clone(),
            )?))
        } else {
            None
        };

        let binance_linear_client = if app_cfg.exchange_toggles.binance_linear {
            let binance_cfg = exchange_cfgs
                .binance_linear
                .as_ref()
                .ok_or_else(|| AppError::MissingConfig("binance_linear exchange config"))?;

            Some(Arc::new(ApiClient::new(
                "binance_linear",
                binance_cfg.api_base_url.clone(),
                http_limiters.clone(),
                ingest_metrics.clone(),
            )))
        } else {
            None
        };

        let hyperliquid_perp_client = if app_cfg.exchange_toggles.hyperliquid_perp {
            let hyper_cfg = exchange_cfgs
                .hyperliquid_perp
                .as_ref()
                .ok_or_else(|| AppError::MissingConfig("hyperliquid_perp exchange config"))?;

            Some(Arc::new(ApiClient::new(
                "hyperliquid_perp",
                hyper_cfg.api_base_url.clone(),
                http_limiters.clone(),
                ingest_metrics.clone(),
            )))
        } else {
            None
        };

        Ok((
            http_limiters,
            binance_linear_client,
            hyperliquid_perp_client,
        ))
    }

    pub fn bootstrap_ws(
        app_cfg: &AppConfig,
        exchange_cfgs: &ExchangeConfigs,
        ingest_metrics: Option<Arc<IngestMetrics>>,
    ) -> AppResult<(
        Option<Arc<WsLimiterRegistry>>,
        Option<Arc<WsClient>>,
        Option<Arc<WsClient>>,
    )> {
        // Create limiters only if any WS exchange is enabled
        let any_ws =
            app_cfg.exchange_toggles.binance_linear || app_cfg.exchange_toggles.hyperliquid_perp;

        let ws_limiters = if any_ws {
            Some(Arc::new(WsLimiterRegistry::new(
                app_cfg,
                ingest_metrics.clone(),
            )?))
        } else {
            None
        };

        let binance_linear_ws = if app_cfg.exchange_toggles.binance_linear {
            let binance_cfg = exchange_cfgs
                .binance_linear
                .as_ref()
                .ok_or_else(|| AppError::MissingConfig("binance_linear exchange config"))?;

            Some(Arc::new(WsClient::new(
                "binance_linear",
                binance_cfg.clone(),
                ingest_metrics.clone(),
                Some(app_cfg),
            )))
        } else {
            None
        };

        let hyperliquid_perp_ws = if app_cfg.exchange_toggles.hyperliquid_perp {
            let hyper_cfg = exchange_cfgs
                .hyperliquid_perp
                .as_ref()
                .ok_or_else(|| AppError::MissingConfig("hyperliquid_perp exchange config"))?;

            Some(Arc::new(WsClient::new(
                "hyperliquid_perp",
                hyper_cfg.clone(),
                ingest_metrics.clone(),
                Some(app_cfg),
            )))
        } else {
            None
        };

        Ok((ws_limiters, binance_linear_ws, hyperliquid_perp_ws))
    }
}

/// Redis

impl AppDeps {
    pub fn spawn_redis_health_loop(&mut self) -> AppResult<()> {
        let redis = self
            .redis
            .as_ref()
            .ok_or(AppError::Disabled("Redis disabled".into()))?;

        if self.health_loop_handles.redis.is_some() {
            return Ok(()); // already running
        }
        let token = CancellationToken::new();
        let handle = Arc::clone(&redis.manager).spawn_health_loop(token.clone());
        self.health_loop_handles.redis = Some(handle);
        self.health_loop_handles.redis_cancel = Some(token);

        Ok(())
    }

    pub fn redis_can_publish(&self) -> bool {
        if !self.redis_enabled.load(Ordering::Relaxed) {
            return false;
        }
        self.redis
            .as_ref()
            .map(|r| r.manager.can_publish())
            .unwrap_or(false)
    }

    pub fn redis_can_assign_new_symbol(&self) -> bool {
        if !self.redis_enabled.load(Ordering::Relaxed) {
            return false;
        }
        self.redis
            .as_ref()
            .map(|r| r.manager.can_assign_new_symbol())
            .unwrap_or(false)
    }

    pub fn redis_cancel_health_loop(&self) {
        // 1) Signal cancellation
        if let Some(token) = &self.health_loop_handles.redis_cancel {
            token.cancel();
        }

        // // 2) Optionally abort the task if you want immediate stop
        // if let Some(handle) = self.health_loop_handles.redis.take() {
        //     handle.abort();
        // }
    }
}

/// DB

impl AppDeps {
    pub fn can_admite_new_streams(&self) -> Option<String> {
        if !self.db_enabled.load(Ordering::Relaxed) {
            return Some("Can't admit new streams: DB disabled".into());
        }

        let db = match self.db.as_ref() {
            Some(db) => db,
            None => return Some("Can't admit new streams: DB not initialized".into()),
        };

        // health: Arc<DBHealthController>
        let can_admit = db.health.clone().as_ref().can_admit_new_stream();

        if can_admit.is_none() {
            None
        } else {
            Some("Can't admit new streams: DB is full".into())
        }
    }

    pub fn spawn_db_health_loop(&mut self) -> AppResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or(AppError::Disabled("DB not initialized".into()))?;

        if self.health_loop_handles.db.is_some() {
            return Ok(()); // already running
        }
        let toekn = CancellationToken::new();
        let handle = Arc::clone(&db.health).spawn_health_loop(toekn.clone());
        self.health_loop_handles.db = Some(handle);
        self.health_loop_handles.db_cancel = Some(toekn);

        Ok(())
    }

    pub async fn add_db_pool(&self, shard: crate::db::ShardConfig) -> AppResult<()> {
        if !self.db_enabled.load(Ordering::Relaxed) {
            return Err(AppError::Disabled(
                "Can't add new db pool: DB disabled".into(),
            ));
        }

        let db = self.db.as_ref().ok_or_else(|| {
            AppError::Disabled("Can't add new db pool: DB not initialized".into())
        })?;

        let verify = self.app_cfgs.db.verify;

        db.pools.add_pool(shard, verify).await
    }

    pub async fn replace_db_pool(&self, shard: crate::db::ShardConfig) -> AppResult<()> {
        if !self.db_enabled.load(Ordering::Relaxed) {
            return Err(AppError::Disabled(
                "Can't add new db pool: DB disabled".into(),
            ));
        }

        let db = self.db.as_ref().ok_or_else(|| {
            AppError::Disabled("Can't add new db pool: DB not initialized".into())
        })?;

        let verify = self.app_cfgs.db.verify;

        db.pools.replace_pool(shard, verify).await
    }

    pub fn db_cancel_health_loop(&self) {
        if let Some(token) = &self.health_loop_handles.db_cancel {
            token.cancel();
        }
    }
}

/// Redis DB publish wrappers

impl AppDeps {
    pub async fn redis_publish(
        &self,
        exchange: &str,
        symbol: &str,
        kind: crate::redis::StreamKind,
        fields: &[(&str, &str)],
    ) -> AppResult<crate::redis::PublishOutcome> {
        self.redis_publisher
            .publish(exchange, symbol, kind, fields)
            .await
    }

    pub async fn db_write(&self, batch: crate::app::ports::AnyDbBatch<'_>) -> AppResult<()> {
        self.db_writer.write_batch(batch).await
    }
}
// -------------------------
// 4 toggle methods (runtime)
// -------------------------
impl AppDeps {
    pub fn disable_db(&self) {
        self.db_enabled.store(false, Ordering::Relaxed);
    }

    pub fn enable_db(&self) {
        self.db_enabled.store(true, Ordering::Relaxed);
    }

    pub fn disable_redis(&self) {
        self.redis_enabled.store(false, Ordering::Relaxed);
    }

    pub fn enable_redis(&self) {
        self.redis_enabled.store(true, Ordering::Relaxed);
    }

    pub fn is_db_enabled(&self) -> bool {
        self.db_enabled.load(Ordering::Relaxed)
    }

    pub fn is_redis_enabled(&self) -> bool {
        self.redis_enabled.load(Ordering::Relaxed)
    }
}

/// Instruments Loader
///
impl AppDeps {
    pub async fn load_all_instruments(
        &self,
    ) -> AppResult<Vec<crate::ingest::instruments::InstrumentSpec>> {
        let inst = self.instruments_loader.clone().as_ref().load_all().await;
        inst
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn appdeps_new_smoke_with_prints() {
        println!("==============================");
        println!("🚀 AppDeps smoke test starting");
        println!("==============================");

        println!("➡️  Calling AppDeps::new() ...");

        match AppDeps::new().await {
            Ok(deps) => {
                println!("✅ AppDeps::new() succeeded");

                println!("--- Summary ---");
                println!("App config loaded");
                println!("Exchange configs loaded");

                println!("HTTP limiters: {}", deps.http_limiters.is_some());
                println!("WS limiters: {}", deps.ws_limiters.is_some());

                println!("DB enabled: {}", deps.db.is_some());

                println!("Redis client initialized");
                println!("Redis manager initialized");

                println!("---------------------------");
                println!("🎉 Smoke test PASSED");
                println!("---------------------------");
            }
            Err(e) => {
                println!("❌ AppDeps::new() FAILED");
                println!("Error:\n{e:#}");
                panic!("Smoke test failed");
            }
        }
    }
}
