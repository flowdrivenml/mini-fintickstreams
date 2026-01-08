// src/redis/manager.rs

use crate::error::AppResult;
use crate::redis::config::RedisConfig;
use crate::redis::gate::RedisGate;
use crate::redis::health::evaluator::HealthEvaluator;
use crate::redis::health::poller::{HealthPoller, RedisProbe};
use crate::redis::health::types::HealthStatus;
use crate::redis::latency::RedisPublishLatency;
use crate::redis::metrics::RedisMetrics;
use crate::redis::streams::{StreamKeyBuilder, StreamKind};

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::time::sleep;

/// Minimal interface needed to publish to Redis Streams.
/// Your future Redis client will implement this.
#[async_trait::async_trait]
pub trait RedisStreamPublisher: Send + Sync {
    /// Publish to a stream using XADD with retention policy.
    ///
    /// - stream_key: full key ("stream:binance:BTCUSDT:trades")
    /// - maxlen/approx: retention policy
    /// - fields: flat field/value pairs (already serialized)
    async fn xadd(
        &self,
        stream_key: &str,
        maxlen: u64,
        approx: bool,
        fields: &[(&str, &str)],
    ) -> AppResult<String>;
}

/// Result of calling publish: we never want Redis to be “hard required”.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    /// Published to Redis successfully.
    Published,

    /// Skipped publishing because Redis is disabled/unhealthy, or policy blocked onboarding.
    Skipped,

    /// Tried to publish but failed (still best-effort, caller continues DB path).
    Failed,
}

/// Coordinates:
/// - health polling/evaluation
/// - policy gating
/// - publish latency tracking
/// - symbol onboarding (add streams only when healthy)
#[derive(Debug)]
pub struct RedisManager<T> {
    cfg: RedisConfig,

    // Key naming
    pub keys: StreamKeyBuilder,

    // Health + gating
    poller: HealthPoller,
    evaluator: HealthEvaluator,
    gate: Arc<RedisGate>,

    // Metrics + latency
    metrics: RedisMetrics,
    latency: Arc<RedisPublishLatency>,

    // Redis I/O object (future RedisClient)
    io: Arc<T>,

    // Producer-side assignment:
    // If a symbol is assigned, we attempt Redis publishing for it (subject to gate.can_publish()).
    assigned_symbols: Mutex<HashSet<(String, String)>>, // (exchange, symbol)
}

impl<T> RedisManager<T>
where
    T: RedisProbe + RedisStreamPublisher + 'static,
{
    pub fn new(cfg: RedisConfig, io: Arc<T>, metrics: RedisMetrics) -> AppResult<Self> {
        // Build key builder from validated config
        let keys = StreamKeyBuilder::from_config(&cfg)?;

        // Construct latency tracker from config
        let latency = Arc::new(RedisPublishLatency::from_config(&cfg));

        // Health components
        let poller = HealthPoller::from_config(&cfg.capacity);
        let evaluator = HealthEvaluator::new(cfg.capacity.clone());

        // Gate
        let gate = Arc::new(RedisGate::new(cfg.failover.clone(), metrics.clone()));

        Ok(Self {
            cfg,
            keys,
            poller,
            evaluator,
            gate,
            metrics,
            latency,
            io,
            assigned_symbols: Mutex::new(HashSet::new()),
        })
    }

    /// Start the health loop in a background task.
    ///
    /// Caller decides where to hold/join the task; this returns the JoinHandle.
    pub fn spawn_health_loop(
        self: &Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let this = Arc::clone(self);

        tokio::spawn(async move {
            let interval = this.poller.poll_interval();

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::info!("health loop shutting down");
                        break;
                    }

                    _ = async {
                        // 1) Pull rolling p99
                        let p99 = this.latency.p99_ms();

                        // 2) Poll backend
                        let snap = this.poller.poll_once(this.io.as_ref(), p99).await;

                        // 3) Evaluate thresholds
                        let status = this.evaluator.evaluate(snap);

                        // 4) Apply policy
                        this.gate.apply_health(&status);

                        sleep(interval).await;
                    } => {}
                }
            }
        })
    }

    /// Fast query helpers
    #[inline]
    pub fn can_publish(&self) -> bool {
        self.cfg.enabled && self.gate.can_publish()
    }

    #[inline]
    pub fn can_assign_new_symbol(&self) -> bool {
        self.cfg.enabled && self.gate.can_assign_new_symbol()
    }

    pub fn gate(&self) -> Arc<RedisGate> {
        Arc::clone(&self.gate)
    }

    pub fn latency(&self) -> Arc<RedisPublishLatency> {
        Arc::clone(&self.latency)
    }

    /// Producer-side onboarding:
    /// - If symbol already assigned, nothing happens.
    /// - If not assigned, we only assign if gate allows onboarding.
    fn ensure_assigned(&self, exchange: &str, symbol: &str) -> bool {
        let mut set = self
            .assigned_symbols
            .lock()
            .expect("assigned_symbols mutex poisoned");
        let key = (exchange.to_string(), symbol.to_string());

        if set.contains(&key) {
            return true;
        }

        if !self.can_assign_new_symbol() {
            return false;
        }

        set.insert(key);
        true
    }

    /// Best-effort publish to Redis Streams.
    ///
    /// This DOES NOT replace your DB write path.
    /// Caller should always do DB writes regardless of outcome.
    pub async fn publish(
        &self,
        exchange: &str,
        symbol: &str,
        kind: StreamKind,
        fields: &[(&str, &str)],
    ) -> AppResult<PublishOutcome> {
        // Global runtime switch
        if !self.cfg.enabled {
            return Ok(PublishOutcome::Skipped);
        }

        // Respect per-kind publish flags
        if !self.kind_enabled(kind) {
            return Ok(PublishOutcome::Skipped);
        }

        // Onboard symbol only if policy allows
        if !self.ensure_assigned(exchange, symbol) {
            // This is your "stop assigning new" behavior
            return Ok(PublishOutcome::Skipped);
        }

        // Gate publish (health may have disabled Redis)
        if !self.gate.can_publish() {
            return Ok(PublishOutcome::Skipped);
        }

        let stream_key = self.keys.key(exchange, symbol, kind);

        let maxlen = self.cfg.retention.maxlen;
        let approx = self.cfg.retention.approx;

        // Measure publish latency
        let t0 = Instant::now();
        let res = self.io.xadd(&stream_key, maxlen, approx, fields).await;
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;

        // Update metrics + rolling latency (best effort)
        self.latency.observe_ms(elapsed_ms);
        self.metrics.observe_publish_latency(elapsed_ms / 1000.0);

        match res {
            Ok(_id) => {
                self.metrics.inc_published(1);
                Ok(PublishOutcome::Published)
            }
            Err(_e) => {
                self.metrics.inc_publish_failure();

                // Optional: if you want to aggressively disable on repeated errors,
                // do that in a separate module/window. For now: just record failure.
                Ok(PublishOutcome::Failed)
            }
        }
    }

    fn kind_enabled(&self, kind: StreamKind) -> bool {
        match kind {
            StreamKind::Trades => self.cfg.streams.publish_trades,
            StreamKind::Depth => self.cfg.streams.publish_depth,
            StreamKind::Liquidations => self.cfg.streams.publish_liquidations,
            StreamKind::Funding => self.cfg.streams.publish_funding,
            StreamKind::OpenInterest => self.cfg.streams.publish_open_interest,
        }
    }

    /// Optional convenience: manually disable Redis.
    pub fn disable_manual(&self) {
        self.gate.disable_manual();
    }

    /// Optional convenience: manually re-enable Redis.
    pub fn enable_manual(&self) {
        self.gate.enable_manual();
    }

    /// Optional: clear assignments (e.g., if you want to re-onboard after a long disable).
    pub fn clear_assignments(&self) {
        let mut set = self
            .assigned_symbols
            .lock()
            .expect("assigned_symbols mutex poisoned");
        set.clear();
    }
}
