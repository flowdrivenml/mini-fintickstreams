use crate::db::Batch as DbBatch;
use crate::db::DbHandler;
use crate::db::{DepthDeltaDBRow, FundingDBRow, LiquidationDBRow, OpenInterestDBRow, TradeDBRow};
use crate::error::AppResult;
use crate::redis::client::RedisClient;
use crate::redis::manager::{PublishOutcome, RedisManager};
use crate::redis::streams::StreamKind;
use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::{Arc, atomic::AtomicBool, atomic::Ordering};

// Keep this alias consistent with your codebase.
pub type AppRedisManager = RedisManager<RedisClient>;

/// ------------------------------
/// Redis publishing trait
/// ------------------------------
#[async_trait]
pub trait RedisPublisher: Send + Sync + Debug {
    async fn publish(
        &self,
        exchange: &str,
        symbol: &str,
        kind: StreamKind,
        fields: &[(&str, &str)],
    ) -> AppResult<PublishOutcome>;
}

/// Real Redis publisher: forwards to your RedisManager
#[derive(Clone, Debug)]
pub struct RealRedisPublisher {
    enabled: Arc<AtomicBool>,
    manager: Arc<AppRedisManager>,
}

impl RealRedisPublisher {
    pub fn new(enabled: Arc<AtomicBool>, manager: Arc<AppRedisManager>) -> Self {
        Self { enabled, manager }
    }
}

#[async_trait]
impl RedisPublisher for RealRedisPublisher {
    async fn publish(
        &self,
        exchange: &str,
        symbol: &str,
        kind: StreamKind,
        fields: &[(&str, &str)],
    ) -> AppResult<PublishOutcome> {
        if !self.enabled.load(Ordering::Relaxed) {
            return Ok(PublishOutcome::Skipped);
        }
        self.manager.publish(exchange, symbol, kind, fields).await
    }
}

#[derive(Clone, Debug)]
pub struct NoopRedisPublisher {
    _enabled: Arc<AtomicBool>,
}

impl NoopRedisPublisher {
    pub fn new(enabled: Arc<AtomicBool>) -> Self {
        Self { _enabled: enabled }
    }
}

#[async_trait]
impl RedisPublisher for NoopRedisPublisher {
    async fn publish(
        &self,
        _exchange: &str,
        _symbol: &str,
        _kind: StreamKind,
        _fields: &[(&str, &str)],
    ) -> AppResult<PublishOutcome> {
        Ok(PublishOutcome::Skipped)
    }
}

/// ------------------------------
/// DB writing trait
/// ------------------------------
///
pub enum AnyDbBatch<'a> {
    Trades(&'a mut DbBatch<TradeDBRow>),
    Liquidations(&'a mut DbBatch<LiquidationDBRow>),
    DepthDeltas(&'a mut DbBatch<DepthDeltaDBRow>),
    Fundings(&'a mut DbBatch<FundingDBRow>),
    OpenInterests(&'a mut DbBatch<OpenInterestDBRow>),
}

impl<'a> From<&'a mut DbBatch<OpenInterestDBRow>> for AnyDbBatch<'a> {
    fn from(b: &'a mut DbBatch<OpenInterestDBRow>) -> Self {
        AnyDbBatch::OpenInterests(b)
    }
}

impl<'a> From<&'a mut DbBatch<TradeDBRow>> for AnyDbBatch<'a> {
    fn from(b: &'a mut DbBatch<TradeDBRow>) -> Self {
        AnyDbBatch::Trades(b)
    }
}

impl<'a> From<&'a mut DbBatch<LiquidationDBRow>> for AnyDbBatch<'a> {
    fn from(b: &'a mut DbBatch<LiquidationDBRow>) -> Self {
        AnyDbBatch::Liquidations(b)
    }
}

impl<'a> From<&'a mut DbBatch<DepthDeltaDBRow>> for AnyDbBatch<'a> {
    fn from(b: &'a mut DbBatch<DepthDeltaDBRow>) -> Self {
        AnyDbBatch::DepthDeltas(b)
    }
}

impl<'a> From<&'a mut DbBatch<FundingDBRow>> for AnyDbBatch<'a> {
    fn from(b: &'a mut DbBatch<FundingDBRow>) -> Self {
        AnyDbBatch::Fundings(b)
    }
}

#[async_trait]
pub trait DbWriter: Send + Sync + Debug {
    async fn write_batch(&self, batch: AnyDbBatch<'_>) -> AppResult<()>;
}

/// Real DB writer: downcasts Batch<T> to the supported concrete Batch<Row> types.
#[derive(Clone, Debug)]
pub struct RealDbWriter {
    enabled: Arc<AtomicBool>,
    handler: Arc<DbHandler>,
}

impl RealDbWriter {
    pub fn new(enabled: Arc<AtomicBool>, handler: Arc<DbHandler>) -> Self {
        Self { enabled, handler }
    }
}

#[async_trait]
impl DbWriter for RealDbWriter {
    async fn write_batch(&self, batch: AnyDbBatch<'_>) -> AppResult<()> {
        if !self.enabled.load(Ordering::Relaxed) {
            return Ok(());
        }

        match batch {
            AnyDbBatch::Trades(b) => self.handler.write_batch(b).await,
            AnyDbBatch::Liquidations(b) => self.handler.write_batch(b).await,
            AnyDbBatch::DepthDeltas(b) => self.handler.write_batch(b).await,
            AnyDbBatch::Fundings(b) => self.handler.write_batch(b).await,
            AnyDbBatch::OpenInterests(b) => self.handler.write_batch(b).await,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NoopDbWriter {
    // keep the flag so you can "enable later" if you ever swap impls
    // (for now noop ignores it, but we keep it consistent)
    _enabled: Arc<AtomicBool>,
}

impl NoopDbWriter {
    pub fn new(enabled: Arc<AtomicBool>) -> Self {
        Self { _enabled: enabled }
    }
}

#[async_trait]
impl DbWriter for NoopDbWriter {
    async fn write_batch(&self, _batch: AnyDbBatch<'_>) -> AppResult<()> {
        Ok(())
    }
}
