use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};

use crate::db::config::TimescaleDbConfig;
use crate::db::metrics::DbMetrics;
use crate::db::pools::DbPools;
use crate::db::{Batch, BatchKey, DbHandler};

use crate::db::rows::{
    DepthDeltaDBRow, FundingDBRow, LiquidationDBRow, OpenInterestDBRow, TradeDBRow,
};

fn db_tests_enabled() -> bool {
    std::env::var("RUN_DB_TESTS").ok().as_deref() == Some("1")
}

fn override_dsn_if_present(cfg: &mut TimescaleDbConfig) {
    if let Ok(dsn) = std::env::var("TEST_DATABASE_URL") {
        println!("[test] Overriding DSN using TEST_DATABASE_URL");
        for shard in &mut cfg.shards {
            shard.dsn_env = dsn.clone();
        }
    }
}

fn key(stream: &str, symbol: &str) -> BatchKey {
    BatchKey {
        exchange: "binance_linear".to_string(),
        stream: stream.to_string(),
        symbol: symbol.to_string(),
    }
}

#[tokio::test]
async fn write_batches_for_all_row_types() {
    if !db_tests_enabled() {
        println!("[test] Skipping DB integration test (set RUN_DB_TESTS=1)");
        return;
    }

    println!("[test] ===== DB WRITE INTEGRATION TEST START =====");

    // --------------------------------------------------
    // Load config
    // --------------------------------------------------
    println!("[test] Loading TimescaleDB config...");
    let mut cfg = TimescaleDbConfig::load().expect("failed to load config");
    cfg.writer.batch_size = 2;
    override_dsn_if_present(&mut cfg);

    println!(
        "[test] Loaded config: {} shard(s), batch_size={}, flush_interval_ms={}",
        cfg.shards.len(),
        cfg.writer.batch_size,
        cfg.writer.flush_interval_ms
    );

    // --------------------------------------------------
    // Pools
    // --------------------------------------------------
    println!("[test] Creating DbPools (with verification)...");
    let pools = Arc::new(
        DbPools::new(cfg.clone(), true)
            .await
            .expect("failed to create DbPools"),
    );
    println!("[test] DbPools ready");

    // --------------------------------------------------
    // Metrics
    // --------------------------------------------------
    println!("[test] Initializing metrics...");
    let metrics = Arc::new(DbMetrics::new().expect("metrics init failed"));
    println!("[test] Metrics ready");

    // --------------------------------------------------
    // Handler
    // --------------------------------------------------
    println!("[test] Creating DbHandler...");
    let handler = DbHandler::new(pools, cfg.writer.clone(), metrics);
    println!("[test] DbHandler ready");

    let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

    // --------------------------------------------------
    // Trades
    // --------------------------------------------------
    println!("[test] Writing trades batch...");
    let mut trades = Batch::new(
        key("trades", "BTCUSDT"),
        vec![
            TradeDBRow {
                time: ts,
                symbol: "BTCUSDT".into(),
                side: 0,
                price_i: 42_000_000,
                qty_i: 1000,
                trade_id: Some(1),
                is_maker: Some(true),
            },
            TradeDBRow {
                time: ts + chrono::Duration::milliseconds(1),
                symbol: "BTCUSDT".into(),
                side: 1,
                price_i: 42_001_000,
                qty_i: 500,
                trade_id: Some(2),
                is_maker: Some(false),
            },
        ],
        &cfg.writer,
    );

    handler
        .write_batch(&mut trades)
        .await
        .expect("write trades");
    println!("[test] Trades batch written");

    // --------------------------------------------------
    // Depth deltas
    // --------------------------------------------------
    println!("[test] Writing depth deltas batch...");
    let mut depth = Batch::new(
        key("depth", "BTCUSDT"),
        vec![
            DepthDeltaDBRow {
                time: ts,
                symbol: "BTCUSDT".into(),
                side: 0,
                price_i: 42_000_000,
                size_i: 100,
                seq: Some(1),
            },
            DepthDeltaDBRow {
                time: ts,
                symbol: "BTCUSDT".into(),
                side: 1,
                price_i: 42_010_000,
                size_i: 200,
                seq: Some(2),
            },
        ],
        &cfg.writer,
    );

    handler.write_batch(&mut depth).await.expect("write depth");
    println!("[test] Depth deltas batch written");

    // --------------------------------------------------
    // Open interest
    // --------------------------------------------------
    println!("[test] Writing open interest batch...");
    let mut oi = Batch::new(
        key("open_interest", "BTCUSDT"),
        vec![OpenInterestDBRow {
            time: ts,
            symbol: "BTCUSDT".into(),
            oi_i: 123_456,
        }],
        &cfg.writer,
    );

    handler
        .write_batch(&mut oi)
        .await
        .expect("write open interest");
    println!("[test] Open interest batch written");

    // --------------------------------------------------
    // Funding
    // --------------------------------------------------
    println!("[test] Writing funding batch...");
    let mut funding = Batch::new(
        key("funding", "BTCUSDT"),
        vec![FundingDBRow {
            time: ts,
            symbol: "BTCUSDT".into(),
            funding_rate: 10,
            funding_time: Some(ts + chrono::Duration::hours(8)),
        }],
        &cfg.writer,
    );

    handler
        .write_batch(&mut funding)
        .await
        .expect("write funding");
    println!("[test] Funding batch written");

    // --------------------------------------------------
    // Liquidations
    // --------------------------------------------------
    println!("[test] Writing liquidations batch...");
    let mut liqs = Batch::new(
        key("liquidations", "BTCUSDT"),
        vec![LiquidationDBRow {
            time: ts,
            symbol: "BTCUSDT".into(),
            side: 0,
            price_i: Some(41_900_000),
            qty_i: 250,
            liq_id: Some(42),
        }],
        &cfg.writer,
    );

    handler
        .write_batch_with_retry(&mut liqs, 2, Duration::from_millis(50))
        .await
        .expect("write liquidations");

    println!("[test] Liquidations batch written");

    println!("[test] ===== DB WRITE INTEGRATION TEST OK =====");
}
