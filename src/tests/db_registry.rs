use crate::app::control::make_batch_key;
use crate::app::{
    ExchangeId, StartStreamParams, StreamKind, StreamKnobs, StreamSpec, StreamTransport,
};
use crate::db::DbHandler;
use crate::db::config::TimescaleDbConfig;
use crate::db::metrics::DbMetrics;
use crate::db::pools::DbPools;
use sqlx::Row;
use std::str::FromStr;
use std::sync::Arc;

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

fn spec_from_params(p: &StartStreamParams) -> StreamSpec {
    let exchange: &'static str = match p.exchange {
        ExchangeId::BinanceLinear => "binance_linear",
        ExchangeId::HyperliquidPerp => "hyperliquid_perp",
        ExchangeId::BybitLinear => "bybit_linear",
    };

    StreamSpec {
        exchange,
        instrument: p.symbol.clone(),
        kind: p.kind,
        transport: p.transport,
    }
}

async fn make_handler() -> (Arc<DbPools>, DbHandler) {
    let mut cfg = TimescaleDbConfig::load(false, 0).expect("failed to load config");
    override_dsn_if_present(&mut cfg);

    let pools = Arc::new(
        DbPools::new(cfg.clone(), true)
            .await
            .expect("failed to create DbPools"),
    );

    let metrics = Arc::new(DbMetrics::new().expect("metrics init failed"));
    let handler = DbHandler::new(pools.clone(), cfg.writer.clone(), metrics);

    (pools, handler)
}

async fn fetch_registry_row(
    pools: &DbPools,
    spec: &StreamSpec,
) -> Option<(bool, bool, i32, i64, i32, i32, bool)> {
    let exchange_id = ExchangeId::from_str(spec.exchange).ok()?;
    let batch_key =
        make_batch_key(exchange_id, spec.transport, spec.kind, &spec.instrument).ok()?;

    let shard_id = pools
        .shard_id_for(&batch_key.exchange, &batch_key.stream, &batch_key.symbol)
        .await
        .ok()?;

    let pool = pools.pool_by_id(&shard_id).await.ok()?;

    let stream_id =
        crate::app::StreamId::new(spec.exchange, &spec.instrument, spec.kind, spec.transport).0;

    let row = sqlx::query(
        r#"
        SELECT
          disable_db_writes,
          disable_redis_publishes,
          flush_rows,
          flush_interval_ms,
          chunk_rows,
          hard_cap_rows,
          enabled
        FROM mini_fintickstreams.stream_registry
        WHERE stream_id = $1
        "#,
    )
    .bind(stream_id)
    .fetch_optional(&pool)
    .await
    .expect("query registry");

    row.map(|r| {
        (
            r.get("disable_db_writes"),
            r.get("disable_redis_publishes"),
            r.get("flush_rows"),
            r.get("flush_interval_ms"),
            r.get("chunk_rows"),
            r.get("hard_cap_rows"),
            r.get("enabled"),
        )
    })
}

fn test_cases() -> Vec<StartStreamParams> {
    vec![
        StartStreamParams {
            exchange: ExchangeId::HyperliquidPerp,
            transport: StreamTransport::Ws,
            kind: StreamKind::Trades,
            symbol: "BTC".to_string(),
        },
        StartStreamParams {
            exchange: ExchangeId::BinanceLinear,
            transport: StreamTransport::Ws,
            kind: StreamKind::Trades,
            symbol: "BTCUSDT".to_string(),
        },
    ]
}

#[tokio::test]
async fn db_registry_insert() {
    if !db_tests_enabled() {
        println!("[test] Skipping DB integration test (set RUN_DB_TESTS=1)");
        return;
    }

    let (pools, handler) = make_handler().await;
    let knobs = StreamKnobs::default();

    for p in test_cases() {
        let spec = spec_from_params(&p);

        handler
            .upsert_stream_registry(&spec, &knobs, true)
            .await
            .expect("upsert failed");

        let row = fetch_registry_row(&pools, &spec)
            .await
            .expect("row should exist");

        assert_eq!(row.6, true);
        assert_eq!(row.0, false);
        assert_eq!(row.2, 1000);
    }

    println!("[test] registry insert OK");
}

#[tokio::test]
async fn db_registry_update_knobs() {
    if !db_tests_enabled() {
        println!("[test] Skipping DB integration test (set RUN_DB_TESTS=1)");
        return;
    }

    let (pools, handler) = make_handler().await;

    let knobs = StreamKnobs {
        disable_db_writes: true,
        disable_redis_publishes: true,
        flush_rows: 7,
        flush_interval_ms: 1234,
        chunk_rows: 9,
        hard_cap_rows: 11,
    };

    for p in test_cases() {
        let spec = spec_from_params(&p);

        handler
            .update_stream_knobs(&spec, &knobs)
            .await
            .expect("update failed");

        let row = fetch_registry_row(&pools, &spec)
            .await
            .expect("row should exist");

        assert_eq!(row.0, true);
        assert_eq!(row.1, true);
        assert_eq!(row.2, 7);
        assert_eq!(row.3, 1234);
        assert_eq!(row.4, 9);
        assert_eq!(row.5, 11);
    }

    println!("[test] registry knob update OK");
}

#[tokio::test]
async fn db_registry_remove() {
    if !db_tests_enabled() {
        println!("[test] Skipping DB integration test (set RUN_DB_TESTS=1)");
        return;
    }

    let (pools, handler) = make_handler().await;

    for p in test_cases() {
        let spec = spec_from_params(&p);

        handler.remove_stream(&spec).await.expect("remove failed");

        let row = fetch_registry_row(&pools, &spec).await;
        assert!(row.is_none(), "row should be gone");
    }

    println!("[test] registry remove OK");
}

#[tokio::test]
async fn db_registry_load_enabled_streams_returns_expected_vec() {
    if !db_tests_enabled() {
        println!("[test] Skipping DB integration test (set RUN_DB_TESTS=1)");
        return;
    }

    let (_pools, handler) = make_handler().await;

    // Insert 2 streams (we'll use the same two as in your other tests)
    let knobs = StreamKnobs::default();
    let cases = test_cases();

    for p in &cases {
        let spec = spec_from_params(p);
        handler
            .upsert_stream_registry(&spec, &knobs, true)
            .await
            .expect("upsert failed");
    }

    // Query back all enabled streams
    let mut got = handler
        .load_enabled_streams_from_registry()
        .await
        .expect("load_enabled_streams_from_registry failed");

    // Normalize for comparison (order-independent)
    got.sort_by(|a, b| {
        (a.exchange as u8, a.transport as u8, a.kind as u8, &a.symbol).cmp(&(
            b.exchange as u8,
            b.transport as u8,
            b.kind as u8,
            &b.symbol,
        ))
    });

    let mut expected = cases
        .into_iter()
        .map(|p| StartStreamParams {
            exchange: p.exchange,
            transport: p.transport,
            kind: p.kind,
            symbol: p.symbol,
        })
        .collect::<Vec<_>>();

    expected.sort_by(|a, b| {
        (a.exchange as u8, a.transport as u8, a.kind as u8, &a.symbol).cmp(&(
            b.exchange as u8,
            b.transport as u8,
            b.kind as u8,
            &b.symbol,
        ))
    });

    // Assert: returned vector contains at least these two items.
    // (We use "contains" logic because DB might already have other enabled streams.)
    for e in expected {
        assert!(
            got.iter().any(|x| {
                x.exchange == e.exchange
                    && x.transport == e.transport
                    && x.kind == e.kind
                    && x.symbol == e.symbol
            }),
            "missing expected stream in result: exchange={:?} transport={:?} kind={:?} symbol={}",
            e.exchange,
            e.transport,
            e.kind,
            e.symbol
        );
        println!("{:?}", e);
    }

    println!("[test] registry load_enabled_streams OK");
}
