use std::sync::Arc;

use crate::redis::client::RedisClient;
use crate::redis::config::RedisConfig;
use crate::redis::manager::{PublishOutcome, RedisManager};
use crate::redis::metrics::RedisMetrics;
use crate::redis::streams::StreamKind;

/// Integration test:
/// - loads RedisConfig from TOML
/// - publishes more entries than retention allows
/// - verifies Redis stream is trimmed
/// - prints retention behavior for observation
#[tokio::test]
async fn redis_stream_retention_is_enforced() {
    println!("\n[TEST] loading Redis config from TOML");

    let cfg = RedisConfig::load_default().expect("failed to load RedisConfig from default TOML");

    println!(
        "[TEST] retention.maxlen = {}, approx = {}",
        cfg.retention.maxlen, cfg.retention.approx
    );
    println!("[TEST] redis enabled = {}", cfg.enabled);

    assert!(cfg.enabled, "Redis must be enabled for this test");
    assert!(
        cfg.retention.maxlen > 0,
        "retention.maxlen must be > 0 for this test"
    );

    // ------------------------------------------------------------
    // Connect Redis client
    // ------------------------------------------------------------
    println!("[TEST] connecting Redis client");

    let client = RedisClient::connect_from_config(&cfg)
        .await
        .expect("failed to connect Redis client");

    let client = Arc::new(client);

    // ------------------------------------------------------------
    // Build RedisManager
    // ------------------------------------------------------------
    let metrics = RedisMetrics::new().expect("failed to create RedisMetrics");
    let manager = RedisManager::new(cfg.clone(), Arc::clone(&client), metrics)
        .expect("failed to create RedisManager");

    // ------------------------------------------------------------
    // Test stream identifiers
    // ------------------------------------------------------------
    let exchange = "test";
    let symbol = "BTCUSDT";
    let kind = StreamKind::Trades;

    let stream_key = manager.keys.key(exchange, symbol, kind);

    println!("[TEST] using stream key: {}", stream_key);

    // Ensure clean state
    {
        let mut conn = client.clone();
        let _: redis::Value = redis::cmd("DEL")
            .arg(&stream_key)
            .query_async(&mut conn.manager.clone())
            .await
            .expect("failed to delete existing stream");
    }

    // ------------------------------------------------------------
    // Publish more entries than retention allows
    // ------------------------------------------------------------
    let publish_count = cfg.retention.maxlen * 2 + 5;

    println!(
        "[TEST] publishing {} messages (retention maxlen = {})",
        publish_count, cfg.retention.maxlen
    );

    for i in 0..publish_count {
        let seq = i.to_string();

        let fields: [(&str, &str); 3] = [("seq", seq.as_str()), ("price", "100.0"), ("qty", "1.0")];

        let outcome = manager
            .publish(exchange, symbol, kind, &fields)
            .await
            .expect("publish returned error");

        if i % 1000 == 0 {
            println!("[TEST] publish #{i}");
            println!("[TEST] publish #{i} -> {:?}", outcome);
        }

        assert_ne!(
            outcome,
            PublishOutcome::Failed,
            "publish should not fail in healthy Redis"
        );
    }

    // ------------------------------------------------------------
    // Inspect stream length
    // ------------------------------------------------------------
    let stream_len: u64 = {
        let mut conn = client.manager.clone();
        redis::cmd("XLEN")
            .arg(&stream_key)
            .query_async(&mut conn)
            .await
            .expect("XLEN failed")
    };

    println!(
        "[TEST] stream length after publishes = {} (expected <= {})",
        stream_len, cfg.retention.maxlen
    );

    if cfg.retention.approx {
        assert!(
            stream_len <= cfg.retention.maxlen + 5,
            "approx retention exceeded tolerance: len={stream_len}"
        );
    } else {
        assert_eq!(
            stream_len, cfg.retention.maxlen,
            "exact retention not enforced"
        );
    }

    // ------------------------------------------------------------
    // Cleanup
    // ------------------------------------------------------------
    {
        let mut conn = client.manager.clone();
        let _: redis::Value = redis::cmd("DEL")
            .arg(&stream_key)
            .query_async(&mut conn)
            .await
            .expect("failed to cleanup stream");
    }

    println!("[TEST] cleanup complete\n");
}

