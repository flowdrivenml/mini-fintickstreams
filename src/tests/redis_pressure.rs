use crate::redis::client::RedisClient;
use crate::redis::config::RedisConfig;
use crate::redis::health::poller::RedisProbe;
use crate::redis::manager::{PublishOutcome, RedisManager};
use crate::redis::metrics::RedisMetrics;
use crate::redis::streams::StreamKind;
use redis::aio::ConnectionLike;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

/// Stress / structure test:
/// - limits Redis memory
/// - spawns many streams
/// - publishes in rounds
/// - prints memory + gate state
///
/// This test is OBSERVATIONAL, not brittle.
#[tokio::test]
async fn redis_under_memory_pressure_structure_test() {
    println!("\n[PRESSURE TEST] loading Redis config");

    let cfg = RedisConfig::load_default().expect("failed to load RedisConfig");

    assert!(cfg.enabled, "Redis must be enabled");

    let client = Arc::new(
        RedisClient::connect_from_config(&cfg)
            .await
            .expect("failed to connect Redis"),
    );

    // ------------------------------------------------------------
    // Configure Redis memory limits
    // ------------------------------------------------------------
    {
        let mut conn = client.manager.clone();

        redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory")
            .arg("1gb")
            .query_async::<()>(&mut conn)
            .await
            .expect("failed to set maxmemory");

        redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory-policy")
            .arg("allkeys-lru")
            .query_async::<()>(&mut conn)
            .await
            .expect("failed to set maxmemory-policy");
    }

    println!("[PRESSURE TEST] Redis maxmemory set to 1GB");
    println!("[PRESSURE TEST] eviction policy = allkeys-lru");

    // ------------------------------------------------------------
    // Build RedisManager
    // ------------------------------------------------------------
    let metrics = RedisMetrics::new().expect("failed to create metrics");

    let manager = Arc::new(
        RedisManager::new(cfg.clone(), Arc::clone(&client), metrics)
            .expect("failed to create RedisManager"),
    );
    let token = CancellationToken::new();
    // Optionally start health loop
    manager.spawn_health_loop(token);

    // ------------------------------------------------------------
    // Parameters
    // ------------------------------------------------------------
    let exchange = "pressure";
    let kind = StreamKind::Trades;

    let symbol_count = 2_000; // number of streams
    let rounds = 20; // publish rounds
    let sleep_between = Duration::from_millis(200);

    println!(
        "[PRESSURE TEST] spawning {} streams, {} rounds",
        symbol_count, rounds
    );

    // ------------------------------------------------------------
    // Publish in rounds
    // ------------------------------------------------------------
    for round in 0..rounds {
        println!("\n[ROUND {round}] publishing");

        for i in 0..symbol_count {
            let symbol = format!("SYMBOL_{i:05}");
            let seq = format!("{round}-{i}");

            let fields: [(&str, &str); 2] = [("seq", seq.as_str()), ("price", "100.0")];

            let outcome = manager
                .publish(exchange, &symbol, kind, &fields)
                .await
                .unwrap_or(PublishOutcome::Failed);

            // We don't assert here — just observe
            if outcome == PublishOutcome::Failed {
                println!("[ROUND {round}] publish failed for symbol={symbol}");
            }
        }

        // --------------------------------------------------------
        // Memory + health snapshot
        // --------------------------------------------------------
        let (used, max, pct) = client
            .memory_info()
            .await
            .expect("failed to read memory info");

        println!(
            "[ROUND {round}] memory: used={}MB max={:?}MB pct={:?}",
            used / 1024 / 1024,
            max.map(|m| m / 1024 / 1024),
            pct
        );

        println!(
            "[ROUND {round}] gate: can_publish={} can_assign_new_symbol={}",
            manager.can_publish(),
            manager.can_assign_new_symbol()
        );

        sleep(sleep_between).await;
    }

    println!("\n[PRESSURE TEST] completed without known crash\n");
}

#[tokio::test]
async fn redis_intentional_memory_exhaustion() {
    println!("\n[CRASH TEST] loading Redis config");

    let cfg = RedisConfig::load_default().expect("failed to load RedisConfig");

    assert!(cfg.enabled, "Redis must be enabled");

    let client = Arc::new(
        RedisClient::connect_from_config(&cfg)
            .await
            .expect("failed to connect Redis"),
    );

    // ------------------------------------------------------------
    // HARD memory limit: 5 MB
    // ------------------------------------------------------------
    {
        let mut conn = client.manager.clone();

        redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory")
            .arg("5mb")
            .query_async::<()>(&mut conn)
            .await
            .expect("failed to set maxmemory");

        redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory-policy")
            .arg("allkeys-lru")
            .query_async::<()>(&mut conn)
            .await
            .expect("failed to set eviction policy");
    }

    println!("[CRASH TEST] Redis maxmemory = 5MB");
    println!("[CRASH TEST] eviction policy = allkeys-lru");

    // ------------------------------------------------------------
    // RedisManager + health loop
    // ------------------------------------------------------------
    let metrics = RedisMetrics::new().expect("failed to create metrics");

    let manager = Arc::new(
        RedisManager::new(cfg.clone(), Arc::clone(&client), metrics)
            .expect("failed to create RedisManager"),
    );
    let token = CancellationToken::new();
    manager.spawn_health_loop(token);

    // ------------------------------------------------------------
    // Stress parameters (aggressive)
    // ------------------------------------------------------------
    let exchange = "crash";
    let kind = StreamKind::Trades;

    let symbol_count = 500;
    let rounds = 50;

    // VERY LARGE PAYLOAD (10 KB)
    let payload = "X".repeat(10_000);

    println!(
        "[CRASH TEST] symbols={}, rounds={}, payload={} bytes",
        symbol_count,
        rounds,
        payload.len()
    );

    // ------------------------------------------------------------
    // Publish until Redis breaks
    // ------------------------------------------------------------
    for round in 0..rounds {
        println!("\n[ROUND {round}] publishing");

        for i in 0..symbol_count {
            let symbol = format!("SYMBOL_{i:05}");
            let seq = format!("{round}-{i}");

            let fields: [(&str, &str); 3] = [
                ("seq", seq.as_str()),
                ("price", "100.0"),
                ("blob", payload.as_str()),
            ];

            let outcome = manager
                .publish(exchange, &symbol, kind, &fields)
                .await
                .unwrap_or(PublishOutcome::Failed);

            if matches!(outcome, PublishOutcome::Failed) {
                println!("[ROUND {round}] publish FAILED for {symbol}");
            }
        }

        // --------------------------------------------------------
        // Memory + health snapshot
        // --------------------------------------------------------
        let (used, max, pct) = client.memory_info().await.unwrap_or((0, None, None));

        println!(
            "[ROUND {round}] memory: used={}KB max={:?}KB pct={:?}",
            used / 1024,
            max.map(|m| m / 1024),
            pct
        );

        println!(
            "[ROUND {round}] gate: can_publish={} can_assign_new_symbol={}",
            manager.can_publish(),
            manager.can_assign_new_symbol()
        );

        sleep(Duration::from_millis(200)).await;
    }

    println!("\n[CRASH TEST] completed without process crash\n");
}
