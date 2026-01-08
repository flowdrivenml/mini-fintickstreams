use std::sync::Arc;
use std::time::Duration;

use crate::redis::client::RedisClient;
use crate::redis::config::RedisConfig;
use crate::redis::manager::{PublishOutcome, RedisManager};
use crate::redis::metrics::RedisMetrics;
use crate::redis::streams::StreamKind;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

/// FAILURE TEST:
/// - Redis starts healthy
/// - Redis is forcefully shut down
/// - Publishes must fail gracefully
/// - No panic / crash allowed
#[tokio::test]
async fn redis_hard_failure_is_survivable() {
    println!("\n[FAILURE TEST] loading Redis config");

    let cfg = RedisConfig::load_default().expect("failed to load RedisConfig");

    assert!(cfg.enabled);

    let client = Arc::new(
        RedisClient::connect_from_config(&cfg)
            .await
            .expect("failed to connect Redis"),
    );

    let metrics = RedisMetrics::new().expect("failed to create metrics");

    let manager = Arc::new(
        RedisManager::new(cfg.clone(), Arc::clone(&client), metrics)
            .expect("failed to create RedisManager"),
    );
    let token = CancellationToken::new();
    manager.spawn_health_loop(token);

    let exchange = "failure";
    let symbol = "BTCUSDT";
    let kind = StreamKind::Trades;

    // ------------------------------------------------------------
    // Phase 1: Redis healthy
    // ------------------------------------------------------------
    println!("[FAILURE TEST] publishing while Redis is healthy");

    for i in 0..5 {
        let seq = i.to_string();
        let fields = [("seq", seq.as_str())];

        let outcome = manager
            .publish(exchange, symbol, kind, &fields)
            .await
            .expect("publish errored");

        println!("[FAILURE TEST] publish #{i} -> {:?}", outcome);
        assert_eq!(outcome, PublishOutcome::Published);
    }

    // ------------------------------------------------------------
    // Phase 2: Kill Redis
    // ------------------------------------------------------------
    println!("[FAILURE TEST] SHUTTING DOWN REDIS");

    {
        let mut conn = client.manager.clone();

        // This command will terminate Redis immediately
        let _ = redis::cmd("SHUTDOWN")
            .arg("NOSAVE")
            .query_async::<()>(&mut conn)
            .await;
    }

    // Give OS + client time to notice
    sleep(Duration::from_secs(1)).await;

    // ------------------------------------------------------------
    // Phase 3: Redis is dead
    // ------------------------------------------------------------
    println!("[FAILURE TEST] publishing after Redis shutdown");

    for i in 0..10 {
        let seq = format!("after-{i}");
        let fields = [("seq", seq.as_str())];

        let outcome = manager
            .publish(exchange, symbol, kind, &fields)
            .await
            .unwrap_or(PublishOutcome::Failed);

        println!(
            "[FAILURE TEST] publish after shutdown #{i} -> {:?}",
            outcome
        );

        // IMPORTANT: failure is allowed, panic is not
        assert_ne!(
            outcome,
            PublishOutcome::Published,
            "should not publish after Redis shutdown"
        );
    }

    println!("\n[FAILURE TEST] completed without panic\n");
}
