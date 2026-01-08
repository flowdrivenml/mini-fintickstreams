// src/ingest/ws/ws_deserialize_tests.rs
#![cfg(test)]

use crate::app::config::load_app_config;
use crate::error::{AppError, AppResult};
use crate::ingest::config::ExchangeConfigs;
use crate::ingest::spec::Ctx;
use crate::ingest::ws::ws_client::{WsClient, WsEvent, WsTestHook};

use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

// Types to test
use crate::ingest::datamap::sources::binance_linear::types::{
    BinanceLinearWsAggTrade, BinanceLinearWsDepthUpdate, BinanceLinearWsForceOrder,
};

use crate::ingest::datamap::sources::hyperliquid_perp::types::{
    HyperliquidPerpWsDepthUpdate, HyperliquidPerpWsOIFundingUpdate, HyperliquidPerpWsTrade,
};

fn mk_ctx_btc() -> Ctx {
    let mut ctx = Ctx::new();
    ctx.insert("symbol".to_string(), "btcusdt".to_string()); // Binance stream templates
    ctx.insert("coin".to_string(), "BTC".to_string()); // Hyperliquid expects uppercase
    ctx
}

fn is_test_done(err: &AppError) -> bool {
    matches!(err, AppError::Internal(s) if s == "__TEST_DONE__")
}

fn pretty_json(v: &JsonValue) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| "<json pretty failed>".into())
}

/// Returns true if the json looks like a Binance event with event type `event`.
fn is_binance_event(v: &JsonValue, event: &str) -> bool {
    v.get("e").and_then(|x| x.as_str()) == Some(event)
}

/// Returns true if the json looks like a Hyperliquid WS message for `channel`.
/// Hyperliquid payloads vary; this supports a few common shapes:
/// - {"channel":"trades", ...}
/// - {"channel":"l2Book", ...}
/// - {"type":"trades", ...} (fallback)
/// - {"channel":"...", "data":{...}} (still matches)
fn is_hyper_channel(v: &JsonValue, channel: &str) -> bool {
    // Most HL messages: {"channel":"...", "data": ...} or {"channel":"pong"}
    if v.get("channel").and_then(|x| x.as_str()) == Some(channel) {
        return true;
    }

    // Some variants might use "type" at the top-level
    if v.get("type").and_then(|x| x.as_str()) == Some(channel) {
        return true;
    }

    // Optional: handle nested "data.type" or "data.channel" if HL ever wraps differently
    if v.get("data")
        .and_then(|d| d.get("type"))
        .and_then(|x| x.as_str())
        == Some(channel)
    {
        return true;
    }

    if v.get("data")
        .and_then(|d| d.get("channel"))
        .and_then(|x| x.as_str())
        == Some(channel)
    {
        return true;
    }

    false
}

/// Hook that:
/// - listens to Text frames
/// - parses JSON
/// - filters using `should_try`
/// - deserializes into `T`
/// - stops after `n_ok` successful deserializations
async fn stop_after_n_typed_messages<T>(
    label: &'static str,
    n_ok: usize,
    should_try: impl FnMut(&JsonValue) -> bool + Send + 'static,
) -> impl FnMut(WsEvent) -> std::pin::Pin<Box<dyn std::future::Future<Output = AppResult<()>> + Send>>
+ Send
+ 'static
where
    T: DeserializeOwned + Send + 'static,
{
    use tokio::sync::Mutex;

    let ok_count = Arc::new(AtomicUsize::new(0));
    let should_try = Arc::new(Mutex::new(should_try));

    move |ev: WsEvent| {
        let ok_count = ok_count.clone();
        let should_try = should_try.clone();

        Box::pin(async move {
            let WsEvent::Text(s) = ev else {
                return Ok(());
            };

            let v: JsonValue = match serde_json::from_str(&s) {
                Ok(v) => v,
                Err(e) => {
                    println!("--- {label} got non-JSON text ---");
                    println!("{s}");
                    return Err(AppError::Json(e));
                }
            };

            println!("v = {}", pretty_json(&v));

            // Ignore messages that are not the payload for this test (acks, heartbeats, etc.)
            let should_attempt = {
                let mut f = should_try.lock().await;
                (*f)(&v)
            };
            if !should_attempt {
                return Ok(());
            }

            // Try typed deserialization.
            match serde_json::from_value::<T>(v.clone()) {
                Ok(_msg) => {
                    let c = ok_count.fetch_add(1, Ordering::SeqCst) + 1;
                    println!("[{label}] deserialization OK #{c}");
                    if c >= n_ok {
                        return Err(AppError::Internal("__TEST_DONE__".into()));
                    }
                    Ok(())
                }
                Err(e) => {
                    println!("--- {label} DESERIALIZATION FAILED ---");
                    println!("serde error = {e}");
                    println!("payload = {}", pretty_json(&v));
                    Err(AppError::Json(e))
                }
            }
        })
    }
}

/* ----------------------------- BINANCE ----------------------------- */

#[tokio::test]
async fn ws_binance_depth_update_deserializes_10() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config()?;
    let ex = ExchangeConfigs::new(&appcfg)?;
    let cfg = ex
        .binance_linear
        .as_ref()
        .expect("binance_linear config must exist");

    // Uses whatever your config struct is; we only need to pass it to run_stream.
    let stream = cfg
        .ws
        .get("depth_update")
        .expect("missing [ws.depth_update] in binance config");

    let client = WsClient::new("binance_linear", cfg.clone(), None, None);
    let ctx = mk_ctx_btc();

    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(5),
        ..Default::default()
    };

    let res = client
        .run_stream(
            None,
            stream,
            ctx,
            stop_after_n_typed_messages::<BinanceLinearWsDepthUpdate>(
                "binance.depth_update",
                10,
                |v| is_binance_event(v, "depthUpdate"),
            )
            .await,
            Some(&mut hook),
            None,
        )
        .await;

    match res {
        Err(e) if is_test_done(&e) => Ok(()),
        other => other,
    }
}

#[tokio::test]
async fn ws_binance_trades_deserializes_10() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config()?;
    let ex = ExchangeConfigs::new(&appcfg)?;
    let cfg = ex
        .binance_linear
        .as_ref()
        .expect("binance_linear config must exist");

    let stream = cfg
        .ws
        .get("trades")
        .expect("missing [ws.trades] in binance config");

    let client = WsClient::new("binance_linear", cfg.clone(), None, None);
    let ctx = mk_ctx_btc();

    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(5),
        ..Default::default()
    };

    let res = client
        .run_stream(
            None,
            stream,
            ctx,
            stop_after_n_typed_messages::<BinanceLinearWsAggTrade>("binance.trades", 10, |v| {
                is_binance_event(v, "aggTrade")
            })
            .await,
            Some(&mut hook),
            None,
        )
        .await;

    match res {
        Err(e) if is_test_done(&e) => Ok(()),
        other => other,
    }
}

// It works, i guess,  liquidations do not happen all the time :) well it should works, i hope
#[tokio::test]
async fn ws_binance_liquidations_deserializes_10() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config()?;
    let ex = ExchangeConfigs::new(&appcfg)?;
    let cfg = ex
        .binance_linear
        .as_ref()
        .expect("binance_linear config must exist");

    let stream = cfg
        .ws
        .get("liquidations")
        .expect("missing [ws.liquidations] in binance config");

    let client = WsClient::new("binance_linear", cfg.clone(), None, None);
    let ctx = mk_ctx_btc();

    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(5),
        ..Default::default()
    };

    let res = client
        .run_stream(
            None,
            stream,
            ctx,
            stop_after_n_typed_messages::<BinanceLinearWsForceOrder>(
                "binance.liquidations",
                10,
                |v| is_binance_event(v, "forceOrder"),
            )
            .await,
            Some(&mut hook),
            None,
        )
        .await;

    match res {
        Err(e) if is_test_done(&e) => Ok(()),
        other => other,
    }
}

/* --------------------------- HYPERLIQUID ---------------------------- */
/*
IMPORTANT:
Hyperliquid WS message shapes vary by subscription. These predicates are "best guess":
- trades -> channel == "trades"
- depth_update -> channel == "l2Book"
- oi_funding -> channel == "oiFunding" (placeholder-ish)

If your actual payloads use different channel/type strings, update the predicates.
The tests will still print the failing payload if deserialization is attempted and fails.
*/

#[tokio::test]
async fn ws_hyperliquid_depth_update_deserializes_10() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config()?;
    let ex = ExchangeConfigs::new(&appcfg)?;
    let cfg = ex
        .hyperliquid_perp
        .as_ref()
        .expect("hyperliquid_perp config must exist");

    let stream = cfg
        .ws
        .get("depth_update")
        .expect("missing [ws.depth_update] in hyperliquid config");

    let client = WsClient::new("hyperliquid_perp", cfg.clone(), None, None);
    let ctx = mk_ctx_btc();

    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(5),
        ..Default::default()
    };

    let res = client
        .run_stream(
            None,
            stream,
            ctx,
            stop_after_n_typed_messages::<HyperliquidPerpWsDepthUpdate>(
                "hyperliquid.depth_update",
                10,
                |v| is_hyper_channel(v, "l2Book"),
            )
            .await,
            Some(&mut hook),
            None,
        )
        .await;

    match res {
        Err(e) if is_test_done(&e) => Ok(()),
        other => other,
    }
}

#[tokio::test]
async fn ws_hyperliquid_trades_deserializes_10() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config()?;
    let ex = ExchangeConfigs::new(&appcfg)?;
    let cfg = ex
        .hyperliquid_perp
        .as_ref()
        .expect("hyperliquid_perp config must exist");

    let stream = cfg
        .ws
        .get("trades")
        .expect("missing [ws.trades] in hyperliquid config");

    let client = WsClient::new("hyperliquid_perp", cfg.clone(), None, None);
    let ctx = mk_ctx_btc();

    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(5),
        ..Default::default()
    };

    let res = client
        .run_stream(
            None,
            stream,
            ctx,
            stop_after_n_typed_messages::<HyperliquidPerpWsTrade>("hyperliquid.trades", 10, |v| {
                is_hyper_channel(v, "trades")
            })
            .await,
            Some(&mut hook),
            None,
        )
        .await;

    match res {
        Err(e) if is_test_done(&e) => Ok(()),
        other => other,
    }
}

#[tokio::test]
async fn ws_hyperliquid_oi_funding_deserializes_10() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config()?;
    let ex = ExchangeConfigs::new(&appcfg)?;
    let cfg = ex
        .hyperliquid_perp
        .as_ref()
        .expect("hyperliquid_perp config must exist");

    let stream = cfg
        .ws
        .get("oi_funding")
        .expect("missing [ws.oi_funding] in hyperliquid config");

    let client = WsClient::new("hyperliquid_perp", cfg.clone(), None, None);
    let ctx = mk_ctx_btc();

    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(5),
        ..Default::default()
    };

    let res = client
        .run_stream(
            None,
            stream,
            ctx,
            stop_after_n_typed_messages::<HyperliquidPerpWsOIFundingUpdate>(
                "hyperliquid.oi_funding",
                10,
                |v| is_hyper_channel(v, "activeAssetCtx"), // adjust to your actual channel/type string
            )
            .await,
            Some(&mut hook),
            None,
        )
        .await;

    match res {
        Err(e) if is_test_done(&e) => Ok(()),
        other => other,
    }
}

#[tokio::test]
async fn ws_hyperliquid_oi_funding_print_first_50() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config()?;
    let ex = ExchangeConfigs::new(&appcfg)?;
    let cfg = ex
        .hyperliquid_perp
        .as_ref()
        .expect("hyperliquid_perp config must exist");

    let stream = cfg
        .ws
        .get("oi_funding")
        .expect("missing [ws.oi_funding] in hyperliquid config");

    let client = WsClient::new("hyperliquid_perp", cfg.clone(), None, None);
    let ctx = mk_ctx_btc();

    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(5),
        ..Default::default()
    };

    // Print first N text messages, then stop.
    let n: usize = 50;
    let seen = Arc::new(AtomicUsize::new(0));
    let seen2 = seen.clone();

    let res = client
        .run_stream(
            None,
            stream,
            ctx,
            move |ev: WsEvent| {
                let seen2 = seen2.clone();
                Box::pin(async move {
                    if let WsEvent::Text(s) = ev {
                        let c = seen2.fetch_add(1, Ordering::SeqCst) + 1;

                        println!("\n--- hyperliquid.oi_funding text #{c} ---");

                        // Pretty-print JSON if possible, else raw.
                        match serde_json::from_str::<JsonValue>(&s) {
                            Ok(v) => println!("{}", pretty_json(&v)),
                            Err(_) => println!("{s}"),
                        }

                        if c >= n {
                            return Err(AppError::Internal("__TEST_DONE__".into()));
                        }
                    }
                    Ok(())
                })
            },
            Some(&mut hook),
            None,
        )
        .await;

    match res {
        Err(e) if is_test_done(&e) => Ok(()),
        other => other,
    }
}
