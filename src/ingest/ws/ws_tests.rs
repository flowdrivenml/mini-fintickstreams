// src/ingest/ws/ws_tests.rs

#![cfg(test)]

use crate::app::config::load_app_config;
use crate::error::{AppError, AppResult};
use crate::ingest::config::ExchangeConfigs;
use crate::ingest::spec::Ctx;
use crate::ingest::ws::limiter_registry::WsLimiterRegistry;
use crate::ingest::ws::ws_client::{WsClient, WsEvent, WsTestHook};

use futures_util::{SinkExt, StreamExt};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::protocol::Message};

fn mk_ctx_btc() -> Ctx {
    let mut ctx = Ctx::new();
    ctx.insert("symbol".to_string(), "btcusdt".to_string()); // Binance
    ctx.insert("coin".to_string(), "BTC".to_string()); // Hyperliquid
    ctx
}

fn is_test_done(err: &AppError) -> bool {
    match err {
        AppError::Internal(s) => s == "__TEST_DONE__",
        _ => false,
    }
}

async fn stop_after_n_text_messages(
    n: usize,
) -> impl FnMut(WsEvent) -> std::pin::Pin<Box<dyn std::future::Future<Output = AppResult<()>> + Send>>
+ Send
+ 'static {
    let seen = Arc::new(AtomicUsize::new(0));

    move |ev: WsEvent| {
        let seen = seen.clone();
        Box::pin(async move {
            if let WsEvent::Text(s) = ev {
                let c = seen.fetch_add(1, Ordering::SeqCst) + 1;
                println!("[ws-test] text #{c}: {s}");
                if c >= n {
                    return Err(AppError::Internal("__TEST_DONE__".into()));
                }
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn test_live_ws_binance_trades_no_limiter_10_messages() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config(false, 0)?;
    let ex = ExchangeConfigs::new(&appcfg, false, 0)?;
    let cfg = ex
        .binance_linear
        .as_ref()
        .expect("binance_linear config must exist in this test");
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
            None, // no limiter
            stream,
            ctx,
            stop_after_n_text_messages(10).await,
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
async fn test_live_ws_hyperliquid_trades_no_limiter_10_messages() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config(false, 0)?;
    let ex = ExchangeConfigs::new(&appcfg, false, 0)?;
    let cfg = ex
        .hyperliquid_perp
        .as_ref()
        .expect("hyperliquid_perp config must exist in this test");
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
            None, // no limiter
            stream,
            ctx,
            stop_after_n_text_messages(10).await,
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
async fn test_live_ws_binance_trades_with_limiters_10_messages() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config(false, 0)?;
    let ex = ExchangeConfigs::new(&appcfg, false, 0)?;
    let cfg = ex
        .binance_linear
        .as_ref()
        .expect("binance_linear config must exist in this test");
    let stream = cfg
        .ws
        .get("trades")
        .expect("missing [ws.trades] in binance config");

    let cfgs = ExchangeConfigs::new(&appcfg, false, 0)?;
    let ws_limiters = WsLimiterRegistry::new(&appcfg, &cfgs, None)?;

    let client = WsClient::new("binance_linear", cfg.clone(), None, None);

    let ctx = mk_ctx_btc();
    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(5),
        ..Default::default()
    };

    let res = client
        .run_stream(
            Some(&ws_limiters), // limiter enabled
            stream,
            ctx,
            stop_after_n_text_messages(10).await,
            Some(&mut hook),
            None,
        )
        .await;

    println!(
        "[test] reconnect attempts observed = {}",
        hook.reconnect_attempts
    );

    println!("[test] disconnect reasons = {:?}", hook.disconnects);

    match res {
        Err(e) if is_test_done(&e) => Ok(()),
        other => other,
    }
}

#[tokio::test]
async fn test_live_ws_hyperliquid_trades_with_limiters_10_messages() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let appcfg = load_app_config(false, 0)?;
    let ex = ExchangeConfigs::new(&appcfg, false, 0)?;
    let cfg = ex
        .hyperliquid_perp
        .as_ref()
        .expect("hyperliquid_perp config must exist in this test");
    let stream = cfg
        .ws
        .get("trades")
        .expect("missing [ws.trades] in hyperliquid config");

    let cfgs = ExchangeConfigs::new(&appcfg, false, 0)?;
    let ws_limiters = WsLimiterRegistry::new(&appcfg, &cfgs, None)?;

    let client = WsClient::new("hyperliquid_perp", cfg.clone(), None, None);

    let ctx = mk_ctx_btc();
    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(5),
        ..Default::default()
    };

    let res = client
        .run_stream(
            Some(&ws_limiters), // limiter enabled
            stream,
            ctx,
            stop_after_n_text_messages(2).await,
            Some(&mut hook),
            None,
        )
        .await;

    println!(
        "[test] reconnect attempts observed = {}",
        hook.reconnect_attempts
    );

    println!("[test] disconnect reasons = {:?}", hook.disconnects);

    match res {
        Err(e) if is_test_done(&e) => Ok(()),
        other => other,
    }
}

// --- Local WS server tests (deterministic ping/pong + reconnect)

async fn spawn_local_ws_server_ping_close(
    addr: &str,
    accept_count: Arc<AtomicUsize>,
    pong_count: Arc<AtomicUsize>,
) -> AppResult<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| AppError::Internal(format!("ws test server bind error: {e}")))?;

    loop {
        let (tcp, _peer) = listener
            .accept()
            .await
            .map_err(|e| AppError::Internal(format!("ws test server accept error: {e}")))?;

        let accept_count = accept_count.clone();
        let pong_count = pong_count.clone();

        tokio::spawn(async move {
            let ws = accept_async(tcp).await.expect("accept_async");
            let (mut write, mut read) = ws.split();

            accept_count.fetch_add(1, Ordering::SeqCst);

            // Client will send a subscribe payload immediately; read and ignore it (or timeout).
            let _ = tokio::time::timeout(Duration::from_secs(2), read.next()).await;

            // Send Ping and expect Pong.
            let payload = b"ping_payload".to_vec();
            write.send(Message::Ping(payload.clone().into())).await.ok();

            // Wait for pong (best effort).
            if let Ok(Some(Ok(Message::Pong(p)))) =
                tokio::time::timeout(Duration::from_secs(2), read.next()).await
            {
                if p == payload {
                    pong_count.fetch_add(1, Ordering::SeqCst);
                }
            }

            // Send a couple messages so the client can forward events.
            write
                .send(Message::Text(r#"{"type":"test","n":1}"#.into()))
                .await
                .ok();
            write
                .send(Message::Text(r#"{"type":"test","n":2}"#.into()))
                .await
                .ok();

            // Close to force reconnect.
            write.send(Message::Close(None)).await.ok();
        });
    }
}

#[tokio::test]
async fn test_local_ws_reconnects_and_pongs_no_limiter_binance() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    let accept_count = Arc::new(AtomicUsize::new(0));
    let pong_count = Arc::new(AtomicUsize::new(0));

    // Bind once, keep listener alive, and pass it to the server task (Binance-style).
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| AppError::Internal(format!("ws test server bind error: {e}")))?;
    let local_addr = listener.local_addr().unwrap();

    let server_accepts = accept_count.clone();
    let server_pongs = pong_count.clone();
    tokio::spawn(async move {
        let _ =
            spawn_local_ws_server_ping_close_listener(listener, server_accepts, server_pongs).await;
    });

    // Load real Binance config and override ws_base_url to local ws://
    let appcfg = load_app_config(false, 0)?;
    let ex = ExchangeConfigs::new(&appcfg, false, 0)?;
    let mut cfg = ex
        .binance_linear
        .as_ref()
        .expect("binance_linear config must exist")
        .clone();

    cfg.ws_base_url = format!("ws://{}", local_addr);
    cfg.ws_connection_timeout_seconds = 0;
    cfg.ws_heartbeat_type = None; // Binance-style: server Ping -> client Pong

    // Binance stream config: uses stream_title and stream_id
    let stream = cfg
        .ws
        .get("trades")
        .expect("missing [ws.trades] in binance config")
        .clone();

    let client = WsClient::new("binance_linear", cfg, None, None);

    // Binance ctx: ensure stream_title renders to lowercase symbol
    let ctx = mk_ctx_btc();

    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(3),
        ..Default::default()
    };

    let res = tokio::time::timeout(Duration::from_secs(10), async {
        client
            .run_stream(
                None, // no limiter
                &stream,
                ctx,
                |ev| {
                    Box::pin(async move {
                        match &ev {
                            WsEvent::Ping(p) => {
                                println!("[local-binance] got ping bytes={}", p.len());
                            }
                            WsEvent::Pong(p) => {
                                println!("[local-binance] got pong bytes={}", p.len());
                            }
                            WsEvent::Text(s) => {
                                // On Binance we expect an ack like {"result":null,"id":"1"} first.
                                println!("[local-binance] got text: {s}");
                            }
                            _ => {}
                        }
                        Ok(())
                    })
                },
                Some(&mut hook),
                None,
            )
            .await
    })
    .await;

    match res {
        Ok(Ok(())) => {
            let accepts = accept_count.load(Ordering::SeqCst);
            let pongs = pong_count.load(Ordering::SeqCst);
            println!("[local-binance] accepts={accepts} pongs={pongs}");
            assert!(accepts >= 1, "expected at least 1 accept");
            assert!(pongs >= 1, "expected at least 1 pong");
            Ok(())
        }
        Ok(Err(e)) => Err(e),
        Err(_) => Err(AppError::Internal(
            "local binance reconnect test timed out".into(),
        )),
    }
}

use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn test_local_ws_cancel_stops_stuck_read_loop() -> AppResult<()> {
    crate::telemetry::init_for_tests();

    // Bind local WS server
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| AppError::Internal(format!("ws test server bind error: {e}")))?;
    let local_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = spawn_local_ws_server_silent_listener(listener).await;
    });

    // Load real Binance config and override ws_base_url to local ws://
    let appcfg = load_app_config(false, 0)?;
    let ex = ExchangeConfigs::new(&appcfg, false, 0)?;
    let mut cfg = ex
        .binance_linear
        .as_ref()
        .expect("binance_linear config must exist")
        .clone();

    cfg.ws_base_url = format!("ws://{}", local_addr);

    // Important: disable connection timeout so the only exit is cancellation
    cfg.ws_connection_timeout_seconds = 0;

    // Disable client-driven heartbeat (server is silent; we only want cancel to stop it)
    cfg.ws_heartbeat_type = None;

    let stream = cfg
        .ws
        .get("trades")
        .expect("missing [ws.trades] in binance config")
        .clone();

    let client = WsClient::new("binance_linear", cfg, None, None);

    let ctx = mk_ctx_btc();

    let mut hook: WsTestHook = WsTestHook {
        max_reconnect_attempts: Some(100), // should never matter; we cancel before reconnect
        ..Default::default()
    };

    let cancel = CancellationToken::new();

    // Run stream in a task
    let cancel_for_stream = cancel.clone();
    let run_task = tokio::spawn(async move {
        client
            .run_stream(
                None,
                &stream,
                ctx,
                |_ev| Box::pin(async move { Ok(()) }),
                Some(&mut hook),
                Some(cancel_for_stream),
            )
            .await
    });

    // Let it connect + get stuck in read loop
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cancel should stop it promptly
    cancel.cancel();

    // Must exit quickly (this asserts your select!-based inner loop is correct)
    let res = tokio::time::timeout(Duration::from_secs(2), run_task).await;

    match res {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => Err(e),
        Ok(Err(join_err)) => Err(AppError::Internal(format!("task join error: {join_err}"))),
        Err(_) => Err(AppError::Internal(
            "cancel test timed out (likely stuck on read.next() / no select!)".into(),
        )),
    }
}

async fn spawn_local_ws_server_ping_close_listener(
    listener: TcpListener,
    accept_count: Arc<AtomicUsize>,
    pong_count: Arc<AtomicUsize>,
) -> AppResult<()> {
    loop {
        let (tcp, _peer) = listener
            .accept()
            .await
            .map_err(|e| AppError::Internal(format!("ws test server accept error: {e}")))?;

        let accept_count = accept_count.clone();
        let pong_count = pong_count.clone();

        tokio::spawn(async move {
            let ws = accept_async(tcp).await.expect("accept_async");
            let (mut write, mut read) = ws.split();

            accept_count.fetch_add(1, Ordering::SeqCst);

            // Read the client's subscribe message (Binance-style JSON)
            let _ = tokio::time::timeout(Duration::from_secs(2), read.next()).await;

            // Ping + expect Pong payload match
            let payload = b"binance_ping".to_vec();
            write.send(Message::Ping(payload.clone().into())).await.ok();

            if let Ok(Some(Ok(Message::Pong(p)))) =
                tokio::time::timeout(Duration::from_secs(2), read.next()).await
            {
                if p == payload {
                    pong_count.fetch_add(1, Ordering::SeqCst);
                }
            }

            // Send Binance-style subscribe ACK
            let _ = write
                .send(Message::Text(r#"{"result":null,"id":"1"}"#.into()))
                .await;

            // Close to force reconnect
            let _ = write.send(Message::Close(None)).await;
        });
    }
}

// --- Local WS server: accepts WS, reads subscribe, then stays silent forever.
async fn spawn_local_ws_server_silent_listener(listener: TcpListener) -> AppResult<()> {
    loop {
        let (tcp, _peer) = listener
            .accept()
            .await
            .map_err(|e| AppError::Internal(format!("ws test server accept error: {e}")))?;

        tokio::spawn(async move {
            let ws = accept_async(tcp).await.expect("accept_async");
            let (_write, mut read) = ws.split();

            // Read the client's subscribe message (best-effort, so test doesn't hang here)
            let _ = tokio::time::timeout(Duration::from_secs(2), read.next()).await;

            // Now: be completely silent forever (or long enough)
            futures_util::future::pending::<()>().await;
        });
    }
}
