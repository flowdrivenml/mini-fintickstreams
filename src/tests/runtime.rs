use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};

use crate::app::StartStreamParams;
use crate::app::runtime::AppRuntime;
use crate::app::stream_types::{ExchangeId, StreamId, StreamKind, StreamTransport};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn run_stream_smoke(
    exchange: ExchangeId,
    symbol: &str,
    kind: StreamKind,
    transport: StreamTransport,
) {
    let _guard = env_lock().lock().await;

    // Oneshoot mode for stream loops
    unsafe {
        std::env::set_var("APP_TEST_ONESHOT", "1");
    }

    eprintln!(
        "[test] starting smoke: exchange={:?}, symbol={}, kind={:?}, transport={:?}",
        exchange, symbol, kind, transport
    );

    eprintln!("[test] building AppRuntime...");
    let rt = AppRuntime::new(false, 0).await.expect("runtime build");
    eprintln!("[test] runtime ready");

    let symbol = symbol.to_string();
    let stream_id = StreamId::new(exchange.as_str(), symbol.as_str(), kind, transport);

    let params = StartStreamParams {
        exchange,
        symbol: symbol.clone(),
        kind,
        transport,
    };

    eprintln!("[test] calling add_stream...");
    if let Err(e) = rt.add_stream(params).await {
        panic!("add_stream failed: {:#}", e);
    }
    eprintln!("[test] add_stream returned Ok");

    assert!(rt.state.contains(&stream_id).await);

    // Wait for the stream's own cancel token to flip (oneshot path)
    eprintln!("[test] waiting for stream cancellation token to be cancelled...");
    timeout(Duration::from_secs(7200), async {
        loop {
            let cancelled = rt
                .state
                .with_handle(&stream_id, |h| h.cancel.is_cancelled())
                .await
                .unwrap_or(false);

            if cancelled {
                break;
            }

            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for stream self-cancel");

    eprintln!("[test] stream token cancelled; now stopping/joining and removing...");

    // Join worker task(s) and remove from registry
    rt.state
        .stop_and_remove(&stream_id)
        .await
        .expect("stop_and_remove");

    eprintln!("[test] stream stopped/removed");

    // Shut down background tasks cleanly
    eprintln!("[test] stopping background health loops...");
    rt.stop_db_health().await;
    rt.stop_redis_health().await;
    rt.stop_runtime_health().await;

    if let Some(jh) = rt.take_runtime_health_task().await {
        jh.abort();
    }

    unsafe {
        std::env::remove_var("APP_TEST_ONESHOT");
    }

    eprintln!("[test] done");
}

// ============================================================================
// 1 endpoint -> 1 test
// ============================================================================

// -------------------------
// BINANCE LINEAR (BTCUSDT)
// -------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_binance_linear_oi_http_smoke() {
    run_stream_smoke(
        ExchangeId::BinanceLinear,
        "BTCUSDT",
        StreamKind::OpenInterest,
        StreamTransport::HttpPoll,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_binance_linear_funding_http_smoke() {
    run_stream_smoke(
        ExchangeId::BinanceLinear,
        "BTCUSDT",
        StreamKind::Funding,
        StreamTransport::HttpPoll,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_binance_linear_trades_ws_smoke() {
    run_stream_smoke(
        ExchangeId::BinanceLinear,
        "BTCUSDT",
        StreamKind::Trades,
        StreamTransport::Ws,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_binance_linear_l2book_ws_smoke() {
    run_stream_smoke(
        ExchangeId::BinanceLinear,
        "BTCUSDT",
        StreamKind::L2Book,
        StreamTransport::Ws,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_binance_linear_liquidations_ws_smoke() {
    run_stream_smoke(
        ExchangeId::BinanceLinear,
        "BTCUSDT",
        StreamKind::Liquidations,
        StreamTransport::Ws,
    )
    .await;
}

// -------------------------
// HYPERLIQUID PERP (BTC)
// -------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_hyperliquid_perp_trades_ws_smoke() {
    run_stream_smoke(
        ExchangeId::HyperliquidPerp,
        "BTC", // IMPORTANT: Hyperliquid uses "BTC", not "BTCUSDT"
        StreamKind::Trades,
        StreamTransport::Ws,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_hyperliquid_perp_l2book_ws_smoke() {
    run_stream_smoke(
        ExchangeId::HyperliquidPerp,
        "BTC",
        StreamKind::L2Book,
        StreamTransport::Ws,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_hyperliquid_perp_oifunding_ws_smoke() {
    run_stream_smoke(
        ExchangeId::HyperliquidPerp,
        "BTC",
        StreamKind::FundingOpenInterest,
        StreamTransport::Ws,
    )
    .await;
}

// -------------------------
// BYBIT LINEAR (BTCUSDT)
// -------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_bybit_linear_trades_ws_smoke() {
    run_stream_smoke(
        ExchangeId::BybitLinear,
        "BTCUSDT",
        StreamKind::Trades,
        StreamTransport::Ws,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_bybit_linear_l2book_ws_smoke() {
    run_stream_smoke(
        ExchangeId::BybitLinear,
        "BTCUSDT",
        StreamKind::L2Book,
        StreamTransport::Ws,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_bybit_linear_liquidations_ws_smoke() {
    run_stream_smoke(
        ExchangeId::BybitLinear,
        "BTCUSDT",
        StreamKind::Liquidations,
        StreamTransport::Ws,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_adds_stream_bybit_linear_oifunding_ws_smoke() {
    run_stream_smoke(
        ExchangeId::BybitLinear,
        "BTCUSDT",
        StreamKind::FundingOpenInterest,
        StreamTransport::Ws,
    )
    .await;
}
