use super::helpers::binance_ws_request_id;
use crate::app::control::batch::make_empty_batch;
use crate::app::runtime::AppRuntime;
use crate::app::state::StreamHandle;
use crate::app::state::StreamKnobs;
use crate::app::stream_types::{ExchangeId, StreamKind, StreamTransport};
use crate::app::stream_types::{StreamId, StreamSpec, StreamStatus};
use crate::db::WriterConfig;
use crate::db::rows::{
    DepthDeltaDBRow, FundingDBRow, LiquidationDBRow, OpenInterestDBRow, TradeDBRow,
};
use crate::error::{AppError, AppResult};
use crate::ingest::Ctx;
use crate::ingest::datamap::ctx::MapCtx;
use crate::ingest::datamap::event::MapEnvelope;
use crate::ingest::datamap::event::MarketEvent;
use crate::ingest::datamap::sources::binance_linear::types::{
    BinanceLinearWsAggTrade, BinanceLinearWsDepthUpdate, BinanceLinearWsForceOrder,
};
use crate::ingest::datamap::sources::hyperliquid_perp::types::{
    HyperliquidPerpWsDepthUpdate, HyperliquidPerpWsOIFundingUpdate, HyperliquidPerpWsTrade,
};
use crate::ingest::traits::MapToEvents;
use crate::ingest::ws::WsEvent;
use crate::redis::StreamKind as RedisStreamKind;
use crate::redis::fields::as_publish_fields;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing;

pub async fn ws_binancelinear_aggtrades(
    runtime: &AppRuntime,
    mut ctx: Ctx,
    map_ctx: MapCtx,
    map_envelope: MapEnvelope,
    stream_spec: StreamSpec,
    symbol: String,
    stream_id: StreamId,
) -> AppResult<()> {
    let exchange = ExchangeId::BinanceLinear;
    let transport = StreamTransport::Ws;
    let kind = StreamKind::Trades;

    // --- clone owned handles BEFORE spawn (no &runtime inside task) ---
    let deps = runtime.deps.clone();

    // Grab shared deps as owned handles we can move into the task.
    let ws_client = runtime
        .deps
        .as_ref()
        .binance_linear_ws
        .clone()
        .ok_or_else(|| AppError::Disabled("Binance Linear exchange is disabled!".into()))?;

    // ---- Build ids/spec/status for registry ----
    let stream_id_for_task = stream_id.clone();
    let req_id = binance_ws_request_id(&stream_id_for_task.to_string());
    let stream_status = StreamStatus::Running;
    let cancel = CancellationToken::new();

    // Move values into the spawned task
    let symbol_for_task = symbol.clone();
    let map_envelope_for_task = Arc::new(map_envelope);
    let cancel_for_task = cancel.clone();
    let cancel_for_test = cancel.clone();
    let map_ctx_for_task = Arc::new(map_ctx);

    // Make a db batch
    let writer_cfg = match runtime.deps.as_ref().db.as_ref() {
        Some(db) => db.cfg.writer.clone(),
        None => WriterConfig::default(),
    };

    let db_batch = make_empty_batch::<TradeDBRow>(
        exchange,
        transport,
        kind,
        symbol_for_task.clone(),
        writer_cfg,
    )?;

    // Runtime knobs for redis / db
    let (knobs_tx, mut knobs_rx) =
        tokio::sync::watch::channel(crate::app::StreamKnobs::from_deps(deps.clone()));

    let batch = Arc::new(tokio::sync::Mutex::new(db_batch));

    let knobs_task = crate::app::spawn_knobs_batch_flush_task(
        Arc::clone(&batch),
        knobs_rx.clone(),
        cancel_for_task.clone(),
    );

    let task: JoinHandle<()> = tokio::spawn(async move {
        let deps = deps.clone();

        // ✅ create counter first
        let test_msg_counter = Arc::new(AtomicUsize::new(0));

        let span = tracing::info_span!(
            "stream.ws",
            exchange = exchange.as_str(),
            symbol = %symbol_for_task,
            kind = ?kind,
            transport = ?transport,
            stream_id = %stream_id_for_task,
        );
        let _enter = span.enter();

        // ------------------------------------------------------------
        // Resolve WS stream from exchange configs
        // ------------------------------------------------------------

        let deps_for_closure = deps.clone();

        // borrow configs from a local Arc, not from deps
        let exchange_cfgs = deps.exchange_cfgs.clone();
        let cfg = exchange_cfgs
            .binance_linear
            .as_ref()
            .expect("binance_linear config must exist");

        let stream = cfg
            .ws
            .get("trades")
            .expect("missing [ws.trades] in binance config");

        // Build WS ctx (minimum fields typically used by control templates)
        // Adjust keys if your templates expect different names.
        ctx.entry("symbol".to_lowercase().to_string())
            .or_insert_with(|| symbol_for_task.clone());
        ctx.entry("stream_id".to_string()).or_insert_with(|| req_id);

        // Optional limiter registry if you have one in deps; otherwise None
        // (adjust field name if your AppDeps differs)
        // if you have limiters, borrow from a local Arc, not deps
        let ws_limiters_arc = deps.ws_limiters.clone(); // Option<Arc<WsLimiterRegistry>>
        let ws_limiters = ws_limiters_arc.as_deref(); // Option<&WsLimiterRegistry>

        // before on_event
        let test_counter = Arc::new(AtomicUsize::new(0));
        // ------------------------------------------------------------
        // on_event: parse -> map_to_events -> redis/db (same logic)
        // ------------------------------------------------------------
        let mut on_event = move |ev: WsEvent| {
            // clone for this invocation (moved into async block)
            let test_counter = Arc::clone(&test_counter);

            let deps = deps_for_closure.clone();
            let cancel_for_item = cancel_for_test.clone();
            let knobs: StreamKnobs = *knobs_rx.borrow_and_update();
            let batch = Arc::clone(&batch);
            let map_ctx = Arc::clone(&map_ctx_for_task);
            let map_envelope = Arc::clone(&map_envelope_for_task);
            let deps = deps.clone();

            async move {
                let text = match ev {
                    WsEvent::Text(s) => s,
                    WsEvent::Binary(_) => return Ok(()), // ignore (or add gzip handling if needed)
                    WsEvent::Ping(_) | WsEvent::Pong(_) => return Ok(()),
                    WsEvent::Close(_) => return Ok(()),
                };

                let v: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => return Ok(()), // ignore non-json
                };

                // 1) Ignore subscribe/unsubscribe acks: {"result":null,"id":...}
                if v.get("result").is_some() && v.get("id").is_some() {
                    return Ok(());
                }

                // 2) Handle combined stream wrapper: {"stream": "...", "data": {...}}
                let payload = v.get("data").cloned().unwrap_or_else(|| v.clone());

                // 3) Only attempt aggTrade
                if payload.get("e").and_then(|x| x.as_str()) != Some("aggTrade") {
                    return Ok(());
                }

                // 4) Now typed deserialization is safe-ish
                let item: BinanceLinearWsAggTrade = serde_json::from_value(payload)
                    .map_err(|e| AppError::Internal(format!("ws trades deserialize error: {e}")))?;

                let events = item.map_to_events(&map_ctx, Some((*map_envelope).clone()))?;

                // 1) Convert to DB rows (no lock yet)
                let trade_db_rows: Vec<TradeDBRow> = events
                    .iter()
                    .filter_map(|e| match e {
                        MarketEvent::Trade(t) => Some(TradeDBRow::from(t.clone())),
                        _ => None,
                    })
                    .collect();

                // 2) Publish to redis (no lock)
                if !knobs.disable_redis_publishes {
                    for e in &events {
                        if let Some((kind, ex, sym, fields)) = e.as_redis_publish() {
                            let fields_ref = as_publish_fields(&fields);
                            deps.redis_publish(ex, sym, kind, &fields_ref).await?;
                        }
                    }
                }

                // 3) Now lock batch and extend + write
                if !knobs.disable_db_writes {
                    let mut guard = batch.lock().await;
                    guard.rows.extend(trade_db_rows);
                    deps.db_write((&mut *guard).into()).await?;
                }

                // 4) TEST ESCAPE HATCH
                // 4) TEST ESCAPE HATCH (after N processed messages)
                if std::env::var_os("APP_TEST_ONESHOT").is_some() {
                    let n = test_counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if n >= 5 {
                        println!("{:?}", events);
                        tracing::info!(
                            processed_messages = n,
                            "APP_TEST_ONESHOT set: cancelling stream after 5 processed messages"
                        );
                        cancel_for_item.cancel();
                    }
                }

                Ok::<(), AppError>(())
            }
        };

        // One call runs the reconnect/breaker loop internally.
        // Cancellation is handled by passing our stream cancel token through.
        if let Err(e) = ws_client
            .run_stream(
                ws_limiters,
                stream,
                ctx,
                &mut on_event,
                None, // no test hook in production
                Some(cancel_for_task.clone()),
            )
            .await
        {
            // Normal cancellations should end cleanly; log unexpected errors.
            if !cancel_for_task.is_cancelled() {
                tracing::warn!(error=?e, "ws stream exited with error");
            }
        }
    });

    // Build handle + register in state
    let handle = StreamHandle::new(
        stream_spec,
        stream_status,
        cancel,
        task,
        knobs_tx,
        vec![knobs_task],
    );

    runtime.state.insert(stream_id, handle).await?;
    Ok(())
}

pub async fn ws_binancelinear_depth(
    runtime: &AppRuntime,
    mut ctx: Ctx,
    map_ctx: MapCtx,
    map_envelope: MapEnvelope,
    stream_spec: StreamSpec,
    symbol: String,
    stream_id: StreamId,
) -> AppResult<()> {
    let exchange = ExchangeId::BinanceLinear;
    let transport = StreamTransport::Ws;
    let kind = StreamKind::L2Book;

    // --- clone owned handles BEFORE spawn (no &runtime inside task) ---
    let deps = runtime.deps.clone();

    // Grab shared deps as owned handles we can move into the task.
    let ws_client = runtime
        .deps
        .as_ref()
        .binance_linear_ws
        .clone()
        .ok_or_else(|| AppError::Disabled("Binance Linear exchange is disabled!".into()))?;

    // ---- Build ids/spec/status for registry ----
    let stream_id_for_task = stream_id.clone();
    let req_id = binance_ws_request_id(&stream_id_for_task.to_string());
    let stream_status = StreamStatus::Running;
    let cancel = CancellationToken::new();

    // Move values into the spawned task
    let symbol_for_task = symbol.clone();
    let map_envelope_for_task = Arc::new(map_envelope);
    let cancel_for_task = cancel.clone();
    let cancel_for_test = cancel.clone();
    let map_ctx_for_task = Arc::new(map_ctx);

    // Make a db batch
    let writer_cfg = match runtime.deps.as_ref().db.as_ref() {
        Some(db) => db.cfg.writer.clone(),
        None => WriterConfig::default(),
    };

    let db_batch = make_empty_batch::<DepthDeltaDBRow>(
        exchange,
        transport,
        kind,
        symbol_for_task.clone(),
        writer_cfg,
    )?;

    // Runtime knobs for redis / db
    let (knobs_tx, mut knobs_rx) =
        tokio::sync::watch::channel(crate::app::StreamKnobs::from_deps(deps.clone()));

    let batch = Arc::new(tokio::sync::Mutex::new(db_batch));

    let knobs_task = crate::app::spawn_knobs_batch_flush_task(
        Arc::clone(&batch),
        knobs_rx.clone(),
        cancel_for_task.clone(),
    );

    let task: JoinHandle<()> = tokio::spawn(async move {
        let deps = deps.clone();

        // ✅ create counter first
        let test_msg_counter = Arc::new(AtomicUsize::new(0));

        let span = tracing::info_span!(
            "stream.ws",
            exchange = exchange.as_str(),
            symbol = %symbol_for_task,
            kind = ?kind,
            transport = ?transport,
            stream_id = %stream_id_for_task,
        );
        let _enter = span.enter();

        // ------------------------------------------------------------
        // Resolve WS stream from exchange configs
        // ------------------------------------------------------------

        let deps_for_closure = deps.clone();

        // borrow configs from a local Arc, not from deps
        let exchange_cfgs = deps.exchange_cfgs.clone();
        let cfg = exchange_cfgs
            .binance_linear
            .as_ref()
            .expect("binance_linear config must exist");

        let stream = cfg
            .ws
            .get("depth_update")
            .expect("missing [ws.trades] in binance config");

        // Build WS ctx (minimum fields typically used by control templates)
        // Adjust keys if your templates expect different names.
        ctx.entry("symbol".to_lowercase().to_string())
            .or_insert_with(|| symbol_for_task.clone());
        ctx.entry("stream_id".to_string()).or_insert_with(|| req_id);

        // Optional limiter registry if you have one in deps; otherwise None
        // (adjust field name if your AppDeps differs)
        // if you have limiters, borrow from a local Arc, not deps
        let ws_limiters_arc = deps.ws_limiters.clone(); // Option<Arc<WsLimiterRegistry>>
        let ws_limiters = ws_limiters_arc.as_deref(); // Option<&WsLimiterRegistry>

        // before on_event
        let test_counter = Arc::new(AtomicUsize::new(0));
        // ------------------------------------------------------------
        // on_event: parse -> map_to_events -> redis/db (same logic)
        // ------------------------------------------------------------
        let mut on_event = move |ev: WsEvent| {
            // clone for this invocation (moved into async block)
            let test_counter = Arc::clone(&test_counter);

            let deps = deps_for_closure.clone();
            let cancel_for_item = cancel_for_test.clone();
            let knobs: StreamKnobs = *knobs_rx.borrow_and_update();
            let batch = Arc::clone(&batch);
            let map_ctx = Arc::clone(&map_ctx_for_task);
            let map_envelope = Arc::clone(&map_envelope_for_task);
            let deps = deps.clone();

            async move {
                let text = match ev {
                    WsEvent::Text(s) => s,
                    WsEvent::Binary(_) => return Ok(()), // ignore (or add gzip handling if needed)
                    WsEvent::Ping(_) | WsEvent::Pong(_) => return Ok(()),
                    WsEvent::Close(_) => return Ok(()),
                };

                let v: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => return Ok(()), // ignore non-json
                };

                // 1) Ignore subscribe/unsubscribe acks: {"result":null,"id":...}
                if v.get("result").is_some() && v.get("id").is_some() {
                    return Ok(());
                }

                // 2) Handle combined stream wrapper: {"stream": "...", "data": {...}}
                let payload = v.get("data").cloned().unwrap_or_else(|| v.clone());

                // 3) Only attempt Dept
                if payload.get("e").and_then(|x| x.as_str()) != Some("depthUpdate") {
                    return Ok(());
                }

                // 4) Now typed deserialization is safe-ish
                let item: BinanceLinearWsDepthUpdate =
                    serde_json::from_value(payload).map_err(|e| {
                        AppError::Internal(format!("ws depth update deserialize error: {e}"))
                    })?;

                let events = item.map_to_events(&map_ctx, Some((*map_envelope).clone()))?;

                // 1) Convert to DB rows (no lock yet)
                let trade_db_rows: Vec<DepthDeltaDBRow> = events
                    .iter()
                    .filter_map(|e| match e {
                        MarketEvent::DepthDelta(t) => Some(DepthDeltaDBRow::from(t.clone())),
                        _ => None,
                    })
                    .collect();

                // 2) Publish to redis (no lock)
                if !knobs.disable_redis_publishes {
                    for e in &events {
                        if let Some((kind, ex, sym, fields)) = e.as_redis_publish() {
                            let fields_ref = as_publish_fields(&fields);
                            deps.redis_publish(ex, sym, kind, &fields_ref).await?;
                        }
                    }
                }

                // 3) Now lock batch and extend + write
                if !knobs.disable_db_writes {
                    let mut guard = batch.lock().await;
                    guard.rows.extend(trade_db_rows);
                    deps.db_write((&mut *guard).into()).await?;
                }

                // 4) TEST ESCAPE HATCH
                // 4) TEST ESCAPE HATCH (after N processed messages)
                if std::env::var_os("APP_TEST_ONESHOT").is_some() {
                    let n = test_counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if n >= 5 {
                        println!("{:?}", events);
                        tracing::info!(
                            processed_messages = n,
                            "APP_TEST_ONESHOT set: cancelling stream after 5 processed messages"
                        );
                        cancel_for_item.cancel();
                    }
                }

                Ok::<(), AppError>(())
            }
        };

        // One call runs the reconnect/breaker loop internally.
        // Cancellation is handled by passing our stream cancel token through.
        if let Err(e) = ws_client
            .run_stream(
                ws_limiters,
                stream,
                ctx,
                &mut on_event,
                None, // no test hook in production
                Some(cancel_for_task.clone()),
            )
            .await
        {
            // Normal cancellations should end cleanly; log unexpected errors.
            if !cancel_for_task.is_cancelled() {
                tracing::warn!(error=?e, "ws stream exited with error");
            }
        }
    });

    // Build handle + register in state
    let handle = StreamHandle::new(
        stream_spec,
        stream_status,
        cancel,
        task,
        knobs_tx,
        vec![knobs_task],
    );

    runtime.state.insert(stream_id, handle).await?;
    Ok(())
}

pub async fn ws_binancelinear_liquidation(
    runtime: &AppRuntime,
    mut ctx: Ctx,
    map_ctx: MapCtx,
    map_envelope: MapEnvelope,
    stream_spec: StreamSpec,
    symbol: String,
    stream_id: StreamId,
) -> AppResult<()> {
    let exchange = ExchangeId::BinanceLinear;
    let transport = StreamTransport::Ws;
    let kind = StreamKind::Liquidations;

    // --- clone owned handles BEFORE spawn (no &runtime inside task) ---
    let deps = runtime.deps.clone();

    // Grab shared deps as owned handles we can move into the task.
    let ws_client = runtime
        .deps
        .as_ref()
        .binance_linear_ws
        .clone()
        .ok_or_else(|| AppError::Disabled("Binance Linear exchange is disabled!".into()))?;

    // ---- Build ids/spec/status for registry ----
    let stream_id_for_task = stream_id.clone();
    let req_id = binance_ws_request_id(&stream_id_for_task.to_string());
    let stream_status = StreamStatus::Running;
    let cancel = CancellationToken::new();

    // Move values into the spawned task
    let symbol_for_task = symbol.clone();
    let map_envelope_for_task = Arc::new(map_envelope);
    let cancel_for_task = cancel.clone();
    let cancel_for_test = cancel.clone();
    let map_ctx_for_task = Arc::new(map_ctx);

    // Make a db batch
    let writer_cfg = match runtime.deps.as_ref().db.as_ref() {
        Some(db) => db.cfg.writer.clone(),
        None => WriterConfig::default(),
    };

    let db_batch = make_empty_batch::<LiquidationDBRow>(
        exchange,
        transport,
        kind,
        symbol_for_task.clone(),
        writer_cfg,
    )?;

    // Runtime knobs for redis / db
    let (knobs_tx, mut knobs_rx) =
        tokio::sync::watch::channel(crate::app::StreamKnobs::from_deps(deps.clone()));

    let batch = Arc::new(tokio::sync::Mutex::new(db_batch));

    let knobs_task = crate::app::spawn_knobs_batch_flush_task(
        Arc::clone(&batch),
        knobs_rx.clone(),
        cancel_for_task.clone(),
    );

    let task: JoinHandle<()> = tokio::spawn(async move {
        let deps = deps.clone();

        // ✅ create counter first
        let test_msg_counter = Arc::new(AtomicUsize::new(0));

        let span = tracing::info_span!(
            "stream.ws",
            exchange = exchange.as_str(),
            symbol = %symbol_for_task,
            kind = ?kind,
            transport = ?transport,
            stream_id = %stream_id_for_task,
        );
        let _enter = span.enter();

        // ------------------------------------------------------------
        // Resolve WS stream from exchange configs
        // ------------------------------------------------------------

        let deps_for_closure = deps.clone();

        // borrow configs from a local Arc, not from deps
        let exchange_cfgs = deps.exchange_cfgs.clone();
        let cfg = exchange_cfgs
            .binance_linear
            .as_ref()
            .expect("binance_linear config must exist");

        let stream = cfg
            .ws
            .get("liquidations")
            .expect("missing [ws.liquidations] in binance config");

        // Build WS ctx (minimum fields typically used by control templates)
        // Adjust keys if your templates expect different names.
        ctx.entry("symbol".to_lowercase().to_string())
            .or_insert_with(|| symbol_for_task.clone());
        ctx.entry("stream_id".to_string()).or_insert_with(|| req_id);

        // Optional limiter registry if you have one in deps; otherwise None
        // (adjust field name if your AppDeps differs)
        // if you have limiters, borrow from a local Arc, not deps
        let ws_limiters_arc = deps.ws_limiters.clone(); // Option<Arc<WsLimiterRegistry>>
        let ws_limiters = ws_limiters_arc.as_deref(); // Option<&WsLimiterRegistry>

        // before on_event
        let test_counter = Arc::new(AtomicUsize::new(0));
        // ------------------------------------------------------------
        // on_event: parse -> map_to_events -> redis/db (same logic)
        // ------------------------------------------------------------
        let mut on_event = move |ev: WsEvent| {
            // clone for this invocation (moved into async block)
            let test_counter = Arc::clone(&test_counter);

            let deps = deps_for_closure.clone();
            let cancel_for_item = cancel_for_test.clone();
            let knobs: StreamKnobs = *knobs_rx.borrow_and_update();
            let batch = Arc::clone(&batch);
            let map_ctx = Arc::clone(&map_ctx_for_task);
            let map_envelope = Arc::clone(&map_envelope_for_task);
            let deps = deps.clone();

            async move {
                let text = match ev {
                    WsEvent::Text(s) => s,
                    WsEvent::Binary(_) => return Ok(()), // ignore (or add gzip handling if needed)
                    WsEvent::Ping(_) | WsEvent::Pong(_) => return Ok(()),
                    WsEvent::Close(_) => return Ok(()),
                };

                let v: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => return Ok(()), // ignore non-json
                };

                // 1) Ignore subscribe/unsubscribe acks: {"result":null,"id":...}
                if v.get("result").is_some() && v.get("id").is_some() {
                    return Ok(());
                }

                // 2) Handle combined stream wrapper: {"stream": "...", "data": {...}}
                let payload = v.get("data").cloned().unwrap_or_else(|| v.clone());

                // 3) Only attempt Dept
                if payload.get("e").and_then(|x| x.as_str()) != Some("forceOrder") {
                    return Ok(());
                }

                // 4) Now typed deserialization is safe-ish
                let item: BinanceLinearWsForceOrder =
                    serde_json::from_value(payload).map_err(|e| {
                        AppError::Internal(format!("ws force order deserialize error: {e}"))
                    })?;

                let events = item.map_to_events(&map_ctx, Some((*map_envelope).clone()))?;

                // 1) Convert to DB rows (no lock yet)
                let trade_db_rows: Vec<LiquidationDBRow> = events
                    .iter()
                    .filter_map(|e| match e {
                        MarketEvent::Liquidation(t) => Some(LiquidationDBRow::from(t.clone())),
                        _ => None,
                    })
                    .collect();

                // 2) Publish to redis (no lock)
                if !knobs.disable_redis_publishes {
                    for e in &events {
                        if let Some((kind, ex, sym, fields)) = e.as_redis_publish() {
                            let fields_ref = as_publish_fields(&fields);
                            deps.redis_publish(ex, sym, kind, &fields_ref).await?;
                        }
                    }
                }

                // 3) Now lock batch and extend + write
                if !knobs.disable_db_writes {
                    let mut guard = batch.lock().await;
                    guard.rows.extend(trade_db_rows);
                    deps.db_write((&mut *guard).into()).await?;
                }

                // 4) TEST ESCAPE HATCH
                // 4) TEST ESCAPE HATCH (after N processed messages)
                if std::env::var_os("APP_TEST_ONESHOT").is_some() {
                    let n = test_counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if n >= 5 {
                        println!("{:?}", events);
                        tracing::info!(
                            processed_messages = n,
                            "APP_TEST_ONESHOT set: cancelling stream after 5 processed messages"
                        );
                        cancel_for_item.cancel();
                    }
                }

                Ok::<(), AppError>(())
            }
        };

        // One call runs the reconnect/breaker loop internally.
        // Cancellation is handled by passing our stream cancel token through.
        if let Err(e) = ws_client
            .run_stream(
                ws_limiters,
                stream,
                ctx,
                &mut on_event,
                None, // no test hook in production
                Some(cancel_for_task.clone()),
            )
            .await
        {
            // Normal cancellations should end cleanly; log unexpected errors.
            if !cancel_for_task.is_cancelled() {
                tracing::warn!(error=?e, "ws stream exited with error");
            }
        }
    });

    // Build handle + register in state
    let handle = StreamHandle::new(
        stream_spec,
        stream_status,
        cancel,
        task,
        knobs_tx,
        vec![knobs_task],
    );

    runtime.state.insert(stream_id, handle).await?;
    Ok(())
}

pub async fn ws_hyperliquidperp_depth(
    runtime: &AppRuntime,
    mut ctx: Ctx,
    map_ctx: MapCtx,
    map_envelope: MapEnvelope,
    stream_spec: StreamSpec,
    symbol: String,
    stream_id: StreamId,
) -> AppResult<()> {
    let exchange = ExchangeId::HyperliquidPerp;
    let transport = StreamTransport::Ws;
    let kind = StreamKind::L2Book;

    // --- clone owned handles BEFORE spawn (no &runtime inside task) ---
    let deps = runtime.deps.clone();

    // Grab shared deps as owned handles we can move into the task.
    let ws_client = runtime
        .deps
        .as_ref()
        .hyperliquid_perp_ws
        .clone()
        .ok_or_else(|| AppError::Disabled("Hyperliquid Perp exchange is disabled!".into()))?;

    // ---- Build ids/spec/status for registry ----
    let stream_id_for_task = stream_id.clone();
    let stream_status = StreamStatus::Running;
    let cancel = CancellationToken::new();

    // Move values into the spawned task
    let symbol_for_task = symbol.clone();
    let map_envelope_for_task = Arc::new(map_envelope);
    let cancel_for_task = cancel.clone();
    let cancel_for_test = cancel.clone();
    let map_ctx_for_task = Arc::new(map_ctx);

    // Make a db batch
    let writer_cfg = match runtime.deps.as_ref().db.as_ref() {
        Some(db) => db.cfg.writer.clone(),
        None => WriterConfig::default(),
    };

    let db_batch = make_empty_batch::<DepthDeltaDBRow>(
        exchange,
        transport,
        kind,
        symbol_for_task.clone(),
        writer_cfg,
    )?;

    // Runtime knobs for redis / db
    let (knobs_tx, mut knobs_rx) =
        tokio::sync::watch::channel(crate::app::StreamKnobs::from_deps(deps.clone()));

    let batch = Arc::new(tokio::sync::Mutex::new(db_batch));

    let knobs_task = crate::app::spawn_knobs_batch_flush_task(
        Arc::clone(&batch),
        knobs_rx.clone(),
        cancel_for_task.clone(),
    );

    let task: JoinHandle<()> = tokio::spawn(async move {
        let deps = deps.clone();

        // ✅ create counter first
        let test_msg_counter = Arc::new(AtomicUsize::new(0));

        let span = tracing::info_span!(
            "stream.ws",
            exchange = exchange.as_str(),
            symbol = %symbol_for_task,
            kind = ?kind,
            transport = ?transport,
            stream_id = %stream_id_for_task,
        );
        let _enter = span.enter();

        // ------------------------------------------------------------
        // Resolve WS stream from exchange configs
        // ------------------------------------------------------------

        let deps_for_closure = deps.clone();

        // borrow configs from a local Arc, not from deps
        let exchange_cfgs = deps.exchange_cfgs.clone();
        let cfg = exchange_cfgs
            .hyperliquid_perp
            .as_ref()
            .expect("hyperliquid_perp config must exist");

        let stream = cfg
            .ws
            .get("depth_update")
            .expect("missing [ws.depth_update] in hyperliquid perp config");

        // Build WS ctx (minimum fields typically used by control templates)
        // Adjust keys if your templates expect different names.
        ctx.entry("coin".to_string())
            .or_insert_with(|| symbol_for_task.clone());

        // Optional limiter registry if you have one in deps; otherwise None
        // (adjust field name if your AppDeps differs)
        // if you have limiters, borrow from a local Arc, not deps
        let ws_limiters_arc = deps.ws_limiters.clone(); // Option<Arc<WsLimiterRegistry>>
        let ws_limiters = ws_limiters_arc.as_deref(); // Option<&WsLimiterRegistry>

        // before on_event
        let test_counter = Arc::new(AtomicUsize::new(0));
        // ------------------------------------------------------------
        // on_event: parse -> map_to_events -> redis/db (same logic)
        // ------------------------------------------------------------
        let mut on_event = move |ev: WsEvent| {
            // clone for this invocation (moved into async block)
            let test_counter = Arc::clone(&test_counter);

            let deps = deps_for_closure.clone();
            let cancel_for_item = cancel_for_test.clone();
            let knobs: StreamKnobs = *knobs_rx.borrow_and_update();
            let batch = Arc::clone(&batch);
            let map_ctx = Arc::clone(&map_ctx_for_task);
            let map_envelope = Arc::clone(&map_envelope_for_task);
            let deps = deps.clone();

            async move {
                let text = match ev {
                    WsEvent::Text(s) => s,
                    WsEvent::Binary(_) => return Ok(()), // ignore (or add gzip handling if needed)
                    WsEvent::Ping(_) | WsEvent::Pong(_) => return Ok(()),
                    WsEvent::Close(_) => return Ok(()),
                };

                let v: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => return Ok(()), // ignore non-json
                };

                // 1) Ignore subscription response (and any non-l2Book channels)
                let channel = match v.get("channel").and_then(|x| x.as_str()) {
                    Some(c) => c,
                    None => return Ok(()),
                };

                if channel != "l2Book" {
                    return Ok(());
                }

                // 3) Now typed deserialization is safe-ish
                let item: HyperliquidPerpWsDepthUpdate =
                    serde_json::from_value(v).map_err(|e| {
                        AppError::Internal(format!("ws force order deserialize error: {e}"))
                    })?;

                let events = item.map_to_events(&map_ctx, Some((*map_envelope).clone()))?;

                // 1) Convert to DB rows (no lock yet)
                let trade_db_rows: Vec<DepthDeltaDBRow> = events
                    .iter()
                    .filter_map(|e| match e {
                        MarketEvent::DepthDelta(t) => Some(DepthDeltaDBRow::from(t.clone())),
                        _ => None,
                    })
                    .collect();

                // 2) Publish to redis (no lock)
                if !knobs.disable_redis_publishes {
                    for e in &events {
                        if let Some((kind, ex, sym, fields)) = e.as_redis_publish() {
                            let fields_ref = as_publish_fields(&fields);
                            deps.redis_publish(ex, sym, kind, &fields_ref).await?;
                        }
                    }
                }

                // 3) Now lock batch and extend + write
                if !knobs.disable_db_writes {
                    let mut guard = batch.lock().await;
                    guard.rows.extend(trade_db_rows);
                    deps.db_write((&mut *guard).into()).await?;
                }

                // 4) TEST ESCAPE HATCH
                // 4) TEST ESCAPE HATCH (after N processed messages)
                if std::env::var_os("APP_TEST_ONESHOT").is_some() {
                    let n = test_counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if n >= 5 {
                        println!("{:?}", events);
                        tracing::info!(
                            processed_messages = n,
                            "APP_TEST_ONESHOT set: cancelling stream after 5 processed messages"
                        );
                        cancel_for_item.cancel();
                    }
                }

                Ok::<(), AppError>(())
            }
        };

        // One call runs the reconnect/breaker loop internally.
        // Cancellation is handled by passing our stream cancel token through.
        if let Err(e) = ws_client
            .run_stream(
                ws_limiters,
                stream,
                ctx,
                &mut on_event,
                None, // no test hook in production
                Some(cancel_for_task.clone()),
            )
            .await
        {
            // Normal cancellations should end cleanly; log unexpected errors.
            if !cancel_for_task.is_cancelled() {
                tracing::warn!(error=?e, "ws stream exited with error");
            }
        }
    });

    // Build handle + register in state
    let handle = StreamHandle::new(
        stream_spec,
        stream_status,
        cancel,
        task,
        knobs_tx,
        vec![knobs_task],
    );

    runtime.state.insert(stream_id, handle).await?;
    Ok(())
}

pub async fn ws_hyperliquidperp_trades(
    runtime: &AppRuntime,
    mut ctx: Ctx,
    map_ctx: MapCtx,
    map_envelope: MapEnvelope,
    stream_spec: StreamSpec,
    symbol: String,
    stream_id: StreamId,
) -> AppResult<()> {
    let exchange = ExchangeId::HyperliquidPerp;
    let transport = StreamTransport::Ws;
    let kind = StreamKind::Trades;

    // --- clone owned handles BEFORE spawn (no &runtime inside task) ---
    let deps = runtime.deps.clone();

    // Grab shared deps as owned handles we can move into the task.
    let ws_client = runtime
        .deps
        .as_ref()
        .hyperliquid_perp_ws
        .clone()
        .ok_or_else(|| AppError::Disabled("Hyperliquid Perp exchange is disabled!".into()))?;

    // ---- Build ids/spec/status for registry ----
    let stream_id_for_task = stream_id.clone();
    let stream_status = StreamStatus::Running;
    let cancel = CancellationToken::new();

    // Move values into the spawned task
    let symbol_for_task = symbol.clone();
    let map_envelope_for_task = Arc::new(map_envelope);
    let cancel_for_task = cancel.clone();
    let cancel_for_test = cancel.clone();
    let map_ctx_for_task = Arc::new(map_ctx);

    // Make a db batch
    let writer_cfg = match runtime.deps.as_ref().db.as_ref() {
        Some(db) => db.cfg.writer.clone(),
        None => WriterConfig::default(),
    };

    let db_batch = make_empty_batch::<TradeDBRow>(
        exchange,
        transport,
        kind,
        symbol_for_task.clone(),
        writer_cfg,
    )?;

    // Runtime knobs for redis / db
    let (knobs_tx, mut knobs_rx) =
        tokio::sync::watch::channel(crate::app::StreamKnobs::from_deps(deps.clone()));

    let batch = Arc::new(tokio::sync::Mutex::new(db_batch));

    let knobs_task = crate::app::spawn_knobs_batch_flush_task(
        Arc::clone(&batch),
        knobs_rx.clone(),
        cancel_for_task.clone(),
    );

    let task: JoinHandle<()> = tokio::spawn(async move {
        let deps = deps.clone();

        // ✅ create counter first
        let test_msg_counter = Arc::new(AtomicUsize::new(0));

        let span = tracing::info_span!(
            "stream.ws",
            exchange = exchange.as_str(),
            symbol = %symbol_for_task,
            kind = ?kind,
            transport = ?transport,
            stream_id = %stream_id_for_task,
        );
        let _enter = span.enter();

        // ------------------------------------------------------------
        // Resolve WS stream from exchange configs
        // ------------------------------------------------------------

        let deps_for_closure = deps.clone();

        // borrow configs from a local Arc, not from deps
        let exchange_cfgs = deps.exchange_cfgs.clone();
        let cfg = exchange_cfgs
            .hyperliquid_perp
            .as_ref()
            .expect("hyperliquid_perp config must exist");

        let stream = cfg
            .ws
            .get("trades")
            .expect("missing [ws.trades] in hyperliquid perp config");

        // Build WS ctx (minimum fields typically used by control templates)
        // Adjust keys if your templates expect different names.
        ctx.entry("coin".to_string())
            .or_insert_with(|| symbol_for_task.clone());

        // Optional limiter registry if you have one in deps; otherwise None
        // (adjust field name if your AppDeps differs)
        // if you have limiters, borrow from a local Arc, not deps
        let ws_limiters_arc = deps.ws_limiters.clone(); // Option<Arc<WsLimiterRegistry>>
        let ws_limiters = ws_limiters_arc.as_deref(); // Option<&WsLimiterRegistry>

        // before on_event
        let test_counter = Arc::new(AtomicUsize::new(0));
        // ------------------------------------------------------------
        // on_event: parse -> map_to_events -> redis/db (same logic)
        // ------------------------------------------------------------
        let mut on_event = move |ev: WsEvent| {
            // clone for this invocation (moved into async block)
            let test_counter = Arc::clone(&test_counter);

            let deps = deps_for_closure.clone();
            let cancel_for_item = cancel_for_test.clone();
            let knobs: StreamKnobs = *knobs_rx.borrow_and_update();
            let batch = Arc::clone(&batch);
            let map_ctx = Arc::clone(&map_ctx_for_task);
            let map_envelope = Arc::clone(&map_envelope_for_task);
            let deps = deps.clone();

            async move {
                let text = match ev {
                    WsEvent::Text(s) => s,
                    WsEvent::Binary(_) => return Ok(()), // ignore (or add gzip handling if needed)
                    WsEvent::Ping(_) | WsEvent::Pong(_) => return Ok(()),
                    WsEvent::Close(_) => return Ok(()),
                };

                let v: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => return Ok(()), // ignore non-json
                };

                // 1) Ignore subscription response (and any non-l2Book channels)
                let channel = match v.get("channel").and_then(|x| x.as_str()) {
                    Some(c) => c,
                    None => return Ok(()),
                };

                if channel != "trades" {
                    return Ok(());
                }

                // 3) Now typed deserialization is safe-ish
                let item: HyperliquidPerpWsTrade = serde_json::from_value(v)
                    .map_err(|e| AppError::Internal(format!("ws trades deserialize error: {e}")))?;

                let events = item.map_to_events(&map_ctx, Some((*map_envelope).clone()))?;

                // 1) Convert to DB rows (no lock yet)
                let trade_db_rows: Vec<TradeDBRow> = events
                    .iter()
                    .filter_map(|e| match e {
                        MarketEvent::Trade(t) => Some(TradeDBRow::from(t.clone())),
                        _ => None,
                    })
                    .collect();

                // 2) Publish to redis (no lock)
                if !knobs.disable_redis_publishes {
                    for e in &events {
                        if let Some((kind, ex, sym, fields)) = e.as_redis_publish() {
                            let fields_ref = as_publish_fields(&fields);
                            deps.redis_publish(ex, sym, kind, &fields_ref).await?;
                        }
                    }
                }

                // 3) Now lock batch and extend + write
                if !knobs.disable_db_writes {
                    let mut guard = batch.lock().await;
                    guard.rows.extend(trade_db_rows);
                    deps.db_write((&mut *guard).into()).await?;
                }

                // 4) TEST ESCAPE HATCH
                // 4) TEST ESCAPE HATCH (after N processed messages)
                if std::env::var_os("APP_TEST_ONESHOT").is_some() {
                    let n = test_counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if n >= 5 {
                        println!("{:?}", events);
                        tracing::info!(
                            processed_messages = n,
                            "APP_TEST_ONESHOT set: cancelling stream after 5 processed messages"
                        );
                        cancel_for_item.cancel();
                    }
                }

                Ok::<(), AppError>(())
            }
        };

        // One call runs the reconnect/breaker loop internally.
        // Cancellation is handled by passing our stream cancel token through.
        if let Err(e) = ws_client
            .run_stream(
                ws_limiters,
                stream,
                ctx,
                &mut on_event,
                None, // no test hook in production
                Some(cancel_for_task.clone()),
            )
            .await
        {
            // Normal cancellations should end cleanly; log unexpected errors.
            if !cancel_for_task.is_cancelled() {
                tracing::warn!(error=?e, "ws stream exited with error");
            }
        }
    });

    // Build handle + register in state
    let handle = StreamHandle::new(
        stream_spec,
        stream_status,
        cancel,
        task,
        knobs_tx,
        vec![knobs_task],
    );

    runtime.state.insert(stream_id, handle).await?;
    Ok(())
}

pub async fn ws_hyperliquidperp_oifunding(
    runtime: &AppRuntime,
    mut ctx: Ctx,
    map_ctx: MapCtx,
    map_envelope: MapEnvelope,
    stream_spec: StreamSpec,
    symbol: String,
    stream_id: StreamId,
) -> AppResult<()> {
    let exchange = ExchangeId::HyperliquidPerp;
    let transport = StreamTransport::Ws;
    let kind = StreamKind::FundingOpenInterest;

    // --- clone owned handles BEFORE spawn (no &runtime inside task) ---
    let deps = runtime.deps.clone();

    // Grab shared deps as owned handles we can move into the task.
    let ws_client = runtime
        .deps
        .as_ref()
        .hyperliquid_perp_ws
        .clone()
        .ok_or_else(|| AppError::Disabled("Hyperliquid Perp exchange is disabled!".into()))?;

    // ---- Build ids/spec/status for registry ----
    let stream_id_for_task = stream_id.clone();
    let stream_status = StreamStatus::Running;
    let cancel = CancellationToken::new();

    // Move values into the spawned task
    let symbol_for_task = symbol.clone();
    let map_envelope_for_task = Arc::new(map_envelope);
    let cancel_for_task = cancel.clone();
    let cancel_for_test = cancel.clone();
    let map_ctx_for_task = Arc::new(map_ctx);

    // Make a db batch
    let writer_cfg = match runtime.deps.as_ref().db.as_ref() {
        Some(db) => db.cfg.writer.clone(),
        None => WriterConfig::default(),
    };

    let db_batch_oi = make_empty_batch::<OpenInterestDBRow>(
        exchange,
        transport,
        kind,
        symbol_for_task.clone(),
        writer_cfg.clone(),
    )?;
    let db_batch_funding = make_empty_batch::<FundingDBRow>(
        exchange,
        transport,
        kind,
        symbol_for_task.clone(),
        writer_cfg,
    )?;

    // Runtime knobs for redis / db
    let (knobs_tx, mut knobs_rx) =
        tokio::sync::watch::channel(crate::app::StreamKnobs::from_deps(deps.clone()));

    let batch_oi = Arc::new(tokio::sync::Mutex::new(db_batch_oi));
    let batch_funding = Arc::new(tokio::sync::Mutex::new(db_batch_funding));

    let knobs_task_oi = crate::app::spawn_knobs_batch_flush_task(
        Arc::clone(&batch_oi),
        knobs_rx.clone(),
        cancel_for_task.clone(),
    );

    let knobs_task_funding = crate::app::spawn_knobs_batch_flush_task(
        Arc::clone(&batch_funding),
        knobs_rx.clone(),
        cancel_for_task.clone(),
    );

    let task: JoinHandle<()> = tokio::spawn(async move {
        let deps = deps.clone();

        // ✅ create counter first
        let test_msg_counter = Arc::new(AtomicUsize::new(0));

        let span = tracing::info_span!(
            "stream.ws",
            exchange = exchange.as_str(),
            symbol = %symbol_for_task,
            kind = ?kind,
            transport = ?transport,
            stream_id = %stream_id_for_task,
        );
        let _enter = span.enter();

        // ------------------------------------------------------------
        // Resolve WS stream from exchange configs
        // ------------------------------------------------------------

        let deps_for_closure = deps.clone();

        // borrow configs from a local Arc, not from deps
        let exchange_cfgs = deps.exchange_cfgs.clone();
        let cfg = exchange_cfgs
            .hyperliquid_perp
            .as_ref()
            .expect("hyperliquid_perp config must exist");

        let stream = cfg
            .ws
            .get("oi_funding")
            .expect("missing [ws.oi_funding] in hyperliquid perp config");

        // Build WS ctx (minimum fields typically used by control templates)
        // Adjust keys if your templates expect different names.
        ctx.entry("coin".to_string())
            .or_insert_with(|| symbol_for_task.clone());

        // Optional limiter registry if you have one in deps; otherwise None
        // (adjust field name if your AppDeps differs)
        // if you have limiters, borrow from a local Arc, not deps
        let ws_limiters_arc = deps.ws_limiters.clone(); // Option<Arc<WsLimiterRegistry>>
        let ws_limiters = ws_limiters_arc.as_deref(); // Option<&WsLimiterRegistry>

        // before on_event
        let test_counter = Arc::new(AtomicUsize::new(0));
        // ------------------------------------------------------------
        // on_event: parse -> map_to_events -> redis/db (same logic)
        // ------------------------------------------------------------
        let mut on_event = move |ev: WsEvent| {
            // clone for this invocation (moved into async block)
            let test_counter = Arc::clone(&test_counter);

            let deps = deps_for_closure.clone();
            let cancel_for_item = cancel_for_test.clone();
            let knobs: StreamKnobs = *knobs_rx.borrow_and_update();
            let batch_oi = Arc::clone(&batch_oi);
            let batch_funding = Arc::clone(&batch_funding);
            let map_ctx = Arc::clone(&map_ctx_for_task);
            let map_envelope = Arc::clone(&map_envelope_for_task);
            let deps = deps.clone();

            async move {
                let text = match ev {
                    WsEvent::Text(s) => s,
                    WsEvent::Binary(_) => return Ok(()), // ignore (or add gzip handling if needed)
                    WsEvent::Ping(_) | WsEvent::Pong(_) => return Ok(()),
                    WsEvent::Close(_) => return Ok(()),
                };

                let v: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => return Ok(()), // ignore non-json
                };

                // 1) Ignore subscription response (and any non-l2Book channels)
                let channel = match v.get("channel").and_then(|x| x.as_str()) {
                    Some(c) => c,
                    None => return Ok(()),
                };

                if channel != "activeAssetCtx" {
                    return Ok(());
                }

                // 3) Now typed deserialization is safe-ish
                let item: HyperliquidPerpWsOIFundingUpdate =
                    serde_json::from_value(v).map_err(|e| {
                        AppError::Internal(format!("ws oi_funding deserialize error: {e}"))
                    })?;

                let events = item.map_to_events(&map_ctx, Some((*map_envelope).clone()))?;

                // 1) Convert to DB rows (no lock yet)
                let oi_db_rows: Vec<OpenInterestDBRow> = events
                    .iter()
                    .filter_map(|e| match e {
                        MarketEvent::OpenInterest(t) => Some(OpenInterestDBRow::from(t.clone())),
                        _ => None,
                    })
                    .collect();

                // 1) Convert to DB rows (no lock yet)
                let funding_db_rows: Vec<FundingDBRow> = events
                    .iter()
                    .filter_map(|e| match e {
                        MarketEvent::Funding(t) => Some(FundingDBRow::from(t.clone())),
                        _ => None,
                    })
                    .collect();

                // 2) Publish to redis (no lock)
                if !knobs.disable_redis_publishes {
                    for e in &events {
                        // Force stream kind based on the event variant (single responsibility)
                        let forced_stream_kind = match e {
                            MarketEvent::OpenInterest(_) => RedisStreamKind::OpenInterest,
                            MarketEvent::Funding(_) => RedisStreamKind::Funding,
                            _ => continue,
                        };

                        // Use the payload from as_redis_publish(), but ignore its stream kind
                        if let Some((kind, ex, sym, fields)) = e.as_redis_publish() {
                            let fields_ref = as_publish_fields(&fields);
                            deps.redis_publish(ex, sym, forced_stream_kind, &fields_ref)
                                .await?;
                        }
                    }
                }

                // 3) Now lock batch and extend + write
                if !knobs.disable_db_writes {
                    let mut guard_oi = batch_oi.lock().await;
                    let mut guard_funding = batch_funding.lock().await;
                    guard_oi.rows.extend(oi_db_rows);
                    guard_funding.rows.extend(funding_db_rows);
                    deps.db_write((&mut *guard_oi).into()).await?;
                    deps.db_write((&mut *guard_funding).into()).await?;
                }

                // 4) TEST ESCAPE HATCH
                // 4) TEST ESCAPE HATCH (after N processed messages)
                if std::env::var_os("APP_TEST_ONESHOT").is_some() {
                    let n = test_counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if n >= 5 {
                        println!("{:?}", events);
                        tracing::info!(
                            processed_messages = n,
                            "APP_TEST_ONESHOT set: cancelling stream after 5 processed messages"
                        );
                        cancel_for_item.cancel();
                    }
                }

                Ok::<(), AppError>(())
            }
        };

        // One call runs the reconnect/breaker loop internally.
        // Cancellation is handled by passing our stream cancel token through.
        if let Err(e) = ws_client
            .run_stream(
                ws_limiters,
                stream,
                ctx,
                &mut on_event,
                None, // no test hook in production
                Some(cancel_for_task.clone()),
            )
            .await
        {
            // Normal cancellations should end cleanly; log unexpected errors.
            if !cancel_for_task.is_cancelled() {
                tracing::warn!(error=?e, "ws stream exited with error");
            }
        }
    });

    // Build handle + register in state
    let handle = StreamHandle::new(
        stream_spec,
        stream_status,
        cancel,
        task,
        knobs_tx,
        vec![knobs_task_oi, knobs_task_funding],
    );

    runtime.state.insert(stream_id, handle).await?;
    Ok(())
}
