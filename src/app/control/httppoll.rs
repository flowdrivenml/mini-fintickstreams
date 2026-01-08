use crate::app::control::batch::make_empty_batch;
use crate::app::runtime::AppRuntime;
use crate::app::state::StreamHandle;
use crate::app::state::StreamKnobs;
use crate::app::stream_types::{ExchangeId, StreamKind, StreamTransport};
use crate::app::stream_types::{StreamId, StreamSpec, StreamStatus};
use crate::db::WriterConfig;
use crate::db::rows::{DepthDeltaDBRow, FundingDBRow, OpenInterestDBRow};
use crate::error::{AppError, AppResult};
use crate::ingest::datamap::ctx::MapCtx;
use crate::ingest::datamap::event::MapEnvelope;
use crate::ingest::datamap::event::MarketEvent;
use crate::ingest::datamap::sources::binance_linear::types::{
    BinanceLinearDepthSnapshot, BinanceLinearFundingRateSnapshot, BinanceLinearOpenInterestSnapshot,
};
use crate::ingest::datamap::sources::hyperliquid_perp::types::HyperliquidPerpDepthSnapshot;
use crate::ingest::spec::types::HttpRequestSpec;
use crate::ingest::traits::MapToEvents;
use crate::redis::fields::as_publish_fields;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing;

pub async fn http_poll_binancelinear_oi(
    runtime: &AppRuntime,
    http_spec: HttpRequestSpec,
    map_ctx: MapCtx,
    map_envelope: MapEnvelope,
    stream_spec: StreamSpec,
    symbol: String,
    stream_id: StreamId,
) -> AppResult<()> {
    let exchange = ExchangeId::BinanceLinear;
    let transport = StreamTransport::HttpPoll;
    let kind = StreamKind::OpenInterest;

    // --- clone owned handles BEFORE spawn (no &runtime inside task) ---
    let deps = runtime.deps.clone();

    // Grab shared deps as owned handles we can move into the task.
    let api_client = runtime
        .deps
        .as_ref()
        .binance_linear_client
        .clone()
        .ok_or_else(|| AppError::Disabled("Binance Linear exchange is disabled!".into()))?;

    // ---- Build ids/spec/status for registry ----
    // You need to adapt these to your actual definitions.
    // let stream_id = StreamId::new(exchange.as_str(), symbol.as_str(), kind, transport);
    let stream_id_for_task = stream_id.clone();
    let stream_status = StreamStatus::Running;
    let cancel = CancellationToken::new();

    // Move values into the spawned task
    let symbol_for_task = symbol.clone();
    let http_spec_for_task = http_spec.clone();
    let map_envelope_for_task = Arc::new(map_envelope);
    let cancel_for_task = cancel.clone(); // you already have this
    let cancel_for_test = cancel.clone();
    let map_ctx_for_task = Arc::new(map_ctx);

    // Make a db batch
    let writer_cfg = match runtime.deps.as_ref().db.as_ref() {
        Some(db) => db.cfg.writer.clone(),
        None => WriterConfig::default(),
    };
    let db_batch = make_empty_batch::<OpenInterestDBRow>(
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
        knobs_rx.clone(),        // clone receiver for the watcher
        cancel_for_task.clone(), // same stream cancel token
    );

    let task: JoinHandle<()> = tokio::spawn(async move {
        // Start with an empty batch (owned by the task)

        let deps = deps.clone();

        // optional: create a useful tracing span for this stream
        let span = tracing::info_span!(
            "stream.http_poll",
            exchange = exchange.as_str(),
            symbol = %symbol_for_task,
            kind = ?kind,
            transport = ?transport,
            stream_id = %stream_id_for_task,
        );

        let _enter = span.enter();

        // Backoff for retrying poll errors
        let mut backoff = Duration::from_millis(250);
        let max_backoff = Duration::from_secs(10);

        // Define on_item; it captures &mut batch for each call
        let mut on_item = move |item: BinanceLinearOpenInterestSnapshot| {
            let cancel_for_item = cancel_for_test.clone();
            {
                let knobs: StreamKnobs = *knobs_rx.borrow_and_update();
                let batch = Arc::clone(&batch);
                let map_ctx = Arc::clone(&map_ctx_for_task);
                let map_envelope = Arc::clone(&map_envelope_for_task);
                let deps = deps.clone();

                async move {
                    // let res = BinanceLinearOpenInterestSnapshot::from_json_str(item.as_str())?;
                    let events = item.map_to_events(&map_ctx, Some((*map_envelope).clone()))?;

                    // 1) Convert to DB rows (no lock yet)
                    let oi_db_rows: Vec<OpenInterestDBRow> = events
                        .iter()
                        .filter_map(|e| match e {
                            MarketEvent::OpenInterest(oi) => {
                                Some(OpenInterestDBRow::from(oi.clone()))
                            }
                            _ => None,
                        })
                        .collect();

                    // 2) Publish to redis (no lock). If you want "latest only", publish only last().
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
                        guard.rows.extend(oi_db_rows);
                        deps.db_write((&mut *guard).into()).await?;
                    }

                    // 4) TEST ESCAPE HATCH
                    // Testing weather everything works fine
                    if std::env::var_os("APP_TEST_ONESHOT").is_some() {
                        println!("{:?}", events);
                        tracing::info!(
                            "APP_TEST_ONESHOT set: cancelling stream after first processed item"
                        );
                        cancel_for_item.cancel();
                    }
                    Ok::<(), AppError>(())
                }
            }
        };

        loop {
            tokio::select! {
                // graceful cancel: exit loop
                _ = cancel_for_task.cancelled() => {
                    tracing::info!("stream cancellation requested");
                    break;
                }

                // run the poll loop; if it returns, decide whether to retry
                res = api_client.poll_json_spec::<BinanceLinearOpenInterestSnapshot, _, _>(http_spec_for_task.clone(), &mut on_item) => {
                    match res {
                        Ok(()) => {
                            // For a "stream", returning Ok usually means "ended unexpectedly".
                            // We restart with a small backoff to avoid hot looping.
                            tracing::warn!("poll_json_spec ended (Ok); restarting after backoff");
                        }
                        Err(e) => {
                            // transient-ish by default; retry with backoff.
                            // If you want fatal classification, do it here.
                            tracing::warn!(error=?e, "poll_json_spec exited with error; retrying after backoff");
                        }
                    }

                    tokio::select! {
                        _ = cancel_for_task.cancelled() => {
                            tracing::info!("stream cancellation requested during backoff");
                            break;
                        }
                        _ = tokio::time::sleep(backoff) => {}
                    }

                    // exponential backoff with cap
                    backoff = std::cmp::min(max_backoff, backoff * 2);
                    continue;
                }
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

pub async fn http_poll_binancelinear_funding(
    runtime: &AppRuntime,
    http_spec: HttpRequestSpec,
    map_ctx: MapCtx,
    map_envelope: MapEnvelope,
    stream_spec: StreamSpec,
    symbol: String,
    stream_id: StreamId,
) -> AppResult<()> {
    let exchange = ExchangeId::BinanceLinear;
    let transport = StreamTransport::HttpPoll;
    let kind = StreamKind::Funding;

    // --- clone owned handles BEFORE spawn (no &runtime inside task) ---
    let deps = runtime.deps.clone();

    // Grab shared deps as owned handles we can move into the task.
    let api_client = runtime
        .deps
        .as_ref()
        .binance_linear_client
        .clone()
        .ok_or_else(|| AppError::Disabled("Binance Linear exchange is disabled!".into()))?;

    // ---- Build ids/spec/status for registry ----
    // You need to adapt these to your actual definitions.
    // let stream_id = StreamId::new(exchange.as_str(), symbol.as_str(), kind, transport);
    let stream_id_for_task = stream_id.clone();
    let stream_status = StreamStatus::Running;
    let cancel = CancellationToken::new();

    // Move values into the spawned task
    let symbol_for_task = symbol.clone();
    let http_spec_for_task = http_spec.clone();
    let map_envelope_for_task = Arc::new(map_envelope);
    let cancel_for_task = cancel.clone(); // you already have this
    let cancel_for_test = cancel.clone();
    let map_ctx_for_task = Arc::new(map_ctx);

    // Make a db batch
    let writer_cfg = match runtime.deps.as_ref().db.as_ref() {
        Some(db) => db.cfg.writer.clone(),
        None => WriterConfig::default(),
    };
    let db_batch = make_empty_batch::<FundingDBRow>(
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
        knobs_rx.clone(),        // clone receiver for the watcher
        cancel_for_task.clone(), // same stream cancel token
    );

    let task: JoinHandle<()> = tokio::spawn(async move {
        // Start with an empty batch (owned by the task)

        let deps = deps.clone();

        // optional: create a useful tracing span for this stream
        let span = tracing::info_span!(
            "stream.http_poll",
            exchange = exchange.as_str(),
            symbol = %symbol_for_task,
            kind = ?kind,
            transport = ?transport,
            stream_id = %stream_id_for_task,
        );

        let _enter = span.enter();

        // Backoff for retrying poll errors
        let mut backoff = Duration::from_millis(250);
        let max_backoff = Duration::from_secs(10);

        // Define on_item; it captures &mut batch for each call
        let mut on_item = move |item: Vec<BinanceLinearFundingRateSnapshot>| {
            let cancel_for_item = cancel_for_test.clone();
            {
                let knobs: StreamKnobs = *knobs_rx.borrow_and_update();
                let batch = Arc::clone(&batch);
                let map_ctx = Arc::clone(&map_ctx_for_task);
                let map_envelope = Arc::clone(&map_envelope_for_task);
                let deps = deps.clone();

                async move {
                    // let res = BinanceLinearOpenInterestSnapshot::from_json_str(item.as_str())?;
                    let events = item.map_to_events(&map_ctx, Some((*map_envelope).clone()))?;

                    // 1) Convert to DB rows (no lock yet)
                    let funding_db_rows: Vec<FundingDBRow> = events
                        .iter()
                        .filter_map(|e| match e {
                            MarketEvent::Funding(funding) => {
                                Some(FundingDBRow::from(funding.clone()))
                            }
                            _ => None,
                        })
                        .collect();

                    // 2) Publish to redis (no lock). If you want "latest only", publish only last().
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
                        guard.rows.extend(funding_db_rows);
                        deps.db_write((&mut *guard).into()).await?;
                    }

                    // 4) TEST ESCAPE HATCH
                    // Testing weather everything works fine
                    if std::env::var_os("APP_TEST_ONESHOT").is_some() {
                        println!("{:?}", events);
                        tracing::info!(
                            "APP_TEST_ONESHOT set: cancelling stream after first processed item"
                        );
                        cancel_for_item.cancel();
                    }
                    Ok::<(), AppError>(())
                }
            }
        };

        loop {
            tokio::select! {
                // graceful cancel: exit loop
                _ = cancel_for_task.cancelled() => {
                    tracing::info!("stream cancellation requested");
                    break;
                }

                // run the poll loop; if it returns, decide whether to retry
                res = api_client.poll_json_spec::<Vec<BinanceLinearFundingRateSnapshot>, _, _>(http_spec_for_task.clone(), &mut on_item) => {
                    match res {
                        Ok(()) => {
                            // For a "stream", returning Ok usually means "ended unexpectedly".
                            // We restart with a small backoff to avoid hot looping.
                            tracing::warn!("poll_json_spec ended (Ok); restarting after backoff");
                        }
                        Err(e) => {
                            // transient-ish by default; retry with backoff.
                            // If you want fatal classification, do it here.
                            tracing::warn!(error=?e, "poll_json_spec exited with error; retrying after backoff");
                        }
                    }

                    tokio::select! {
                        _ = cancel_for_task.cancelled() => {
                            tracing::info!("stream cancellation requested during backoff");
                            break;
                        }
                        _ = tokio::time::sleep(backoff) => {}
                    }

                    // exponential backoff with cap
                    backoff = std::cmp::min(max_backoff, backoff * 2);
                    continue;
                }
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

pub async fn http_binance_linear_depth_snap(
    runtime: &AppRuntime,
    http_spec: HttpRequestSpec,
    map_ctx: MapCtx,
    map_envelope: MapEnvelope,
    symbol: String,
) -> AppResult<()> {
    let exchange = ExchangeId::BinanceLinear;
    let transport = StreamTransport::HttpPoll;
    let kind = StreamKind::L2Book;

    let deps = runtime.deps.clone();

    let api_client = runtime
        .deps
        .as_ref()
        .binance_linear_client
        .clone()
        .ok_or_else(|| AppError::Disabled("Binance Linear exchange is disabled!".into()))?;

    let knobs: StreamKnobs = crate::app::StreamKnobs::from_deps(deps.clone());

    // 1) Fetch once
    let snap: BinanceLinearDepthSnapshot = api_client.execute_json(&http_spec).await?;

    // 2) Map to events
    let events = snap.map_to_events(&map_ctx, Some(map_envelope))?;
    let batch_size = events.len();

    // 3) Convert events -> DB rows (no locks; one-shot)
    //
    // Adjust the MarketEvent variant + DB row type to match your actual depth event model.
    // Examples people commonly use: MarketEvent::Depth, MarketEvent::OrderBook, etc.
    let depth_db_rows: Vec<DepthDeltaDBRow> = events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::DepthDelta(d) => Some(DepthDeltaDBRow::from(d.clone())),
            _ => None,
        })
        .collect();

    // 4) Publish to redis (optional)
    if !knobs.disable_redis_publishes {
        for e in &events {
            if let Some((kind, ex, sym, fields)) = e.as_redis_publish() {
                let fields_ref = as_publish_fields(&fields);
                deps.redis_publish(ex, sym, kind, &fields_ref).await?;
            }
        }
    }

    // 5) Write to DB (optional)
    if !knobs.disable_db_writes && !depth_db_rows.is_empty() {
        let writer_cfg = match runtime.deps.as_ref().db.as_ref() {
            Some(db) => db.cfg.writer.clone(),
            None => WriterConfig::default(),
        };

        let mut batch =
            make_empty_batch::<DepthDeltaDBRow>(exchange, transport, kind, symbol, writer_cfg)?;
        batch.flush_rows = batch_size; // just insert everything on the full depth first snapshot
        batch.hard_cap_rows = batch_size * 5;

        batch.rows.extend(depth_db_rows);
        deps.db_write((&mut batch).into()).await?;
    }
    if std::env::var_os("APP_TEST_ONESHOT").is_some() {
        println!("{:?}", events.iter().take(5).collect::<Vec<_>>());
        tracing::info!("APP_TEST_ONESHOT set: The test worked fine");
    }

    Ok(())
}

pub async fn http_hyperliquid_perp_depth_snap(
    runtime: &AppRuntime,
    http_spec: HttpRequestSpec,
    map_ctx: MapCtx,
    map_envelope: MapEnvelope,
    symbol: String,
) -> AppResult<()> {
    let exchange = ExchangeId::HyperliquidPerp;
    let transport = StreamTransport::HttpPoll;
    let kind = StreamKind::L2Book;

    let deps = runtime.deps.clone();

    let api_client = runtime
        .deps
        .as_ref()
        .hyperliquid_perp_client
        .clone()
        .ok_or_else(|| AppError::Disabled("Hyperliquid perp exchange is disabled!".into()))?;

    let knobs: StreamKnobs = crate::app::StreamKnobs::from_deps(deps.clone());

    // 1) Fetch once
    let snap: HyperliquidPerpDepthSnapshot = api_client.execute_json(&http_spec).await?;

    // 2) Map to events
    let events = snap.map_to_events(&map_ctx, Some(map_envelope))?;
    let batch_size = events.len();

    // 3) Convert events -> DB rows (no locks; one-shot)
    //
    // Adjust the MarketEvent variant + DB row type to match your actual depth event model.
    // Examples people commonly use: MarketEvent::Depth, MarketEvent::OrderBook, etc.
    let depth_db_rows: Vec<DepthDeltaDBRow> = events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::DepthDelta(d) => Some(DepthDeltaDBRow::from(d.clone())),
            _ => None,
        })
        .collect();

    // 4) Publish to redis (optional)
    if !knobs.disable_redis_publishes {
        for e in &events {
            if let Some((kind, ex, sym, fields)) = e.as_redis_publish() {
                let fields_ref = as_publish_fields(&fields);
                deps.redis_publish(ex, sym, kind, &fields_ref).await?;
            }
        }
    }

    // 5) Write to DB (optional)
    if !knobs.disable_db_writes && !depth_db_rows.is_empty() {
        let writer_cfg = match runtime.deps.as_ref().db.as_ref() {
            Some(db) => db.cfg.writer.clone(),
            None => WriterConfig::default(),
        };

        let mut batch =
            make_empty_batch::<DepthDeltaDBRow>(exchange, transport, kind, symbol, writer_cfg)?;
        batch.flush_rows = batch_size; // just insert everything on the full depth first snapshot
        batch.hard_cap_rows = batch_size * 5;

        batch.rows.extend(depth_db_rows);
        deps.db_write((&mut batch).into()).await?;
    }

    if std::env::var_os("APP_TEST_ONESHOT").is_some() {
        println!("{:?}", events);
        tracing::info!("APP_TEST_ONESHOT set: The test worked fine");
    }

    Ok(())
}
