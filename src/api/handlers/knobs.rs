use axum::{
    Json,
    extract::{Path, State},
};

use crate::api::error::ApiError;
use crate::api::types::{KnobsResp, PatchKnobsRequest};
use crate::app::AppRuntime;
use crate::app::stream_types::{ExchangeId, StreamId, StreamKind, StreamTransport};
use crate::error::AppError;

type KnobsPath = (String, String, StreamKind, StreamTransport);
//             ^exchange_str, symbol, kind, transport

fn parse_exchange_id(exchange_str: &str) -> Result<ExchangeId, AppError> {
    // If ExchangeId already implements FromStr, use that.
    // Otherwise, map known strings here.
    match exchange_str {
        "binance_linear" => Ok(ExchangeId::BinanceLinear),
        "hyperliquid_perp" => Ok(ExchangeId::HyperliquidPerp),
        other => Err(AppError::InvalidArgument(format!(
            "unknown exchange: {other}"
        ))),
    }
}

pub async fn get(
    State(app): State<AppRuntime>,
    Path((exchange_str, symbol, kind, transport)): Path<KnobsPath>,
) -> Result<Json<KnobsResp>, ApiError> {
    let exchange = parse_exchange_id(&exchange_str)?;
    let id = StreamId::new(exchange.as_str(), symbol.as_str(), kind, transport);

    let Some(knobs) = app.stream_knobs_snapshot(&id).await else {
        return Err(ApiError(AppError::StreamNotFound(
            "stream not found".into(),
        )));
    };

    Ok(Json(KnobsResp { knobs }))
}

pub async fn patch(
    State(app): State<AppRuntime>,
    Path((exchange_str, symbol, kind, transport)): Path<KnobsPath>,
    Json(req): Json<PatchKnobsRequest>,
) -> Result<Json<&'static str>, ApiError> {
    let exchange = parse_exchange_id(&exchange_str)?;
    let id = StreamId::new(exchange.as_str(), symbol.as_str(), kind, transport);

    // Apply only the fields provided
    if let Some(disable) = req.disable_db_writes {
        // your runtime API sets "enabled", so invert
        app.set_stream_db_writes_enabled(&id, !disable).await?;
    }
    if let Some(disable) = req.disable_redis_publishes {
        app.set_stream_redis_publishes_enabled(&id, !disable)
            .await?;
    }
    if let Some(v) = req.flush_rows {
        app.set_stream_flush_rows(&id, v).await?;
    }
    if let Some(v) = req.flush_interval_ms {
        app.set_stream_flush_interval_ms(&id, v).await?;
    }
    if let Some(v) = req.chunk_rows {
        app.set_stream_chunk_rows(&id, v).await?;
    }
    if let Some(v) = req.hard_cap_rows {
        app.set_stream_hard_cap_rows(&id, v).await?;
    }

    Ok(Json("ok"))
}
