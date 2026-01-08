use axum::{
    Json,
    extract::{Path, Query, State},
};

use crate::api::error::ApiError;
use crate::api::types::{
    AddStreamRequest, AvailableStreamDto, ListStreamsQuery, RemoveStreamRequest, StreamCountResp,
    StreamRow, StreamSpecDto, StreamStatusResp,
};
use crate::app::stream_types::{ExchangeId, StreamId, StreamKind, StreamTransport};
use crate::app::{AppRuntime, StartStreamParams};
use crate::error::AppError;

// --------------------------------------------------
// Parsers (string -> enums) for path usage
// --------------------------------------------------

fn parse_exchange_id(exchange_str: &str) -> Result<ExchangeId, AppError> {
    match exchange_str {
        "binance_linear" => Ok(ExchangeId::BinanceLinear),
        "hyperliquid_perp" => Ok(ExchangeId::HyperliquidPerp),
        other => Err(AppError::InvalidArgument(format!(
            "unknown exchange: {other}"
        ))),
    }
}

fn parse_kind(kind: &str) -> Result<StreamKind, AppError> {
    match kind {
        "Trades" => Ok(StreamKind::Trades),
        "L2Book" => Ok(StreamKind::L2Book),
        "Liquidations" => Ok(StreamKind::Liquidations),
        "OpenInterest" => Ok(StreamKind::OpenInterest),
        "Funding" => Ok(StreamKind::Funding),
        "FundingOpenInterest" => Ok(StreamKind::FundingOpenInterest),
        other => Err(AppError::InvalidArgument(format!("unknown kind: {other}"))),
    }
}

fn parse_transport(t: &str) -> Result<StreamTransport, AppError> {
    match t {
        "Ws" => Ok(StreamTransport::Ws),
        "HttpPoll" => Ok(StreamTransport::HttpPoll),
        other => Err(AppError::InvalidArgument(format!(
            "unknown transport: {other}"
        ))),
    }
}

// --------------------------------------------------
// Capabilities
// --------------------------------------------------

/// GET /streams/capabilities
pub async fn capabilities(State(app): State<AppRuntime>) -> Json<Vec<AvailableStreamDto>> {
    let out = app
        .stream_capabilities()
        .into_iter()
        .map(|s| AvailableStreamDto {
            exchange: s.exchange.as_str().to_string(),
            transport: format!("{:?}", s.transport),
            kind: format!("{:?}", s.kind),
            note: s.note,
        })
        .collect();

    Json(out)
}

/// GET /streams/capabilities/:exchange
pub async fn capabilities_exchange(
    State(app): State<AppRuntime>,
    Path(exchange_str): Path<String>,
) -> Result<Json<Vec<AvailableStreamDto>>, ApiError> {
    let exchange = parse_exchange_id(&exchange_str)?;

    let out = app
        .stream_capabilities_for_exchange(exchange)
        .into_iter()
        .map(|s| AvailableStreamDto {
            exchange: s.exchange.as_str().to_string(),
            transport: format!("{:?}", s.transport),
            kind: format!("{:?}", s.kind),
            note: s.note,
        })
        .collect();

    Ok(Json(out))
}

// --------------------------------------------------
// Streams control + list
// --------------------------------------------------

/// GET /streams?exchange=...&symbol=...&kind=...&transport=...
pub async fn list(
    State(app): State<AppRuntime>,
    Query(q): Query<ListStreamsQuery>,
) -> Result<Json<Vec<StreamRow>>, ApiError> {
    let mut rows = app.list_streams().await;

    if let Some(ex) = &q.exchange {
        rows.retain(|(_, _, spec)| spec.exchange == *ex);
    }
    if let Some(sym) = &q.symbol {
        rows.retain(|(_, _, spec)| spec.instrument == *sym);
    }
    if let Some(kind) = q.kind {
        rows.retain(|(_, _, spec)| spec.kind == kind);
    }
    if let Some(transport) = q.transport {
        rows.retain(|(_, _, spec)| spec.transport == transport);
    }

    let out = rows
        .into_iter()
        .map(|(id, status, spec)| StreamRow {
            id: id.to_string(),
            status: format!("{status:?}"),
            exchange: spec.exchange.to_string(),
            symbol: spec.instrument.to_string(),
            kind: spec.kind,
            transport: spec.transport,
        })
        .collect();

    Ok(Json(out))
}

/// POST /streams
pub async fn add(
    State(app): State<AppRuntime>,
    Json(req): Json<AddStreamRequest>,
) -> Result<Json<&'static str>, ApiError> {
    let p = StartStreamParams {
        exchange: req.exchange,
        transport: req.transport,
        kind: req.kind,
        symbol: req.symbol,
    };

    app.add_stream(p).await?;
    Ok(Json("ok"))
}

/// DELETE /streams
pub async fn remove(
    State(app): State<AppRuntime>,
    Json(req): Json<RemoveStreamRequest>,
) -> Result<Json<&'static str>, ApiError> {
    let p = StartStreamParams {
        exchange: req.exchange,
        transport: req.transport,
        kind: req.kind,
        symbol: req.symbol,
    };

    app.remove_stream(p).await?;
    Ok(Json("ok"))
}

// --------------------------------------------------
// Streams status endpoints
// --------------------------------------------------

/// GET /streams/count
pub async fn count(State(app): State<AppRuntime>) -> Json<StreamCountResp> {
    let count = app.stream_count().await;
    Json(StreamCountResp { count })
}

/// GET /streams/:exchange/:symbol/:kind/:transport
pub async fn get_one(
    State(app): State<AppRuntime>,
    Path((exchange_str, symbol, kind_str, transport_str)): Path<(String, String, String, String)>,
) -> Result<Json<StreamStatusResp>, ApiError> {
    let exchange = parse_exchange_id(&exchange_str)?;
    let kind = parse_kind(&kind_str)?;
    let transport = parse_transport(&transport_str)?;

    let id = StreamId::new(exchange.as_str(), symbol.as_str(), kind, transport);

    let rows = app.list_streams().await;
    let found = rows.into_iter().find(|(sid, _, _)| *sid == id);

    let Some((sid, status, spec)) = found else {
        return Err(ApiError(AppError::StreamNotFound(
            "stream not found".into(),
        )));
    };

    Ok(Json(StreamStatusResp {
        id: sid.to_string(),
        status: format!("{status:?}"),
        spec: StreamSpecDto {
            exchange: spec.exchange.to_string(),
            symbol: spec.instrument.to_string(),
            kind: format!("{:?}", spec.kind),
            transport: format!("{:?}", spec.transport),
        },
    }))
}
