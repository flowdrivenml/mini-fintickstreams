use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;

use crate::app::{AppRuntime, stream_types::ExchangeId};

#[derive(Debug, Deserialize)]
pub struct LimiterQuery {
    /// Optional exchange filter: "binance_linear" | "hyperliquid_perp"
    pub exchange: Option<String>,
}

pub async fn get_limiters(
    State(rt): State<AppRuntime>,
    Query(q): Query<LimiterQuery>,
) -> impl IntoResponse {
    let exchange: Option<ExchangeId> = match q.exchange.as_deref() {
        None => None,
        Some("binance_linear") => Some(ExchangeId::BinanceLinear),
        Some("hyperliquid_perp") => Some(ExchangeId::HyperliquidPerp),
        Some(other) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("unknown exchange '{other}'"),
            )
                .into_response();
        }
    };

    match rt.runtime_limiter_info(exchange).await {
        Ok(info) => (StatusCode::OK, Json(info)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to fetch limiter info: {e}"),
        )
            .into_response(),
    }
}

