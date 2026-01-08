use axum::{
    Json,
    extract::{Query, State},
};

use crate::api::error::ApiError;
use crate::api::types::{
    InstrumentExistsQuery, InstrumentExistsResp, InstrumentsCountResp, InstrumentsQuery,
};
use crate::app::AppRuntime;
use crate::error::AppError;
use crate::ingest::instruments::spec::InstrumentSpec;

pub async fn list(
    State(app): State<AppRuntime>,
    Query(q): Query<InstrumentsQuery>,
) -> Result<Json<Vec<InstrumentSpec>>, ApiError> {
    let out = match (q.exchange.as_deref(), q.kind) {
        (Some(ex), Some(kind)) => app.retrieve_instruments_by_exchange_kind(ex, kind),
        (Some(ex), None) => app.retrieve_instruments_by_exchange(ex),
        (None, Some(kind)) => app.retrieve_instruments_by_kind(kind),
        (None, None) => {
            // If you want "all instruments", you can add a method for that.
            // For now, require at least one filter so you don't dump 10k symbols by accident.
            return Err(ApiError(AppError::InvalidArgument(
                "provide at least one of: exchange, kind".into(),
            )));
        }
    };

    Ok(Json(out))
}

pub async fn count(
    State(app): State<AppRuntime>,
    Query(q): Query<InstrumentsQuery>,
) -> Result<Json<InstrumentsCountResp>, ApiError> {
    let items = match (q.exchange.as_deref(), q.kind) {
        (Some(ex), Some(kind)) => app.retrieve_instruments_by_exchange_kind(ex, kind),
        (Some(ex), None) => app.retrieve_instruments_by_exchange(ex),
        (None, Some(kind)) => app.retrieve_instruments_by_kind(kind),
        (None, None) => {
            return Err(ApiError(AppError::InvalidArgument(
                "provide at least one of: exchange, kind".into(),
            )));
        }
    };

    Ok(Json(InstrumentsCountResp { count: items.len() }))
}

/// GET /instruments/exists?exchange=...&symbol=...
pub async fn exists(
    State(app): State<AppRuntime>,
    Query(q): Query<InstrumentExistsQuery>,
) -> Json<InstrumentExistsResp> {
    let exists = app.exists(&q.exchange, &q.symbol);
    Json(InstrumentExistsResp { exists })
}

/// POST /instruments/refresh
pub async fn refresh(State(app): State<AppRuntime>) -> Result<Json<&'static str>, ApiError> {
    app.refresh_instruments_registry().await?;
    Ok(Json("ok"))
}
