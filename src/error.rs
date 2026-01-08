/// Crate-wide result type.
pub type AppResult<T> = std::result::Result<T, AppError>;

use reqwest::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    // =========
    // Config / startup
    // =========
    #[error("Configuration file IO error: {0}")]
    ConfigIo(#[from] std::io::Error),

    #[error("Failed to parse TOML config: {0}")]
    ConfigToml(#[from] toml::de::Error),

    #[error("Missing configuration field: {0}")]
    MissingConfig(&'static str),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    // =========
    // Networking / WebSocket / HTTP
    // =========
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("HTTP transport error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Invalid URL/URI: {0}")]
    Uri(#[from] http::uri::InvalidUri),

    /// Remote API returned a non-success HTTP status
    #[error("API error from {service}: status={status}, body={body}")]
    Api {
        service: String,
        status: StatusCode,
        body: String,
    },

    // =========
    // Serialization / deserialization
    // =========
    #[error("JSON serialization/deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    // =========
    // Database
    // =========
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    // =========
    // Metrics / Prometheus
    // =========
    #[error("Prometheus registry error: {0}")]
    Prometheus(#[from] prometheus::Error),

    // =========
    // Application-domain errors
    // =========
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    #[error("Stream already exists: {0}")]
    StreamAlreadyExists(String),

    #[error("Rate limit exceeded: {details}")]
    RateLimited { details: String },

    #[error("Failed to spawn task: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("Shutdown requested")]
    Shutdown,

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Redis logic error: {0}")]
    RedisLogic(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Optional: if you use tokio::sync::oneshot a lot.
    #[error("Oneshot receive canceled")]
    OneshotRecv,

    /// Optional: mpsc send failed because receiver is dropped.
    #[error("Channel send failed (receiver dropped)")]
    ChannelSend,

    /// `tokio::time::timeout(...)` elapsed before completion.
    #[error("Operation timed out")]
    Timeout(#[from] tokio::time::error::Elapsed),

    /// Optional: broadcast receive lagged (messages dropped).
    #[error("Broadcast lagged by {0} messages")]
    BroadcastLagged(u64),

    #[error("Exchange disabled: {0}")]
    ExchangeDisabled(String),

    #[error("Disabled error: {0}")]
    Disabled(String),
}

// ============================
// Axum HTTP adapter
// ============================

#[cfg(feature = "axum")]
mod axum_impl {
    use super::AppError;
    use axum::{
        Json,
        http::StatusCode,
        response::{IntoResponse, Response},
    };
    use serde::Serialize;

    /// Thin HTTP wrapper so core errors don't depend on HTTP.
    #[derive(Debug)]
    pub struct ApiError(pub AppError);

    #[derive(Debug, Serialize)]
    struct ErrorBody {
        error: String,
        kind: &'static str,
    }

    impl From<AppError> for ApiError {
        fn from(e: AppError) -> Self {
            Self(e)
        }
    }

    impl IntoResponse for ApiError {
        fn into_response(self) -> Response {
            let (status, kind, msg) = map_error(&self.0);
            (status, Json(ErrorBody { error: msg, kind })).into_response()
        }
    }

    fn map_error(e: &AppError) -> (StatusCode, &'static str, String) {
        match e {
            // Admission / disabled
            AppError::Disabled(reason) => {
                (StatusCode::SERVICE_UNAVAILABLE, "disabled", reason.clone())
            }
            AppError::ExchangeDisabled(reason) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "exchange_disabled",
                reason.clone(),
            ),

            // Streams
            AppError::StreamNotFound(msg) => {
                (StatusCode::NOT_FOUND, "stream_not_found", msg.clone())
            }
            AppError::StreamAlreadyExists(msg) => {
                (StatusCode::CONFLICT, "stream_exists", msg.clone())
            }

            // Bad inputs
            AppError::InvalidArgument(msg) => {
                (StatusCode::BAD_REQUEST, "invalid_argument", msg.clone())
            }
            AppError::MissingConfig(field) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "missing_config",
                field.to_string(),
            ),
            AppError::InvalidConfig(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid_config",
                msg.clone(),
            ),

            // Rate limit
            AppError::RateLimited { details } => (
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                details.clone(),
            ),

            // Timeouts / shutdown
            AppError::Timeout(_) => (StatusCode::GATEWAY_TIMEOUT, "timeout", e.to_string()),
            AppError::Shutdown => (StatusCode::SERVICE_UNAVAILABLE, "shutdown", e.to_string()),

            // External API error (you used reqwest::StatusCode in the enum)
            AppError::Api {
                status,
                body,
                service,
            } => {
                // convert reqwest::StatusCode -> axum/http::StatusCode
                let code = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
                (code, "upstream_api_error", format!("{}: {}", service, body))
            }

            // Typical infra-ish errors: treat as 502/503
            AppError::Reqwest(_) | AppError::WebSocket(_) | AppError::Redis(_) => {
                (StatusCode::BAD_GATEWAY, "upstream_transport", e.to_string())
            }

            // DB errors usually mean dependency down or query failed
            AppError::Sqlx(_) => (StatusCode::SERVICE_UNAVAILABLE, "db_error", e.to_string()),

            // Everything else
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()),
        }
    }

    // Re-export so callers can use crate::error::ApiError
    pub use ApiError as AxumError;
}

#[cfg(feature = "axum")]
pub use axum_impl::AxumError as ApiError;
