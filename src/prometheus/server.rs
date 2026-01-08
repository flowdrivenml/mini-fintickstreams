use crate::error::{AppError, AppResult};
use crate::prometheus::config::PrometheusConfig;

use axum::{
    Router,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::IntoResponse,
    routing::get,
};
use std::{net::SocketAddr, sync::Arc};
use tower_http::compression::CompressionLayer;

type GatherFn = Arc<dyn Fn() -> AppResult<String> + Send + Sync>;

#[derive(Clone)]
struct AppState {
    gather: GatherFn,
    content_type: HeaderValue,
}

pub async fn run_metrics_server<G>(gather: G) -> AppResult<()>
where
    G: Fn() -> AppResult<String> + Send + Sync + 'static,
{
    let cfg = PrometheusConfig::load_default()?;
    let addr: SocketAddr = format!("{}:{}", cfg.bind_addr, cfg.port)
        .parse()
        .map_err(|e| AppError::InvalidConfig(format!("Invalid bind/port: {e}")))?;

    let state = AppState {
        gather: Arc::new(gather),
        content_type: HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    };

    // Dynamic route path is fine: Router::route accepts &str.
    let app = Router::new()
        .route(&cfg.metrics_path, get(metrics_handler))
        .route("/healthz", get(healthz_handler))
        .layer(CompressionLayer::new())
        .with_state(state);

    tracing::info!(
        bind_addr = %cfg.bind_addr,
        port = cfg.port,
        path = %cfg.metrics_path,
        "prometheus metrics server starting (axum)"
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to bind metrics server: {e}")))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| AppError::Internal(format!("Metrics server error: {e}")))?;

    Ok(())
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    match (state.gather)() {
        Ok(text) => {
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, state.content_type.clone());
            (StatusCode::OK, headers, text).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to gather metrics");
            (StatusCode::INTERNAL_SERVER_ERROR, "gather metrics failed\n").into_response()
        }
    }
}

async fn healthz_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

/// Graceful shutdown for Ctrl+C and SIGTERM (k8s).
async fn shutdown_signal() {
    // Ctrl+C always available.
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    // SIGTERM on Unix (k8s sends this).
    #[cfg(unix)]
    let sigterm = async {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut s) = signal(SignalKind::terminate()) {
            let _ = s.recv().await;
        }
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm => {},
    }

    tracing::info!("metrics server shutdown signal received");
}
