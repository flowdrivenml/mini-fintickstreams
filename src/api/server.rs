use crate::app::runtime::AppRuntime;
use crate::error::{AppError, AppResult};
use axum::Router;
use std::net::SocketAddr;

use super::{build_router, config::ApiConfig};

pub async fn run_api_server(runtime: AppRuntime) -> AppResult<()> {
    let cfg = ApiConfig::load_default()?;
    let addr: SocketAddr = format!("{}:{}", cfg.bind_addr, cfg.port)
        .parse()
        .map_err(|e| AppError::InvalidConfig(format!("api.toml: invalid bind/port: {e}")))?;

    let app: Router = build_router(runtime);

    tracing::info!(
        bind_addr = %cfg.bind_addr,
        port = cfg.port,
        "api server starting (axum)"
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to bind API server: {e}")))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| AppError::Internal(format!("API server error: {e}")))?;

    Ok(())
}

/// Graceful shutdown for Ctrl+C and SIGTERM (k8s).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

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

    tracing::info!("api server shutdown signal received");
}
