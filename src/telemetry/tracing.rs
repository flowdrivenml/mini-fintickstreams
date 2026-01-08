use tracing_subscriber::{EnvFilter, fmt};

/// Initialize tracing with:
/// - `RUST_LOG` / `RUST_LOG_STYLE` support via EnvFilter
/// - a sensible default if RUST_LOG is not set
///
/// Call this once at startup (main), and optionally from tests.
pub fn init() {
    // Example default: your crate at info, noisy deps at warn
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,sqlx=warn,tokio_tungstenite=warn"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .compact()
        .init();
}

/// Test-friendly init (won't panic if called multiple times).
pub fn init_for_tests() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .compact()
        .try_init();
}
