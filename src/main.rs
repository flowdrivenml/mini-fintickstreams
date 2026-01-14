mod cli;

use clap::Parser;
use tokio::runtime::Builder;
use tracing::debug;

use crate::cli::{Cli, ConfigSource, ShutdownAction};
use mini_fintickstreams::api::server::run_api_server;
use mini_fintickstreams::app::runtime::AppRuntime;
use mini_fintickstreams::error::AppResult;
use mini_fintickstreams::prometheus::server::run_metrics_server;
use mini_fintickstreams::telemetry::tracing as app_tracing;

// --config env --shutdown-action none
//
// --config env --shutdown-action restore-streams
//
// --config file --shutdown-action none
//
// --config file --shutdown-action restore-streams
// ./mini-fintickstreams --config env --shutdown-action none
// RUST_BACKTRACE=1 ./target/release/mini-fintickstreams --config env --shutdown-action none

fn main() -> AppResult<()> {
    let cli = Cli::parse();

    let rt = Builder::new_multi_thread()
        .worker_threads(cli.workers)
        .enable_all()
        .build()
        .expect("tokio runtime build failed");

    rt.block_on(async move {
        app_tracing::init();
        debug!(?cli, "starting");

        let from_env = matches!(cli.config, ConfigSource::Env);
        let runtime = AppRuntime::new(from_env, cli.stream_version).await?;

        // ✅ Restore on startup (your logic)
        match cli.shutdown_action {
            ShutdownAction::None => {}
            ShutdownAction::RestoreStreams => {
                debug!("shutdown_action=RestoreStreams: running restore");
                runtime.on_crash().await?;
            }
        }

        let gather = {
            let rt = runtime.clone();
            move || rt.encode_prometheus_text()
        };

        let api_task = run_api_server(runtime.clone(), from_env, cli.stream_version);
        let metrics_task = run_metrics_server(gather);

        // ✅ Keep running until one task errors/exits, or Ctrl+C
        tokio::select! {
            res = api_task => res?,
            res = metrics_task => res?,
            _ = tokio::signal::ctrl_c() => {
                debug!("shutdown signal received");
            }
        }

        debug!("exiting");
        Ok(())
    })
}
