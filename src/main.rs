use mini_fintickstreams::api::server::run_api_server;
use mini_fintickstreams::app::runtime::AppRuntime;
use mini_fintickstreams::error::AppResult;
use mini_fintickstreams::prometheus::server::run_metrics_server;
use mini_fintickstreams::telemetry::tracing as app_tracing;

#[tokio::main]
async fn main() -> AppResult<()> {
    app_tracing::init();

    let runtime = AppRuntime::new().await?;

    let gather = {
        let rt = runtime.clone();
        move || rt.encode_prometheus_text()
    };

    let api_task = run_api_server(runtime.clone());
    let metrics_task = run_metrics_server(gather);

    tokio::select! {
        res = api_task => res?,
        res = metrics_task => res?,
    }

    Ok(())
}
