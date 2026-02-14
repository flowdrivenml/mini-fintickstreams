// tests/api_deserialize.rs
use crate::app::config::load_app_config;
use crate::error::{AppError, AppResult};
use crate::ingest::config::ExchangeConfigs;
use crate::ingest::datamap::FromJsonStr;
use crate::ingest::datamap::sources::binance_linear::types::{
    BinanceLinearDepthSnapshot, BinanceLinearFundingRateSnapshot, BinanceLinearOpenInterestSnapshot,
};
use crate::ingest::datamap::sources::bybit_linear::types::{
    BybitLinearDepthSnapshot, BybitLinearExchangeInfoSnapshot, BybitLinearServerTimeSnapshot,
};
use crate::ingest::datamap::sources::hyperliquid_perp::types::HyperliquidPerpDepthSnapshot;
use crate::ingest::http::api_client::ApiClient;
use crate::ingest::spec::resolve::resolve_http_request;
use crate::ingest::spec::{Ctx, ParamPlacement};
use serde_json::Value as JsonValue;

/* ----------------------------- helpers ----------------------------- */

fn print_spec(label: &str, spec: &crate::ingest::spec::HttpRequestSpec) {
    println!("--- {label} resolved spec ---");
    println!("method   = {:?}", spec.method);
    println!("path     = {}", spec.path);
    println!("query    = {:?}", spec.query);
    println!("json     = {:?}", spec.json_body);
}

fn pretty_json(v: &JsonValue) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| "<json pretty failed>".into())
}

async fn call_json_dbg<T>(
    label: &str,
    client: &ApiClient,
    spec: &crate::ingest::spec::HttpRequestSpec,
) -> AppResult<T>
where
    T: FromJsonStr,
{
    print_spec(label, spec);

    let resp = client.execute(spec).await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    println!("{label}: http status = {status}");
    if let Ok(v) = serde_json::from_str::<JsonValue>(&body) {
        println!("{label}: body (json) = {}", pretty_json(&v));
    } else {
        println!("{label}: body (raw) = {body}");
    }

    if !status.is_success() {
        return Err(AppError::Api {
            service: client.name.to_string(),
            status,
            body,
        });
    }

    T::from_json_str(&body)
}

fn base_ctx() -> Ctx {
    let mut ctx = Ctx::new();
    ctx.insert("symbol".into(), "btcusdt".into());
    ctx.insert("coin".into(), "BTC".into());
    ctx
}

/* --------------------------- BINANCE: DEPTH -------------------------- */

#[tokio::test]
async fn binance_depth_deserializes() -> AppResult<()> {
    let appconfig = load_app_config(false, 0)?;
    let exchanges = ExchangeConfigs::new(&appconfig, false, 0)?;
    let binance = exchanges.binance_linear.as_ref().unwrap();

    let ctx = base_ctx();
    let ep = binance.api.get("depth").unwrap();
    let spec = resolve_http_request(ep, &ctx, ParamPlacement::Query)?;

    let client = ApiClient::new("binance_depth_it", binance.api_base_url.clone(), None, None);

    let _: BinanceLinearDepthSnapshot = call_json_dbg("binance.depth", &client, &spec).await?;

    Ok(())
}

/* --------------------- BINANCE: OPEN INTEREST ------------------------ */

#[tokio::test]
async fn binance_open_interest_deserializes() -> AppResult<()> {
    let appconfig = load_app_config(false, 0)?;
    let exchanges = ExchangeConfigs::new(&appconfig, false, 0)?;
    let binance = exchanges.binance_linear.as_ref().unwrap();

    let ctx = base_ctx();
    let ep = binance.api.get("open_interest").unwrap();
    let spec = resolve_http_request(ep, &ctx, ParamPlacement::Query)?;

    let client = ApiClient::new("binance_oi_it", binance.api_base_url.clone(), None, None);

    let _: BinanceLinearOpenInterestSnapshot =
        call_json_dbg("binance.open_interest", &client, &spec).await?;

    Ok(())
}

/* --------------------- BINANCE: FUNDING RATE ------------------------- */

#[tokio::test]
async fn binance_funding_rate_deserializes() -> AppResult<()> {
    let appconfig = load_app_config(false, 0)?;
    let exchanges = ExchangeConfigs::new(&appconfig, false, 0)?;
    let binance = exchanges.binance_linear.as_ref().unwrap();

    let ctx = base_ctx();
    let ep = binance.api.get("funding_rate").unwrap();
    let spec = resolve_http_request(ep, &ctx, ParamPlacement::Query)?;

    let client = ApiClient::new("binance_fr_it", binance.api_base_url.clone(), None, None);

    let _: BinanceLinearFundingRateSnapshot =
        call_json_dbg("binance.funding_rate", &client, &spec).await?;

    Ok(())
}

/* ------------------------ HYPERLIQUID: DEPTH ------------------------- */

#[tokio::test]
async fn hyperliquid_depth_deserializes() -> AppResult<()> {
    let appconfig = load_app_config(false, 0)?;
    let exchanges = ExchangeConfigs::new(&appconfig, false, 0)?;
    let hyper = exchanges.hyperliquid_perp.as_ref().unwrap();

    let ctx = base_ctx();
    let ep = hyper.api.get("depth").unwrap();
    let spec = resolve_http_request(ep, &ctx, ParamPlacement::JsonBody)?;

    let client = ApiClient::new(
        "hyperliquid_depth_it",
        hyper.api_base_url.clone(),
        None,
        None,
    );

    let _: HyperliquidPerpDepthSnapshot =
        call_json_dbg("hyperliquid.depth", &client, &spec).await?;

    Ok(())
}

// tests/api_deserialize.rs (additions for BYBIT)

/* ----------------------------- helpers ----------------------------- */
/* (keep your existing helpers as-is) */

fn bybit_ctx() -> Ctx {
    let mut ctx = Ctx::new();
    // Bybit REST uses symbol like "BTCUSDT" and requires category="linear"
    ctx.insert("symbol".into(), "BTCUSDT".into());
    ctx.insert("category".into(), "linear".into());
    ctx
}

/* --------------------------- BYBIT: DEPTH --------------------------- */

#[tokio::test]
async fn bybit_depth_deserializes() -> AppResult<()> {
    let appconfig = load_app_config(false, 0)?;
    let exchanges = ExchangeConfigs::new(&appconfig, false, 0)?;
    let bybit = exchanges.bybit_linear.as_ref().unwrap();

    let ctx = bybit_ctx();
    let ep = bybit.api.get("depth").unwrap();
    let spec = resolve_http_request(ep, &ctx, ParamPlacement::Query)?;

    let client = ApiClient::new("bybit_depth_it", bybit.api_base_url.clone(), None, None);

    let _: BybitLinearDepthSnapshot = call_json_dbg("bybit.depth", &client, &spec).await?;

    Ok(())
}

/* ------------------------ BYBIT: SERVER TIME ------------------------ */

#[tokio::test]
async fn bybit_server_time_deserializes() -> AppResult<()> {
    let appconfig = load_app_config(false, 0)?;
    let exchanges = ExchangeConfigs::new(&appconfig, false, 0)?;
    let bybit = exchanges.bybit_linear.as_ref().unwrap();

    let ctx = bybit_ctx();
    let ep = bybit.api.get("server_time").unwrap();
    let spec = resolve_http_request(ep, &ctx, ParamPlacement::Query)?;

    let client = ApiClient::new("bybit_time_it", bybit.api_base_url.clone(), None, None);

    let _: BybitLinearServerTimeSnapshot =
        call_json_dbg("bybit.server_time", &client, &spec).await?;

    Ok(())
}

/* ------------------------ BYBIT: EXCHANGE INFO ---------------------- */

#[tokio::test]
async fn bybit_exchange_info_deserializes() -> AppResult<()> {
    let appconfig = load_app_config(false, 0)?;
    let exchanges = ExchangeConfigs::new(&appconfig, false, 0)?;
    let bybit = exchanges.bybit_linear.as_ref().unwrap();

    let ctx = bybit_ctx();
    let ep = bybit.api.get("exchange_info").unwrap();
    let spec = resolve_http_request(ep, &ctx, ParamPlacement::Query)?;

    let client = ApiClient::new(
        "bybit_exchange_info_it",
        bybit.api_base_url.clone(),
        None,
        None,
    );

    let _: BybitLinearExchangeInfoSnapshot =
        call_json_dbg("bybit.exchange_info", &client, &spec).await?;

    Ok(())
}
