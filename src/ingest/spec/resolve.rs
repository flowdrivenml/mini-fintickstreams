use super::template::{render_params_as_query, render_string, render_toml_as_json};
use super::types::{Ctx, HttpRequestSpec, ParamPlacement, WsControlSpec};
use crate::error::{AppError, AppResult};
use crate::ingest::config::{ApiEndpoint, ExchangeConfig, WsStream};
use reqwest::Method;
use std::collections::BTreeMap;

/// Parse an HTTP method string from config into reqwest::Method.
pub fn parse_method(method: &str) -> AppResult<Method> {
    Method::from_bytes(method.trim().as_bytes())
        .map_err(|_| AppError::InvalidConfig(format!("Invalid HTTP method '{}'", method)))
}

/// Resolve an ApiEndpoint + ctx into a concrete HttpRequestSpec.
///
/// ParamPlacement decides whether `params` becomes query params (Binance GET style)
/// or JSON body (Hyperliquid POST /info style).
pub fn resolve_http_request(
    ep: &ApiEndpoint,
    ctx: &Ctx,
    placement: ParamPlacement,
) -> AppResult<HttpRequestSpec> {
    let method = parse_method(&ep.method)?;

    let mut spec = HttpRequestSpec {
        method,
        path: ep.endpoint.clone(),
        query: Vec::new(),
        headers: Vec::new(),
        json_body: None,
        weight: ep.weight as u32,
        interval_seconds: ep.interval_seconds,
    };

    if let Some(params) = &ep.params {
        match placement {
            ParamPlacement::Query => {
                spec.query = render_params_as_query(params, ctx)?;
            }
            ParamPlacement::JsonBody => {
                spec.json_body = Some(render_toml_as_json(params, ctx)?);
            }
        }
    }

    Ok(spec)
}

/// Resolve a WsStream's stream_title into a concrete WsSubscriptionSpec.
/// - If stream_title is a string => rendered string => WsSubscriptionSpec::Text
/// - If stream_title is a table => rendered json => WsSubscriptionSpec::Json
// pub fn resolve_ws_stream_title(stream: &WsStream, ctx: &Ctx) -> AppResult<WsSubscriptionSpec> {
//     match &stream.stream_title {
//         StringOrTable::String(s) => Ok(WsSubscriptionSpec::Text(render_string(s, ctx)?)),
//         StringOrTable::Table(v) => Ok(WsSubscriptionSpec::Json(render_toml_as_json(v, ctx)?)),
//     }
// }

pub fn seed_ws_stream_ctx(stream: &WsStream, ctx: &mut Ctx) -> AppResult<()> {
    if let Some(s) = stream.stream_title.as_deref() {
        let rendered = render_string(s, ctx)?;
        ctx.insert("stream_title".to_string(), rendered);
    }

    if let Some(c) = stream.coin.as_deref() {
        let rendered = render_string(c, ctx)?;
        ctx.insert("coin".to_string(), rendered);
    }

    if let Some(t) = stream.subscription_type.as_deref() {
        let rendered = render_string(t, ctx)?;
        ctx.insert("subscription_type".to_string(), rendered);
    }

    Ok(())
}

/// Resolve ExchangeConfig's subscribe/unsubscribe templates into a WsControlSpec.
///
/// This is generic:
/// - You provide ctx entries like:
///   - "stream_title" (Binance expects it in params array)
///   - "stream_id"    (if your template uses it)
///   - "subscription_type"/"coin"/etc (Hyperliquid-style nested tables)
///
/// Typically you will:
/// 1) resolve_ws_stream_title(...) to get a stream title
/// 2) put it into ctx as "stream_title" (stringified or already-json depending on your template style)
/// 3) call this function to render the final messages.
pub fn resolve_ws_control(config: &ExchangeConfig, ctx: &Ctx) -> AppResult<WsControlSpec> {
    let sub_tpl = config.ws_subscribe_msg.as_ref().ok_or_else(|| {
        AppError::InvalidConfig("ws_subscribe_msg missing in exchange config".into())
    })?;

    let unsub_tpl = config.ws_unsubscribe_msg.as_ref().ok_or_else(|| {
        AppError::InvalidConfig("ws_unsubscribe_msg missing in exchange config".into())
    })?;

    // These templates are TOML tables (or values), so render to JSON.
    let sub_json = render_toml_as_json(sub_tpl, ctx)?;
    let unsub_json = render_toml_as_json(unsub_tpl, ctx)?;

    Ok(WsControlSpec {
        subscribe: sub_json,
        unsubscribe: unsub_json,
    })
}

/// Convenience: resolve all HTTP endpoints by name using the same ctx and placement.
/// Returns a new map { endpoint_name -> HttpRequestSpec }.
pub fn resolve_all_http(
    api: &BTreeMap<String, ApiEndpoint>,
    ctx: &Ctx,
    placement: ParamPlacement,
) -> AppResult<BTreeMap<String, HttpRequestSpec>> {
    let mut out = BTreeMap::new();
    for (name, ep) in api {
        out.insert(name.clone(), resolve_http_request(ep, ctx, placement)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::config::load_app_config;
    use crate::ingest::config::ExchangeConfigs;

    use serde_json::Value as JsonValue;

    fn pretty_json(v: &JsonValue) -> String {
        serde_json::to_string_pretty(v).unwrap_or_else(|_| "<json pretty failed>".into())
    }

    #[test]
    fn spec_resolve_smoke_test_with_prints() -> AppResult<()> {
        // 1) Load configs
        let appconfig = load_app_config()?;
        let exchangeconfigs = ExchangeConfigs::new(&appconfig)?;

        let binance = exchangeconfigs.binance_linear.as_ref().ok_or_else(|| {
            AppError::InvalidConfig("binance_linear missing in ExchangeConfigs".into())
        })?;

        let hyper = exchangeconfigs.hyperliquid_perp.as_ref().ok_or_else(|| {
            AppError::InvalidConfig("hyperliquid_perp missing in ExchangeConfigs".into())
        })?;

        // 2) Build a base ctx (you can tweak these values)
        // NOTE: your Binance configs use uppercase like BTCUSDT in practice, but this is just a test.
        let mut ctx: Ctx = Ctx::new();
        ctx.insert("symbol".into(), "btcusdt".into());
        ctx.insert("coin".into(), "btcperp".into());

        // 3) Test parse_method()
        println!("--- parse_method ---");
        let m_get = parse_method("GET")?;
        let m_post = parse_method("POST")?;
        println!("GET -> {:?}", m_get);
        println!("POST -> {:?}", m_post);

        // 4) Test resolve_http_request() on a Binance endpoint (Query params)
        println!("\n--- resolve_http_request (binance: Query) ---");
        let binance_depth = binance
            .api
            .get("depth")
            .ok_or_else(|| AppError::InvalidConfig("binance api.depth missing".into()))?;

        let spec_depth = resolve_http_request(binance_depth, &ctx, ParamPlacement::Query)?;
        println!(
            "binance.depth spec => method={:?} path={}",
            spec_depth.method, spec_depth.path
        );
        println!("query={:?}", spec_depth.query);
        println!("json_body={:?}", spec_depth.json_body);

        // 5) Test resolve_http_request() on a Hyperliquid endpoint (JSON body)
        println!("\n--- resolve_http_request (hyperliquid: JsonBody) ---");
        let hyper_depth = hyper
            .api
            .get("depth")
            .ok_or_else(|| AppError::InvalidConfig("hyperliquid api.depth missing".into()))?;

        let spec_hyper = resolve_http_request(hyper_depth, &ctx, ParamPlacement::JsonBody)?;
        println!(
            "hyper.depth spec => method={:?} path={}",
            spec_hyper.method, spec_hyper.path
        );
        println!("query={:?}", spec_hyper.query);
        println!(
            "json_body={}",
            spec_hyper
                .json_body
                .as_ref()
                .map(pretty_json)
                .unwrap_or_else(|| "<none>".into())
        );

        // 6) Resolve WS control for Binance (seed ctx from WsStream, then render templates)
        println!("\n--- resolve_ws_control (binance) ---");
        let binance_trades = binance
            .ws
            .get("trades")
            .ok_or_else(|| AppError::InvalidConfig("binance ws.trades missing".into()))?;

        let mut ws_ctx_binance = ctx.clone();
        seed_ws_stream_ctx(binance_trades, &mut ws_ctx_binance)?;
        // Most Binance subscribe templates need an id
        ws_ctx_binance.insert("stream_id".into(), "1".into());

        let binance_ctrl = resolve_ws_control(binance, &ws_ctx_binance)?;
        println!(
            "binance subscribe msg => {}",
            pretty_json(&binance_ctrl.subscribe)
        );
        println!(
            "binance unsubscribe msg => {}",
            pretty_json(&binance_ctrl.unsubscribe)
        );

        // 7) Resolve WS control for Hyperliquid (seed ctx, ensure subscription_type if template expects it)
        println!("\n--- resolve_ws_control (hyperliquid) ---");
        let hyper_trades = hyper
            .ws
            .get("trades")
            .ok_or_else(|| AppError::InvalidConfig("hyperliquid ws.trades missing".into()))?;

        let mut ws_ctx_hyper = ctx.clone();
        seed_ws_stream_ctx(hyper_trades, &mut ws_ctx_hyper)?;

        // If your Hyperliquid ws stream config doesn't provide subscription_type,
        // but your template expects it, set a default for the smoke test.
        ws_ctx_hyper
            .entry("subscription_type".into())
            .or_insert_with(|| "trades".into());

        let hyper_ctrl = resolve_ws_control(hyper, &ws_ctx_hyper)?;
        println!(
            "hyper subscribe msg => {}",
            pretty_json(&hyper_ctrl.subscribe)
        );
        println!(
            "hyper unsubscribe msg => {}",
            pretty_json(&hyper_ctrl.unsubscribe)
        );

        // 8) Test resolve_all_http()
        println!("\n--- resolve_all_http ---");
        let all_binance = resolve_all_http(&binance.api, &ctx, ParamPlacement::Query)?;
        println!("resolved {} binance http endpoints", all_binance.len());

        let all_hyper = resolve_all_http(&hyper.api, &ctx, ParamPlacement::JsonBody)?;
        println!("resolved {} hyperliquid http endpoints", all_hyper.len());

        Ok(())
    }
}
