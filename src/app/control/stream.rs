use crate::app::AppRuntime;
use crate::app::StreamKnobs;
use crate::app::control::helpers::{ctx_with_symbol, resolve_api_endpoint};
use crate::app::control::httppoll::http_poll_binancelinear_oi;
use crate::app::stream_types::{ExchangeId, StreamId, StreamKind, StreamSpec, StreamTransport};
use crate::error::{AppError, AppResult};
use crate::ingest::datamap::ctx::MapCtx;
use crate::ingest::datamap::event::MapEnvelope;
use crate::ingest::spec::ParamPlacement;
use crate::ingest::spec::resolve::resolve_http_request;

/// Parameters to start a stream.
#[derive(Debug, Clone)]
pub struct StartStreamParams {
    pub exchange: ExchangeId,
    pub transport: StreamTransport,
    pub kind: StreamKind,
    pub symbol: String,
}

/// Start a stream:
/// - Builds StreamId + StreamSpec
/// - Spawns worker task
/// - Inserts StreamHandle into AppState
pub async fn start_stream(
    app: &AppRuntime,
    p: StartStreamParams,
    add_to_db_registry: bool,
) -> AppResult<()> {
    // 1) Build StreamSpec / StreamId
    let deps = app.deps.clone();

    app.ensure_runtime_ok_for_admission()?;

    let exchange_str = p.exchange.as_str();
    let spec = StreamSpec {
        exchange: exchange_str,
        instrument: p.symbol.to_string(),
        kind: p.kind,
        transport: p.transport,
    };
    let id = StreamId::new(exchange_str, p.symbol.as_str(), p.kind, p.transport);

    // 2) Ensure not already running
    if app.state.contains(&id).await {
        return Err(AppError::StreamAlreadyExists(id.to_string()));
    }

    let knobs = StreamKnobs::from_deps(deps.clone());

    // 4) Build mapping context / envelope (placeholders)
    let map_ctx = build_map_ctx(app, &spec)?;
    let map_envelope = build_map_envelope(&p)?;

    // 5) Build Symbol CTX
    let mut ctx = ctx_with_symbol(p.exchange, p.transport, &p.symbol);

    // 6) Resolve Param Placement for http
    let http_placement = ParamPlacement::for_exchange(p.exchange);

    match spec.transport {
        StreamTransport::HttpPoll => {
            let ep = resolve_api_endpoint(&deps.exchange_cfgs, p.exchange, spec.kind)?;
            let reqspec = resolve_http_request(&ep, &ctx, http_placement)?;

            match spec.kind {
                StreamKind::OpenInterest => match p.exchange.as_str() {
                    "binance_linear" => {
                        http_poll_binancelinear_oi(
                            app,
                            reqspec,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    _ => Err(AppError::Internal("Unsupported Exchange".to_string())),
                },

                StreamKind::Funding => match p.exchange.as_str() {
                    "binance_linear" => {
                        crate::app::control::httppoll::http_poll_binancelinear_funding(
                            app,
                            reqspec,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    _ => Err(AppError::Internal("Unsupported Exchange".to_string())),
                },

                _ => Err(AppError::Internal("Unsupported Stream Kind".to_string())),
            }?;
        }

        StreamTransport::Ws => {
            match spec.kind {
                // -------------------------
                // BINANCE LINEAR
                // -------------------------
                StreamKind::Trades => match p.exchange.as_str() {
                    "binance_linear" => {
                        crate::app::control::ws::ws_binancelinear_aggtrades(
                            app,
                            ctx,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    "hyperliquid_perp" => {
                        crate::app::control::ws::ws_hyperliquidperp_trades(
                            app,
                            ctx,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    "bybit_linear" => {
                        crate::app::control::ws::ws_bybitlinear_trades(
                            app,
                            ctx,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    _ => Err(AppError::Internal("Unsupported Exchange".to_string())),
                },

                StreamKind::L2Book => match p.exchange.as_str() {
                    "binance_linear" => {
                        let ep = resolve_api_endpoint(&deps.exchange_cfgs, p.exchange, spec.kind)?;
                        let reqspec = resolve_http_request(&ep, &ctx, http_placement)?;
                        // Snap the whole orderbook at the current moment via API
                        crate::app::control::httppoll::http_binance_linear_depth_snap(
                            app,
                            reqspec,
                            map_ctx.clone(),
                            map_envelope.clone(),
                            p.symbol.clone(),
                        )
                        .await?;
                        crate::app::control::ws::ws_binancelinear_depth(
                            app,
                            ctx,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    "hyperliquid_perp" => {
                        let ep = resolve_api_endpoint(&deps.exchange_cfgs, p.exchange, spec.kind)?;
                        let reqspec = resolve_http_request(&ep, &ctx, http_placement)?;
                        // Snap the whole orderbook at the current moment via API
                        crate::app::control::httppoll::http_hyperliquid_perp_depth_snap(
                            app,
                            reqspec,
                            map_ctx.clone(),
                            map_envelope.clone(),
                            p.symbol.clone(),
                        )
                        .await?;
                        crate::app::control::ws::ws_hyperliquidperp_depth(
                            app,
                            ctx,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    "bybit_linear" => {
                        let http_ctx =
                            ctx_with_symbol(p.exchange, StreamTransport::HttpPoll, &p.symbol);
                        let ep = resolve_api_endpoint(&deps.exchange_cfgs, p.exchange, spec.kind)?;
                        let reqspec = resolve_http_request(&ep, &http_ctx, http_placement)?;
                        // Snap the whole orderbook at the current moment via API
                        crate::app::control::httppoll::http_bybit_linear_depth_snap(
                            app,
                            reqspec,
                            map_ctx.clone(),
                            map_envelope.clone(),
                            p.symbol.clone(),
                        )
                        .await?;
                        crate::app::control::ws::ws_bybitlinear_depth(
                            app,
                            ctx,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    _ => Err(AppError::Internal("Unsupported Exchange".to_string())),
                },

                StreamKind::Liquidations => match p.exchange.as_str() {
                    "binance_linear" => {
                        crate::app::control::ws::ws_binancelinear_liquidation(
                            app,
                            ctx,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    "bybit_linear" => {
                        crate::app::control::ws::ws_bybitlinear_liquidation(
                            app,
                            ctx,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    _ => Err(AppError::Internal("Unsupported Exchange".to_string())),
                },

                // -------------------------
                // HYPERLIQUID PERP (combined OI + Funding stream)
                // -------------------------
                StreamKind::FundingOpenInterest => match p.exchange.as_str() {
                    "hyperliquid_perp" => {
                        crate::app::control::ws::ws_hyperliquidperp_oifunding(
                            app,
                            ctx,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    "bybit_linear" => {
                        crate::app::control::ws::ws_bybitlinear_oifunding(
                            app,
                            ctx,
                            map_ctx,
                            map_envelope,
                            spec.clone(),
                            p.symbol.clone(),
                            id,
                        )
                        .await
                    }
                    _ => Err(AppError::Internal("Unsupported Exchange".to_string())),
                },

                // If you *also* want to allow requesting OI or Funding individually for HL,
                // you can either:
                //  - error out (strict), OR
                //  - route both to the combined stream (loose).
                StreamKind::OpenInterest | StreamKind::Funding => match p.exchange.as_str() {
                    "hyperliquid_perp" => Err(AppError::Internal(
                        "hyperliquid_perp: use FundingOpenInterest (combined) stream kind"
                            .to_string(),
                    )),
                    "bybit_linear" => Err(AppError::Internal(
                        "bybit_linear: use FundingOpenInterest (combined) stream kind".to_string(),
                    )),
                    _ => Err(AppError::Internal("Unsupported Exchange".to_string())),
                },

                _ => Err(AppError::Internal("Unsupported Stream Kind".to_string())),
            }?;
        }
    }

    if add_to_db_registry {
        match app.deps.as_ref().db.as_ref() {
            Some(db) => {
                db.handler
                    .as_ref()
                    .upsert_stream_registry(&spec, &knobs, true)
                    .await
            }
            None => Ok(()),
        }?;
    }

    Ok(())
}
// =====================================================================================
// Map ctx/envelope placeholders (replace with real mapping build)
// =====================================================================================

fn build_map_ctx(app: &AppRuntime, spec: &StreamSpec) -> AppResult<MapCtx> {
    let registry = app.instruments_registry.clone().load_full();
    let deps = app.deps.clone(); // Arc<AppDeps> lives for function scope
    let app_cfgs = deps.app_cfgs.clone(); // whatever smart ptr this is
    let cfgs = app_cfgs.as_ref(); // borrow tied to `app_cfgs` lifetime
    MapCtx::new(registry, cfgs, spec.exchange, &spec.instrument)
}

fn build_map_envelope(par: &StartStreamParams) -> AppResult<MapEnvelope> {
    let symbol = par.symbol.clone();
    let exchange = par.exchange.clone().as_str().to_string();
    Ok(MapEnvelope::new(exchange, Some(symbol)))
}
