use crate::app::stream_types::{ExchangeId, StreamKind, StreamTransport};
use crate::error::{AppError, AppResult};
use crate::ingest::config::{ApiEndpoint, ExchangeConfigs, WsStream};
use crate::ingest::spec::{Ctx, ParamPlacement};
use std::{fmt, str::FromStr};

impl StreamKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "Trades",
            Self::L2Book => "L2Book",
            Self::Ticker => "Ticker",
            Self::Funding => "Funding",
            Self::OpenInterest => "OpenInterest",
            Self::Liquidations => "Liquidations",
            Self::FundingOpenInterest => "FundingOpenInterest",
        }
    }

    /// Optional: handy for DB reads, gives a clearer error context.
    pub fn try_from_db(s: &str) -> Result<Self, AppError> {
        s.parse::<Self>().map_err(|e| {
            // Keep the original message but add context.
            AppError::InvalidArgument(format!("invalid StreamKind from DB: {s} ({e})"))
        })
    }
}

impl fmt::Display for StreamKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StreamKind {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Trades" => Ok(Self::Trades),
            "L2Book" => Ok(Self::L2Book),
            "Ticker" => Ok(Self::Ticker),
            "Funding" => Ok(Self::Funding),
            "OpenInterest" => Ok(Self::OpenInterest),
            "Liquidations" => Ok(Self::Liquidations),
            "FundingOpenInterest" => Ok(Self::FundingOpenInterest),
            _ => Err(AppError::InvalidArgument(format!(
                "invalid StreamKind: '{s}' (expected one of: Trades, L2Book, Ticker, Funding, OpenInterest, Liquidations, FundingOpenInterest)"
            ))),
        }
    }
}

pub fn binance_ws_request_id(stream_id: &str) -> String {
    // Allowed: A-Z a-z 0-9 - _
    // Replace anything else with '_', then clamp to 36 chars.
    let mut out = String::with_capacity(stream_id.len().min(36));
    for ch in stream_id.chars() {
        let ok = ch.is_ascii_alphanumeric() || ch == '-' || ch == '_';
        out.push(if ok { ch } else { '_' });
        if out.len() == 36 {
            break;
        }
    }
    if out.is_empty() {
        // never return empty: Binance requires 1..=36 chars if string
        "_".to_string()
    } else {
        out
    }
}

impl StreamKind {
    /// Resolve exchange-specific endpoint keys for this stream kind.
    ///
    /// The returned keys are looked up in:
    /// - `exchange_cfg.api` if transport == HttpPoll
    /// - `exchange_cfg.ws`  if transport == Ws
    ///
    /// Returns an error if the (exchange, transport, kind) combination
    /// is not supported.
    pub fn endpoint_key(
        self,
        exchange: ExchangeId,
        transport: StreamTransport,
    ) -> AppResult<&'static str> {
        use ExchangeId::*;
        use StreamKind::*;
        use StreamTransport::*;

        let key = match (exchange, transport, self) {
            // --------------------------
            // Binance Linear — HTTP
            // --------------------------
            (BinanceLinear, HttpPoll, L2Book) => "depth",
            (BinanceLinear, HttpPoll, OpenInterest) => "open_interest",
            (BinanceLinear, HttpPoll, Funding) => "funding_rate",

            // --------------------------
            // Binance Linear — WS
            // --------------------------
            (BinanceLinear, Ws, L2Book) => "depth_update",
            (BinanceLinear, Ws, Trades) => "trades",
            (BinanceLinear, Ws, Liquidations) => "liquidations",

            // --------------------------
            // Hyperliquid — HTTP
            // --------------------------
            (HyperliquidPerp, HttpPoll, L2Book) => "depth",

            // --------------------------
            // Hyperliquid — WS
            // --------------------------
            (HyperliquidPerp, Ws, L2Book) => "depth_update",
            (HyperliquidPerp, Ws, Trades) => "trades",
            (HyperliquidPerp, Ws, FundingOpenInterest) => "oi_funding",

            // --------------------------
            // Bybit Linear — HTTP
            // --------------------------
            (BybitLinear, HttpPoll, L2Book) => "depth",

            // --------------------------
            // Bybit Linear — WS
            // --------------------------
            (BybitLinear, Ws, L2Book) => "depth_update",
            (BybitLinear, Ws, Trades) => "trades",
            (BybitLinear, Ws, FundingOpenInterest) => "oi_funding",
            (BybitLinear, Ws, Liquidations) => "liquidations",

            // Unsupported
            _ => {
                return Err(AppError::InvalidConfig(format!(
                    "unsupported stream combination: exchange={exchange:?}, transport={transport:?}, kind={self:?}"
                )));
            }
        };

        Ok(key)
    }
}

impl ExchangeId {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExchangeId::BinanceLinear => "binance_linear",
            ExchangeId::HyperliquidPerp => "hyperliquid_perp",
            ExchangeId::BybitLinear => "bybit_linear",
        }
    }
}

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ExchangeId {
    type Err = AppError;

    fn from_str(s: &str) -> AppResult<Self> {
        match s {
            "binance_linear" => Ok(Self::BinanceLinear),
            "hyperliquid_perp" => Ok(Self::HyperliquidPerp),
            "bybit_linear" => Ok(Self::BybitLinear),
            _ => Err(AppError::InvalidArgument(format!(
                "invalid ExchangeId: {s}"
            ))),
        }
    }
}

impl StreamTransport {
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamTransport::Ws => "ws",
            StreamTransport::HttpPoll => "api",
        }
    }
}

impl fmt::Display for StreamTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StreamTransport {
    type Err = AppError;

    fn from_str(s: &str) -> AppResult<Self> {
        match s {
            "ws" => Ok(Self::Ws),
            "api" => Ok(Self::HttpPoll),
            _ => Err(AppError::InvalidArgument(format!(
                "invalid StreamTransport: {s}"
            ))),
        }
    }
}

impl ParamPlacement {
    /// Placement rule based on ExchangeId:
    /// - binance_linear -> Query
    /// - hyperliquid_perp -> JsonBody
    pub fn for_exchange(exchange: ExchangeId) -> Self {
        match exchange {
            ExchangeId::BinanceLinear => Self::Query,
            ExchangeId::HyperliquidPerp => Self::JsonBody,
            ExchangeId::BybitLinear => Self::Query,
        }
    }

    /// Same rule, but accepts an exchange key as &str
    /// (e.g. "binance_linear", "hyperliquid_perp").
    pub fn for_exchange_str(exchange: &str) -> AppResult<Self> {
        let exchange_id = ExchangeId::from_str(exchange)?;
        Ok(Self::for_exchange(exchange_id))
    }
}

pub fn ctx_with_symbol(exchange: ExchangeId, transport: StreamTransport, symbol: &str) -> Ctx {
    let mut ctx = Ctx::new();

    let normalized_symbol = match (exchange, transport) {
        // WS Binance → lowercase
        (ExchangeId::BinanceLinear, StreamTransport::Ws) => symbol.to_lowercase(),

        // Everything else → uppercase
        _ => symbol.to_uppercase(),
    };

    match exchange {
        ExchangeId::BinanceLinear => {
            ctx.insert("symbol".into(), normalized_symbol);
        }
        ExchangeId::HyperliquidPerp => {
            ctx.insert("coin".into(), normalized_symbol);
        }
        ExchangeId::BybitLinear => {
            ctx.insert("symbol".into(), normalized_symbol);
        }
    }

    ctx
}

/// Same as `ctx_with_symbol`, but accepts exchange as &str
pub fn ctx_with_symbol_str(
    exchange: &str,
    transport: StreamTransport,
    symbol: &str,
) -> AppResult<Ctx> {
    let exchange_id = ExchangeId::from_str(exchange)?;
    Ok(ctx_with_symbol(exchange_id, transport, symbol))
}

/// Resolve an ApiEndpoint from loaded ExchangeConfigs for a given exchange + stream kind (HTTP).
pub fn resolve_api_endpoint(
    configs: &ExchangeConfigs,
    exchange: ExchangeId,
    kind: StreamKind,
) -> AppResult<ApiEndpoint> {
    // 1) get the exchange config from the container
    let cfg = match exchange {
        ExchangeId::BinanceLinear => configs
            .binance_linear
            .as_ref()
            .ok_or_else(|| AppError::InvalidConfig("missing binance_linear config".into()))?,
        ExchangeId::HyperliquidPerp => configs
            .hyperliquid_perp
            .as_ref()
            .ok_or_else(|| AppError::InvalidConfig("missing hyperliquid_perp config".into()))?,
        ExchangeId::BybitLinear => configs
            .bybit_linear
            .as_ref()
            .ok_or_else(|| AppError::InvalidConfig("missing bybit_linear config".into()))?,
    };

    // 2) compute the endpoint key for HTTP
    let endpoint_key = kind.endpoint_key(exchange, StreamTransport::HttpPoll)?;

    // 3) lookup in cfg.api table
    let ep = cfg.api.get(endpoint_key).ok_or_else(|| {
        AppError::InvalidConfig(format!(
            "api endpoint key not found in config: exchange={exchange:?}, kind={kind:?}, key={endpoint_key}"
        ))
    })?;

    Ok(ep.clone())
}

/// Same as resolve_api_endpoint, but accepts exchange as &str
/// (e.g. "binance_linear", "hyperliquid_perp").
pub fn resolve_api_endpoint_str(
    configs: &ExchangeConfigs,
    exchange: &str,
    kind: StreamKind,
) -> AppResult<ApiEndpoint> {
    let exchange_id = ExchangeId::from_str(exchange)?;
    resolve_api_endpoint(configs, exchange_id, kind)
}

/// Resolve a WsStream from loaded ExchangeConfigs for a given exchange + stream kind (WS).
pub fn resolve_ws_stream(
    configs: &ExchangeConfigs,
    exchange: ExchangeId,
    kind: StreamKind,
) -> AppResult<WsStream> {
    // 1) get the exchange config from the container
    let cfg = match exchange {
        ExchangeId::BinanceLinear => configs
            .binance_linear
            .as_ref()
            .ok_or_else(|| AppError::InvalidConfig("missing binance_linear config".into()))?,
        ExchangeId::HyperliquidPerp => configs
            .hyperliquid_perp
            .as_ref()
            .ok_or_else(|| AppError::InvalidConfig("missing hyperliquid_perp config".into()))?,
        ExchangeId::BybitLinear => configs
            .bybit_linear
            .as_ref()
            .ok_or_else(|| AppError::InvalidConfig("missing bybit_linear config".into()))?,
    };

    // 2) compute the endpoint key for WS
    let stream_key = kind.endpoint_key(exchange, StreamTransport::Ws)?;

    // 3) lookup in cfg.ws table
    let ws = cfg.ws.get(stream_key).ok_or_else(|| {
        AppError::InvalidConfig(format!(
            "ws stream key not found in config: exchange={exchange:?}, kind={kind:?}, key={stream_key}"
        ))
    })?;

    Ok(ws.clone())
}

/// Same as resolve_ws_stream, but accepts exchange as &str
/// (e.g. "binance_linear", "hyperliquid_perp").
pub fn resolve_ws_stream_str(
    configs: &ExchangeConfigs,
    exchange: &str,
    kind: StreamKind,
) -> AppResult<WsStream> {
    let exchange_id = ExchangeId::from_str(exchange)?;
    resolve_ws_stream(configs, exchange_id, kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::config::load_app_config;
    use crate::app::stream_types::{ExchangeId, StreamKind, StreamTransport};
    use crate::error::AppError;
    use crate::ingest::config::ExchangeConfigs;
    use std::str::FromStr;

    #[test]
    fn stream_module_smoke_test_real_configs() {
        // If load_app_config / ExchangeConfigs::new return AppResult<T>,
        // this test can just unwrap() everything.
        let app_cfgs = load_app_config(false, 0).unwrap();
        let exchange_cfgs = ExchangeConfigs::new(&app_cfgs, false, 0).unwrap();

        println!("================= CONFIG LOADED =================");
        println!(
            "binance_linear loaded: {}",
            exchange_cfgs.binance_linear.is_some()
        );
        println!(
            "hyperliquid_perp loaded: {}",
            exchange_cfgs.hyperliquid_perp.is_some()
        );

        // -----------------------------
        // 1) endpoint_key mappings
        // -----------------------------
        println!("\n================= ENDPOINT KEY MAPPINGS =================");

        let supported = [
            // Binance HTTP
            (
                ExchangeId::BinanceLinear,
                StreamTransport::HttpPoll,
                StreamKind::L2Book,
                "depth",
            ),
            (
                ExchangeId::BinanceLinear,
                StreamTransport::HttpPoll,
                StreamKind::OpenInterest,
                "open_interest",
            ),
            (
                ExchangeId::BinanceLinear,
                StreamTransport::HttpPoll,
                StreamKind::Funding,
                "funding_rate",
            ),
            // Binance WS
            (
                ExchangeId::BinanceLinear,
                StreamTransport::Ws,
                StreamKind::L2Book,
                "depth_update",
            ),
            (
                ExchangeId::BinanceLinear,
                StreamTransport::Ws,
                StreamKind::Trades,
                "trades",
            ),
            (
                ExchangeId::BinanceLinear,
                StreamTransport::Ws,
                StreamKind::Liquidations,
                "liquidations",
            ),
            // Hyperliquid HTTP
            (
                ExchangeId::HyperliquidPerp,
                StreamTransport::HttpPoll,
                StreamKind::L2Book,
                "depth",
            ),
            // Hyperliquid WS
            (
                ExchangeId::HyperliquidPerp,
                StreamTransport::Ws,
                StreamKind::L2Book,
                "depth_update",
            ),
            (
                ExchangeId::HyperliquidPerp,
                StreamTransport::Ws,
                StreamKind::Trades,
                "trades",
            ),
            (
                ExchangeId::HyperliquidPerp,
                StreamTransport::Ws,
                StreamKind::FundingOpenInterest,
                "oi_funding",
            ),
        ];

        for (ex, tr, kind, expected_key) in supported {
            let key = kind.endpoint_key(ex, tr).unwrap();
            println!(
                "[OK] exchange={:?}, transport={:?}, kind={:?} => key='{}'",
                ex, tr, kind, key
            );
            assert_eq!(key, expected_key);
        }

        // Unsupported combination should produce InvalidConfig
        println!("\n================= UNSUPPORTED COMBO =================");
        let unsupported = StreamKind::Funding
            .endpoint_key(ExchangeId::HyperliquidPerp, StreamTransport::HttpPoll);
        match unsupported {
            Err(AppError::InvalidConfig(msg)) => {
                println!("[EXPECTED ERROR] {msg}");
                assert!(msg.contains("unsupported stream combination"));
            }
            other => panic!("expected InvalidConfig for unsupported combo, got: {other:?}"),
        }

        // -----------------------------
        // 2) Display / FromStr roundtrip
        // -----------------------------
        println!("\n================= PARSE + DISPLAY ROUNDTRIP =================");

        for s in ["binance_linear", "hyperliquid_perp"] {
            let ex = ExchangeId::from_str(s).unwrap();
            println!("ExchangeId: '{s}' -> {:?} -> '{}'", ex, ex);
            assert_eq!(ex.to_string(), s);
        }

        for s in ["api", "ws"] {
            let tr = StreamTransport::from_str(s).unwrap();
            println!("StreamTransport: '{s}' -> {:?} -> '{}'", tr, tr);
            assert_eq!(tr.to_string(), s);
        }

        // -----------------------------
        // 3) ParamPlacement rules
        // -----------------------------
        println!("\n================= PARAM PLACEMENT =================");

        let p_bin = ParamPlacement::for_exchange(ExchangeId::BinanceLinear);
        let p_hl = ParamPlacement::for_exchange(ExchangeId::HyperliquidPerp);
        println!("ParamPlacement BinanceLinear -> {:?}", p_bin);
        println!("ParamPlacement HyperliquidPerp -> {:?}", p_hl);
        assert_eq!(p_bin, ParamPlacement::Query);
        assert_eq!(p_hl, ParamPlacement::JsonBody);

        let p_bin_s = ParamPlacement::for_exchange_str("binance_linear").unwrap();
        let p_hl_s = ParamPlacement::for_exchange_str("hyperliquid_perp").unwrap();
        println!("ParamPlacement str 'binance_linear' -> {:?}", p_bin_s);
        println!("ParamPlacement str 'hyperliquid_perp' -> {:?}", p_hl_s);
        assert_eq!(p_bin_s, ParamPlacement::Query);
        assert_eq!(p_hl_s, ParamPlacement::JsonBody);

        // -----------------------------
        // 4) ctx_with_symbol normalization
        // -----------------------------
        println!("\n================= CTX SYMBOL NORMALIZATION =================");

        let symbol_in = "BtCuSdT";

        // HTTP Binance -> UPPER, key "symbol"
        let ctx = ctx_with_symbol(
            ExchangeId::BinanceLinear,
            StreamTransport::HttpPoll,
            symbol_in,
        );
        println!("HTTP Binance ctx: {:?}", ctx);
        assert_eq!(ctx.get("symbol").map(|s| s.as_str()), Some("BTCUSDT"));
        assert!(ctx.get("coin").is_none());

        // WS Binance -> lower, key "symbol"
        let ctx = ctx_with_symbol(ExchangeId::BinanceLinear, StreamTransport::Ws, symbol_in);
        println!("WS Binance ctx: {:?}", ctx);
        assert_eq!(ctx.get("symbol").map(|s| s.as_str()), Some("btcusdt"));
        assert!(ctx.get("coin").is_none());

        // HTTP Hyperliquid -> UPPER, key "coin"
        let ctx = ctx_with_symbol(
            ExchangeId::HyperliquidPerp,
            StreamTransport::HttpPoll,
            symbol_in,
        );
        println!("HTTP Hyperliquid ctx: {:?}", ctx);
        assert_eq!(ctx.get("coin").map(|s| s.as_str()), Some("BTCUSDT"));
        assert!(ctx.get("symbol").is_none());

        // WS Hyperliquid -> UPPER, key "coin"
        let ctx = ctx_with_symbol(ExchangeId::HyperliquidPerp, StreamTransport::Ws, symbol_in);
        println!("WS Hyperliquid ctx: {:?}", ctx);
        assert_eq!(ctx.get("coin").map(|s| s.as_str()), Some("BTCUSDT"));
        assert!(ctx.get("symbol").is_none());

        // str wrapper
        let ctx = ctx_with_symbol_str("binance_linear", StreamTransport::Ws, symbol_in).unwrap();
        println!("WS Binance ctx (str): {:?}", ctx);
        assert_eq!(ctx.get("symbol").map(|s| s.as_str()), Some("btcusdt"));

        // -----------------------------
        // 5) resolve_api_endpoint (real config lookup)
        // -----------------------------
        println!("\n================= RESOLVE API ENDPOINTS =================");

        // Pick only kinds that are supported over HTTP per exchange.
        let api_cases = [
            (ExchangeId::BinanceLinear, StreamKind::L2Book),
            (ExchangeId::BinanceLinear, StreamKind::OpenInterest),
            (ExchangeId::BinanceLinear, StreamKind::Funding),
            (ExchangeId::HyperliquidPerp, StreamKind::L2Book),
        ];

        for (ex, kind) in api_cases {
            let ep = resolve_api_endpoint(&exchange_cfgs, ex, kind).unwrap();
            let key = kind.endpoint_key(ex, StreamTransport::HttpPoll).unwrap();

            println!(
                "[API] exchange={:?}, kind={:?}, key='{}' => method='{}' endpoint='{}' weight={} interval={} params_present={}",
                ex,
                kind,
                key,
                ep.method,
                ep.endpoint,
                ep.weight,
                ep.interval_seconds,
                ep.params.is_some()
            );

            // Basic sanity: endpoint/method non-empty, interval > 0 (depends on your config)
            assert!(!ep.endpoint.is_empty());
            assert!(!ep.method.is_empty());
        }

        // _str variant smoke
        let ep =
            resolve_api_endpoint_str(&exchange_cfgs, "binance_linear", StreamKind::L2Book).unwrap();
        println!(
            "[API str] binance_linear L2Book => {} {}",
            ep.method, ep.endpoint
        );
        assert!(!ep.endpoint.is_empty());

        // -----------------------------
        // 6) resolve_ws_stream (real config lookup)
        // -----------------------------
        println!("\n================= RESOLVE WS STREAMS =================");

        let ws_cases = [
            (ExchangeId::BinanceLinear, StreamKind::L2Book),
            (ExchangeId::BinanceLinear, StreamKind::Trades),
            (ExchangeId::BinanceLinear, StreamKind::Liquidations),
            (ExchangeId::HyperliquidPerp, StreamKind::L2Book),
            (ExchangeId::HyperliquidPerp, StreamKind::Trades),
            (ExchangeId::HyperliquidPerp, StreamKind::FundingOpenInterest),
        ];

        for (ex, kind) in ws_cases {
            let ws = resolve_ws_stream(&exchange_cfgs, ex, kind).unwrap();
            let key = kind.endpoint_key(ex, StreamTransport::Ws).unwrap();

            println!(
                "[WS] exchange={:?}, kind={:?}, key='{}' => stream_title={:?} coin={:?} subscription_type={:?}",
                ex, kind, key, ws.stream_title, ws.coin, ws.subscription_type
            );

            // At least one of these should typically be present depending on your exchange format.
            assert!(
                ws.stream_title.is_some() || ws.coin.is_some() || ws.subscription_type.is_some(),
                "ws stream looks empty for exchange={ex:?} kind={kind:?} key={key}"
            );
        }

        // _str variant smoke
        let ws =
            resolve_ws_stream_str(&exchange_cfgs, "hyperliquid_perp", StreamKind::Trades).unwrap();
        println!(
            "[WS str] hyperliquid_perp Trades => stream_title={:?} coin={:?} subscription_type={:?}",
            ws.stream_title, ws.coin, ws.subscription_type
        );

        println!("\n================= DONE =================");
    }

    #[cfg(test)]
    mod bybit_linear_tests {
        use super::*;
        use crate::app::config::load_app_config;
        use crate::app::stream_types::{ExchangeId, StreamKind, StreamTransport};
        use crate::error::AppError;
        use crate::ingest::config::ExchangeConfigs;
        use std::str::FromStr;

        #[test]
        fn stream_module_smoke_test_real_configs_bybit_linear() {
            let app_cfgs = load_app_config(false, 0).unwrap();
            let exchange_cfgs = ExchangeConfigs::new(&app_cfgs, false, 0).unwrap();

            println!("================= CONFIG LOADED (BYBIT) =================");
            println!(
                "bybit_linear loaded: {}",
                exchange_cfgs.bybit_linear.is_some()
            );
            assert!(
                exchange_cfgs.bybit_linear.is_some(),
                "bybit_linear config must exist for this test"
            );

            // -----------------------------
            // 1) endpoint_key mappings
            // -----------------------------
            println!("\n================= ENDPOINT KEY MAPPINGS (BYBIT) =================");

            let supported = [
                // Bybit HTTP
                (
                    ExchangeId::BybitLinear,
                    StreamTransport::HttpPoll,
                    StreamKind::L2Book,
                    "depth", // adjust if your bybit HTTP depth key is named differently
                ),
                // Bybit WS
                (
                    ExchangeId::BybitLinear,
                    StreamTransport::Ws,
                    StreamKind::L2Book,
                    "depth_update",
                ),
                (
                    ExchangeId::BybitLinear,
                    StreamTransport::Ws,
                    StreamKind::Trades,
                    "trades",
                ),
                (
                    ExchangeId::BybitLinear,
                    StreamTransport::Ws,
                    StreamKind::Liquidations,
                    "liquidations",
                ),
                (
                    ExchangeId::BybitLinear,
                    StreamTransport::Ws,
                    StreamKind::FundingOpenInterest,
                    "oi_funding",
                ),
            ];

            for (ex, tr, kind, expected_key) in supported {
                let key = kind.endpoint_key(ex, tr).unwrap();
                println!(
                    "[OK] exchange={:?}, transport={:?}, kind={:?} => key='{}'",
                    ex, tr, kind, key
                );
                assert_eq!(key, expected_key);
            }

            // -----------------------------
            // 2) Display / FromStr roundtrip
            // -----------------------------
            println!("\n================= PARSE + DISPLAY ROUNDTRIP (BYBIT) =================");

            let ex = ExchangeId::from_str("bybit_linear").unwrap();
            println!("ExchangeId: 'bybit_linear' -> {:?} -> '{}'", ex, ex);
            assert_eq!(ex.to_string(), "bybit_linear");

            // -----------------------------
            // 3) ParamPlacement rules
            // -----------------------------
            println!("\n================= PARAM PLACEMENT (BYBIT) =================");

            let p = ParamPlacement::for_exchange(ExchangeId::BybitLinear);
            println!("ParamPlacement BybitLinear -> {:?}", p);

            // Most setups use Query for Bybit REST; if your code uses JsonBody, change this.
            assert_eq!(p, ParamPlacement::Query);

            let p_s = ParamPlacement::for_exchange_str("bybit_linear").unwrap();
            println!("ParamPlacement str 'bybit_linear' -> {:?}", p_s);
            assert_eq!(p_s, ParamPlacement::Query);

            // -----------------------------
            // 4) ctx_with_symbol normalization
            // -----------------------------
            println!("\n================= CTX SYMBOL NORMALIZATION (BYBIT) =================");

            let symbol_in = "BtCuSdT";

            // HTTP Bybit -> typically UPPER, key "symbol"
            let ctx = ctx_with_symbol(
                ExchangeId::BybitLinear,
                StreamTransport::HttpPoll,
                symbol_in,
            );
            println!("HTTP Bybit ctx: {:?}", ctx);
            assert_eq!(ctx.get("symbol").map(|s| s.as_str()), Some("BTCUSDT"));
            assert!(ctx.get("coin").is_none());

            // WS Bybit -> should be UPPER, key "symbol" (important for topic like publicTrade.BTCUSDT)
            let ctx = ctx_with_symbol(ExchangeId::BybitLinear, StreamTransport::Ws, symbol_in);
            println!("WS Bybit ctx: {:?}", ctx);
            assert_eq!(ctx.get("symbol").map(|s| s.as_str()), Some("BTCUSDT"));
            assert!(ctx.get("coin").is_none());

            // str wrapper
            let ctx = ctx_with_symbol_str("bybit_linear", StreamTransport::Ws, symbol_in).unwrap();
            println!("WS Bybit ctx (str): {:?}", ctx);
            assert_eq!(ctx.get("symbol").map(|s| s.as_str()), Some("BTCUSDT"));

            // -----------------------------
            // 5) resolve_api_endpoint (real config lookup)
            // -----------------------------
            println!("\n================= RESOLVE API ENDPOINTS (BYBIT) =================");

            let api_cases = [(ExchangeId::BybitLinear, StreamKind::L2Book)];

            for (ex, kind) in api_cases {
                let ep = resolve_api_endpoint(&exchange_cfgs, ex, kind).unwrap();
                let key = kind.endpoint_key(ex, StreamTransport::HttpPoll).unwrap();

                println!(
                    "[API] exchange={:?}, kind={:?}, key='{}' => method='{}' endpoint='{}' weight={} interval={} params_present={}",
                    ex,
                    kind,
                    key,
                    ep.method,
                    ep.endpoint,
                    ep.weight,
                    ep.interval_seconds,
                    ep.params.is_some()
                );

                assert!(!ep.endpoint.is_empty());
                assert!(!ep.method.is_empty());
            }

            // _str variant smoke
            let ep = resolve_api_endpoint_str(&exchange_cfgs, "bybit_linear", StreamKind::L2Book)
                .unwrap();
            println!(
                "[API str] bybit_linear L2Book => {} {}",
                ep.method, ep.endpoint
            );
            assert!(!ep.endpoint.is_empty());

            // -----------------------------
            // 6) resolve_ws_stream (real config lookup)
            // -----------------------------
            println!("\n================= RESOLVE WS STREAMS (BYBIT) =================");

            let ws_cases = [
                (ExchangeId::BybitLinear, StreamKind::L2Book),
                (ExchangeId::BybitLinear, StreamKind::Trades),
                (ExchangeId::BybitLinear, StreamKind::Liquidations),
                (ExchangeId::BybitLinear, StreamKind::FundingOpenInterest),
            ];

            for (ex, kind) in ws_cases {
                let ws = resolve_ws_stream(&exchange_cfgs, ex, kind).unwrap();
                let key = kind.endpoint_key(ex, StreamTransport::Ws).unwrap();

                println!(
                    "[WS] exchange={:?}, kind={:?}, key='{}' => stream_title={:?} coin={:?} subscription_type={:?}",
                    ex, kind, key, ws.stream_title, ws.coin, ws.subscription_type
                );

                assert!(
                    ws.stream_title.is_some()
                        || ws.coin.is_some()
                        || ws.subscription_type.is_some(),
                    "ws stream looks empty for exchange={ex:?} kind={kind:?} key={key}"
                );
            }

            // _str variant smoke
            let ws =
                resolve_ws_stream_str(&exchange_cfgs, "bybit_linear", StreamKind::Trades).unwrap();
            println!(
                "[WS str] bybit_linear Trades => stream_title={:?} coin={:?} subscription_type={:?}",
                ws.stream_title, ws.coin, ws.subscription_type
            );

            println!("\n================= DONE (BYBIT) =================");
        }
    }
}
