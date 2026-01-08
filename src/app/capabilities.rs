use crate::app::stream_types::{ExchangeId, StreamKind, StreamTransport};
use serde::{Deserialize, Serialize};

/// A single "capability" row describing what can be started.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailableStream {
    pub exchange: ExchangeId,
    pub transport: StreamTransport,
    pub kind: StreamKind,
    /// Optional human hint (e.g., "combined stream only", "requires depth snapshot", etc.)
    pub note: Option<&'static str>,
}

/// Returns all stream types currently supported by `start_stream` routing.
///
/// Keep this list in sync with the match-arms in `start_stream`.
pub fn list_available_streams() -> Vec<AvailableStream> {
    use ExchangeId::*;
    use StreamKind::*;
    use StreamTransport::*;

    vec![
        // -------------------------
        // HTTP POLL
        // -------------------------
        AvailableStream {
            exchange: BinanceLinear,
            transport: HttpPoll,
            kind: OpenInterest,
            note: None,
        },
        AvailableStream {
            exchange: BinanceLinear,
            transport: HttpPoll,
            kind: Funding,
            note: None,
        },
        // -------------------------
        // WS
        // -------------------------
        // BINANCE LINEAR
        AvailableStream {
            exchange: BinanceLinear,
            transport: Ws,
            kind: Trades,
            note: None,
        },
        AvailableStream {
            exchange: BinanceLinear,
            transport: Ws,
            kind: L2Book,
            note: Some("will also do an HTTP depth snapshot before WS"),
        },
        AvailableStream {
            exchange: BinanceLinear,
            transport: Ws,
            kind: Liquidations,
            note: None,
        },
        // HYPERLIQUID PERP
        AvailableStream {
            exchange: HyperliquidPerp,
            transport: Ws,
            kind: Trades,
            note: None,
        },
        AvailableStream {
            exchange: HyperliquidPerp,
            transport: Ws,
            kind: L2Book,
            note: Some("will also do an HTTP depth snapshot before WS"),
        },
        AvailableStream {
            exchange: HyperliquidPerp,
            transport: Ws,
            kind: FundingOpenInterest,
            note: Some("combined OI+Funding stream; request this instead of OI/Funding"),
        },
    ]
}

/// Convenience: list capabilities for a single exchange.
pub fn list_available_for_exchange(exchange: ExchangeId) -> Vec<AvailableStream> {
    list_available_streams()
        .into_iter()
        .filter(|s| s.exchange == exchange)
        .collect()
}

/// Convenience: check if (exchange, transport, kind) is supported.
pub fn is_supported(exchange: ExchangeId, transport: StreamTransport, kind: StreamKind) -> bool {
    list_available_streams()
        .into_iter()
        .any(|s| s.exchange == exchange && s.transport == transport && s.kind == kind)
}

/// Optional: if you want to enforce "don't allow OI/Funding individually for HL"
/// the routing already errors; this helper documents that rule.
pub fn unsupported_reason(
    exchange: ExchangeId,
    transport: StreamTransport,
    kind: StreamKind,
) -> Option<&'static str> {
    use ExchangeId::*;
    use StreamKind::*;
    use StreamTransport::*;

    match (exchange, transport, kind) {
        (HyperliquidPerp, Ws, OpenInterest) | (HyperliquidPerp, Ws, Funding) => {
            Some("hyperliquid_perp: use FundingOpenInterest (combined) stream kind")
        }
        _ => None,
    }
}
