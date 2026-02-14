use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum ExchangeId {
    BinanceLinear,
    HyperliquidPerp,
    BybitLinear,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum StreamTransport {
    Ws,
    HttpPoll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum StreamKind {
    Trades,
    L2Book,
    Ticker,
    Funding,
    OpenInterest,
    Liquidations,
    FundingOpenInterest,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct StreamId(pub String);

// helper
impl StreamId {
    pub fn new(
        exchange: &str,
        instrument: &str,
        kind: StreamKind,
        transport: StreamTransport,
    ) -> Self {
        Self(format!("{exchange}:{instrument}:{transport:?}:{kind:?}"))
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub enum StreamStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed { last_error: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamSpec {
    pub exchange: &'static str,
    pub instrument: String,
    pub kind: StreamKind,
    pub transport: StreamTransport,
}
