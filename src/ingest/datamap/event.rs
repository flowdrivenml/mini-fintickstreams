use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct MapEnvelope {
    pub exchange: String,
    pub symbol: Option<String>,
}

impl MapEnvelope {
    pub fn new(exchange: impl Into<String>, symbol: Option<impl Into<String>>) -> Self {
        Self {
            exchange: exchange.into(),
            symbol: symbol.map(Into::into),
        }
    }

    // Convenience if you often have &str and want no allocation when symbol is None
    pub fn new_no_symbol(exchange: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
            symbol: None,
        }
    }
}

/// Trade direction convention: 0=buy, 1=sell (matches DB).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TradeSide {
    Buy,
    Sell,
}

impl TradeSide {
    #[inline]
    pub fn as_i16(self) -> i16 {
        match self {
            TradeSide::Buy => 0,
            TradeSide::Sell => 1,
        }
    }
}

/// Orderbook side convention for depth rows: 0=bid, 1=ask (matches DB).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BookSide {
    Bid,
    Ask,
}

impl BookSide {
    #[inline]
    pub fn as_i16(self) -> i16 {
        match self {
            BookSide::Bid => 0,
            BookSide::Ask => 1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradeRow {
    pub exchange: &'static str,
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub side: TradeSide, // 0=buy, 1=sell via as_i16()
    pub price_i: i64,    // scaled
    pub qty_i: i64,      // scaled
    pub trade_id: Option<i64>,
    pub is_maker: Option<bool>,
}

/// One depth level delta update (DB stores one row per price level).
///
/// `size_i == 0` means delete level.
#[derive(Debug, Clone)]
pub struct DepthDeltaRow {
    pub exchange: &'static str,
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub side: BookSide, // 0=bid, 1=ask via as_i16()
    pub price_i: i64,   // scaled
    pub size_i: i64,    // scaled (0 = delete)
    pub seq: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct OpenInterestRow {
    pub exchange: &'static str,
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub oi_i: i64, // scaled
}

#[derive(Debug, Clone)]
pub struct FundingRow {
    pub exchange: &'static str,
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub funding_rate: i64,
    pub funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct LiquidationRow {
    pub exchange: &'static str,
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub side: i16,            // keep i16 to match your DB "your convention"
    pub price_i: Option<i64>, // optional
    pub qty_i: i64,           // scaled
    pub liq_id: Option<i64>,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    Trade(TradeRow),
    DepthDelta(DepthDeltaRow),
    OpenInterest(OpenInterestRow),
    Funding(FundingRow),
    Liquidation(LiquidationRow),
}

impl MarketEvent {
    #[inline]
    pub fn exchange(&self) -> &'static str {
        match self {
            MarketEvent::Trade(x) => x.exchange,
            MarketEvent::DepthDelta(x) => x.exchange,
            MarketEvent::OpenInterest(x) => x.exchange,
            MarketEvent::Funding(x) => x.exchange,
            MarketEvent::Liquidation(x) => x.exchange,
        }
    }

    #[inline]
    pub fn symbol(&self) -> &str {
        match self {
            MarketEvent::Trade(x) => &x.symbol,
            MarketEvent::DepthDelta(x) => &x.symbol,
            MarketEvent::OpenInterest(x) => &x.symbol,
            MarketEvent::Funding(x) => &x.symbol,
            MarketEvent::Liquidation(x) => &x.symbol,
        }
    }

    #[inline]
    pub fn time(&self) -> DateTime<Utc> {
        match self {
            MarketEvent::Trade(x) => x.time,
            MarketEvent::DepthDelta(x) => x.time,
            MarketEvent::OpenInterest(x) => x.time,
            MarketEvent::Funding(x) => x.time,
            MarketEvent::Liquidation(x) => x.time,
        }
    }
}
