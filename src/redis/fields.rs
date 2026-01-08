use crate::ingest::datamap::event::{
    DepthDeltaRow, FundingRow, LiquidationRow, MarketEvent, OpenInterestRow, TradeRow,
};
use crate::redis::streams::StreamKind;

pub type RedisFields = Vec<(&'static str, String)>;

pub fn as_publish_fields(fields: &RedisFields) -> Vec<(&str, &str)> {
    fields.iter().map(|(k, v)| (*k, v.as_str())).collect()
}

pub trait ToRedisPublish {
    fn redis_kind(&self) -> StreamKind;
    fn redis_exchange(&self) -> &str;
    fn redis_symbol(&self) -> &str;
    fn redis_fields(&self) -> RedisFields;
}

// -----------------------
// OpenInterestRow (yours)
// -----------------------
impl ToRedisPublish for OpenInterestRow {
    fn redis_kind(&self) -> StreamKind {
        StreamKind::OpenInterest
    }
    fn redis_exchange(&self) -> &str {
        self.exchange
    }
    fn redis_symbol(&self) -> &str {
        &self.symbol
    }
    fn redis_fields(&self) -> RedisFields {
        vec![
            ("time", self.time.to_rfc3339()),
            ("oi_i", self.oi_i.to_string()),
        ]
    }
}

// -----------------------
// TradeRow
// -----------------------
impl ToRedisPublish for TradeRow {
    fn redis_kind(&self) -> StreamKind {
        StreamKind::Trades
    }
    fn redis_exchange(&self) -> &str {
        self.exchange
    }
    fn redis_symbol(&self) -> &str {
        &self.symbol
    }
    fn redis_fields(&self) -> RedisFields {
        vec![
            ("time", self.time.to_rfc3339()),
            ("side", self.side.as_i16().to_string()),
            ("price_i", self.price_i.to_string()),
            ("qty_i", self.qty_i.to_string()),
            (
                "trade_id",
                self.trade_id.map(|x| x.to_string()).unwrap_or_default(),
            ),
            (
                "is_maker",
                self.is_maker.map(|x| x.to_string()).unwrap_or_default(),
            ),
        ]
    }
}

// -----------------------
// DepthDeltaRow
// -----------------------
impl ToRedisPublish for DepthDeltaRow {
    fn redis_kind(&self) -> StreamKind {
        StreamKind::Depth
    }
    fn redis_exchange(&self) -> &str {
        self.exchange
    }
    fn redis_symbol(&self) -> &str {
        &self.symbol
    }
    fn redis_fields(&self) -> RedisFields {
        vec![
            ("time", self.time.to_rfc3339()),
            ("side", self.side.as_i16().to_string()),
            ("price_i", self.price_i.to_string()),
            ("size_i", self.size_i.to_string()),
            ("seq", self.seq.map(|x| x.to_string()).unwrap_or_default()),
        ]
    }
}

// -----------------------
// FundingRow
// -----------------------
impl ToRedisPublish for FundingRow {
    fn redis_kind(&self) -> StreamKind {
        StreamKind::Funding
    }
    fn redis_exchange(&self) -> &str {
        self.exchange
    }
    fn redis_symbol(&self) -> &str {
        &self.symbol
    }
    fn redis_fields(&self) -> RedisFields {
        vec![
            ("time", self.time.to_rfc3339()),
            ("funding_rate", self.funding_rate.to_string()),
            (
                "funding_time",
                self.funding_time
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_default(),
            ),
        ]
    }
}

// -----------------------
// LiquidationRow
// -----------------------
impl ToRedisPublish for LiquidationRow {
    fn redis_kind(&self) -> StreamKind {
        StreamKind::Liquidations
    }
    fn redis_exchange(&self) -> &str {
        self.exchange
    }
    fn redis_symbol(&self) -> &str {
        &self.symbol
    }
    fn redis_fields(&self) -> RedisFields {
        vec![
            ("time", self.time.to_rfc3339()),
            ("side", self.side.to_string()),
            (
                "price_i",
                self.price_i.map(|x| x.to_string()).unwrap_or_default(),
            ),
            ("qty_i", self.qty_i.to_string()),
            (
                "liq_id",
                self.liq_id.map(|x| x.to_string()).unwrap_or_default(),
            ),
        ]
    }
}

// ------------------------------------------------------------
// Optional thing: MarketEvent -> Option<(kind, ex, sym, fields)>
// ------------------------------------------------------------
impl MarketEvent {
    pub fn as_redis_publish(&self) -> Option<(StreamKind, &str, &str, RedisFields)> {
        match self {
            MarketEvent::Trade(r) => Some((
                r.redis_kind(),
                r.redis_exchange(),
                r.redis_symbol(),
                r.redis_fields(),
            )),
            MarketEvent::DepthDelta(r) => Some((
                r.redis_kind(),
                r.redis_exchange(),
                r.redis_symbol(),
                r.redis_fields(),
            )),
            MarketEvent::OpenInterest(r) => Some((
                r.redis_kind(),
                r.redis_exchange(),
                r.redis_symbol(),
                r.redis_fields(),
            )),
            MarketEvent::Funding(r) => Some((
                r.redis_kind(),
                r.redis_exchange(),
                r.redis_symbol(),
                r.redis_fields(),
            )),
            MarketEvent::Liquidation(r) => Some((
                r.redis_kind(),
                r.redis_exchange(),
                r.redis_symbol(),
                r.redis_fields(),
            )),
        }
    }
}

