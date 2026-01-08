// ingest/exchanges/binance/types.rs
use crate::error::{AppError, AppResult};
use crate::ingest::datamap::traits::FromJsonStr;
use serde::{Deserialize, Deserializer};

fn de_u64_from_str_or_num<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StrOrNum {
        Str(String),
        Num(u64),
    }

    match StrOrNum::deserialize(d)? {
        StrOrNum::Num(n) => Ok(n),
        StrOrNum::Str(s) => s.parse::<u64>().map_err(serde::de::Error::custom),
    }
}

pub type PriceLevel = [String; 2];

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,

    /// Event time (ms)
    #[serde(rename = "E")]
    pub event_time_ms: u64,

    /// Transaction time (ms)
    #[serde(rename = "T")]
    pub transact_time_ms: u64,

    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

impl FromJsonStr for BinanceLinearDepthSnapshot {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

//
// ---- REST: Funding Rate (list) ----
//
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearFundingRateSnapshot {
    pub symbol: String,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    #[serde(rename = "fundingTime")]
    pub funding_time_ms: u64,
    #[serde(rename = "markPrice")]
    pub mark_price: String,
}

impl FromJsonStr for BinanceLinearFundingRateSnapshot {
    fn from_json_str(s: &str) -> AppResult<Self> {
        let mut v: Vec<BinanceLinearFundingRateSnapshot> =
            serde_json::from_str(s).map_err(AppError::Json)?;

        match v.len() {
            1 => Ok(v.remove(0)),
            0 => Err(AppError::InvalidConfig(
                "funding_rate response was empty".into(),
            )),
            n => Err(AppError::InvalidConfig(format!(
                "funding_rate response contained {n} entries, expected exactly 1"
            ))),
        }
    }
}

//
// ---- REST: Global Long/Short Account Ratio (list) ----
//
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearGlobalLongShortAccountSnapshot {
    pub symbol: String,
    #[serde(rename = "longShortRatio")]
    pub long_short_ratio: String,
    #[serde(rename = "longAccount")]
    pub long_account: String,
    #[serde(rename = "shortAccount")]
    pub short_account: String,
    pub timestamp: String, // sample shows string
}

impl FromJsonStr for Vec<BinanceLinearGlobalLongShortAccountSnapshot> {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

//
// ---- REST: Exchange Info ----
//
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearExchangeInfoSnapshot {
    #[serde(rename = "exchangeFilters")]
    pub exchange_filters: Vec<serde_json::Value>,
    #[serde(rename = "rateLimits")]
    pub rate_limits: Vec<BinanceLinearRateLimit>,
    #[serde(rename = "serverTime")]
    pub server_time_ms: u64,
    pub assets: Vec<BinanceLinearAssetInfo>,
    pub symbols: Vec<BinanceLinearSymbolInfo>,
    pub timezone: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearRateLimit {
    pub interval: String,
    #[serde(rename = "intervalNum")]
    pub interval_num: u64,
    pub limit: u64,
    #[serde(rename = "rateLimitType")]
    pub rate_limit_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearAssetInfo {
    pub asset: String,
    #[serde(rename = "marginAvailable")]
    pub margin_available: bool,
    #[serde(rename = "autoAssetExchange")]
    pub auto_asset_exchange: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearSymbolInfo {
    pub symbol: String,
    pub pair: String,

    #[serde(rename = "contractType")]
    pub contract_type: String,

    #[serde(rename = "deliveryDate")]
    pub delivery_date_ms: u64,

    #[serde(rename = "onboardDate")]
    pub onboard_date_ms: u64,

    pub status: String,

    // These are strings in Binance payloads (keep as String; parse later if needed)
    #[serde(rename = "maintMarginPercent")]
    pub maint_margin_percent: String,

    #[serde(rename = "requiredMarginPercent")]
    pub required_margin_percent: String,

    #[serde(rename = "baseAsset")]
    pub base_asset: String,

    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,

    #[serde(rename = "marginAsset")]
    pub margin_asset: String,

    // Binance uses ints here (usually), but keep u32/u64 either is fine
    #[serde(rename = "pricePrecision")]
    pub price_precision: u32,

    #[serde(rename = "quantityPrecision")]
    pub quantity_precision: u32,

    #[serde(rename = "baseAssetPrecision")]
    pub base_asset_precision: u32,

    #[serde(rename = "quotePrecision")]
    pub quote_precision: u32,

    #[serde(rename = "underlyingType")]
    pub underlying_type: String,

    // Sometimes absent or empty; default avoids failures
    #[serde(rename = "underlyingSubType", default)]
    pub underlying_sub_type: Vec<String>,

    // Often missing in exchangeInfo; make it optional
    #[serde(rename = "settlePlan", default)]
    pub settle_plan: Option<i64>,

    #[serde(rename = "triggerProtect")]
    pub trigger_protect: String,

    // These show in your JSON snippet; keeping them is helpful and safe
    #[serde(rename = "liquidationFee")]
    pub liquidation_fee: String,

    #[serde(rename = "marketTakeBound")]
    pub market_take_bound: String,

    // Optional-ish in some payloads; default gives 0 if missing
    #[serde(rename = "maxMoveOrderLimit", default)]
    pub max_move_order_limit: u64,

    // Filters is present, but default makes it resilient if missing
    #[serde(default)]
    pub filters: Vec<BinanceLinearSymbolFilter>,

    #[serde(rename = "orderTypes", default)]
    pub order_types: Vec<String>,

    #[serde(rename = "timeInForce", default)]
    pub time_in_force: Vec<String>,

    // Present in your JSON; default makes it resilient
    #[serde(rename = "permissionSets", default)]
    pub permission_sets: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "filterType")]
pub enum BinanceLinearSymbolFilter {
    #[serde(rename = "PRICE_FILTER")]
    PriceFilter {
        #[serde(rename = "maxPrice")]
        max_price: String,
        #[serde(rename = "minPrice")]
        min_price: String,
        #[serde(rename = "tickSize")]
        tick_size: String,
    },
    #[serde(rename = "LOT_SIZE")]
    LotSize {
        #[serde(rename = "maxQty")]
        max_qty: String,
        #[serde(rename = "minQty")]
        min_qty: String,
        #[serde(rename = "stepSize")]
        step_size: String,
    },
    #[serde(rename = "MARKET_LOT_SIZE")]
    MarketLotSize {
        #[serde(rename = "maxQty")]
        max_qty: String,
        #[serde(rename = "minQty")]
        min_qty: String,
        #[serde(rename = "stepSize")]
        step_size: String,
    },
    #[serde(rename = "MAX_NUM_ORDERS")]
    MaxNumOrders { limit: u64 },
    #[serde(rename = "MAX_NUM_ALGO_ORDERS")]
    MaxNumAlgoOrders { limit: u64 },
    #[serde(rename = "MIN_NOTIONAL")]
    MinNotional { notional: String },
    #[serde(rename = "PERCENT_PRICE")]
    PercentPrice {
        #[serde(rename = "multiplierUp")]
        multiplier_up: String,
        #[serde(rename = "multiplierDown")]
        multiplier_down: String,
        #[serde(
            rename = "multiplierDecimal",
            deserialize_with = "de_u64_from_str_or_num"
        )]
        multiplier_decimal: u64,
    },
    #[serde(other)]
    Unknown,
}

impl FromJsonStr for BinanceLinearExchangeInfoSnapshot {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

//
// ---- REST: Open Interest ----
//
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearOpenInterestSnapshot {
    #[serde(rename = "openInterest")]
    pub open_interest: String,
    pub symbol: String,
    pub time: u64,
}

impl FromJsonStr for BinanceLinearOpenInterestSnapshot {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

//
// ---- REST: Top Traders Accounts (list) ----
//
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearTopTradersAccountsSnapshot {
    pub symbol: String,
    #[serde(rename = "longShortRatio")]
    pub long_short_ratio: String,
    #[serde(rename = "longAccount")]
    pub long_account: String,
    #[serde(rename = "shortAccount")]
    pub short_account: String,
    pub timestamp: String,
}

impl FromJsonStr for Vec<BinanceLinearTopTradersAccountsSnapshot> {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

//
// ---- REST: Top Traders Positions (list) ----
//
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearTopTradersPositionsSnapshot {
    pub symbol: String,
    #[serde(rename = "longShortRatio")]
    pub long_short_ratio: String,
    #[serde(rename = "longAccount")]
    pub long_account: String,
    #[serde(rename = "shortAccount")]
    pub short_account: String,
    pub timestamp: String,
}

impl FromJsonStr for Vec<BinanceLinearTopTradersPositionsSnapshot> {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}
//
// ---- WS: Depth Update ----
//
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearWsDepthUpdate {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time_ms: u64,
    #[serde(rename = "T")]
    pub transact_time_ms: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub final_update_id: u64,
    #[serde(rename = "pu")]
    pub prev_final_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<PriceLevel>,
    #[serde(rename = "a")]
    pub asks: Vec<PriceLevel>,
}

impl FromJsonStr for BinanceLinearWsDepthUpdate {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}
//
// ---- WS: Liquidation (forceOrder) ----
//
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearWsForceOrder {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time_ms: u64,
    #[serde(rename = "o")]
    pub order: BinanceLinearForceOrder,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearForceOrder {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "o")]
    pub order_type: String,
    #[serde(rename = "f")]
    pub time_in_force: String,
    #[serde(rename = "q")]
    pub qty: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "ap")]
    pub avg_price: String,
    #[serde(rename = "X")]
    pub status: String,
    #[serde(rename = "l")]
    pub last_filled_qty: String,
    #[serde(rename = "z")]
    pub cum_filled_qty: String,
    #[serde(rename = "T")]
    pub trade_time_ms: u64,
}

impl FromJsonStr for BinanceLinearWsForceOrder {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}
//
// ---- WS: Agg Trade ----
//
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceLinearWsAggTrade {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time_ms: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "a")]
    pub agg_trade_id: u64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub qty: String,
    #[serde(rename = "f")]
    pub first_trade_id: u64,
    #[serde(rename = "l")]
    pub last_trade_id: u64,
    #[serde(rename = "T")]
    pub trade_time_ms: u64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

impl FromJsonStr for BinanceLinearWsAggTrade {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use std::fs;
    use std::path::PathBuf;

    fn testdata_path(file_name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("ingest")
            .join("datamap")
            .join("testdata")
            .join(file_name)
    }

    fn load_and_parse<T: DeserializeOwned>(struct_name: &str) -> bool {
        let file_name = format!("{struct_name}.json");
        let path = testdata_path(&file_name);

        println!("\n==> checking {}", path.display());

        let s = match fs::read_to_string(&path) {
            Ok(s) => {
                println!("    OK: file found");
                s
            }
            Err(e) => {
                println!("    ERROR: missing or unreadable file ({})", e);
                return false;
            }
        };

        match serde_json::from_str::<T>(&s) {
            Ok(_) => {
                println!("    OK: json parsed successfully");
                true
            }
            Err(e) => {
                println!("    ERROR: json parse failed: {}", e);
                false
            }
        }
    }

    #[test]
    fn binance_testdata_json_parses() {
        println!("\n=== Binance JSON testdata parse test ===");

        let mut ok = true;

        // ---- Depth snapshot ----
        ok &= load_and_parse::<BinanceLinearDepthSnapshot>("BinanceLinearDepthSnapshot");

        // ---- REST lists ----
        ok &= load_and_parse::<Vec<BinanceLinearFundingRateSnapshot>>(
            "BinanceLinearFundingRateSnapshot",
        );
        ok &= load_and_parse::<Vec<BinanceLinearGlobalLongShortAccountSnapshot>>(
            "BinanceLinearGlobalLongShortAccountSnapshot",
        );
        ok &= load_and_parse::<Vec<BinanceLinearTopTradersAccountsSnapshot>>(
            "BinanceLinearTopTradersAccountsSnapshot",
        );
        ok &= load_and_parse::<Vec<BinanceLinearTopTradersPositionsSnapshot>>(
            "BinanceLinearTopTradersPositionsSnapshot",
        );

        // ---- REST single objects ----
        ok &= load_and_parse::<BinanceLinearExchangeInfoSnapshot>(
            "BinanceLinearExchangeInfoSnapshot",
        );
        ok &= load_and_parse::<BinanceLinearOpenInterestSnapshot>(
            "BinanceLinearOpenInterestSnapshot",
        );

        // ---- WS ----
        ok &= load_and_parse::<BinanceLinearWsDepthUpdate>("BinanceLinearWsDepthUpdate");
        ok &= load_and_parse::<BinanceLinearWsForceOrder>("BinanceLinearWsForceOrder");
        ok &= load_and_parse::<BinanceLinearWsAggTrade>("BinanceLinearWsAggTrade");

        if ok {
            println!("\n=== ALL Binance testdata JSON parsed successfully ===");
        } else {
            println!("\n=== Binance testdata JSON ERRORS detected ===");
            panic!("one or more Binance testdata files missing or failed to parse");
        }
    }
}
