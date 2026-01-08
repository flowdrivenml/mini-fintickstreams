use chrono::{DateTime, Utc};

use crate::error::{AppError, AppResult};
use crate::ingest::datamap::ctx::MapCtx;
use crate::ingest::datamap::event::MapEnvelope;
use crate::ingest::datamap::event::{
    BookSide, DepthDeltaRow, FundingRow, MarketEvent, OpenInterestRow, TradeRow, TradeSide,
};
use crate::ingest::datamap::sources::hyperliquid_perp::types::{
    Hyperliquid_book_level, Hyperliquid_levels, HyperliquidPerpDepthSnapshot,
    HyperliquidPerpWsDepthUpdate, HyperliquidPerpWsOIFundingUpdate, HyperliquidPerpWsTrade,
};
use crate::ingest::traits::MapToEvents;

const EXCHANGE: &str = "hyperliquid_perp";

fn ms_to_utc(ms: u64) -> AppResult<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(ms as i64)
        .ok_or_else(|| AppError::Internal(format!("invalid ms timestamp: {ms}")))
}

fn map_book_levels(
    ctx: &MapCtx,
    coin: &str,
    time: DateTime<Utc>,
    seq: Option<i64>,
    side: BookSide,
    levels: &[Hyperliquid_book_level],
) -> AppResult<Vec<MarketEvent>> {
    let mut out = Vec::with_capacity(levels.len());

    for lvl in levels {
        // Hyperliquid: px, sz are strings
        let price_str = &lvl.px;
        let size_str = &lvl.sz;

        let price_i = ctx.price_str_to_i64(price_str)?;

        // If sz == 0 => delete (same meaning as your DB)
        let size_i = if size_str == "0" || size_str == "0.0" {
            0_i64
        } else {
            // Hyperliquid size is base units (per your earlier loader comment),
            // but calling the generic converter keeps future flexibility.
            ctx.book_size_to_base_i64(size_str, price_str)?
        };

        out.push(MarketEvent::DepthDelta(DepthDeltaRow {
            exchange: EXCHANGE,
            time,
            symbol: coin.to_string(),
            side,
            price_i,
            size_i,
            seq,
        }));
    }

    Ok(out)
}

fn map_levels(
    ctx: &MapCtx,
    coin: &str,
    time: DateTime<Utc>,
    seq: Option<i64>,
    levels: &Hyperliquid_levels,
) -> AppResult<Vec<MarketEvent>> {
    // Hyperliquid: levels = [bids, asks]
    let bids = &levels[0];
    let asks = &levels[1];

    let mut out = Vec::with_capacity(bids.len() + asks.len());

    out.extend(map_book_levels(ctx, coin, time, seq, BookSide::Bid, bids)?);
    out.extend(map_book_levels(ctx, coin, time, seq, BookSide::Ask, asks)?);

    Ok(out)
}

//
// -------------------- REST: L2 snapshot -> DepthDelta* --------------------
//
impl MapToEvents for HyperliquidPerpDepthSnapshot {
    fn map_to_events(self, ctx: &MapCtx, _env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let time = ms_to_utc(self.time)?;
        // No canonical sequence in this struct; keep None
        let seq = None;

        map_levels(ctx, &self.coin, time, seq, &self.levels)
    }
}

//
// -------------------- WS: depth update -> DepthDelta* --------------------
//
impl MapToEvents for HyperliquidPerpWsDepthUpdate {
    fn map_to_events(self, ctx: &MapCtx, _env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let d = self.data;

        let time = ms_to_utc(d.time)?;
        let seq = None; // Hyperliquid doesn't provide a simple monotonic id here

        map_levels(ctx, &d.coin, time, seq, &d.levels)
    }
}

//
// -------------------- WS: OI + Funding -> TWO events --------------------
//
impl MapToEvents for HyperliquidPerpWsOIFundingUpdate {
    fn map_to_events(self, ctx: &MapCtx, _env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let coin = self.data.coin;
        let a = self.data.ctx;

        // Hyperliquid WS payload doesn't include a timestamp here; use ingest time.
        let time = ctx.now;

        // OI
        let oi_i = ctx.open_interest_str_to_i64(&a.open_interest)?;

        // Funding: parse funding rate to f64
        let funding_rate = ctx.funding_str_to_i64(&a.funding)?;

        let oi_evt = MarketEvent::OpenInterest(OpenInterestRow {
            exchange: EXCHANGE,
            time,
            symbol: coin.clone(),
            oi_i,
        });

        // funding_time: none (unless you want to derive schedule; keep None for now)
        let funding_evt = MarketEvent::Funding(FundingRow {
            exchange: EXCHANGE,
            time,
            symbol: coin,
            funding_rate,
            funding_time: None,
        });

        Ok(vec![oi_evt, funding_evt])
    }
}

//
// -------------------- WS: trades -> Trade* --------------------
//
impl MapToEvents for HyperliquidPerpWsTrade {
    fn map_to_events(self, ctx: &MapCtx, _env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let mut out = Vec::with_capacity(self.data.len());

        for t in self.data {
            let side = match t.side.as_str() {
                "B" => TradeSide::Buy,
                "S" | "A" => TradeSide::Sell, // Hyperliquid often uses A=ask
                other => {
                    return Err(AppError::Internal(format!(
                        "unknown hyperliquid trade side='{other}'"
                    )));
                }
            };

            let (price_i, qty_i) = ctx.trade_to_scaled_i64(&t.px, &t.sz)?;

            out.push(MarketEvent::Trade(TradeRow {
                exchange: EXCHANGE,
                time: ms_to_utc(t.time)?,
                symbol: t.coin,
                side,
                price_i,
                qty_i,
                trade_id: Some(i64::try_from(t.tid).map_err(|_| {
                    AppError::Internal(format!("hyperliquid tid out of i64 range: {}", t.tid))
                })?),
                is_maker: None, // not present in payload
            }));
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::app::config::load_app_config;
    use crate::error::AppResult;
    use crate::ingest::config::ExchangeConfigs;
    use crate::ingest::instruments::loader::InstrumentSpecLoader;
    use crate::ingest::instruments::registry::InstrumentRegistry;

    use crate::ingest::datamap::ctx::MapCtx;
    use crate::ingest::datamap::event::MarketEvent;
    use crate::ingest::traits::MapToEvents;

    use crate::ingest::datamap::sources::hyperliquid_perp::types::{
        HyperliquidPerpDepthSnapshot, HyperliquidPerpWsDepthUpdate,
        HyperliquidPerpWsOIFundingUpdate, HyperliquidPerpWsTrade,
    };

    fn testdata_path(file_name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("ingest")
            .join("datamap")
            .join("testdata")
            .join(file_name)
    }

    fn load_and_parse<T: DeserializeOwned>(struct_name: &str) -> AppResult<T> {
        let file_name = format!("{struct_name}.json");
        let path = testdata_path(&file_name);

        println!("\n==> checking {}", path.display());

        let s = fs::read_to_string(&path)?;
        println!("    OK: file found");

        let parsed = serde_json::from_str::<T>(&s)?;
        println!("    OK: json parsed successfully");

        Ok(parsed)
    }

    fn preview(label: &str, evs: &[MarketEvent], max: usize) {
        println!("    OK: mapped {label} -> {} event(s)", evs.len());
        for e in evs.iter().take(max) {
            println!("    sample: {:?}", e);
        }
    }

    async fn mk_ctx() -> AppResult<MapCtx> {
        let appconfig = load_app_config()?;
        let exchangeconfigs = ExchangeConfigs::new(&appconfig)?;
        let loader = InstrumentSpecLoader::new(exchangeconfigs, None, None)?;
        let specs = loader.load_all().await?;
        let reg = InstrumentRegistry::build(specs)?;

        // Leak for 'static in tests (fine)
        let reg = Arc::new(reg);
        let exchange = "hyperliquid_perp";
        let symbol = "BTC";

        Ok(MapCtx::new(reg, &appconfig, &exchange, &symbol)?)
    }

    #[tokio::test]
    async fn hyperliquid_map_testdata_maps_without_errors() -> AppResult<()> {
        println!("\n=== Hyperliquid MAP testdata mapping test ===");

        let ctx = mk_ctx().await?;

        // REST depth snapshot
        let snap = load_and_parse::<HyperliquidPerpDepthSnapshot>("HyperliquidPerpDepthSnapshot")?;
        let snap_events = snap.map_to_events(&ctx, None)?;
        // Depth can be big; show only 3
        preview("PerpDepthSnapshot", &snap_events, 3);
        assert!(!snap_events.is_empty());

        // WS depth update
        let du = load_and_parse::<HyperliquidPerpWsDepthUpdate>("HyperliquidPerpWsDepthUpdate")?;
        let du_events = du.map_to_events(&ctx, None)?;
        preview("WsDepthUpdate", &du_events, 3);
        assert!(!du_events.is_empty());

        // WS OI+Funding update (should yield 2 events)
        let of =
            load_and_parse::<HyperliquidPerpWsOIFundingUpdate>("HyperliquidPerpWsOIFundingUpdate")?;
        let of_events = of.map_to_events(&ctx, None)?;
        preview("WsOIFundingUpdate", &of_events, 2);
        assert_eq!(of_events.len(), 2, "expected [OpenInterest, Funding]");

        // WS trades (many)
        let tr = load_and_parse::<HyperliquidPerpWsTrade>("HyperliquidPerpWsTrade")?;
        let tr_events = tr.map_to_events(&ctx, None)?;
        preview("WsTrade", &tr_events, 3);
        assert!(!tr_events.is_empty());

        println!("\n=== ALL Hyperliquid testdata mapped successfully ===");
        Ok(())
    }
}
