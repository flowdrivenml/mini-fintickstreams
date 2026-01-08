use chrono::{DateTime, Utc};

use crate::error::{AppError, AppResult};
use crate::ingest::datamap::ctx::MapCtx;
use crate::ingest::datamap::event::{
    BookSide, DepthDeltaRow, FundingRow, LiquidationRow, MarketEvent, OpenInterestRow, TradeRow,
    TradeSide,
};
use crate::ingest::traits::MapToEvents;

use crate::ingest::datamap::sources::binance_linear::types::{
    BinanceLinearDepthSnapshot, BinanceLinearFundingRateSnapshot,
    BinanceLinearOpenInterestSnapshot, BinanceLinearWsAggTrade, BinanceLinearWsDepthUpdate,
    BinanceLinearWsForceOrder, PriceLevel,
};

use crate::ingest::datamap::event::MapEnvelope;

const EXCHANGE: &str = "binance_linear";

fn ms_to_utc(ms: u64) -> AppResult<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(ms as i64)
        .ok_or_else(|| AppError::Internal(format!("invalid ms timestamp: {ms}")))
}

fn map_levels_to_depth_events(
    ctx: &MapCtx,
    symbol: &str,
    time: DateTime<Utc>,
    seq: Option<i64>,
    side: BookSide,
    levels: Vec<PriceLevel>,
    is_snapshot: bool,
) -> AppResult<Vec<MarketEvent>> {
    let mut out = Vec::with_capacity(levels.len());

    for lvl in levels {
        // lvl = [price_str, qty_str]
        let price_str = &lvl[0];
        let size_str = &lvl[1];

        let price_i = ctx.price_str_to_i64(price_str)?;
        let size_i = if size_str == "0" || size_str == "0.0" {
            0_i64
        } else {
            // convert reported size to BASE using price, then scale to qty_scale
            ctx.book_size_to_base_i64(size_str, price_str)?
        };

        out.push(MarketEvent::DepthDelta(DepthDeltaRow {
            exchange: EXCHANGE,
            time,
            symbol: symbol.to_string(),
            side,
            price_i,
            size_i,
            seq,
        }));
    }

    // `is_snapshot` is not stored in DB row; if you need it later,
    // you can either add it to DepthDeltaRow or emit a synthetic marker event.
    let _ = is_snapshot;

    Ok(out)
}

//
// -------------------- WS: Agg Trade -> MarketEvent::Trade --------------------
//
impl MapToEvents for BinanceLinearWsAggTrade {
    fn map_to_events(self, ctx: &MapCtx, env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        // Binance semantics: m = buyer is maker => seller is taker => taker-side = SELL
        let side = if self.is_buyer_maker {
            TradeSide::Sell
        } else {
            TradeSide::Buy
        };

        let (price_i, qty_i) = ctx.trade_to_scaled_i64(&self.price, &self.qty)?;

        let row = TradeRow {
            exchange: EXCHANGE,
            time: ms_to_utc(self.trade_time_ms)?,
            symbol: self.symbol,
            side,
            price_i,
            qty_i,
            trade_id: Some(self.agg_trade_id as i64),
            is_maker: Some(self.is_buyer_maker),
        };

        Ok(vec![MarketEvent::Trade(row)])
    }
}

//
// -------------------- WS: Depth Update -> MarketEvent::DepthDelta* --------------------
//
impl MapToEvents for BinanceLinearWsDepthUpdate {
    fn map_to_events(self, ctx: &MapCtx, env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        // Use transact_time_ms (T) as closer-to-book-apply time; event_time_ms is also fine.
        let time = ms_to_utc(self.transact_time_ms)?;
        let seq = Some(self.final_update_id as i64);

        let mut out = Vec::with_capacity(self.bids.len() + self.asks.len());

        out.extend(map_levels_to_depth_events(
            ctx,
            &self.symbol,
            time,
            seq,
            BookSide::Bid,
            self.bids,
            false,
        )?);

        out.extend(map_levels_to_depth_events(
            ctx,
            &self.symbol,
            time,
            seq,
            BookSide::Ask,
            self.asks,
            false,
        )?);

        Ok(out)
    }
}

//
// -------------------- REST: Depth Snapshot -> MarketEvent::DepthDelta* --------------------
//
// This maps snapshot levels as "set this level to size" (size_i=0 means delete).
// DB doesn't store `is_snapshot`, but these rows are still useful to seed state if you replay.
//
impl MapToEvents for BinanceLinearDepthSnapshot {
    fn map_to_events(self, ctx: &MapCtx, env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let env = env.ok_or_else(|| {
            AppError::Internal(
                "BinanceLinearDepthSnapshot requires MapEnvelope with symbol".to_string(),
            )
        })?;

        let symbol = env.symbol.ok_or_else(|| {
            AppError::Internal(
                "MapEnvelope.symbol is required for BinanceLinearDepthSnapshot".to_string(),
            )
        })?;

        let time = ms_to_utc(self.transact_time_ms)?;
        let seq = Some(self.last_update_id as i64);

        let mut out = Vec::new();

        out.extend(map_levels_to_depth_events(
            ctx,
            symbol.as_str(),
            time,
            seq,
            BookSide::Bid,
            self.bids,
            true,
        )?);

        out.extend(map_levels_to_depth_events(
            ctx,
            symbol.as_str(),
            time,
            seq,
            BookSide::Ask,
            self.asks,
            true,
        )?);

        Ok(out)
    }
}

/// Helper for depth snapshot mapping because the REST snapshot struct lacks a symbol field.
pub fn map_depth_snapshot(
    ctx: &MapCtx,
    symbol: &str,
    snapshot: BinanceLinearDepthSnapshot,
) -> AppResult<Vec<MarketEvent>> {
    let time = ms_to_utc(snapshot.transact_time_ms)?;
    let seq = Some(snapshot.last_update_id as i64);

    let mut out = Vec::with_capacity(snapshot.bids.len() + snapshot.asks.len());

    out.extend(map_levels_to_depth_events(
        ctx,
        symbol,
        time,
        seq,
        BookSide::Bid,
        snapshot.bids,
        true,
    )?);

    out.extend(map_levels_to_depth_events(
        ctx,
        symbol,
        time,
        seq,
        BookSide::Ask,
        snapshot.asks,
        true,
    )?);

    Ok(out)
}

//
// -------------------- REST: Open Interest -> MarketEvent::OpenInterest --------------------
//
impl MapToEvents for BinanceLinearOpenInterestSnapshot {
    fn map_to_events(self, ctx: &MapCtx, env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let time = ms_to_utc(self.time)?;
        let oi_i = ctx.open_interest_str_to_i64(&self.open_interest)?;

        Ok(vec![MarketEvent::OpenInterest(OpenInterestRow {
            exchange: EXCHANGE,
            time,
            symbol: self.symbol,
            oi_i,
        })])
    }
}

//
// -------------------- REST: Funding Rate list -> MarketEvent::Funding* --------------------
//
impl MapToEvents for BinanceLinearFundingRateSnapshot {
    fn map_to_events(self, ctx: &MapCtx, _env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let funding_time = ms_to_utc(self.funding_time_ms)?;

        let _rate = self
            .funding_rate
            .parse::<f64>()
            .map_err(|e| AppError::Internal(format!("funding_rate parse failed: {e}")))?;

        Ok(vec![MarketEvent::Funding(FundingRow {
            exchange: EXCHANGE,
            time: ctx.now,
            symbol: self.symbol,
            funding_rate: ctx.funding_str_to_i64(&self.funding_rate)?,
            funding_time: Some(funding_time),
        })])
    }
}

impl MapToEvents for Vec<BinanceLinearFundingRateSnapshot> {
    fn map_to_events(self, ctx: &MapCtx, env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let mut out = Vec::with_capacity(self.len());
        for snap in self {
            out.extend(snap.map_to_events(ctx, env.clone())?);
        }
        Ok(out)
    }
}

//
// -------------------- WS: Liquidation (forceOrder) -> MarketEvent::Liquidation --------------------
//
impl MapToEvents for BinanceLinearWsForceOrder {
    fn map_to_events(self, ctx: &MapCtx, env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let o = self.order;

        let time = ms_to_utc(o.trade_time_ms)?;

        // Your LiquidationRow uses `side: i16` ("your convention").
        // We'll map Binance "S" = BUY/SELL to 0/1 like trades.
        let side_i16 = match o.side.as_str() {
            "BUY" => 0,
            "SELL" => 1,
            other => {
                return Err(AppError::Internal(format!(
                    "unknown Binance forceOrder side='{other}'"
                )));
            }
        };

        // Price fields can be "0" in some liquidation messages. Keep price_i optional.
        let price_i = if o.price == "0" || o.price == "0.0" {
            None
        } else {
            Some(ctx.price_str_to_i64(&o.price)?)
        };

        // Qty: Binance gives `q` (order qty). For liquidation size, many use `z` (cum filled).
        // Choose one. Here we prefer `z` if present/non-zero, else `q`.
        let qty_str = if o.cum_filled_qty != "0" && o.cum_filled_qty != "0.0" {
            &o.cum_filled_qty
        } else {
            &o.qty
        };

        // Convert to BASE-scaled qty. We need a price for Quote/Inverse conversions.
        // If price is missing/zero, fall back to using avg_price if available.
        let price_for_qty = if o.price != "0" && o.price != "0.0" {
            &o.price
        } else {
            &o.avg_price
        };

        let qty_i = ctx.book_size_to_base_i64(qty_str, price_for_qty)?;

        Ok(vec![MarketEvent::Liquidation(LiquidationRow {
            exchange: EXCHANGE,
            time,
            symbol: o.symbol,
            side: side_i16,
            price_i,
            qty_i,
            liq_id: None, // Binance forceOrder doesn't provide an obvious numeric id in this struct
        })])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use std::fs;
    use std::path::PathBuf;

    use crate::app::config::load_app_config;
    use crate::error::AppResult;
    use crate::ingest::config::ExchangeConfigs;
    use crate::ingest::instruments::loader::InstrumentSpecLoader;
    use crate::ingest::instruments::registry::InstrumentRegistry;

    use std::sync::Arc;

    use crate::ingest::datamap::ctx::MapCtx;
    use crate::ingest::datamap::event::MapEnvelope;
    use crate::ingest::datamap::event::MarketEvent;
    use crate::ingest::traits::MapToEvents;

    use crate::ingest::datamap::sources::binance_linear::types::{
        BinanceLinearDepthSnapshot, BinanceLinearFundingRateSnapshot,
        BinanceLinearOpenInterestSnapshot, BinanceLinearWsAggTrade, BinanceLinearWsDepthUpdate,
        BinanceLinearWsForceOrder,
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

    fn preview_events(label: &str, events: &[MarketEvent]) {
        println!(
            "    OK: mapped to {} MarketEvent(s) for {}",
            events.len(),
            label
        );
        if let Some(first) = events.first() {
            println!("    preview first event: {:?}", first);
        }
    }

    /// Creates a real ctx with a real instrument registry (so `ctx.instrument(...)` works).
    // Load real instruments (same approach you used in your loader tests)
    async fn mk_ctx() -> AppResult<MapCtx> {
        let appconfig = load_app_config()?;
        let exchangeconfigs = ExchangeConfigs::new(&appconfig)?;
        let loader = InstrumentSpecLoader::new(exchangeconfigs, None, None)?;
        let specs = loader.load_all().await?;

        // Registry disallows duplicates
        let reg = InstrumentRegistry::build(specs)?;

        // We need to extend lifetime to 'static for this test helper.
        // Simplest: leak the registry for test scope.
        let reg = Arc::new(reg);
        let exchange = "binance_linear";
        let symbol = "BTCUSDT";

        use crate::app::config::load_app_config;
        let appconfig = load_app_config()?;

        Ok(MapCtx::new(reg, &appconfig, &exchange, &symbol)?)
    }

    #[tokio::test]
    async fn binance_map_testdata_maps_without_errors() -> AppResult<()> {
        println!("\n=== Binance MAP testdata mapping test ===");

        let ctx = mk_ctx().await?;

        // -------------------------
        // Depth snapshot (REST)
        // -------------------------
        let depth_snapshot =
            load_and_parse::<BinanceLinearDepthSnapshot>("BinanceLinearDepthSnapshot")?;

        // Depth snapshot JSON has no symbol; we must provide it.
        // Try to infer a reasonable symbol from the registry by choosing the first binance_linear instrument.
        // If you prefer, hardcode "BTCUSDT" if your testdata is always that.
        let snapshot_symbol = "BTCUSDT";

        let env = MapEnvelope {
            exchange: "binance_linear".to_string(),
            symbol: Some(snapshot_symbol.to_string()),
        };

        let depth_events = depth_snapshot.map_to_events(&ctx, Some(env))?;
        // Depth snapshots can be huge, so just print counts + first few.
        println!(
            "    DepthSnapshot mapped events: {} (showing up to 3)",
            depth_events.len()
        );
        for e in depth_events.iter().take(3) {
            println!("    depth sample: {:?}", e);
        }
        assert!(
            !depth_events.is_empty(),
            "expected depth snapshot to map to events"
        );

        // -------------------------
        // Funding rate list (REST)
        // -------------------------
        let funding_list =
            load_and_parse::<BinanceLinearFundingRateSnapshot>("BinanceLinearFundingRateSnapshot")?;
        let funding_events = funding_list.map_to_events(&ctx, None)?;
        preview_events("FundingRateSnapshot(list)", &funding_events);
        assert!(!funding_events.is_empty(), "expected funding list to map");

        // -------------------------
        // Open interest (REST)
        // -------------------------
        let oi = load_and_parse::<BinanceLinearOpenInterestSnapshot>(
            "BinanceLinearOpenInterestSnapshot",
        )?;
        let oi_events = oi.map_to_events(&ctx, None)?;
        preview_events("OpenInterestSnapshot", &oi_events);
        assert_eq!(oi_events.len(), 1, "open interest should map to 1 event");

        // -------------------------
        // WS depth update
        // -------------------------
        let ws_depth = load_and_parse::<BinanceLinearWsDepthUpdate>("BinanceLinearWsDepthUpdate")?;
        let ws_depth_events = ws_depth.map_to_events(&ctx, None)?;
        println!(
            "    WsDepthUpdate mapped events: {} (showing up to 3)",
            ws_depth_events.len()
        );
        for e in ws_depth_events.iter().take(3) {
            println!("    ws depth sample: {:?}", e);
        }
        assert!(
            !ws_depth_events.is_empty(),
            "expected ws depth update to map to events"
        );

        // -------------------------
        // WS liquidation (forceOrder)
        // -------------------------
        let liq = load_and_parse::<BinanceLinearWsForceOrder>("BinanceLinearWsForceOrder")?;
        let liq_events = liq.map_to_events(&ctx, None)?;
        preview_events("WsForceOrder", &liq_events);
        assert_eq!(liq_events.len(), 1, "forceOrder should map to 1 event");

        // -------------------------
        // WS agg trade
        // -------------------------
        let tr = load_and_parse::<BinanceLinearWsAggTrade>("BinanceLinearWsAggTrade")?;
        let trade_events = tr.map_to_events(&ctx, None)?;
        preview_events("WsAggTrade", &trade_events);
        assert_eq!(trade_events.len(), 1, "aggTrade should map to 1 event");

        println!("\n=== ALL Binance testdata mapped successfully ===");
        Ok(())
    }
}
