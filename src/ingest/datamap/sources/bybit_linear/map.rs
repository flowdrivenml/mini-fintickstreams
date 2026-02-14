// ingest/exchanges/bybit/map.rs
use chrono::{DateTime, Utc};

use crate::error::{AppError, AppResult};
use crate::ingest::datamap::ctx::MapCtx;
use crate::ingest::datamap::event::MapEnvelope;
use crate::ingest::datamap::event::{
    BookSide, DepthDeltaRow, FundingRow, LiquidationRow, MarketEvent, OpenInterestRow, TradeRow,
    TradeSide,
};
use crate::ingest::traits::MapToEvents;

use crate::ingest::datamap::sources::bybit_linear::types::{
    BybitLinearDepthSnapshot, BybitLinearWsDepthUpdate, BybitLinearWsLiquidation,
    BybitLinearWsOIFundingUpdate, BybitLinearWsTrade, PriceLevel,
};

const EXCHANGE: &str = "bybit_linear";

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
        let price_str = &lvl[0];
        let size_str = &lvl[1];

        let price_i = ctx.price_str_to_i64(price_str)?;
        let size_i = if size_str == "0" || size_str == "0.0" {
            0_i64
        } else {
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

    let _ = is_snapshot;
    Ok(out)
}

//
// -------------------- WS: Trades -> MarketEvent::Trade* --------------------
//
impl MapToEvents for BybitLinearWsTrade {
    fn map_to_events(self, ctx: &MapCtx, _env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let mut out = Vec::with_capacity(self.data.len());

        for t in self.data {
            let side = match t.side.as_str() {
                "Buy" => TradeSide::Buy,
                "Sell" => TradeSide::Sell,
                other => {
                    return Err(AppError::Internal(format!(
                        "unknown Bybit trade side='{other}'"
                    )));
                }
            };

            let (price_i, qty_i) = ctx.trade_to_scaled_i64(&t.price, &t.qty)?;

            out.push(MarketEvent::Trade(TradeRow {
                exchange: EXCHANGE,
                time: ms_to_utc(t.trade_time_ms)?,
                symbol: t.symbol,
                side,
                price_i,
                qty_i,
                trade_id: None, // Bybit trade id is UUID string
                is_maker: None, // not provided here
            }));
        }

        Ok(out)
    }
}

//
// -------------------- WS: Depth Update -> MarketEvent::DepthDelta* --------------------
//
impl MapToEvents for BybitLinearWsDepthUpdate {
    fn map_to_events(self, ctx: &MapCtx, _env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        // Bybit provides both ts and cts; ts is the event time, cts is cross timestamp.
        // Either works; using ts keeps it consistent with other Bybit WS payloads.
        let time = ms_to_utc(self.ts)?;
        let seq = Some(self.data.seq as i64);

        let mut out = Vec::with_capacity(self.data.bids.len() + self.data.asks.len());

        out.extend(map_levels_to_depth_events(
            ctx,
            &self.data.symbol,
            time,
            seq,
            BookSide::Bid,
            self.data.bids,
            self.update_type == "snapshot",
        )?);

        out.extend(map_levels_to_depth_events(
            ctx,
            &self.data.symbol,
            time,
            seq,
            BookSide::Ask,
            self.data.asks,
            self.update_type == "snapshot",
        )?);

        Ok(out)
    }
}

//
// -------------------- REST: Depth Snapshot -> MarketEvent::DepthDelta* --------------------
//
impl MapToEvents for BybitLinearDepthSnapshot {
    fn map_to_events(self, ctx: &MapCtx, _env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let time = ms_to_utc(self.ts_ms)?;
        let seq = Some(self.seq as i64);

        let mut out = Vec::with_capacity(self.bids.len() + self.asks.len());

        out.extend(map_levels_to_depth_events(
            ctx,
            &self.symbol,
            time,
            seq,
            BookSide::Bid,
            self.bids,
            true,
        )?);

        out.extend(map_levels_to_depth_events(
            ctx,
            &self.symbol,
            time,
            seq,
            BookSide::Ask,
            self.asks,
            true,
        )?);

        Ok(out)
    }
}

/// Helper (optional) if you want the same call pattern as Binance.
pub fn map_depth_snapshot(
    ctx: &MapCtx,
    snapshot: BybitLinearDepthSnapshot,
) -> AppResult<Vec<MarketEvent>> {
    snapshot.map_to_events(ctx, None)
}

//
// -------------------- WS: OI + Funding (tickers) -> MarketEvent::{OpenInterest, Funding}* --------------------
//
impl MapToEvents for BybitLinearWsOIFundingUpdate {
    fn map_to_events(self, ctx: &MapCtx, _env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let time = ms_to_utc(self.ts)?;

        let mut out = Vec::new();

        // ---- Open Interest ----
        if let Some(oi_str) = self.data.openInterest.as_deref() {
            let oi_i = ctx.open_interest_str_to_i64(oi_str)?;
            out.push(MarketEvent::OpenInterest(OpenInterestRow {
                exchange: EXCHANGE,
                time,
                symbol: self.data.symbol.clone(),
                oi_i,
            }));
        }

        // ---- Funding ----
        if let Some(fr_str) = self.data.fundingRate.as_deref() {
            let funding_time = match self.data.nextFundingTime.as_deref() {
                Some(ms_str) if !ms_str.is_empty() => {
                    let ms = ms_str.parse::<u64>().map_err(|e| {
                        AppError::Internal(format!("nextFundingTime parse failed: {e}"))
                    })?;
                    Some(ms_to_utc(ms)?)
                }
                _ => None,
            };

            out.push(MarketEvent::Funding(FundingRow {
                exchange: EXCHANGE,
                time, // use ticker event time
                symbol: self.data.symbol.clone(),
                funding_rate: ctx.funding_str_to_i64(fr_str)?,
                funding_time,
            }));
        }

        Ok(out)
    }
}

//
// -------------------- WS: Liquidation (allLiquidation) -> MarketEvent::Liquidation* --------------------
//
impl MapToEvents for BybitLinearWsLiquidation {
    fn map_to_events(self, ctx: &MapCtx, _env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>> {
        let mut out = Vec::with_capacity(self.data.len());

        for l in self.data {
            let time = ms_to_utc(l.trade_time_ms)?;

            let side_i16 = match l.side.as_str() {
                "Buy" => 0,
                "Sell" => 1,
                other => {
                    return Err(AppError::Internal(format!(
                        "unknown Bybit liquidation side='{other}'"
                    )));
                }
            };

            let price_i = if l.price == "0" || l.price == "0.0" {
                None
            } else {
                Some(ctx.price_str_to_i64(&l.price)?)
            };

            // Convert to BASE-scaled qty (needs price)
            let qty_i = ctx.book_size_to_base_i64(&l.qty, &l.price)?;

            out.push(MarketEvent::Liquidation(LiquidationRow {
                exchange: EXCHANGE,
                time,
                symbol: l.symbol,
                side: side_i16,
                price_i,
                qty_i,
                liq_id: None,
            }));
        }

        Ok(out)
    }
}
