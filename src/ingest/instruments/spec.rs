use crate::error::{AppError, AppResult};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum InstrumentKind {
    Spot,
    PerpLinear,
    PerpInverse,
    FutureLinear,
    FutureInverse,
    Options,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum QtyUnit {
    Base,      // exchange reports size in base
    Quote,     // exchange reports quote notional
    Contracts, // exchange reports number of contracts
}

#[derive(Debug, Clone, Serialize)]
pub struct InstrumentSpec {
    pub exchange: &'static str,
    pub symbol: String,
    pub kind: InstrumentKind,
    /// What the exchange reports as quantity
    pub reported_qty_unit: QtyUnit,
    /// Contract size meaning (only used when reported_qty_unit != Base):
    /// - Linear (PerpLinear/FutureLinear): BASE per contract
    /// - Inverse (PerpInverse/FutureInverse): QUOTE per contract
    pub contract_size: Option<f64>,
    pub delivery_date_ms: Option<u64>,
    pub onboard_date_ms: Option<u64>,
}

impl InstrumentSpec {
    /// Create a new InstrumentSpec with validation.
    pub fn new(
        exchange: &'static str,
        symbol: impl Into<String>,
        kind: InstrumentKind,
        reported_qty_unit: QtyUnit,
        contract_size: Option<f64>,
        delivery_date_ms: Option<u64>,
        onboard_date_ms: Option<u64>,
    ) -> AppResult<Self> {
        // Validate quantity semantics
        match reported_qty_unit {
            QtyUnit::Base => {
                // always valid
            }
            QtyUnit::Quote => {
                // convertible to base using price
            }
            QtyUnit::Contracts => match kind {
                InstrumentKind::PerpLinear
                | InstrumentKind::PerpInverse
                | InstrumentKind::FutureLinear
                | InstrumentKind::FutureInverse
                | InstrumentKind::Options => {
                    if contract_size.is_none() {
                        return Err(AppError::InvalidConfig(
                            "contract_size must be provided when reported_qty_unit=Contracts"
                                .to_string(),
                        ));
                    }
                }
                InstrumentKind::Spot => {
                    return Err(AppError::InvalidConfig(
                        "reported_qty_unit=Contracts is invalid for Spot instruments".to_string(),
                    ));
                }
            },
        }

        // Validate time semantics
        match kind {
            InstrumentKind::FutureLinear | InstrumentKind::FutureInverse => {
                if delivery_date_ms.is_none() {
                    return Err(AppError::InvalidConfig(
                        "delivery_date_ms must be provided for futures instruments".to_string(),
                    ));
                }
            }

            InstrumentKind::Options => {
                if delivery_date_ms.is_none() {
                    return Err(AppError::InvalidConfig(
                        "delivery_date_ms (expiry) must be provided for options instruments"
                            .to_string(),
                    ));
                }
            }

            InstrumentKind::Spot | InstrumentKind::PerpLinear | InstrumentKind::PerpInverse => {
                // delivery_date_ms should generally be None, but we don't enforce hard errors
            }
        }

        Ok(Self {
            exchange,
            symbol: symbol.into(),
            kind,
            reported_qty_unit,
            contract_size,
            delivery_date_ms,
            onboard_date_ms,
        })
    }

    /// Parse a base-10 decimal from an exchange-provided string exactly.
    pub fn dec_str(s: &str) -> AppResult<Decimal> {
        Decimal::from_str(s)
            .map_err(|e| AppError::Internal(format!("decimal parse failed for '{s}': {e}")))
    }

    /// Scale a Decimal into a fixed-point i64 representation (truncate toward zero).
    pub fn scale_i64(x: Decimal, scale: i64) -> AppResult<i64> {
        let scaled = x * Decimal::from(scale);

        if !scaled.fract().is_zero() {
            return Err(AppError::Internal(format!(
                "cannot represent value exactly at scale={scale}: {x} (scaled={scaled})"
            )));
        }

        scaled
            .to_i64()
            .ok_or_else(|| AppError::Internal("scaled decimal out of i64 range".to_string()))
    }

    /// Convert an exchange-reported quantity into BASE quantity.
    ///
    /// Assumptions:
    /// - `price` is quote-per-1-base
    /// - For `Contracts`, contract_size meaning depends on kind:
    ///   - Linear (PerpLinear/FutureLinear): BASE per contract
    ///   - Inverse (PerpInverse/FutureInverse): QUOTE per contract
    pub fn qty_to_base(&self, reported_qty: Decimal, price: Decimal) -> AppResult<Decimal> {
        match self.reported_qty_unit {
            QtyUnit::Base => Ok(reported_qty),

            QtyUnit::Quote => {
                if price.is_zero() {
                    return Err(AppError::Internal(
                        "price is zero; cannot convert quote->base".to_string(),
                    ));
                }
                Ok(reported_qty / price)
            }

            QtyUnit::Contracts => {
                let cs_f64 = self.contract_size.ok_or_else(|| {
                    AppError::Internal("contract_size missing for Contracts qty unit".to_string())
                })?;

                let cs = Decimal::from_f64(cs_f64).ok_or_else(|| {
                    AppError::Internal("contract_size cannot be represented as Decimal".to_string())
                })?;

                match self.kind {
                    // contract_size = BASE per contract
                    InstrumentKind::PerpLinear | InstrumentKind::FutureLinear => {
                        Ok(reported_qty * cs)
                    }

                    // contract_size = QUOTE per contract -> base = (contracts * quote_per_contract) / price
                    InstrumentKind::PerpInverse | InstrumentKind::FutureInverse => {
                        if price.is_zero() {
                            return Err(AppError::Internal(
                                "price is zero; cannot convert inverse contracts->base".to_string(),
                            ));
                        }
                        Ok((reported_qty * cs) / price)
                    }

                    InstrumentKind::Spot => Err(AppError::Internal(
                        "Contracts qty for Spot doesn't make sense".to_string(),
                    )),

                    InstrumentKind::Options => Err(AppError::Internal(
                        "Contracts qty for Options not implemented".to_string(),
                    )),
                }
            }
        }
    }

    /// Convenience: qty + price as strings → BASE qty (Decimal)
    pub fn qty_str_to_base(&self, qty_str: &str, price_str: &str) -> AppResult<Decimal> {
        let qty = Self::dec_str(qty_str)?;
        let price = Self::dec_str(price_str)?;
        self.qty_to_base(qty, price)
    }

    /// Convenience: price + qty strings → scaled integers for DB.
    /// Returns (price_i, qty_i_base).
    pub fn trade_to_scaled_i64(
        &self,
        price_str: &str,
        qty_str: &str,
        price_scale: i64,
        qty_scale: i64,
    ) -> AppResult<(i64, i64)> {
        let price = Self::dec_str(price_str)?;
        let qty = Self::dec_str(qty_str)?;

        let qty_base = self.qty_to_base(qty, price)?;

        let price_i = Self::scale_i64(price, price_scale)?;
        let qty_i = Self::scale_i64(qty_base, qty_scale)?;

        Ok((price_i, qty_i))
    }
}
