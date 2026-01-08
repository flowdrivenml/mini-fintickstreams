//! instruments/loader.rs
//!
//! Loads `InstrumentSpec` sets from exchange "exchange_info"/meta endpoints.
//! Output is a single `Vec<InstrumentSpec>`; indexing/lookup belongs in a registry layer.

use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::error::{AppError, AppResult};
use crate::ingest::config::ExchangeConfigs;
use crate::ingest::http::ApiClient;
use crate::ingest::metrics::IngestMetrics;
use crate::ingest::rate_limiter::RateLimiterRegistry;
use crate::ingest::spec::resolve::resolve_http_request;
use crate::ingest::spec::{Ctx, ParamPlacement};

use crate::ingest::datamap::sources::binance_linear::types::BinanceLinearExchangeInfoSnapshot;
use crate::ingest::datamap::sources::hyperliquid_perp::types::HyperliquidPerpInfoSnapshot;
use crate::ingest::instruments::spec::{InstrumentKind, InstrumentSpec, QtyUnit};

/// Loader that owns API clients and knows how to fetch+parse exchange metadata into `InstrumentSpec`s.
///
/// Parsing is exchange-specific; we keep the fetch/resolve plumbing centralized here.
#[derive(Debug)]
pub struct InstrumentSpecLoader {
    ctx: Ctx,
    exchange_configs: ExchangeConfigs,

    binance_client: Option<ApiClient>,
    hyperliquid_client: Option<ApiClient>,
}

impl InstrumentSpecLoader {
    /// Construct a loader. Creates ApiClients for configured exchanges.
    ///
    /// `exchange_configs` should be created once from app config and passed in.
    pub fn new(
        exchange_configs: ExchangeConfigs,
        limiter_registry: Option<Arc<RateLimiterRegistry>>,
        metrics: Option<Arc<IngestMetrics>>,
    ) -> AppResult<Self> {
        let ctx = Ctx::new();

        // Build clients only if that exchange config exists.
        let binance_client = exchange_configs.binance_linear.as_ref().map(|binance| {
            ApiClient::new(
                "binance_exchange_info",
                binance.api_base_url.clone(),
                limiter_registry.clone(),
                metrics.clone(),
            )
        });

        let hyperliquid_client = exchange_configs.hyperliquid_perp.as_ref().map(|hyper| {
            ApiClient::new(
                "hyperliquid_exchange_info",
                hyper.api_base_url.clone(),
                limiter_registry.clone(),
                metrics.clone(),
            )
        });

        Ok(Self {
            ctx,
            exchange_configs,
            binance_client,
            hyperliquid_client,
        })
    }

    /// Load all enabled exchanges and return a single flat list of instrument specs.
    pub async fn load_all(&self) -> AppResult<Vec<InstrumentSpec>> {
        let mut out: Vec<InstrumentSpec> = Vec::new();

        if self.exchange_configs.binance_linear.is_some() {
            out.extend(self.load_binance_linear_instrument_specs().await?);
        }

        if self.exchange_configs.hyperliquid_perp.is_some() {
            out.extend(self.load_hyperliquid_perp_instrument_specs().await?);
        }

        Ok(out)
    }

    /// Load Binance linear exchange info and parse into instrument specs.
    ///
    pub async fn load_binance_linear_instrument_specs(&self) -> AppResult<Vec<InstrumentSpec>> {
        let binance = self
            .exchange_configs
            .binance_linear
            .as_ref()
            .ok_or_else(|| {
                AppError::InvalidConfig("binance_linear missing in ExchangeConfigs".into())
            })?;

        let ep = binance
            .api
            .get("exchange_info")
            .ok_or_else(|| AppError::InvalidConfig("binance api.exchange_info missing".into()))?;

        let req_spec = resolve_http_request(ep, &self.ctx, ParamPlacement::Query)?;

        let client = self.binance_client.as_ref().ok_or_else(|| {
            AppError::Internal("binance_client not initialized (binance_linear missing?)".into())
        })?;

        let resp = client.execute(&req_spec).await?;
        let body = resp.text().await.map_err(AppError::Reqwest)?;

        let json: JsonValue = serde_json::from_str(&body).map_err(AppError::Json)?;

        // Exchange-specific parsing (stub for now)
        self.parse_binance_linear_exchange_info(&json)
    }

    /// Load Hyperliquid perp exchange info and parse into instrument specs.
    ///
    pub async fn load_hyperliquid_perp_instrument_specs(&self) -> AppResult<Vec<InstrumentSpec>> {
        let hyper = self
            .exchange_configs
            .hyperliquid_perp
            .as_ref()
            .ok_or_else(|| {
                AppError::InvalidConfig("hyperliquid_perp missing in ExchangeConfigs".into())
            })?;

        let ep = hyper.api.get("exchange_info").ok_or_else(|| {
            AppError::InvalidConfig("hyperliquid api.exchange_info missing".into())
        })?;

        let req_spec = resolve_http_request(ep, &self.ctx, ParamPlacement::JsonBody)?;

        let client = self.hyperliquid_client.as_ref().ok_or_else(|| {
            AppError::Internal(
                "hyperliquid_client not initialized (hyperliquid_perp missing?)".into(),
            )
        })?;

        let resp = client.execute(&req_spec).await?;
        let body = resp.text().await.map_err(AppError::Reqwest)?;

        let json: JsonValue = serde_json::from_str(&body).map_err(AppError::Json)?;

        // Exchange-specific parsing (stub for now)
        self.parse_hyperliquid_perp_exchange_info(&json)
    }

    // ------------------------------------------------------------------------
    // Exchange-specific parsing stubs
    // ------------------------------------------------------------------------

    fn parse_binance_linear_exchange_info(
        &self,
        json: &JsonValue,
    ) -> AppResult<Vec<InstrumentSpec>> {
        let snapshot: BinanceLinearExchangeInfoSnapshot =
            serde_json::from_value(json.clone()).map_err(AppError::Json)?;

        let mut out = Vec::with_capacity(snapshot.symbols.len());

        for s in snapshot.symbols {
            // Skip non-trading symbols (optional; remove if you want everything)
            if s.status != "TRADING" {
                continue;
            }

            let (kind, delivery_date_ms) = match s.contract_type.as_str() {
                // Treat TRADIFI_PERPETUAL same as PERPETUAL
                "PERPETUAL" | "TRADIFI_PERPETUAL" => (InstrumentKind::PerpLinear, None),

                // Quarterly futures
                "CURRENT_QUARTER" | "NEXT_QUARTER" => {
                    (InstrumentKind::FutureLinear, Some(s.delivery_date_ms))
                }

                other => {
                    return Err(AppError::Internal(format!(
                        "unknown Binance linear contractType='{other}' for symbol={}",
                        s.symbol
                    )));
                }
            };

            // Binance linear aggTrade `q` is base quantity; we normalize to base anyway.
            let reported_qty_unit = QtyUnit::Base;

            // You requested: contract_size always 1 and not listed there.
            let contract_size = Some(1.0_f64);

            // Onboard date exists in payload; store it.
            let onboard_date_ms = Some(s.onboard_date_ms);

            out.push(InstrumentSpec::new(
                "binance_linear",
                s.symbol,
                kind,
                reported_qty_unit,
                contract_size,
                delivery_date_ms,
                onboard_date_ms,
            )?);
        }

        Ok(out)
    }

    fn parse_hyperliquid_perp_exchange_info(
        &self,
        json: &JsonValue,
    ) -> AppResult<Vec<InstrumentSpec>> {
        let snapshot: HyperliquidPerpInfoSnapshot =
            serde_json::from_value(json.clone()).map_err(AppError::Json)?;

        let mut out = Vec::with_capacity(snapshot.universe.len());

        for u in snapshot.universe {
            // Skip delisted markets (optional; remove if you want everything)
            if u.is_delisted {
                continue;
            }

            // Hyperliquid perp universe entries are perps.
            // Quantity reported (`sz`) is in base units.
            let kind = InstrumentKind::PerpLinear;
            let reported_qty_unit = QtyUnit::Base;

            // You requested: contract_size always 1.
            let contract_size = Some(1.0_f64);

            // Hyperliquid perp info snapshot doesn't include delivery/onboard timestamps.
            let delivery_date_ms = None;
            let onboard_date_ms = None;

            out.push(InstrumentSpec::new(
                "hyperliquid_perp",
                u.name, // e.g. "BTC"
                kind,
                reported_qty_unit,
                contract_size,
                delivery_date_ms,
                onboard_date_ms,
            )?);
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::config::load_app_config;
    use crate::error::AppResult;
    use crate::ingest::config::ExchangeConfigs;
    use crate::ingest::instruments::spec::InstrumentKind; // If you already use tokio in the project, this should work as-is.
    // Otherwise, change to whatever async runtime you use.
    #[tokio::test]
    async fn load_all_instrument_specs_is_not_empty_and_prints() -> AppResult<()> {
        // Load real app config (env/files/etc, whatever your load_app_config does)
        let appconfig = load_app_config()?;
        let exchangeconfigs = ExchangeConfigs::new(&appconfig)?;

        // No rate limiter registry and no metrics, as requested
        let loader = InstrumentSpecLoader::new(exchangeconfigs, None, None)?;

        let specs = loader.load_all().await?;

        // Print fully (run tests with: cargo test -- --nocapture)
        // Debug print is the most robust "full print".
        println!("Loaded {} InstrumentSpec(s):\n{:#?}", specs.len(), specs);

        assert!(
            !specs.is_empty(),
            "Expected non-empty instrument specs from configured exchanges"
        );

        Ok(())
    }

    #[tokio::test]
    async fn load_and_print_future_linear_instruments() -> AppResult<()> {
        let appconfig = load_app_config()?;
        let exchangeconfigs = ExchangeConfigs::new(&appconfig)?;

        let loader = InstrumentSpecLoader::new(exchangeconfigs, None, None)?;

        let specs = loader.load_all().await?;

        let futures: Vec<_> = specs
            .iter()
            .filter(|s| s.kind == InstrumentKind::FutureLinear)
            .collect();

        println!(
            "Loaded {} FutureLinear InstrumentSpec(s):\n{:#?}",
            futures.len(),
            futures
        );

        assert!(
            !futures.is_empty(),
            "Expected at least one FutureLinear instrument"
        );

        Ok(())
    }
}
