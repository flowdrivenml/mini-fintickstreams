//! instruments/registry.rs
//!
//! In-memory index/lookup layer for `InstrumentSpec`.
//! Loader returns a flat Vec; this registry builds fast lookup tables on top.
//!
//! Duplicates by (exchange, symbol) are disallowed (build + update).

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::error::{AppError, AppResult};
use crate::ingest::instruments::spec::{InstrumentKind, InstrumentSpec};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstrumentKey {
    pub exchange: String,
    pub symbol: String,
}

impl InstrumentKey {
    #[inline]
    pub fn new(exchange: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
            symbol: symbol.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstrumentRegistry {
    specs: Vec<InstrumentSpec>,

    // Primary index
    by_key: HashMap<InstrumentKey, usize>,

    // Secondary indices (store indices into `specs`)
    by_exchange: HashMap<String, Vec<usize>>,
    by_kind: HashMap<InstrumentKind, Vec<usize>>,
    by_exchange_kind: HashMap<(String, InstrumentKind), Vec<usize>>,
}

impl InstrumentRegistry {
    /// Build a registry from specs. Duplicates disallowed.
    pub fn build(specs: Vec<InstrumentSpec>) -> AppResult<Self> {
        let mut reg = Self {
            specs: Vec::with_capacity(specs.len()),
            by_key: HashMap::with_capacity(specs.len()),
            by_exchange: HashMap::new(),
            by_kind: HashMap::new(),
            by_exchange_kind: HashMap::new(),
        };

        reg.insert_many(specs)?;
        reg.sort_indices_by_symbol();
        Ok(reg)
    }

    pub fn update(&mut self, new_specs: Vec<InstrumentSpec>) -> AppResult<()> {
        // 1) Remove expired existing instruments
        self.prune_expired();

        // 2) Ignore expired incoming instruments
        let new_specs = new_specs
            .into_iter()
            .filter(|s| !Self::is_expired(s))
            .collect::<Vec<_>>();

        // 3) Insert + sort
        self.insert_many(new_specs)?;
        self.sort_indices_by_symbol();
        Ok(())
    }

    #[inline]
    fn is_expired(spec: &InstrumentSpec) -> bool {
        match spec.delivery_date_ms {
            Some(delivery_ms) => delivery_ms < Self::now_ms(),
            None => false,
        }
    }

    /// Remove expired instruments from registry by rebuilding indices.
    fn prune_expired(&mut self) {
        let kept = self
            .specs
            .drain(..)
            .filter(|s| !Self::is_expired(s))
            .collect::<Vec<_>>();

        self.rebuild_from_specs(kept);
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX_EPOCH")
            .as_millis() as u64
    }

    /// Rebuild all indices from a flat spec vec.
    fn rebuild_from_specs(&mut self, specs: Vec<InstrumentSpec>) {
        self.specs = specs;

        self.by_key.clear();
        self.by_exchange.clear();
        self.by_kind.clear();
        self.by_exchange_kind.clear();

        for (idx, spec) in self.specs.iter().enumerate() {
            let exchange = spec.exchange.to_string();
            let symbol = spec.symbol.clone();
            let kind = spec.kind;

            let key = InstrumentKey::new(exchange.clone(), symbol);

            self.by_key.insert(key, idx);
            self.by_exchange
                .entry(exchange.clone())
                .or_default()
                .push(idx);
            self.by_kind.entry(kind).or_default().push(idx);
            self.by_exchange_kind
                .entry((exchange, kind))
                .or_default()
                .push(idx);
        }

        self.sort_indices_by_symbol();
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.specs.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &InstrumentSpec> {
        self.specs.iter()
    }

    pub fn get(&self, exchange: &str, symbol: &str) -> Option<&InstrumentSpec> {
        let key = InstrumentKey::new(exchange.to_string(), symbol.to_string());
        self.by_key.get(&key).map(|&i| &self.specs[i])
    }

    pub fn require(&self, exchange: &str, symbol: &str) -> AppResult<&InstrumentSpec> {
        self.get(exchange, symbol).ok_or_else(|| {
            AppError::Internal(format!(
                "instrument not found: exchange='{exchange}' symbol='{symbol}'"
            ))
        })
    }

    pub fn by_exchange(&self, exchange: &str) -> impl Iterator<Item = &InstrumentSpec> + '_ {
        self.by_exchange
            .get(exchange)
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
            .map(|i| &self.specs[i])
    }

    pub fn by_exchange_vec(&self, exchange: &str) -> Vec<InstrumentSpec> {
        self.by_exchange(exchange).cloned().collect()
    }

    pub fn by_kind(&self, kind: InstrumentKind) -> impl Iterator<Item = &InstrumentSpec> + '_ {
        self.by_kind
            .get(&kind)
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
            .map(|i| &self.specs[i])
    }

    pub fn by_kind_vec(&self, kind: InstrumentKind) -> Vec<InstrumentSpec> {
        self.by_kind(kind).cloned().collect()
    }

    pub fn by_exchange_kind(
        &self,
        exchange: &str,
        kind: InstrumentKind,
    ) -> impl Iterator<Item = &InstrumentSpec> + '_ {
        self.by_exchange_kind
            .get(&(exchange.to_string(), kind))
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
            .map(|i| &self.specs[i])
    }

    pub fn by_exchange_kind_vec(
        &self,
        exchange: &str,
        kind: InstrumentKind,
    ) -> Vec<InstrumentSpec> {
        self.by_exchange_kind(exchange, kind).cloned().collect()
    }

    #[inline]
    pub fn exists(&self, exchange: &str, symbol: &str) -> bool {
        self.get(exchange, symbol).is_some()
    }

    /// Verify existence, returning an error if missing (useful for request validation paths)
    #[inline]
    pub fn verify_exists(&self, exchange: &str, symbol: &str) -> AppResult<()> {
        if self.exists(exchange, symbol) {
            Ok(())
        } else {
            Err(AppError::Internal(format!(
                "instrument not found: exchange='{exchange}' symbol='{symbol}'"
            )))
        }
    }

    /// Deterministic counts: exchange -> kind -> count
    pub fn counts_by_exchange_and_kind(&self) -> BTreeMap<String, BTreeMap<InstrumentKind, usize>> {
        let mut out: BTreeMap<String, BTreeMap<InstrumentKind, usize>> = BTreeMap::new();

        for ((ex, kind), ids) in &self.by_exchange_kind {
            out.entry(ex.clone()).or_default().insert(*kind, ids.len());
        }

        out
    }

    // ---------------- internal ----------------

    fn insert_many(&mut self, specs: Vec<InstrumentSpec>) -> AppResult<()> {
        // Reject duplicates inside the incoming batch too.
        let mut seen_batch: HashSet<InstrumentKey> = HashSet::new();

        for spec in specs {
            // If your InstrumentSpec uses getters, swap these 3 lines accordingly.
            let exchange: String = spec.exchange.to_string();
            let symbol: String = spec.symbol.clone();
            let kind: InstrumentKind = spec.kind;

            let key = InstrumentKey::new(exchange.clone(), symbol.clone());

            if self.by_key.contains_key(&key) {
                return Err(AppError::Internal(format!(
                    "duplicate instrument key (already in registry): exchange='{exchange}' symbol='{symbol}'"
                )));
            }

            if !seen_batch.insert(key.clone()) {
                return Err(AppError::Internal(format!(
                    "duplicate instrument key (within update batch): exchange='{exchange}' symbol='{symbol}'"
                )));
            }

            let idx = self.specs.len();
            self.specs.push(spec);

            self.by_key.insert(key, idx);
            self.by_exchange
                .entry(exchange.clone())
                .or_default()
                .push(idx);
            self.by_kind.entry(kind).or_default().push(idx);
            self.by_exchange_kind
                .entry((exchange, kind))
                .or_default()
                .push(idx);
        }

        Ok(())
    }

    fn sort_indices_by_symbol(&mut self) {
        let specs = &self.specs;

        for ids in self.by_exchange.values_mut() {
            ids.sort_by(|&a, &b| specs[a].symbol.cmp(&specs[b].symbol));
        }

        for ids in self.by_kind.values_mut() {
            ids.sort_by(|&a, &b| {
                specs[a]
                    .exchange
                    .cmp(&specs[b].exchange)
                    .then_with(|| specs[a].symbol.cmp(&specs[b].symbol))
            });
        }

        for ids in self.by_exchange_kind.values_mut() {
            ids.sort_by(|&a, &b| specs[a].symbol.cmp(&specs[b].symbol));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::app::config::load_app_config;
    use crate::error::AppResult;
    use crate::ingest::config::ExchangeConfigs;
    use crate::ingest::instruments::loader::InstrumentSpecLoader;
    use crate::ingest::instruments::spec::{InstrumentKind, QtyUnit};

    // ------------------------
    // Unit tests (fast)
    // ------------------------

    #[test]
    fn build_rejects_duplicates() -> AppResult<()> {
        let a = InstrumentSpec::new(
            "binance_linear",
            "BTCUSDT",
            InstrumentKind::PerpLinear,
            QtyUnit::Base,
            Some(1.0),
            None,
            Some(1),
        )?;

        let b = InstrumentSpec::new(
            "binance_linear",
            "BTCUSDT",
            InstrumentKind::PerpLinear,
            QtyUnit::Base,
            Some(1.0),
            None,
            Some(2),
        )?;

        let err = InstrumentRegistry::build(vec![a, b]).expect_err("should reject dup");
        assert!(format!("{err:?}").contains("duplicate instrument key"));
        Ok(())
    }

    #[test]
    fn update_rejects_existing_key() -> AppResult<()> {
        let a = InstrumentSpec::new(
            "hyperliquid_perp",
            "BTC",
            InstrumentKind::PerpLinear,
            QtyUnit::Base,
            Some(1.0),
            None,
            None,
        )?;

        let mut reg = InstrumentRegistry::build(vec![a])?;

        let dup = InstrumentSpec::new(
            "hyperliquid_perp",
            "BTC",
            InstrumentKind::PerpLinear,
            QtyUnit::Base,
            Some(1.0),
            None,
            None,
        )?;

        let err = reg
            .update(vec![dup])
            .expect_err("should reject existing key");
        assert!(format!("{err:?}").contains("already in registry"));
        Ok(())
    }

    // ------------------------
    // Integration tests (live HTTP)
    // ------------------------

    #[tokio::test]
    async fn registry_build_from_real_loader_is_not_empty_and_prints() -> AppResult<()> {
        let appconfig = load_app_config()?;
        let exchangeconfigs = ExchangeConfigs::new(&appconfig)?;

        let loader = InstrumentSpecLoader::new(exchangeconfigs, None, None)?;
        let specs = loader.load_all().await?;

        println!("Loader returned {} instrument specs", specs.len());
        println!("{:#?}", specs);

        assert!(!specs.is_empty(), "Expected non-empty instrument specs");

        let reg = InstrumentRegistry::build(specs)?;
        println!("Registry built with {} instruments", reg.len());
        println!(
            "Counts by exchange/kind:\n{:#?}",
            reg.counts_by_exchange_and_kind()
        );

        Ok(())
    }

    #[tokio::test]
    async fn registry_prints_future_linear_from_real_loader() -> AppResult<()> {
        let appconfig = load_app_config()?;
        let exchangeconfigs = ExchangeConfigs::new(&appconfig)?;

        let loader = InstrumentSpecLoader::new(exchangeconfigs, None, None)?;
        let specs = loader.load_all().await?;

        let reg = InstrumentRegistry::build(specs)?;

        let futures: Vec<_> = reg
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
