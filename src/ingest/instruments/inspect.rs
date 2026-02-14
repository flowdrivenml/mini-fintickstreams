use std::collections::BTreeSet;
use std::fs;

use crate::error::{AppError, AppResult};
use crate::ingest::datamap::sources::binance_linear::types::BinanceLinearExchangeInfoSnapshot;
use crate::ingest::datamap::sources::bybit_linear::types::BybitLinearExchangeInfoSnapshot;
use crate::ingest::datamap::sources::hyperliquid_perp::types::HyperliquidPerpInfoSnapshot;
use crate::ingest::datamap::traits::FromJsonStr;

/// Reads the bundled testdata snapshot and returns unique `contractType` values.
pub fn inspect_binance_linear_contract_types_from_testdata() -> AppResult<BTreeSet<String>> {
    let path = "src/ingest/datamap/testdata/BinanceLinearExchangeInfoSnapshot.json";
    let s = fs::read_to_string(path).map_err(AppError::ConfigIo)?;

    let snapshot: BinanceLinearExchangeInfoSnapshot =
        serde_json::from_str(&s).map_err(AppError::Json)?;

    let mut out = BTreeSet::new();
    for sym in snapshot.symbols {
        out.insert(sym.contract_type);
    }
    Ok(out)
}

/// Reads the bundled Hyperliquid perp testdata snapshot and inspects key enum-like fields.
///
/// Currently inspects:
/// - `marginMode` (unique values)
/// - `onlyIsolated` presence
/// - `isDelisted` presence
pub fn inspect_hyperliquid_perp_info_from_testdata() -> AppResult<BTreeSet<String>> {
    let path = "src/ingest/datamap/testdata/HyperliquidPerpInfoSnapshot.json";
    let s = fs::read_to_string(path).map_err(AppError::ConfigIo)?;

    let snapshot: HyperliquidPerpInfoSnapshot = serde_json::from_str(&s).map_err(AppError::Json)?;

    let mut margin_modes = BTreeSet::new();

    for entry in snapshot.universe {
        if let Some(mode) = entry.margin_mode {
            margin_modes.insert(mode);
        }
    }

    Ok(margin_modes)
}

pub fn inspect_bybit_linear_info_from_testdata() -> AppResult<BTreeSet<String>> {
    let path = "src/ingest/datamap/testdata/BybitLinearExchangeInfoSnapshot.json";
    let s = fs::read_to_string(path).map_err(AppError::ConfigIo)?;

    // ✅ unwraps { retCode, retMsg, result: {...} } and returns the inner result
    let snapshot = BybitLinearExchangeInfoSnapshot::from_json_str(&s)?;

    let mut contract_types = BTreeSet::new();
    for inst in snapshot.instruments {
        contract_types.insert(inst.contract_type);
    }

    Ok(contract_types)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inspect_binance_linear_contract_types() -> AppResult<()> {
        let types = inspect_binance_linear_contract_types_from_testdata()?;
        // deterministic order because BTreeSet
        println!("Unique Binance Linear contractType values:");
        for t in &types {
            println!("  - {t}");
        }
        Ok(())
    }

    #[test]
    fn inspect_hyperliquid_perp_margin_modes() -> AppResult<()> {
        let modes = inspect_hyperliquid_perp_info_from_testdata()?;
        println!("Unique Hyperliquid perp marginMode values:");
        if modes.is_empty() {
            println!("  (none present)");
        } else {
            for m in &modes {
                println!("  - {m}");
            }
        }
        Ok(())
    }

    #[test]
    fn inspect_bybit_linear_contract_types() -> AppResult<()> {
        let types = inspect_bybit_linear_info_from_testdata()?;
        // deterministic order because BTreeSet
        println!("Unique Bybit Linear contractType values:");
        for t in &types {
            println!("  - {t}");
        }
        Ok(())
    }
}
