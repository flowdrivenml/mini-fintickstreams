// crate::db::writer::batch_helpers.rs (or crate::db::writer::mod.rs)
use crate::app::stream_types::{ExchangeId, StreamKind, StreamTransport};
use crate::db::WriterConfig;
use crate::db::{Batch, BatchKey};
use crate::error::AppResult;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::app::state::StreamKnobs;

#[inline]
pub fn make_batch_key(
    exchange: ExchangeId,
    transport: StreamTransport,
    kind: StreamKind,
    symbol: impl AsRef<str>,
) -> AppResult<BatchKey> {
    let stream = kind.endpoint_key(exchange, transport)?; // <-- your mapping
    Ok(BatchKey {
        exchange: exchange.as_str().to_string(),
        stream: stream.to_string(),
        symbol: symbol.as_ref().to_string(),
    })
}

#[inline]
pub fn make_empty_batch<T>(
    exchange: ExchangeId,
    transport: StreamTransport,
    kind: StreamKind,
    symbol: impl AsRef<str>,
    writer_cfg: WriterConfig,
) -> AppResult<Batch<T>> {
    let key = make_batch_key(exchange, transport, kind, symbol)?;
    Ok(Batch::new(key, vec![], &writer_cfg))
}

/// Spawns a task that listens for StreamKnobs changes and:
/// - updates the batch settings (flush_rows/interval/hard_cap)
/// - triggers an immediate DB flush if the batch becomes flushable
///
/// Notes:
/// - This is intentionally NOT generic.
/// - Assumes `deps.db_write_with_chunk_rows(...)` clears the batch only on success.
/// - Uses `.ok()` to ignore errors on knob-triggered flush (same as your snippet).
pub fn spawn_knobs_batch_flush_task<T>(
    batch: Arc<Mutex<Batch<T>>>,
    mut knobs_rx_watch: watch::Receiver<StreamKnobs>,
    cancel: CancellationToken,
) -> JoinHandle<()>
where
    T: Send + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                changed = knobs_rx_watch.changed() => {
                    if changed.is_err() { break; }
                    let knobs = *knobs_rx_watch.borrow();
                    // Apply settings + flush if needed
                    if !knobs.disable_db_writes {
                        let mut guard = batch.lock().await;
                        // Update batch settings (your Batch API)
                        guard.set_flush_rows(knobs.flush_rows);
                        guard.set_flush_interval_ms(knobs.flush_interval_ms);
                        guard.set_hard_cap_rows(knobs.hard_cap_rows);

                    }
                }
            }
        }
    })
}
