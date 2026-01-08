use crate::app::AvailableStream;
use crate::app::StartStreamParams;
use crate::app::dependencies::AppDeps;
use crate::app::health::{RuntimeHealthHandle, start_runtime_health_guard};
use crate::app::metrics::AppMetrics;
use crate::app::state::AppState;
use crate::app::state::StreamKnobs;
use crate::app::stream_types::{
    ExchangeId, StreamId, StreamKind, StreamSpec, StreamStatus, StreamTransport,
};
use crate::error::{AppError, AppResult};
use crate::ingest::instruments::registry::InstrumentRegistry;
use crate::ingest::instruments::spec::{InstrumentKind, InstrumentSpec};
use arc_swap::{ArcSwap, Guard};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, info, instrument, warn};

/// Holds a pinned snapshot of the registry so iterators can safely borrow from it.
pub struct InstrumentsView {
    reg: Guard<Arc<InstrumentRegistry>>,
}

#[derive(Clone)]
pub struct AppRuntime {
    pub deps: Arc<AppDeps>,
    pub state: Arc<AppState>,
    pub metrics: Arc<AppMetrics>,
    pub instruments_registry: Arc<ArcSwap<InstrumentRegistry>>,

    // -------------------------
    // Runtime health (GREEN/RED)
    // -------------------------
    pub runtime_health: RuntimeHealthHandle,

    // Keep the JoinHandle private and shared across clones.
    runtime_health_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    runtime_health_cancel: tokio_util::sync::CancellationToken,
}

impl AppRuntime {
    pub async fn new() -> AppResult<Self> {
        crate::crypto_init::init_rustls_crypto_provider();
        let mut deps = AppDeps::new().await?; // <-- mutable, not Arc yet

        deps.spawn_db_health_loop()?;
        deps.spawn_redis_health_loop()?;

        let deps = Arc::new(deps); // <-- now freeze into Arc

        // Assuming deps.app_cfgs is an Arc<AppConfig> or something cloneable
        let cfg = deps.app_cfgs.clone();

        let metrics = Arc::new(AppMetrics::new(
            &cfg.id,
            &cfg.env,
            cfg.config_version,
            cfg.limits.max_active_streams,
        )?);

        let state = Arc::new(AppState::new());

        // Boot markers (your guard will also keep health in sync)
        metrics.set_ready(true);
        metrics.set_health(true);

        // Start runtime health guard (GREEN/RED) + keep join handle privately
        let token = tokio_util::sync::CancellationToken::new();
        let (runtime_health, jh) =
            start_runtime_health_guard(cfg.clone(), Some(metrics.clone()), token.clone())?;

        let runtime_health_task = Arc::new(Mutex::new(Some(jh)));

        // Load instruments
        let instruments = deps.instruments_loader.load_all().await?;
        let registry = InstrumentRegistry::build(instruments)?;

        // ArcSwap wants an Arc<T>
        let instruments_registry = Arc::new(ArcSwap::from(Arc::new(registry)));

        Ok(Self {
            deps,
            state,
            metrics,
            instruments_registry,
            runtime_health,
            runtime_health_task,
            runtime_health_cancel: token,
        })
    }
}
// --------------------------------------------------
// Runtime health checks (admission gating)
// --------------------------------------------------
impl AppRuntime {
    /// Fast check: true if runtime health is GREEN.
    #[inline]
    pub fn runtime_ok(&self) -> bool {
        self.runtime_health.is_green()
    }

    /// Convenience for admission points ("can we add new streams?").
    /// Use this in your stream manager / API handler.
    #[inline]
    pub fn ensure_runtime_ok_for_admission(&self) -> AppResult<()> {
        let runtime_ok = self.runtime_ok();
        let redis_ok = self.deps.redis_can_assign_new_symbol();
        let db_reason = self.deps.can_admite_new_streams(); // Option<String>

        let mut reasons: Vec<String> = Vec::new();

        if !runtime_ok {
            reasons.push("runtime_health_red".into());
            self.metrics.inc_stream_add_denied_runtime_red();
        }

        if !redis_ok {
            reasons.push("redis_unavailable".into());
            self.metrics.inc_stream_add_denied_redis();
        }

        if let Some(r) = db_reason {
            reasons.push(format!("db:{r}"));
            self.metrics.inc_stream_add_denied_db();
        }

        if !reasons.is_empty() {
            tracing::warn!(
                component = "admission",
                runtime_ok,
                redis_ok,
                reasons = ?reasons,
                "stream admission denied"
            );
            return Err(AppError::Disabled(reasons.join(", ")));
        }

        tracing::debug!(component = "admission", "stream admission allowed");
        Ok(())
    }
    /// Optional: if you want to await or abort the background task on shutdown,
    /// call this once (subsequent calls return None).
    pub async fn take_runtime_health_task(&self) -> Option<JoinHandle<()>> {
        self.runtime_health_task.lock().await.take()
    }
}
// --------------------------------------------------
// Instruments registry
// --------------------------------------------------
impl AppRuntime {
    #[instrument(name = "runtime.refresh_instruments_registry", skip_all, err)]
    pub async fn refresh_instruments_registry(&self) -> AppResult<()> {
        let instruments = self.deps.instruments_loader.load_all().await?;
        let new_registry = InstrumentRegistry::build(instruments)?;

        self.instruments_registry.store(Arc::new(new_registry));

        info!(component = "registry", "instruments registry refreshed");
        Ok(())
    }

    #[inline]
    pub fn instruments_guard(&self) -> Guard<Arc<InstrumentRegistry>> {
        self.instruments_registry.load()
    }

    // (no need to instrument pure getters unless you want high-volume tracing)
    pub fn retrieve_instruments_by_exchange(&self, exchange: &str) -> Vec<InstrumentSpec> {
        let guard = self.instruments_registry.load();
        guard.as_ref().by_exchange_vec(exchange)
    }

    pub fn retrieve_instruments_by_kind(&self, kind: InstrumentKind) -> Vec<InstrumentSpec> {
        let guard = self.instruments_registry.load();
        guard.as_ref().by_kind_vec(kind)
    }

    pub fn retrieve_instruments_by_exchange_kind(
        &self,
        exchange: &str,
        kind: InstrumentKind,
    ) -> Vec<InstrumentSpec> {
        let guard = self.instruments_registry.load();
        guard.as_ref().by_exchange_kind_vec(exchange, kind)
    }

    #[inline]
    pub fn exists(&self, exchange: &str, symbol: &str) -> bool {
        let guard = self.instruments_registry.load();
        guard.as_ref().exists(exchange, symbol)
    }
}

// -------------------------------------
// Stream control
// -------------------------------------
impl AppRuntime {
    #[instrument(
        name = "runtime.add_stream",
        skip(self, params),
        fields(
            exchange = %params.exchange,
            symbol = %params.symbol,
            kind = ?params.kind,
            transport = ?params.transport,
        ),
        err
    )]
    pub async fn add_stream(&self, params: StartStreamParams) -> AppResult<()> {
        debug!(component = "streams", "add_stream requested");

        // Optional: admission trace (you already trace denial inside ensure_runtime_ok_for_admission)
        // self.ensure_runtime_ok_for_admission()?;

        let res = crate::app::start_stream(self, params).await;

        match &res {
            Ok(()) => info!(component = "streams", "add_stream succeeded"),
            Err(e) => warn!(component = "streams", error = %e, "add_stream failed"),
        }

        res
    }

    #[instrument(
        name = "runtime.remove_stream",
        skip(self, params),
        fields(
            exchange = %params.exchange,
            symbol = %params.symbol,
            kind = ?params.kind,
            transport = ?params.transport,
        ),
        err
    )]
    pub async fn remove_stream(&self, params: StartStreamParams) -> AppResult<()> {
        let id = StreamId::new(
            params.exchange.as_str(),
            params.symbol.as_str(),
            params.kind,
            params.transport,
        );

        debug!(component = "streams", stream_id = %id, "remove_stream requested");

        let result = self.state.stop_and_remove(&id).await?;

        if result {
            info!(component = "streams", stream_id = %id, "remove_stream succeeded");
            Ok(())
        } else {
            warn!(component = "streams", stream_id = %id, "remove_stream: stream not found");
            Err(AppError::StreamNotFound("stream not found".into()))
        }
    }
}

// --------------------------------------------------
// Health Control
// --------------------------------------------------
impl AppRuntime {
    pub async fn stop_runtime_health(&self) {
        self.runtime_health_cancel.cancel()
    }
    pub async fn stop_db_health(&self) {
        self.deps.db_cancel_health_loop()
    }
    pub async fn stop_redis_health(&self) {
        self.deps.redis_cancel_health_loop()
    }
}

// --------------------------------------------------
// Stream Capabilities
// --------------------------------------------------
impl AppRuntime {
    /// All supported (exchange, transport, kind) stream types.
    pub fn stream_capabilities(&self) -> Vec<AvailableStream> {
        crate::app::list_available_streams()
    }

    /// Capabilities filtered by exchange.
    pub fn stream_capabilities_for_exchange(&self, exchange: ExchangeId) -> Vec<AvailableStream> {
        crate::app::list_available_for_exchange(exchange)
    }

    /// Fast check for whether a stream triple is supported by routing.
    pub fn stream_is_supported(
        &self,
        exchange: ExchangeId,
        transport: StreamTransport,
        kind: StreamKind,
    ) -> bool {
        crate::app::is_supported(exchange, transport, kind)
    }

    /// If there is a known "use X instead of Y" rule, return the reason.
    pub fn stream_unsupported_reason(
        &self,
        exchange: ExchangeId,
        transport: StreamTransport,
        kind: StreamKind,
    ) -> Option<&'static str> {
        crate::app::unsupported_reason(exchange, transport, kind)
    }
}

// --------------------------------------------------
// Stream Status
// --------------------------------------------------

impl AppRuntime {
    /// List all currently registered streams (id, status, spec).
    pub async fn list_streams(&self) -> Vec<(StreamId, StreamStatus, StreamSpec)> {
        self.state.list().await
    }

    /// List just the StreamIds for currently registered streams.
    pub async fn list_stream_ids(&self) -> Vec<StreamId> {
        self.state
            .list()
            .await
            .into_iter()
            .map(|(id, _, _)| id)
            .collect()
    }

    /// Current number of streams in the registry.
    pub async fn stream_count(&self) -> usize {
        self.state.len().await
    }

    /// Filter streams by exchange string (matches `StreamSpec.exchange`).
    pub async fn list_streams_by_exchange(
        &self,
        exchange: &str,
    ) -> Vec<(StreamId, StreamStatus, StreamSpec)> {
        self.state
            .list()
            .await
            .into_iter()
            .filter(|(_, _, spec)| spec.exchange == exchange)
            .collect()
    }

    /// Filter streams by symbol / instrument (matches `StreamSpec.instrument`).
    pub async fn list_streams_by_symbol(
        &self,
        symbol: &str,
    ) -> Vec<(StreamId, StreamStatus, StreamSpec)> {
        self.state
            .list()
            .await
            .into_iter()
            .filter(|(_, _, spec)| spec.instrument == symbol)
            .collect()
    }

    /// Filter streams by kind.
    pub async fn list_streams_by_kind(
        &self,
        kind: StreamKind,
    ) -> Vec<(StreamId, StreamStatus, StreamSpec)> {
        self.state
            .list()
            .await
            .into_iter()
            .filter(|(_, _, spec)| spec.kind == kind)
            .collect()
    }

    /// Filter streams by transport.
    pub async fn list_streams_by_transport(
        &self,
        transport: StreamTransport,
    ) -> Vec<(StreamId, StreamStatus, StreamSpec)> {
        self.state
            .list()
            .await
            .into_iter()
            .filter(|(_, _, spec)| spec.transport == transport)
            .collect()
    }
}

// --------------------------------------------------
// Stream Knobs Control
// --------------------------------------------------
impl AppRuntime {
    #[instrument(
        name = "runtime.set_stream_knobs",
        skip(self, knobs),
        fields(stream_id = %id),
        err
    )]
    pub async fn set_stream_knobs(&self, id: &StreamId, knobs: StreamKnobs) -> AppResult<bool> {
        debug!(component = "knobs", "set_stream_knobs requested");
        let res = self.state.set_knobs(id, knobs).await;
        match &res {
            Ok(changed) => info!(
                component = "knobs",
                changed = *changed,
                "set_stream_knobs done"
            ),
            Err(e) => warn!(component = "knobs", error = %e, "set_stream_knobs failed"),
        }
        res
    }

    #[instrument(
        name = "runtime.stream_knobs_snapshot",
        skip(self),
        fields(stream_id = %id)
    )]
    pub async fn stream_knobs_snapshot(&self, id: &StreamId) -> Option<StreamKnobs> {
        let snap = self.state.knobs_snapshot(id).await;
        debug!(
            component = "knobs",
            found = snap.is_some(),
            "stream_knobs_snapshot"
        );
        snap
    }

    #[instrument(
        name = "runtime.set_stream_db_writes_enabled",
        skip(self),
        fields(stream_id = %id, enabled),
        err
    )]
    pub async fn set_stream_db_writes_enabled(
        &self,
        id: &StreamId,
        enabled: bool,
    ) -> AppResult<bool> {
        let res = self.state.set_db_writes_enabled(id, enabled).await;
        match &res {
            Ok(changed) => info!(
                component = "knobs",
                changed = *changed,
                "db_writes_enabled updated"
            ),
            Err(e) => warn!(component = "knobs", error = %e, "db_writes_enabled update failed"),
        }
        res
    }

    #[instrument(
        name = "runtime.set_stream_redis_publishes_enabled",
        skip(self),
        fields(stream_id = %id, enabled),
        err
    )]
    pub async fn set_stream_redis_publishes_enabled(
        &self,
        id: &StreamId,
        enabled: bool,
    ) -> AppResult<bool> {
        let res = self.state.set_redis_publishes_enabled(id, enabled).await;
        match &res {
            Ok(changed) => info!(
                component = "knobs",
                changed = *changed,
                "redis_publishes_enabled updated"
            ),
            Err(e) => {
                warn!(component = "knobs", error = %e, "redis_publishes_enabled update failed")
            }
        }
        res
    }

    #[instrument(
        name = "runtime.set_stream_flush_rows",
        skip(self),
        fields(stream_id = %id, rows),
        err
    )]
    pub async fn set_stream_flush_rows(&self, id: &StreamId, rows: usize) -> AppResult<bool> {
        let res = self.state.set_flush_rows(id, rows).await;
        match &res {
            Ok(changed) => info!(
                component = "knobs",
                changed = *changed,
                "flush_rows updated"
            ),
            Err(e) => warn!(component = "knobs", error = %e, "flush_rows update failed"),
        }
        res
    }

    #[instrument(
        name = "runtime.set_stream_flush_interval_ms",
        skip(self),
        fields(stream_id = %id, interval_ms),
        err
    )]
    pub async fn set_stream_flush_interval_ms(
        &self,
        id: &StreamId,
        interval_ms: u64,
    ) -> AppResult<bool> {
        let res = self.state.set_flush_interval_ms(id, interval_ms).await;
        match &res {
            Ok(changed) => info!(
                component = "knobs",
                changed = *changed,
                "flush_interval_ms updated"
            ),
            Err(e) => warn!(component = "knobs", error = %e, "flush_interval_ms update failed"),
        }
        res
    }

    #[instrument(
        name = "runtime.set_stream_hard_cap_rows",
        skip(self),
        fields(stream_id = %id, rows),
        err
    )]
    pub async fn set_stream_hard_cap_rows(&self, id: &StreamId, rows: usize) -> AppResult<bool> {
        let res = self.state.set_hard_cap_rows(id, rows).await;
        match &res {
            Ok(changed) => info!(
                component = "knobs",
                changed = *changed,
                "hard_cap_rows updated"
            ),
            Err(e) => warn!(component = "knobs", error = %e, "hard_cap_rows update failed"),
        }
        res
    }

    #[instrument(
        name = "runtime.set_stream_chunk_rows",
        skip(self),
        fields(stream_id = %id, rows),
        err
    )]
    pub async fn set_stream_chunk_rows(&self, id: &StreamId, rows: usize) -> AppResult<bool> {
        let res = self.state.set_chunk_rows(id, rows).await;
        match &res {
            Ok(changed) => info!(
                component = "knobs",
                changed = *changed,
                "chunk_rows updated"
            ),
            Err(e) => warn!(component = "knobs", error = %e, "chunk_rows update failed"),
        }
        res
    }

    #[instrument(
        name = "runtime.set_stream_batch_knobs",
        skip(self),
        fields(
            stream_id = %id,
            flush_rows,
            flush_interval_ms,
            hard_cap_rows,
            chunk_rows
        ),
        err
    )]
    pub async fn set_stream_batch_knobs(
        &self,
        id: &StreamId,
        flush_rows: usize,
        flush_interval_ms: u64,
        hard_cap_rows: usize,
        chunk_rows: usize,
    ) -> AppResult<bool> {
        let res = self
            .state
            .set_batch_knobs(id, flush_rows, flush_interval_ms, hard_cap_rows, chunk_rows)
            .await;

        match &res {
            Ok(changed) => info!(
                component = "knobs",
                changed = *changed,
                "batch knobs updated"
            ),
            Err(e) => warn!(component = "knobs", error = %e, "batch knobs update failed"),
        }

        res
    }
}

// ------------------------------------------------------------------------
// Prrometheus Metrics
// ------------------------------------------------------------------------

impl AppRuntime {
    pub fn encode_prometheus_text(&self) -> AppResult<String> {
        let mut out = String::new();

        // Always include app metrics
        out.push_str(&self.metrics.encode_text()?);

        // Optional: DB metrics
        if let Some(db) = &self.deps.db {
            // separate registries -> encode + append
            out.push_str(&db.metrics.encode_text()?);
        }

        // Optional: Redis metrics
        if let Some(redis) = &self.deps.redis {
            out.push_str(&redis.metrics.encode_text()?);
        }

        // Optional: ingest metrics
        if let Some(ingest) = &self.deps.ingest_metrics {
            out.push_str(&ingest.encode_text()?);
        }

        out.push('\n');

        Ok(out)
    }
}
