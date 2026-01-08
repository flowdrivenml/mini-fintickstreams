use crate::app::AppConfig;
use crate::error::{AppError, AppResult};
use crate::ingest::config::{ExchangeConfig, StringOrTable, WsStream};
use crate::ingest::metrics::IngestMetrics;
use crate::ingest::spec::{Ctx, resolve_ws_control, seed_ws_stream_ctx};
use crate::ingest::ws::limiter_registry::WsLimiterRegistry;
use futures_util::{SinkExt, StreamExt};
use rand::{Rng, rng};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, interval};
use tokio::time::{sleep, sleep_until};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub enum WsEvent {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Option<String>),
}

#[derive(Debug)]
pub struct WsClient {
    pub name: &'static str, // "binance_linear", "hyperliquid_perp"
    pub cfg: ExchangeConfig,
    pub metrics: Option<Arc<IngestMetrics>>,

    pub ws_reconnect_backoff_initial_ms: u64,
    pub ws_reconnect_backoff_max_ms: u64,
    pub ws_reconnect_trip_after_failures: u32,
    pub ws_reconnect_cooldown_seconds: u64,
}

impl WsClient {
    // sensible defaults (same as we recommended for TOML)
    const DEFAULT_WS_RECONNECT_BACKOFF_INITIAL_MS: u64 = 500;
    const DEFAULT_WS_RECONNECT_BACKOFF_MAX_MS: u64 = 30_000;
    const DEFAULT_WS_RECONNECT_TRIP_AFTER_FAILURES: u32 = 10;
    const DEFAULT_WS_RECONNECT_COOLDOWN_SECONDS: u64 = 120;
    pub fn new(
        name: &'static str,
        cfg: ExchangeConfig,
        metrics: Option<Arc<IngestMetrics>>,
        app_cfg: Option<&AppConfig>,
    ) -> Self {
        // pick from app config if provided, otherwise defaults
        let (initial_ms, max_ms, trip_after, cooldown_s) = match app_cfg.as_deref() {
            Some(ac) => (
                ac.streams.ws_reconnect_backoff_initial_ms,
                ac.streams.ws_reconnect_backoff_max_ms,
                ac.streams.ws_reconnect_trip_after_failures,
                ac.streams.ws_reconnect_cooldown_seconds,
            ),
            None => (
                Self::DEFAULT_WS_RECONNECT_BACKOFF_INITIAL_MS,
                Self::DEFAULT_WS_RECONNECT_BACKOFF_MAX_MS,
                Self::DEFAULT_WS_RECONNECT_TRIP_AFTER_FAILURES,
                Self::DEFAULT_WS_RECONNECT_COOLDOWN_SECONDS,
            ),
        };

        Self {
            name,
            cfg,
            metrics,
            ws_reconnect_backoff_initial_ms: initial_ms,
            ws_reconnect_backoff_max_ms: max_ms,
            ws_reconnect_trip_after_failures: trip_after,
            ws_reconnect_cooldown_seconds: cooldown_s,
        }
    }

    /// Run ONE stream per connection.
    ///
    /// ws_limiters is optional to make tests easier (no registry needed).
    /// test_hook is optional to allow terminating the reconnect loop deterministically in tests.
    pub async fn run_stream<F, Fut>(
        &self,
        ws_limiters: Option<&WsLimiterRegistry>,
        stream: &WsStream,
        mut ctx: Ctx,
        mut on_event: F,
        test_hook: Option<&mut WsTestHook>,
        cancel: Option<CancellationToken>,
    ) -> AppResult<()>
    where
        F: FnMut(WsEvent) -> Fut,
        Fut: std::future::Future<Output = AppResult<()>>,
    {
        seed_ws_stream_ctx(stream, &mut ctx)?;

        ctx.entry("stream_id".to_string())
            .or_insert_with(|| "1".to_string());

        let control = resolve_ws_control(&self.cfg, &ctx)?;

        self.connect_loop(
            ws_limiters,
            control.subscribe,
            control.unsubscribe,
            on_event,
            test_hook,
            cancel,
        )
        .await
    }

    async fn connect_loop<F, Fut>(
        &self,
        ws_limiters: Option<&WsLimiterRegistry>,
        subscribe_msg: JsonValue,
        unsubscribe_msg: JsonValue,
        mut on_event: F,
        mut test_hook: Option<&mut WsTestHook>,
        cancel: Option<CancellationToken>,
    ) -> AppResult<()>
    where
        F: FnMut(WsEvent) -> Fut,
        Fut: std::future::Future<Output = AppResult<()>>,
    {
        let cancel = cancel.unwrap_or_else(CancellationToken::new);

        let mut consecutive_failures: u32 = 0;
        let mut backoff_ms: u64 = self.ws_reconnect_backoff_initial_ms;

        loop {
            if cancel.is_cancelled() {
                info!(exchange = self.name, "ws cancelled (outer loop)");
                return Ok(());
            }

            // --- TEST HOOK
            if let Some(h) = test_hook.as_deref_mut() {
                if !h.on_before_reconnect_attempt() {
                    return Ok(());
                }
            }

            // --- RECONNECT limiter
            if let Some(lims) = ws_limiters {
                lims.acquire_reconnect(self.name).await?;
            }

            let url = self.cfg.ws_base_url.clone();
            info!(exchange = self.name, url = %url, "ws connecting");

            let (ws, _resp) = match connect_async(url).await {
                Ok(ok) => ok,
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    warn!(
                        exchange = self.name,
                        failures = consecutive_failures,
                        error = %e,
                        "ws connect failed"
                    );
                    reconnect_sleep(&cancel, self, &mut consecutive_failures, &mut backoff_ms)
                        .await?;
                    continue;
                }
            };

            let (mut write, mut read) = ws.split();

            // --- SUBSCRIBE limiter
            if let Some(lims) = ws_limiters {
                lims.acquire_subscribe(self.name).await?;
            }

            if let Err(e) = send_ws_payload(&mut write, &subscribe_msg).await {
                consecutive_failures = consecutive_failures.saturating_add(1);
                warn!(
                    exchange = self.name,
                    failures = consecutive_failures,
                    error = %e,
                    "ws subscribe failed"
                );
                reconnect_sleep(&cancel, self, &mut consecutive_failures, &mut backoff_ms).await?;
                continue;
            }

            // success path → reset breaker state
            consecutive_failures = 0;
            backoff_ms = self.ws_reconnect_backoff_initial_ms;

            let mut hb = self.heartbeat_sender();

            let timeout_secs = self.cfg.ws_connection_timeout_seconds;
            let deadline = if timeout_secs > 0 {
                Some(Instant::now() + Duration::from_secs(timeout_secs))
            } else {
                None
            };

            let mut close_reason: Option<String> = None;

            loop {
                tokio::select! {
                        _ = cancel.cancelled() => {
                            close_reason = Some("cancelled".into());
                            break;
                        }

                // deadline branch: build an OWNED Sleep future each time (no &mut Sleep)
                _ = async {
                    if let Some(dl) = deadline {
                        tokio::time::sleep_until(dl).await;
                    } else {
                        futures_util::future::pending::<()>().await;
                    }
                } => {
                    close_reason = Some("ws_connection_timeout_seconds reached".into());
                    break;
                }

                _ = async {
                    if let Some(hb_tick) = hb.as_mut() {
                        hb_tick.tick().await;
                    } else {
                        futures_util::future::pending::<()>().await;
                    }
                } => {
                    if let Err(e) = maybe_send_ws_heartbeat(&self.cfg, &mut write).await {
                        close_reason = Some(format!("heartbeat error: {e}"));
                        break;
                    }
                }

                            msg = read.next() => {
                                let msg = match msg {
                                    Some(Ok(m)) => m,
                                    Some(Err(e)) => {
                                        close_reason = Some(format!("read error: {e}"));
                                        error!(exchange = self.name, error = %e, "ws read error");
                                        break;
                                    }
                                    None => {
                                        close_reason = Some("stream ended".into());
                                        break;
                                    }
                                };

                                match msg {
                                    Message::Text(s) => {
                                        if let Some(m) = &self.metrics { m.inc_in(); }
                                        on_event(WsEvent::Text(s.to_string())).await?;
                                        if let Some(m) = &self.metrics { m.inc_processed(); }
                                    }
                                    Message::Binary(b) => {
                                        if let Some(m) = &self.metrics { m.inc_in(); }
                                        on_event(WsEvent::Binary(b.to_vec())).await?;
                                        if let Some(m) = &self.metrics { m.inc_processed(); }
                                    }
                                    Message::Ping(p) => {
                                        on_event(WsEvent::Ping(p.clone().to_vec())).await?;
                                        let _ = write.send(Message::Pong(p)).await;
                                    }
                                    Message::Pong(p) => {
                                        on_event(WsEvent::Pong(p.to_vec())).await?;
                                    }
                                    Message::Close(frame) => {
                                        close_reason = Some(format!("close: {:?}", frame));
                                        let _ = on_event(WsEvent::Close(close_reason.clone())).await;
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
            }
            // best-effort unsubscribe
            let _ = send_ws_payload(&mut write, &unsubscribe_msg).await;

            if cancel.is_cancelled() {
                info!(exchange = self.name, "ws cancelled; not reconnecting");
                return Ok(());
            }

            warn!(
                exchange = self.name,
                reason = %close_reason.clone().unwrap_or_else(|| "unknown".into()),
                "ws reconnecting"
            );

            if let Some(h) = test_hook.as_deref_mut() {
                h.on_disconnected(close_reason.as_deref());
            }

            consecutive_failures = consecutive_failures.saturating_add(1);

            reconnect_sleep(&cancel, self, &mut consecutive_failures, &mut backoff_ms).await?;
        }
    }

    fn heartbeat_sender(&self) -> Option<HeartbeatDriver> {
        let hb_type = self.cfg.ws_heartbeat_type.as_ref()?.to_lowercase();
        if hb_type != "ping" {
            return None;
        }

        let timeout = self.cfg.ws_heartbeat_timeout_seconds.unwrap_or(30);
        let period = std::cmp::max(1, timeout / 2);

        Some(HeartbeatDriver {
            interval: interval(Duration::from_secs(period)),
            frame: self.cfg.ws_heartbeat_frame.clone(),
        })
    }
}

// helper: breaker + backoff + jitter, cancellable sleep
async fn reconnect_sleep(
    cancel: &CancellationToken,
    client: &WsClient,
    consecutive_failures: &mut u32,
    backoff_ms: &mut u64,
) -> AppResult<()> {
    use rand::Rng;
    use tokio::time::sleep;

    if cancel.is_cancelled() {
        return Ok(());
    }

    // --------------------------------------------------
    // Circuit breaker
    // --------------------------------------------------
    if *consecutive_failures >= client.ws_reconnect_trip_after_failures {
        let cooldown = Duration::from_secs(client.ws_reconnect_cooldown_seconds);

        warn!(
            exchange = client.name,
            failures = *consecutive_failures,
            cooldown_secs = client.ws_reconnect_cooldown_seconds,
            "ws breaker tripped; cooling down"
        );

        tokio::select! {
            _ = cancel.cancelled() => {
                return Ok(());
            }
            _ = sleep(cooldown) => {
                *consecutive_failures = 0;
                *backoff_ms = client.ws_reconnect_backoff_initial_ms;
                return Ok(());
            }
        }
    }

    // --------------------------------------------------
    // Exponential backoff with jitter (rand 0.9.x)
    // IMPORTANT: RNG must NOT live across `.await`
    // --------------------------------------------------
    let sleep_ms: u64 = {
        let jitter = 0.2_f64;
        let mut rng = rand::rng(); // ✅ rand 0.9 API
        let j = rng.random_range(-jitter..=jitter);
        ((*backoff_ms as f64) * (1.0 + j)).max(0.0) as u64
    };

    tokio::select! {
        _ = cancel.cancelled() => {
            return Ok(());
        }
        _ = sleep(Duration::from_millis(sleep_ms)) => {}
    }

    // --------------------------------------------------
    // Increase backoff (capped)
    // --------------------------------------------------
    let next = (*backoff_ms).saturating_mul(2);
    *backoff_ms = std::cmp::min(next, client.ws_reconnect_backoff_max_ms);

    Ok(())
}

/// Optional test hook to make reconnect loops deterministic in tests.
#[derive(Debug, Default)]
pub struct WsTestHook {
    /// If Some(n): allow at most n reconnect attempts. After that, stop (return Ok(())).
    pub max_reconnect_attempts: Option<u32>,
    pub reconnect_attempts: u32,
    /// Optional callback-like storage for assertions.
    pub disconnects: Vec<Option<String>>,
}

impl WsTestHook {
    /// Called at the start of each outer loop iteration, before attempting connect/reconnect.
    /// Return false to stop the loop.
    pub fn on_before_reconnect_attempt(&mut self) -> bool {
        self.reconnect_attempts = self.reconnect_attempts.saturating_add(1);
        match self.max_reconnect_attempts {
            Some(max) => self.reconnect_attempts <= max,
            None => true,
        }
    }

    /// Called after a disconnect (inner loop ended) with the close reason (if any).
    pub fn on_disconnected(&mut self, reason: Option<&str>) {
        self.disconnects.push(reason.map(|s| s.to_string()));
    }
}

struct HeartbeatDriver {
    interval: tokio::time::Interval,
    frame: Option<StringOrTable>,
}

impl HeartbeatDriver {
    async fn tick(&mut self) -> Option<()> {
        self.interval.tick().await;
        Some(())
    }
}

async fn send_ws_payload<S>(write: &mut S, payload: &serde_json::Value) -> AppResult<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    write
        .send(Message::Text(payload.to_string().into()))
        .await
        .map_err(|e| AppError::Internal(format!("ws send json error: {e}")))?;
    Ok(())
}

async fn maybe_send_ws_heartbeat<S>(cfg: &ExchangeConfig, write: &mut S) -> AppResult<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let hb_type = match cfg.ws_heartbeat_type.as_deref() {
        Some(t) => t.to_lowercase(),
        None => return Ok(()),
    };

    if hb_type != "ping" {
        return Ok(());
    }

    match &cfg.ws_heartbeat_frame {
        Some(StringOrTable::String(s)) => {
            write
                .send(Message::Text(s.clone().into()))
                .await
                .map_err(|e| AppError::Internal(format!("ws heartbeat send text error: {e}")))?;
        }
        Some(StringOrTable::Table(v)) => {
            let json = serde_json::to_string(v)
                .map_err(|e| AppError::Internal(format!("ws heartbeat serialize error: {e}")))?;

            write
                .send(Message::Text(json.into()))
                .await
                .map_err(|e| AppError::Internal(format!("ws heartbeat send json error: {e}")))?;
        }
        None => {
            write
                .send(Message::Ping(Vec::new().into()))
                .await
                .map_err(|e| AppError::Internal(format!("ws heartbeat send ping error: {e}")))?;
        }
    }

    Ok(())
}
