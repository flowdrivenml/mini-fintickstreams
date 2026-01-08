// src/app/health/runtime_guard.rs

use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant};

use crate::app::config::AppConfig;
use crate::app::metrics::AppMetrics;
use crate::error::AppResult;

use super::eval::{RuntimeEvalState, evaluate_runtime};
use super::sampler::RuntimeSampler;
use super::types::{HealthState, RuntimeDecision, RuntimeRedReasons, RuntimeSnapshot};

/// Cloneable handle the rest of the app can use for fast gating.
///
/// - lock-free read
/// - GREEN=0, RED=1
#[derive(Clone, Debug)]
pub struct RuntimeHealthHandle {
    state: Arc<AtomicU8>,
}

impl RuntimeHealthHandle {
    #[inline]
    pub fn state(&self) -> HealthState {
        match self.state.load(Ordering::Relaxed) {
            0 => HealthState::Green,
            _ => HealthState::Red,
        }
    }

    #[inline]
    pub fn is_green(&self) -> bool {
        self.state() == HealthState::Green
    }

    #[inline]
    pub fn is_red(&self) -> bool {
        self.state() == HealthState::Red
    }
}

/// Starts the runtime health guard background task and returns a handle + join handle.
///
/// Metrics integration is optional: pass `Some(metrics)` to record gauges/counters.
pub fn start_runtime_health_guard(
    cfg: Arc<AppConfig>,
    metrics: Option<Arc<AppMetrics>>,
    shutdown: tokio_util::sync::CancellationToken,
) -> AppResult<(RuntimeHealthHandle, JoinHandle<()>)> {
    let state = Arc::new(AtomicU8::new(0)); // start GREEN
    let handle = RuntimeHealthHandle {
        state: state.clone(),
    };

    // If disabled, return handle (always green) and a tiny task to set gauges once.
    if !cfg.health.enabled || !cfg.health.runtime.enabled {
        let jh = tokio::spawn(async move {
            if let Some(m) = metrics {
                m.set_runtime_health(true);
                m.set_runtime_red_reasons(false, false, false, false, false);
                m.set_health(true);
            }
        });
        return Ok((handle, jh));
    }

    let poll_ms = cfg.health.runtime.poll_interval_ms.max(1);
    let hold_down_ms = cfg.health.runtime.hold_down_ms.max(1);

    // Enable CPU sampling only if BOTH thresholds are configured.
    let enable_cpu =
        cfg.health.runtime.cpu_pct_red.is_some() && cfg.health.runtime.cpu_sustain_sec.is_some();

    let mut sampler = RuntimeSampler::new(enable_cpu)?;

    let jh = tokio::spawn(async move {
        let poll = Duration::from_millis(poll_ms);
        let mut interval = time::interval(poll);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        // Drift bookkeeping
        let mut expected_next = Instant::now() + poll;

        // Evaluator streak state (drift streak, cpu streak)
        let mut eval_state = RuntimeEvalState::default();

        // Hold-down state machine
        let mut applied_state = HealthState::Green;
        let mut desired_state = HealthState::Green;
        let mut desired_since: Option<Instant> = None;

        // last computed reasons (for metrics)
        let mut last_reasons = RuntimeRedReasons::default();

        // Initialize metrics once
        if let Some(m) = metrics.as_ref() {
            m.set_runtime_health(true);
            m.set_runtime_red_reasons(false, false, false, false, false);
            m.set_health(true);
        }

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("runtime health guard shutting down");
                    break;
                }
                _ = interval.tick() => {}
            }
            let now = Instant::now();

            // Drift: how late are we relative to expected schedule?
            let drift_ms = now.saturating_duration_since(expected_next).as_millis() as u64;

            expected_next = expected_next + poll;

            // 1) SAMPLE
            // If sampling fails, we conservatively force RED (reason: drift).
            let decision: RuntimeDecision = match sampler.sample(drift_ms) {
                Ok(snap) => {
                    // 2) EVALUATE
                    evaluate_runtime(&cfg, poll_ms, &snap, &mut eval_state)
                }
                Err(_e) => {
                    let mut rr = RuntimeRedReasons::default();
                    rr.drift = true;

                    // Also reset evaluator streaks to avoid carrying weird partial state.
                    eval_state.reset();

                    // Create a "fake" snapshot is unnecessary; we can directly decide RED.
                    RuntimeDecision::red(rr)
                }
            };

            last_reasons = decision.reasons;
            let next_desired = decision.desired;

            // 3) HOLD-DOWN TRACKING
            if next_desired != desired_state {
                desired_state = next_desired;
                desired_since = Some(now);
            }

            // 4) APPLY HOLD-DOWN
            let should_apply = desired_since
                .map(|since| since.elapsed() >= Duration::from_millis(hold_down_ms))
                .unwrap_or(false);

            if should_apply && desired_state != applied_state {
                applied_state = desired_state;
                desired_since = None;

                match applied_state {
                    HealthState::Green => {
                        state.store(0, Ordering::Relaxed);
                        if let Some(m) = metrics.as_ref() {
                            m.inc_runtime_to_green();
                        }
                    }
                    HealthState::Red => {
                        state.store(1, Ordering::Relaxed);
                        if let Some(m) = metrics.as_ref() {
                            m.inc_runtime_to_red();
                        }
                    }
                }
            }

            // 5) METRICS UPDATE (every poll)
            if let Some(m) = metrics.as_ref() {
                let healthy = applied_state == HealthState::Green;

                m.set_runtime_health(healthy);
                m.set_runtime_red_reasons(
                    last_reasons.rss,
                    last_reasons.avail_mem,
                    last_reasons.fd,
                    last_reasons.drift,
                    last_reasons.cpu,
                );

                // If you want "app_health" to reflect runtime health for now:
                m.set_health(healthy);
            }
        }
    });

    Ok((handle, jh))
}
