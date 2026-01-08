// src/app/health/runtime_eval.rs

use std::sync::Arc;

use super::types::{HealthState, RuntimeDecision, RuntimeRedReasons, RuntimeSnapshot};
use crate::app::config::AppConfig; // adjust path if your AppConfig lives elsewhere

/// Internal evaluator state (kept by the guard between polls).
/// This is NOT config and should not be in AppConfig.
#[derive(Debug, Default, Clone)]
pub struct RuntimeEvalState {
    pub drift_bad_streak: u32,
    pub cpu_bad_streak_ms: u64,
}

impl RuntimeEvalState {
    pub fn reset(&mut self) {
        self.drift_bad_streak = 0;
        self.cpu_bad_streak_ms = 0;
    }
}

/// Evaluate runtime snapshot into a desired health state and reasons.
/// GREEN/RED only: any active reason => desired RED.
///
/// - `poll_interval_ms` is needed to accumulate sustained CPU time.
/// - `state` stores streak counters (drift, cpu) across evaluations.
pub fn evaluate_runtime(
    cfg: &Arc<AppConfig>,
    poll_interval_ms: u64,
    snap: &RuntimeSnapshot,
    state: &mut RuntimeEvalState,
) -> RuntimeDecision {
    // If health or runtime health is disabled, always GREEN.
    // (Caller can also skip calling evaluator; this is just defensive.)
    if !cfg.health.enabled || !cfg.health.runtime.enabled {
        state.reset();
        return RuntimeDecision::green();
    }

    let rt = &cfg.health.runtime;

    let mut reasons = RuntimeRedReasons::default();

    // ----------------------------
    // Memory
    // ----------------------------
    if let Some(max_rss) = rt.max_rss_mb_red {
        if snap.rss_mb >= max_rss {
            reasons.rss = true;
        }
    }

    if let Some(min_avail) = rt.min_avail_mem_mb_red {
        if snap.avail_mb <= min_avail {
            reasons.avail_mem = true;
        }
    }

    // ----------------------------
    // File descriptors
    // ----------------------------
    if let Some(fd_pct_red) = rt.fd_pct_red {
        if snap.fd_pct >= fd_pct_red {
            reasons.fd = true;
        }
    }

    // ----------------------------
    // Drift (Tokio scheduling delay proxy)
    // ----------------------------
    if snap.tick_drift_ms >= rt.tick_drift_ms_red {
        state.drift_bad_streak = state.drift_bad_streak.saturating_add(1);
    } else {
        state.drift_bad_streak = 0;
    }

    if state.drift_bad_streak >= rt.drift_sustain_ticks {
        reasons.drift = true;
    }

    // ----------------------------
    // CPU sustained overload (optional)
    // Only active if BOTH cpu_pct_red and cpu_sustain_sec are set.
    // ----------------------------
    match (rt.cpu_pct_red, rt.cpu_sustain_sec) {
        (Some(cpu_red), Some(sustain_sec)) => {
            if snap.cpu_pct >= cpu_red {
                state.cpu_bad_streak_ms = state.cpu_bad_streak_ms.saturating_add(poll_interval_ms);
            } else {
                state.cpu_bad_streak_ms = 0;
            }

            let sustain_ms = sustain_sec.saturating_mul(1000);
            if sustain_ms > 0 && state.cpu_bad_streak_ms >= sustain_ms {
                reasons.cpu = true;
            }
        }
        (None, None) => {
            // feature disabled; keep counter reset so it doesn't carry across
            state.cpu_bad_streak_ms = 0;
        }
        _ => {
            // misconfigured: you validated config already, so treat as disabled
            state.cpu_bad_streak_ms = 0;
        }
    }

    // ----------------------------
    // Final decision
    // ----------------------------
    if reasons.any() {
        RuntimeDecision::red(reasons)
    } else {
        RuntimeDecision {
            desired: HealthState::Green,
            reasons, // all false
        }
    }
}
