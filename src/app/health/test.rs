// src/app/health/test.rs

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::app::config::load_app_config;
    use crate::error::AppResult;

    use crate::app::health::eval::{RuntimeEvalState, evaluate_runtime};
    use crate::app::health::types::{HealthState, RuntimeSnapshot};

    fn load_cfg() -> Arc<crate::app::config::AppConfig> {
        Arc::new(load_app_config().expect("failed to load app config"))
    }

    #[test]
    fn runtime_health_goes_red_on_high_rss() -> AppResult<()> {
        let cfg = load_cfg();
        let mut state = RuntimeEvalState::default();

        // Force RSS above configured red threshold
        let snap = RuntimeSnapshot {
            rss_mb: cfg
                .health
                .runtime
                .max_rss_mb_red
                .unwrap_or(0)
                .saturating_add(1),
            avail_mb: 10_000,
            fd_pct: 1,
            cpu_pct: 0,
            tick_drift_ms: 0,
        };

        let decision =
            evaluate_runtime(&cfg, cfg.health.runtime.poll_interval_ms, &snap, &mut state);

        println!(
            "[rss test] desired={:?}, reasons={:?}",
            decision.desired, decision.reasons
        );

        assert_eq!(decision.desired, HealthState::Red);
        assert!(decision.reasons.rss);

        Ok(())
    }

    #[test]
    fn runtime_health_drift_requires_sustain() -> AppResult<()> {
        let cfg = load_cfg();
        let mut state = RuntimeEvalState::default();

        let drift_red = cfg.health.runtime.tick_drift_ms_red;
        let sustain = cfg.health.runtime.drift_sustain_ticks;

        println!(
            "[drift test] drift_red={}ms sustain_ticks={}",
            drift_red, sustain
        );

        let base = RuntimeSnapshot {
            rss_mb: 1,
            avail_mb: 10_000,
            fd_pct: 0,
            cpu_pct: 0,
            tick_drift_ms: drift_red + 10,
        };

        // Run sustain_ticks - 1 => still GREEN
        for i in 1..sustain {
            let d = evaluate_runtime(&cfg, cfg.health.runtime.poll_interval_ms, &base, &mut state);
            println!(
                "[drift tick {}] desired={:?}, reasons={:?}",
                i, d.desired, d.reasons
            );
            assert_eq!(d.desired, HealthState::Green);
        }

        // Next tick => RED
        let d = evaluate_runtime(&cfg, cfg.health.runtime.poll_interval_ms, &base, &mut state);

        println!(
            "[drift final] desired={:?}, reasons={:?}",
            d.desired, d.reasons
        );

        assert_eq!(d.desired, HealthState::Red);
        assert!(d.reasons.drift);

        Ok(())
    }

    #[test]
    fn runtime_health_recovers_after_good_tick() -> AppResult<()> {
        let cfg = load_cfg();
        let mut state = RuntimeEvalState::default();

        let bad = RuntimeSnapshot {
            rss_mb: cfg
                .health
                .runtime
                .max_rss_mb_red
                .unwrap_or(0)
                .saturating_add(1),
            avail_mb: 10_000,
            fd_pct: 0,
            cpu_pct: 0,
            tick_drift_ms: 0,
        };

        let good = RuntimeSnapshot {
            rss_mb: 1,
            avail_mb: 10_000,
            fd_pct: 0,
            cpu_pct: 0,
            tick_drift_ms: 0,
        };

        // First go RED
        let d1 = evaluate_runtime(&cfg, cfg.health.runtime.poll_interval_ms, &bad, &mut state);
        println!("[recover test] step1 desired={:?}", d1.desired);
        assert_eq!(d1.desired, HealthState::Red);

        // Then recover
        let d2 = evaluate_runtime(&cfg, cfg.health.runtime.poll_interval_ms, &good, &mut state);
        println!(
            "[recover test] step2 desired={:?}, reasons={:?}",
            d2.desired, d2.reasons
        );
        assert_eq!(d2.desired, HealthState::Green);

        Ok(())
    }

    #[test]
    fn sampler_visualize_runtime_snapshot_values() -> AppResult<()> {
        use std::thread;

        use crate::app::health::sampler::RuntimeSampler;

        // Enable CPU sampling so you can see non-zero values after the first sample.
        let mut sampler = RuntimeSampler::new(true)?;

        println!("--- runtime sampler snapshot visualization ---");

        // Take multiple samples so CPU% has a delta to compute (first is usually 0).
        for i in 0..10 {
            // tick_drift_ms is normally computed by the guard; here we just set 0 for visualization.
            let snap = sampler.sample(0)?;

            println!(
                "sample #{:02}: rss_mb={} avail_mb={} fd_pct={} cpu_pct={} tick_drift_ms={}",
                i, snap.rss_mb, snap.avail_mb, snap.fd_pct, snap.cpu_pct, snap.tick_drift_ms
            );

            // Small sleep so CPU sampler has time delta (>100ms) and values stabilize.
            thread::sleep(std::time::Duration::from_millis(200));
        }

        println!("--- done ---");
        Ok(())
    }
}
