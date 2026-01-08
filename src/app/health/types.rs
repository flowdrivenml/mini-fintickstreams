// src/app/health/types.rs

/// Health state for the runtime guard.
/// GREEN = normal operation
/// RED   = protect the process (deny admissions, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    Green,
    Red,
}

impl HealthState {
    #[inline]
    pub fn is_green(self) -> bool {
        matches!(self, HealthState::Green)
    }

    #[inline]
    pub fn is_red(self) -> bool {
        matches!(self, HealthState::Red)
    }
}

/// Reasons why the runtime guard considers the system "RED".
/// Multi-hot: more than one can be true at the same time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeRedReasons {
    pub rss: bool,
    pub avail_mem: bool,
    pub fd: bool,
    pub drift: bool,
    pub cpu: bool,
}

impl RuntimeRedReasons {
    #[inline]
    pub fn any(&self) -> bool {
        self.rss || self.avail_mem || self.fd || self.drift || self.cpu
    }

    /// Convenience for resetting all reasons to false.
    #[inline]
    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

/// A minimal snapshot of runtime-related measurements.
///
/// Keep this small and stable:
/// - sampler populates it
/// - evaluator uses it
/// - guard may export it for debugging/metrics
#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeSnapshot {
    /// Process resident set size (RAM used by this process), in MB.
    pub rss_mb: u64,

    /// System "available" memory (roughly usable RAM before pressure), in MB.
    pub avail_mb: u64,

    /// File descriptor usage as a percentage (0..=100).
    pub fd_pct: u64,

    /// CPU usage percentage (0..=100). If not sampled, can be 0.
    pub cpu_pct: u64,

    /// Tokio scheduling delay proxy: how late the periodic tick fired, in ms.
    pub tick_drift_ms: u64,
}

/// Output of one evaluation step.
/// The guard will apply hold-down and update shared state accordingly.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeDecision {
    pub desired: HealthState,
    pub reasons: RuntimeRedReasons,
}

impl RuntimeDecision {
    #[inline]
    pub fn green() -> Self {
        Self {
            desired: HealthState::Green,
            reasons: RuntimeRedReasons::default(),
        }
    }

    #[inline]
    pub fn red(reasons: RuntimeRedReasons) -> Self {
        Self {
            desired: HealthState::Red,
            reasons,
        }
    }
}
