use crate::db::config::{AdmissionPolicy, HealthConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    Green = 0,
    Yellow = 1,
    Red = 2,
}

impl HealthState {
    pub fn as_i64(self) -> i64 {
        self as i64
    }
}

#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub state: HealthState,
    pub can_admit_new_stream: bool,
    pub reasons: Vec<&'static str>,
}

impl HealthStatus {
    pub fn new(cfg: &HealthConfig, state: HealthState, reasons: Vec<&'static str>) -> Self {
        let can_admit_new_stream = match cfg.admission_policy {
            AdmissionPolicy::GreenOnly => state == HealthState::Green,
            AdmissionPolicy::GreenOrYellow => state != HealthState::Red,
        };
        Self {
            state,
            can_admit_new_stream,
            reasons,
        }
    }
}
