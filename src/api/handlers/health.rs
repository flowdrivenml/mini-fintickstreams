use axum::{Json, extract::State};

use crate::api::types::HealthResp;
use crate::app::AppRuntime;

pub async fn runtime(State(app): State<AppRuntime>) -> Json<HealthResp> {
    Json(HealthResp {
        ok: app.runtime_ok(),
    })
}

pub async fn db(State(app): State<AppRuntime>) -> Json<HealthResp> {
    // "ok" means: DB gate enabled AND DB initialized AND health says we can admit.
    let ok = app.deps.is_db_enabled()
        && app
            .deps
            .db
            .as_ref()
            .is_some_and(|db| db.health.as_ref().can_admit_new_stream().is_none());

    Json(HealthResp { ok })
}

pub async fn redis(State(app): State<AppRuntime>) -> Json<HealthResp> {
    // "ok" means: redis gate enabled AND redis initialized AND manager can publish
    let ok = app.deps.is_redis_enabled()
        && app
            .deps
            .redis
            .as_ref()
            .is_some_and(|r| r.manager.can_publish());

    Json(HealthResp { ok })
}
