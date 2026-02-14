use axum::{
    Router,
    routing::{delete, get, patch, post},
};

use crate::app::AppRuntime;

use super::handlers::{health, instruments_axum, knobs, limiters, streams, ui};

pub fn build_router(app: AppRuntime) -> Router {
    Router::new()
        // -----------------------
        // Health
        // -----------------------
        .route("/health/runtime", get(health::runtime))
        .route("/health/db", get(health::db))
        .route("/health/redis", get(health::redis))
        // -----------------------
        // Capabilities
        // -----------------------
        .route("/streams/capabilities", get(streams::capabilities))
        // (optional but recommended)
        // .route("/streams/capabilities/:exchange", get(streams::capabilities_exchange))
        // -----------------------
        // Streams control + status
        // -----------------------
        .route("/streams", get(streams::list))
        .route("/streams", post(streams::add))
        .route("/streams", delete(streams::remove))
        .route("/streams/count", get(streams::count))
        .route(
            "/streams/{exchange}/{symbol}/{kind}/{transport}",
            get(streams::get_one),
        )
        // -----------------------
        // Knobs
        // -----------------------
        .route(
            "/streams/{exchange}/{symbol}/{kind}/{transport}/knobs",
            get(knobs::get),
        )
        .route(
            "/streams/{exchange}/{symbol}/{kind}/{transport}/knobs",
            patch(knobs::patch),
        )
        // -----------------------
        // Instruments registry
        // -----------------------
        .route("/instruments", get(instruments_axum::list))
        .route("/instruments/count", get(instruments_axum::count))
        .route("/instruments/exists", get(instruments_axum::exists))
        .route("/instruments/refresh", post(instruments_axum::refresh))
        // -----------------------
        // Limiters (NEW)
        // -----------------------
        .route("/limiters", get(limiters::get_limiters))
        .route("/ui", get(ui::index))
        .route("/ui/health", get(ui::health_panel))
        .route("/ui/streams/table", get(ui::streams_table))
        .route("/ui/streams/start", post(ui::streams_start))
        .route("/ui/streams/stop", post(ui::streams_stop))
        .route(
            "/ui/streams/{exchange}/{symbol}/{kind}/{transport}/knobs",
            get(ui::knobs_panel),
        )
        .route(
            "/ui/streams/{exchange}/{symbol}/{kind}/{transport}/knobs",
            post(ui::knobs_save),
        )
        .route("/ui/limiters", get(ui::limiters_panel))
        .route("/ui/instruments", get(ui::instruments_page)) // optional small page/fragment
        .route("/ui/instruments/refresh", post(ui::instruments_refresh))
        .route("/ui/instruments/exists", post(ui::instruments_exists))
        .route("/ui/instruments/list", get(ui::instruments_list))
        .route("/ui/streams/capabilities", get(ui::capabilities_panel))
        // ✅ ALWAYS last
        .with_state(app)
}
