use crate::app::AppRuntime;
use crate::app::control::stream::StartStreamParams;
use crate::app::state::StreamKnobs;
use crate::app::stream_types::StreamId;
use crate::app::stream_types::{ExchangeId, StreamKind, StreamTransport};
use crate::ingest::instruments::spec::{InstrumentKind, InstrumentSpec};

use axum::{
    extract::{Path, Query, State},
    response::Html,
};
use axum_extra::extract::Form;
use maud::{DOCTYPE, Markup, html};
use serde::Deserialize;
use std::collections::HashMap;

// ============================================================
// Helpers
// ============================================================

fn rotating_tagline() -> &'static str {
    const LINES: &[&str] = &[
        "price is truth, latency is your fucking god now",
        "no signals. just consequences, you absolute clown",
        "liquidity? cute fairy tale for suckers",
        "you're not early, you're just the first retard to get rekt",
        "risk hits like lightning, your logs arrive next Tuesday",
        "the spread wants your soul on a platter",
        "funding rate = vibes priced in blood and hopium",
        "your PNL is a horror movie starring your dumb ass",
        "quiet orderbook = nature's way of saying \"you're fucked\"",
        "the market doesn't teach. it just fucking robs you.",
        "every chart is a crime scene and you're the victim",
        "each green candle is a rental, each red one owns your children",
        "volatility isn't a heartbeat — it's cardiac arrest you begged for",
        "if it looks obvious, congrats: you're the bait, dipshit",
        "leverage = emotional time-travel straight to regret city",
        "the spread knows your wallet address and your mom's maiden name",
        "slippage is the market giggling while it raw-dogs you",
        "your stop-loss? just a polite \"please rob me\" button",
        "orderbook depth is fanfiction for people who hate money",
        "that wick? that's where your dreams went to get curb-stomped",
        "your edge is probably a rounding error + copium",
        "trend is your friend until the invoice has 7 digits",
        "the market is very efficient at making *you* specifically suffer",
        "limit order? it'll fill 0.3 seconds after you cancel, guaranteed",
        "market order = congrats, you bought the literal top wick",
        "best strategy is sleep. unfortunately you're addicted to pain",
        "you can't hedge your own retardation",
        "technical analysis = astrology for people who hate therapy",
        "RSI stands for \"Really Still In? you fucking idiot\"",
        "Bollinger Bands = the market's mood ring and it's pissed",
        "volume is screaming. you're just too regarded to listen",
        "breakout = fancy word for \"exit liquidity go brrr\"",
        "mean reversion is the lie you tell yourself at 3 a.m.",
        "liquidations = the only applause you'll ever get",
        "funding is paying you in pure distilled sarcasm",
        "every bid is cope, every ask is a middle finger",
        "your PNL is God's personal roast session",
        "timeframes = different resolutions at which you can be wrong",
        "you don't trade markets, you trade your own panic attacks",
        "smooth UI = they spent the dev budget on misleading you",
        "quiet book = the calm before your margin call",
        "the wick IS the price. the close is marketing.",
        "if it's pumping on Twitter → already dead, just twitching",
        "your beautiful thesis dies violently at 00:01 UTC",
        "conviction lasts until the first -15% drawdown, then poof",
        "backtest looks amazing because it doesn't contain your pussy hands",
        "risk management = trying to outsmart your own suicide",
        "position sizing = the fine art of not imploding today",
        "best trade is often \"fuck this, I'm out\"",
        "diamond hands = delayed realisation you're broke",
        "paper hands = early admission you're not built for this",
        "pump is temporary. the cringe screenshot is eternal.",
        "if you can't explain your position, it's 100% leverage cope",
        "your liq price is literally the plot twist",
        "market loves perfect symmetry and hates your entry",
        "buy the dip → said the guy currently buying lower lows",
        "chop = the market charging you rent for being optimistic",
        "your stop is where whales spawn baby whales",
        "your latency = someone else's alpha lunch money",
        "strong hands? nah, just numb from holding this corpse",
        "price discovery = discovering how wrong and poor you are",
        "every support level is just a polite suggestion to get rekt",
        "every resistance is the market daring you to YOLO",
        "your take-profit = birthplace of the nastiest reversal",
        "if you feel smart right now → reduce size, moron",
        "if you feel scared → reduce size *anyway*, coward",
        "discipline = actually respecting the stop you set while drunk",
        "patience = watching other degens get liquidated first",
        "your execution is 90% of why your children will hate you",
        "indicators are just expensive wall decoration",
        "moving average = sophisticated moving excuse",
        "MACD = fancy histogram that says \"you're fucked\" slower",
        "the market is liquid… right until you actually need it",
        "deep book until you hit send, then it's a desert",
        "you can't outrun a gap. gaps don't negotiate.",
        "diversification = spreading your misery across more tickers",
        "carry trade = picking up nickels in front of a steamroller",
        "tail risk = the part that eventually fucks your entire soul",
        "you're always one candle away from either wisdom or liquidation",
        "trade small, dream big, journal every embarrassment",
        "your worst trades all started with \"this feels obvious\"",
        "your best trades felt boring as fuck — that's the sign",
        "exciting trades = expensive dopamine hits",
        "the market pays boredom and punishes ego hard",
        "ego is the most leveraged and worst position you'll ever take",
        "hope isn't a strategy. it's a suicide method.",
        "revenge trading = donating money to people who actually win",
        "your logs remember every lie you told yourself",
        "boring is profitable. exciting is bankruptcy speedrun",
    ];

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as usize;

    LINES[nanos % LINES.len()]
}

fn symbol_matches(sym: &str, needle: &str, mode: &str) -> bool {
    let s = sym.to_lowercase();
    let n = needle.to_lowercase();

    match mode {
        "starts" => s.starts_with(&n),
        "ends" => s.ends_with(&n),
        "exact" => s == n,
        _ => s.contains(&n), // default: contains
    }
}

fn parse_instrument_kind_ui(kind: &str) -> Result<InstrumentKind, String> {
    let k = kind.trim();

    // support both exact enum names and lowercase variants
    let normalized = k.to_lowercase().replace('_', "");

    match normalized.as_str() {
        "spot" => Ok(InstrumentKind::Spot),
        "perplinear" => Ok(InstrumentKind::PerpLinear),
        "perpinverse" => Ok(InstrumentKind::PerpInverse),
        "futurelinear" => Ok(InstrumentKind::FutureLinear),
        "futureinverse" => Ok(InstrumentKind::FutureInverse),
        "options" => Ok(InstrumentKind::Options),
        other => Err(format!("unknown InstrumentKind: {other}")),
    }
}

fn parse_exchange_id_ui(exchange_str: &str) -> Result<ExchangeId, String> {
    let normalized = exchange_str.trim().to_lowercase().replace('_', "");
    match normalized.as_str() {
        "binancelinear" => Ok(ExchangeId::BinanceLinear),
        "hyperliquidperp" => Ok(ExchangeId::HyperliquidPerp),
        "bybitlinear" => Ok(ExchangeId::BybitLinear),
        other => Err(format!("unknown exchange: {other}")),
    }
}

fn parse_kind_ui(kind: &str) -> Result<StreamKind, String> {
    match kind.trim() {
        "Trades" => Ok(StreamKind::Trades),
        "L2Book" => Ok(StreamKind::L2Book),
        "Ticker" => Ok(StreamKind::Ticker),
        "Funding" => Ok(StreamKind::Funding),
        "OpenInterest" => Ok(StreamKind::OpenInterest),
        "Liquidations" => Ok(StreamKind::Liquidations),
        "FundingOpenInterest" => Ok(StreamKind::FundingOpenInterest),
        other => Err(format!("unknown kind: {other}")),
    }
}

fn parse_transport_ui(t: &str) -> Result<StreamTransport, String> {
    match t.trim() {
        "Ws" => Ok(StreamTransport::Ws),
        "HttpPoll" => Ok(StreamTransport::HttpPoll),
        other => Err(format!("unknown transport: {other}")),
    }
}

fn pill(ok: bool, label: &str) -> Markup {
    let cls = if ok {
        "badge badge-success badge-outline"
    } else {
        "badge badge-error badge-outline"
    };
    html! { span class=(cls) { (label) } }
}

/// Out-of-band “toast” update into <div id="flash"></div>
fn flash_oob(kind: &str, msg: &str) -> Markup {
    let cls = match kind {
        "success" => "alert alert-success shadow-lg",
        "error" => "alert alert-error shadow-lg",
        "warning" => "alert alert-warning shadow-lg",
        _ => "alert alert-info shadow-lg",
    };

    html! {
        div id="flash" hx-swap-oob="true" class=(cls) {
            div { (msg) }
        }
    }
}

fn status_dot_and_badge(status_str: &str) -> (&'static str, &'static str) {
    if status_str.contains("Running") {
        ("bg-emerald-400", "badge badge-success badge-outline")
    } else if status_str.contains("Starting") {
        ("bg-amber-400", "badge badge-warning badge-outline")
    } else if status_str.contains("Failed") || status_str.contains("Error") {
        ("bg-rose-400", "badge badge-error badge-outline")
    } else if status_str.contains("Stopping") {
        ("bg-amber-400", "badge badge-warning badge-outline")
    } else {
        ("bg-slate-400", "badge badge-ghost")
    }
}

#[derive(Debug, Deserialize)]
pub struct KnobsForm {
    pub disable_db_writes: Option<String>,
    pub disable_redis_publishes: Option<String>,
    pub flush_rows: Option<usize>,
    pub flush_interval_ms: Option<u64>,
    pub chunk_rows: Option<usize>,
    pub hard_cap_rows: Option<usize>,
}

fn checkbox_on(v: &Option<String>) -> bool {
    matches!(v.as_deref(), Some("on") | Some("true") | Some("1"))
}

// ============================================================
// /ui  (Trading Terminal UI)
// ============================================================

pub async fn index(State(_app): State<AppRuntime>) -> Html<String> {
    let page = html! {
        (DOCTYPE)
        html data-theme="dracula" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "mini-fintickstreams" }

                script src="https://unpkg.com/htmx.org@1.9.12" {}
                script src="https://cdn.tailwindcss.com" {}
                link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/daisyui@4.12.10/dist/full.min.css" {}

                style { r#"
                    .glass { background: rgba(18, 18, 28, 0.55); backdrop-filter: blur(10px); }
                    .grid-bg {
                        background-image:
                            radial-gradient(rgba(120,120,255,.10) 1px, transparent 1px),
                            radial-gradient(rgba(120,255,255,.08) 1px, transparent 1px);
                        background-position: 0 0, 12px 12px;
                        background-size: 24px 24px;
                    }
                "# }
            }

            body class="min-h-screen grid-bg bg-gradient-to-br from-[#050814] via-[#070A1A] to-[#050B10]" {
                // Toasts: target for hx-swap-oob="beforeend"
                div id="toasts" class="toast toast-top toast-end z-50" {}

                div class="max-w-7xl mx-auto p-6 space-y-5" {

                    // Top bar
                    div class="navbar glass border border-base-300 rounded-2xl shadow-xl" {
                        div class="flex-1 flex items-center gap-3" {
                            div class="text-xl font-bold tracking-wide" { "mini-fintickstreams" }
                            span class="badge badge-primary badge-outline font-mono" { "LIVE ORDERFLOW" }
                            span class="badge badge-ghost font-mono opacity-80" {
                                (rotating_tagline())
                            }
                        }
                        div class="flex-none flex items-center gap-2" {
                            div
                                id="health"
                                hx-get="/ui/health"
                                hx-trigger="load, every 2s"
                                hx-swap="innerHTML" {
                                span class="loading loading-dots loading-sm" {}
                            }

                            span class="badge badge-outline" {
                                span class="loading loading-ring loading-xs mr-2" {}
                                "live"
                            }
                        }
                    }

                    // Main layout
                    div class="grid grid-cols-1 lg:grid-cols-3 gap-5" {

                        // LEFT: Streams
                        div class="lg:col-span-2 space-y-4" {
                            div class="card glass border border-base-300 shadow-xl rounded-2xl" {
                                div class="card-body space-y-4" {
                                    div class="flex items-center justify-between" {
                                        h2 class="card-title" { "Streams 🌊" }
                                        span class="badge badge-info badge-outline" { "auto-refresh 2s" }
                                    }

                                    div
                                        id="streams-table"
                                        hx-get="/ui/streams/table"
                                        hx-trigger="load, every 2s"
                                        hx-swap="outerHTML settle:150ms" {
                                        div class="skeleton h-24 w-full" {}
                                    }
                                }
                            }
                        }

                        // RIGHT: Controls
                        div class="space-y-4" {

                            // Start Stream panel
                            div class="card glass border border-base-300 shadow-xl rounded-2xl" {
                                div class="card-body space-y-3" {
                                    h3 class="card-title" { "Start Stream ▷" }
                                    p class="text-sm opacity-70" { "Click to control streams. No curl, no JSON." }

                                    form class="grid grid-cols-2 gap-2"
                                        hx-post="/ui/streams/start"
                                        hx-target="#streams-table"
                                        hx-swap="outerHTML settle:150ms" {

                                        div class="form-control col-span-2" {
                                            label class="label py-1" { span class="label-text opacity-80" { "symbol" } }
                                            input class="input input-bordered input-sm font-mono bg-base-200/30"
                                                name="symbol"
                                                placeholder="BTCUSDT"
                                                required;
                                        }

                                        div class="form-control" {
                                            label class="label py-1" { span class="label-text opacity-80" { "exchange" } }
                                            input class="input input-bordered input-sm font-mono bg-base-200/30"
                                                name="exchange"
                                                placeholder="binance_linear"
                                                required;
                                        }

                                        div class="form-control" {
                                            label class="label py-1" { span class="label-text opacity-80" { "kind" } }
                                            input class="input input-bordered input-sm font-mono bg-base-200/30"
                                                name="kind"
                                                placeholder="Trades"
                                                required;
                                        }

                                        div class="form-control col-span-2" {
                                            label class="label py-1" { span class="label-text opacity-80" { "transport" } }
                                            input class="input input-bordered input-sm font-mono bg-base-200/30"
                                                name="transport"
                                                placeholder="Ws"
                                                required;
                                        }

                                        div class="col-span-2 flex gap-2 pt-2" {
                                            button class="btn btn-sm btn-primary flex-1" type="submit" { "Start ▷" }
                                            button class="btn btn-sm btn-ghost" type="reset" { "Clear" }
                                        }
                                    }

                                    div class="divider my-2" {}

                                    // Instruments quick actions
                                    div class="flex items-center justify-between" {
                                        h4 class="font-semibold" { "Instruments 🎹" }
                                        form
                                            hx-post="/ui/instruments/refresh"
                                            hx-target="#instruments-pane"
                                            hx-swap="innerHTML settle:150ms" {
                                            button class="btn btn-xs btn-outline" type="submit" { "Refresh 🔄" }
                                        }
                                    }

                                    div id="instruments-pane" class="text-sm opacity-80" {
                                        "Open "
                                        a class="link" href="/ui/instruments" { "/ui/instruments" }
                                        " for more."
                                    }
                                }
                            }

                            // Capabilities panel
                            div class="card glass border border-base-300 shadow-xl rounded-2xl" {
                                div class="card-body space-y-2" {
                                    h3 class="card-title" { "Capabilities 📡" }
                                    div
                                        id="capabilities"
                                        hx-get="/ui/streams/capabilities"
                                        hx-trigger="load"
                                        hx-swap="innerHTML settle:150ms" {
                                        div class="skeleton h-12 w-full" {}
                                    }
                                }
                            }

                            // Limiters panel
                            div class="card glass border border-base-300 shadow-xl rounded-2xl" {
                                div class="card-body space-y-2" {
                                    h3 class="card-title" { "Limiters 🚦" }
                                    div
                                        id="limiters"
                                        hx-get="/ui/limiters"
                                        hx-trigger="load, every 2s"
                                        hx-swap="innerHTML settle:150ms" {
                                        div class="skeleton h-10 w-full" {}
                                    }
                                }
                            }

                        }
                    }
                }
            }
        }
    };

    Html(page.into_string())
}

// ============================================================
// /ui/health
// ============================================================

pub async fn health_panel(State(app): State<AppRuntime>) -> Html<String> {
    let runtime_ok = app.runtime_ok();

    let db_ok = app.deps.is_db_enabled()
        && app
            .deps
            .db
            .as_ref()
            .is_some_and(|db| db.health.as_ref().can_admit_new_stream().is_none());

    let redis_ok = app.deps.is_redis_enabled()
        && app
            .deps
            .redis
            .as_ref()
            .is_some_and(|r| r.manager.can_publish());

    let out = html! {
        div class="flex flex-wrap gap-2 items-center" {
            (pill(runtime_ok, "runtime"))
            (pill(db_ok, "db"))
            (pill(redis_ok, "redis"))
        }
    };

    Html(out.into_string())
}

// ============================================================
// /ui/streams/table
// ============================================================

pub async fn streams_table(State(app): State<AppRuntime>) -> Html<String> {
    let rows = app.list_streams().await;

    let out = html! {
        div id="streams-table" class="overflow-x-auto rounded-2xl border border-base-300 bg-base-100/30 backdrop-blur" {
            table class="table table-zebra w-full" {
                thead class="sticky top-0 bg-base-200/70 backdrop-blur z-10" {
                    tr {
                        th { "Exchange" }
                        th { "Symbol" }
                        th { "Kind" }
                        th { "Transport" }
                        th { "Status" }
                        th { "Actions" }
                    }
                }
                tbody {
                    @if rows.is_empty() {
                        tr {
                            td colspan="6" class="opacity-70 py-6" {
                                "No active streams. Start one on the right ▷"
                            }
                        }
                    } @else {
                        @for (_id, status, spec) in rows {
                            @let status_str = format!("{status:?}");
                            @let (dot_cls, badge_cls) = status_dot_and_badge(&status_str);

                            // Build knobs slot id ONCE, and reuse for:
                            // - toggle button onclick
                            // - hx-target
                            // - slot <div id=...>
                            @let slot_id = format!(
                                "knobs-{}-{}-{}-{}",
                                spec.exchange.to_string(),
                                spec.instrument.to_string(),
                                format!("{:?}", spec.kind),
                                format!("{:?}", spec.transport),
                            );

                            @let knobs_url = format!(
                                "/ui/streams/{}/{}/{}/{}/knobs",
                                spec.exchange.to_string(),
                                spec.instrument.to_string(),
                                format!("{:?}", spec.kind),
                                format!("{:?}", spec.transport),
                            );

                            // Always prevent default.
                            // If open -> close (clear innerHTML).
                            // If closed -> htmx.ajax() to load fragment into slot.
                            @let toggle_js = format!(
                                "var id='{id}'; var t=document.getElementById(id); \
                                if(t && t.innerHTML.trim()!==''){{ t.innerHTML=''; return false; }} \
                                htmx.ajax('GET','{url}',{{target:'#'+id, swap:'innerHTML'}}); return false;",
                                id = slot_id,
                                url = knobs_url
                            );

                            a class="btn btn-xs btn-outline"
                                href="#"
                                onclick=(toggle_js) {
                                "Knobs"
                            }

                            tr {
                                td class="opacity-90" { (spec.exchange.to_string()) }
                                td class="font-mono font-semibold" { (spec.instrument.to_string()) }
                                td { span class="badge badge-outline" { (format!("{:?}", spec.kind)) } }
                                td { span class="badge badge-outline" { (format!("{:?}", spec.transport)) } }
                                td {
                                    div class="flex items-center gap-2" {
                                        span class={ "inline-block w-2 h-2 rounded-full " (dot_cls) } {}
                                        span class=(badge_cls) { (status_str) }
                                    }
                                }
                                td {
                                    div class="flex items-center gap-2" {
                                        // Knobs toggle (open/close)
                                        a class="btn btn-xs btn-outline"
                                            href="#"
                                            onclick=(toggle_js)
                                            hx-get={ "/ui/streams/" (spec.exchange.to_string()) "/" (spec.instrument.to_string()) "/" (format!("{:?}", spec.kind)) "/" (format!("{:?}", spec.transport)) "/knobs" }
                                            hx-target={ "#" (slot_id.clone()) }
                                            hx-swap="innerHTML settle:150ms" {
                                            "Knobs"
                                        }

                                        // Stop
                                        form
                                            hx-post="/ui/streams/stop"
                                            hx-target="#streams-table"
                                            hx-swap="outerHTML settle:150ms"
                                            hx-confirm="Stop this stream?" {

                                            input type="hidden" name="exchange" value=(spec.exchange.to_string());
                                            input type="hidden" name="symbol" value=(spec.instrument.to_string());
                                            input type="hidden" name="kind" value=(format!("{:?}", spec.kind));
                                            input type="hidden" name="transport" value=(format!("{:?}", spec.transport));

                                            button class="btn btn-xs btn-error" type="submit" { "Stop ⛔" }
                                        }
                                    }

                                    // Knobs slot (drawer)
                                    div class="mt-2" id=(slot_id) { }
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    Html(out.into_string())
}

// ============================================================
// /ui/streams/start  and  /ui/streams/stop
// ============================================================

#[derive(Debug, Deserialize)]
pub struct StartStreamForm {
    pub exchange: String,
    pub symbol: String,
    pub kind: String,
    pub transport: String,
}

#[derive(Debug, Deserialize)]
pub struct StopStreamForm {
    pub exchange: String,
    pub symbol: String,
    pub kind: String,
    pub transport: String,
}

pub async fn streams_start(
    State(app): State<AppRuntime>,
    Form(f): Form<StartStreamForm>,
) -> Html<String> {
    let exchange = match parse_exchange_id_ui(&f.exchange) {
        Ok(v) => v,
        Err(e) => {
            let mut page = streams_table(State(app.clone())).await.0;
            page.push_str(&flash_oob("error", &e).into_string());
            return Html(page);
        }
    };

    let kind = match parse_kind_ui(&f.kind) {
        Ok(v) => v,
        Err(e) => {
            let mut page = streams_table(State(app.clone())).await.0;
            page.push_str(&flash_oob("error", &e).into_string());
            return Html(page);
        }
    };

    let transport = match parse_transport_ui(&f.transport) {
        Ok(v) => v,
        Err(e) => {
            let mut page = streams_table(State(app.clone())).await.0;
            page.push_str(&flash_oob("error", &e).into_string());
            return Html(page);
        }
    };

    let p = StartStreamParams {
        exchange,
        transport,
        kind,
        symbol: f.symbol.clone(),
    };

    match app.add_stream(p).await {
        Ok(_) => {
            let mut page = streams_table(State(app.clone())).await.0;
            page.push_str(&flash_oob("success", "Stream started ✅").into_string());
            Html(page)
        }
        Err(e) => {
            let mut page = streams_table(State(app.clone())).await.0;
            page.push_str(&flash_oob("error", &format!("Start failed: {e}")).into_string());
            Html(page)
        }
    }
}

pub async fn streams_stop(
    State(app): State<AppRuntime>,
    Form(f): Form<StopStreamForm>,
) -> Html<String> {
    let exchange = match parse_exchange_id_ui(&f.exchange) {
        Ok(v) => v,
        Err(e) => {
            let mut page = streams_table(State(app.clone())).await.0;
            page.push_str(&flash_oob("error", &e).into_string());
            return Html(page);
        }
    };

    let kind = match parse_kind_ui(&f.kind) {
        Ok(v) => v,
        Err(e) => {
            let mut page = streams_table(State(app.clone())).await.0;
            page.push_str(&flash_oob("error", &e).into_string());
            return Html(page);
        }
    };

    let transport = match parse_transport_ui(&f.transport) {
        Ok(v) => v,
        Err(e) => {
            let mut page = streams_table(State(app.clone())).await.0;
            page.push_str(&flash_oob("error", &e).into_string());
            return Html(page);
        }
    };

    let p = StartStreamParams {
        exchange,
        transport,
        kind,
        symbol: f.symbol.clone(),
    };

    match app.remove_stream(p).await {
        Ok(_) => {
            let mut page = streams_table(State(app.clone())).await.0;
            page.push_str(&flash_oob("success", "Stream stopped ✅").into_string());
            Html(page)
        }
        Err(e) => {
            let mut page = streams_table(State(app.clone())).await.0;
            page.push_str(&flash_oob("error", &format!("Stop failed: {e}")).into_string());
            Html(page)
        }
    }
}

// ============================================================
// /ui/streams/.../knobs  (GET/POST)  (stub for now)
// ============================================================

pub async fn knobs_panel(
    State(app): State<AppRuntime>,
    Path((exchange_str, symbol, kind_str, transport_str)): Path<(String, String, String, String)>,
) -> Html<String> {
    // Parse ids
    let exchange = match parse_exchange_id_ui(&exchange_str) {
        Ok(v) => v,
        Err(e) => return Html(html! { div class="alert alert-error" { (e) } }.into_string()),
    };
    let kind = match parse_kind_ui(&kind_str) {
        Ok(v) => v,
        Err(e) => return Html(html! { div class="alert alert-error" { (e) } }.into_string()),
    };
    let transport = match parse_transport_ui(&transport_str) {
        Ok(v) => v,
        Err(e) => return Html(html! { div class="alert alert-error" { (e) } }.into_string()),
    };

    let id = StreamId::new(exchange.as_str(), &symbol, kind, transport);

    // Fetch knobs
    let Some(knobs) = app.stream_knobs_snapshot(&id).await else {
        return Html(
            html! {
                div class="alert alert-warning shadow-lg" {
                    div {
                        span class="font-semibold" { "Stream not found" }
                        div class="text-xs opacity-80 mt-1 font-mono" { (id.to_string()) }
                    }
                }
            }
            .into_string(),
        );
    };

    // Must match streams_table() slot id format
    let slot_id = format!(
        "knobs-{}-{}-{}-{}",
        exchange_str, symbol, kind_str, transport_str
    );

    // Close panel instantly (no server roundtrip)
    let close_js = format!(
        "var t=document.getElementById('{id}'); if(t) t.innerHTML=''; return false;",
        id = slot_id
    );

    let out = html! {
        // Important: keep this as a fragment (no DOCTYPE)
        div class="p-4 rounded-2xl border border-base-300 bg-base-200/20 backdrop-blur space-y-3" {

            div class="flex items-center justify-between gap-3" {
                div class="font-semibold" { "Knobs ⚙️" }
                div class="flex items-center gap-2" {
                    span class="badge badge-outline font-mono" { (id.to_string()) }
                    button class="btn btn-xs btn-ghost" type="button" onclick=(close_js) { "Close ✖" }
                }
            }

            form
                class="grid grid-cols-2 gap-3"
                hx-post={ "/ui/streams/" (exchange_str) "/" (symbol) "/" (kind_str) "/" (transport_str) "/knobs" }
                // Return from knobs_save should be OOB close + toast, so we don't need to swap this form.
                hx-swap="none"
            {
                // Toggles
                div class="form-control col-span-2" {
                    label class="label cursor-pointer justify-start gap-3" {
                        input
                            type="checkbox"
                            class="toggle toggle-warning"
                            name="disable_db_writes"
                            checked?[knobs.disable_db_writes];
                        span class="label-text" {
                            span class="font-semibold" { "Disable DB writes" }
                            span class="opacity-70" { " (danger)" }
                        }
                    }
                }

                div class="form-control col-span-2" {
                    label class="label cursor-pointer justify-start gap-3" {
                        input
                            type="checkbox"
                            class="toggle toggle-accent"
                            name="disable_redis_publishes"
                            checked?[knobs.disable_redis_publishes];
                        span class="label-text" {
                            span class="font-semibold" { "Disable Redis publishes" }
                        }
                    }
                }

                // Numbers
                (knob_number("flush_rows", "Flush rows", knobs.flush_rows, "rows"))
                (knob_number_u64("flush_interval_ms", "Flush interval", knobs.flush_interval_ms, "ms"))
                (knob_number("chunk_rows", "Chunk rows", knobs.chunk_rows, "rows"))
                (knob_number("hard_cap_rows", "Hard cap rows", knobs.hard_cap_rows, "rows"))

                div class="col-span-2 flex items-center gap-2 pt-2" {
                    button class="btn btn-sm btn-primary" type="submit" { "Save 💾" }
                    button class="btn btn-sm btn-outline" type="button" onclick=(close_js) { "Close" }
                    span class="ml-auto badge badge-info badge-outline" { "live" }
                }
            }
        }
    };

    Html(out.into_string())
}

// Small helpers to render inputs
fn knob_number(name: &str, label: &str, val: usize, unit: &str) -> Markup {
    html! {
        div class="form-control" {
            label class="label py-1" { span class="label-text opacity-80" { (label) } }
            div class="join w-full" {
                input
                    class="input input-bordered input-sm font-mono join-item w-full bg-base-200/30"
                    type="number"
                    min="0"
                    name=(name)
                    value=(val);
                span class="btn btn-sm btn-ghost join-item pointer-events-none" { (unit) }
            }
        }
    }
}

fn knob_number_u64(name: &str, label: &str, val: u64, unit: &str) -> Markup {
    html! {
        div class="form-control" {
            label class="label py-1" { span class="label-text opacity-80" { (label) } }
            div class="join w-full" {
                input
                    class="input input-bordered input-sm font-mono join-item w-full bg-base-200/30"
                    type="number"
                    min="0"
                    name=(name)
                    value=(val);
                span class="btn btn-sm btn-ghost join-item pointer-events-none" { (unit) }
            }
        }
    }
}

pub async fn knobs_save(
    State(app): State<AppRuntime>,
    Path((exchange_str, symbol, kind_str, transport_str)): Path<(String, String, String, String)>,
    Form(f): Form<KnobsForm>,
) -> Html<String> {
    // parse ids
    let exchange = match parse_exchange_id_ui(&exchange_str) {
        Ok(v) => v,
        Err(e) => {
            return Html(html! { div class="alert alert-error shadow-lg" { (e) } }.into_string());
        }
    };
    let kind = match parse_kind_ui(&kind_str) {
        Ok(v) => v,
        Err(e) => {
            return Html(html! { div class="alert alert-error shadow-lg" { (e) } }.into_string());
        }
    };
    let transport = match parse_transport_ui(&transport_str) {
        Ok(v) => v,
        Err(e) => {
            return Html(html! { div class="alert alert-error shadow-lg" { (e) } }.into_string());
        }
    };

    let id = StreamId::new(exchange.as_str(), &symbol, kind, transport);

    // load snapshot (so missing fields don't zero-out)
    let Some(mut knobs) = app.stream_knobs_snapshot(&id).await else {
        return Html(
            html! { div class="alert alert-warning shadow-lg" { "stream not found" } }
                .into_string(),
        );
    };

    // apply toggles (checkbox present => disabled=true)
    knobs.disable_db_writes = checkbox_on(&f.disable_db_writes);
    knobs.disable_redis_publishes = checkbox_on(&f.disable_redis_publishes);

    // apply numeric knobs only if present
    if let Some(v) = f.flush_rows {
        knobs.flush_rows = v.max(1);
    }
    if let Some(v) = f.flush_interval_ms {
        knobs.flush_interval_ms = v;
    }
    if let Some(v) = f.chunk_rows {
        knobs.chunk_rows = v.max(1);
    }
    if let Some(v) = f.hard_cap_rows {
        knobs.hard_cap_rows = v.max(1);
    }

    // save once (persists to DB + updates runtime)
    match app.set_stream_knobs(&id, knobs).await {
        Ok(_) => {
            // re-render panel (fresh snapshot)
            let mut html = knobs_panel(
                State(app),
                Path((exchange_str, symbol, kind_str, transport_str)),
            )
            .await
            .0;

            // small success message at top of panel
            html = format!(
                r#"<div class="alert alert-success shadow-lg mb-2"><div><span class="font-semibold">Saved ✅</span></div></div>{}"#,
                html
            );

            Html(html)
        }
        Err(e) => Html(
            html! {
                div class="alert alert-error shadow-lg" {
                    div {
                        span class="font-semibold" { "Save failed" }
                        div class="text-xs opacity-80 mt-1 font-mono" { (format!("{e}")) }
                    }
                }
            }
            .into_string(),
        ),
    }
}

// ============================================================
// /ui/limiters  (stub for now)
// ============================================================

pub async fn limiters_panel(State(app): State<AppRuntime>) -> Html<String> {
    let rows = match app.runtime_limiter_info(None).await {
        Ok(v) => v,
        Err(e) => {
            return Html(
                html! {
                    div class="alert alert-error shadow-lg" {
                        div {
                            span class="font-semibold" { "Failed to fetch limiter info" }
                            div class="text-xs opacity-80 mt-1 font-mono" { (format!("{e}")) }
                        }
                    }
                }
                .into_string(),
            );
        }
    };

    if rows.is_empty() {
        return Html(
            html! {
                div class="alert alert-info shadow-lg" {
                    div { "No limiter data available." }
                }
            }
            .into_string(),
        );
    }

    // A small helper: show remaining as a badge + a progress bar.
    // We don't know the true max window, so we use a "soft cap" for visualization.
    fn budget_widget(label: &str, remaining: Option<u32>, soft_cap: u32) -> Markup {
        match remaining {
            None => html! {
                div class="space-y-1" {
                    div class="flex items-center justify-between" {
                        span class="opacity-80" { (label) }
                        span class="badge badge-ghost badge-outline" { "n/a" }
                    }
                }
            },
            Some(r) => {
                let capped = r.min(soft_cap);
                let pct = ((capped as f64 / soft_cap as f64) * 100.0).round() as u32;

                let bar_cls = if r >= (soft_cap / 5).max(1) {
                    "progress progress-success w-full"
                } else if r >= (soft_cap / 20).max(1) {
                    "progress progress-warning w-full"
                } else {
                    "progress progress-error w-full"
                };

                html! {
                    div class="space-y-1" {
                        div class="flex items-center justify-between" {
                            span class="opacity-80" { (label) }
                            span class="badge badge-outline font-mono" { (r) }
                        }
                        progress class=(bar_cls) value=(pct) max="100" {}
                    }
                }
            }
        }
    }

    let out = html! {
        div class="space-y-3 text-sm" {
            @for row in rows {
                @let ex = row.exchange.as_str();

                div class="p-3 rounded-2xl border border-base-300 bg-base-200/20 backdrop-blur space-y-2" {
                    div class="flex items-center justify-between" {
                        span class="font-semibold" { (ex) }
                        span class="badge badge-info badge-outline" { "budgets" }
                    }

                    // Soft caps chosen just for visuals (tweak later):
                    // - HTTP weights often bigger -> 1000
                    // - WS subscribe -> 50
                    // - WS reconnect -> 20
                    (budget_widget("http_remaining", row.http_remaining, 1000))
                    (budget_widget("ws_subscribe_remaining", row.ws_subscribe_remaining, 50))
                    (budget_widget("ws_reconnect_remaining", row.ws_reconnect_remaining, 20))
                }
            }

            div class="text-xs opacity-60 pt-1" {
                "Bars are scaled (soft cap) for visualization, badges show exact remaining."
            }
        }
    };

    Html(out.into_string())
}

// ============================================================
// /ui/instruments (+ refresh/exists/list)
// ============================================================

pub async fn instruments_page(State(_app): State<AppRuntime>) -> Html<String> {
    Html(
        html! {
            (DOCTYPE)
            html data-theme="dracula" {
                head {
                    meta charset="utf-8";
                    meta name="viewport" content="width=device-width, initial-scale=1";
                    title { "Instruments - mini-fintickstreams" }
                    script src="https://unpkg.com/htmx.org@1.9.12" {}
                    script src="https://cdn.tailwindcss.com" {}
                    link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/daisyui@4.12.10/dist/full.min.css" {}
                }
                body class="min-h-screen bg-gradient-to-br from-[#050814] via-[#070A1A] to-[#050B10] p-6" {
                    div class="max-w-3xl mx-auto space-y-4" {
                        a class="btn btn-sm btn-outline" href="/ui" { "← back" }

                        div class="card bg-base-100/20 border border-base-300 backdrop-blur rounded-2xl shadow-xl" {
                            div class="card-body space-y-4" {
                                h1 class="text-2xl font-bold" { "Instruments 🎹" }

                                // --- refresh ---
                                form hx-post="/ui/instruments/refresh"
                                     hx-target="#inst-out"
                                     hx-swap="innerHTML settle:150ms" {
                                    button class="btn btn-sm btn-primary" type="submit" { "Refresh 🔄" }
                                }

                                // --- exists ---
                                div class="divider my-1" {}

                                h2 class="font-semibold" { "Exists 🔎" }
                                form class="grid grid-cols-2 gap-2"
                                    hx-post="/ui/instruments/exists"
                                    hx-target="#inst-out"
                                    hx-swap="innerHTML settle:150ms" {

                                    div class="form-control" {
                                        label class="label py-1" { span class="label-text" { "exchange" } }
                                        input class="input input-bordered input-sm font-mono bg-base-200/30"
                                            name="exchange"
                                            placeholder="binance_linear"
                                            required;
                                    }
                                    div class="form-control" {
                                        label class="label py-1" { span class="label-text" { "symbol" } }
                                        input class="input input-bordered input-sm font-mono bg-base-200/30"
                                            name="symbol"
                                            placeholder="BTCUSDT"
                                            required;
                                    }
                                    div class="col-span-2" {
                                        button class="btn btn-sm btn-outline w-full" type="submit" { "Exists? 🔎" }
                                    }
                                }

                                div id="inst-out" class="text-sm opacity-80" { "Output will appear here." }

                                // --- list/search ---
                                div class="divider my-1" {}

                                h2 class="font-semibold" { "Search / List 📋" }
                                p class="text-xs opacity-70" {
                                    "Tip: type " span class="font-mono" { "btc" } " and choose " span class="font-mono" { "contains/starts/ends/exact" } "."
                                }

                                form id="inst-search"
                                    class="grid grid-cols-2 gap-2"
                                    hx-get="/ui/instruments/list"
                                    hx-target="#inst-list"
                                    hx-swap="innerHTML settle:150ms" {

                                    div class="form-control" {
                                        label class="label py-1" { span class="label-text" { "exchange (optional)" } }
                                        input class="input input-bordered input-sm font-mono bg-base-200/30"
                                            name="exchange"
                                            placeholder="binance_linear";
                                    }

                                    div class="form-control" {
                                        label class="label py-1" { span class="label-text" { "kind (optional)" } }
                                        input class="input input-bordered input-sm font-mono bg-base-200/30"
                                            name="kind"
                                            placeholder="Spot / PerpLinear / ...";
                                    }

                                    div class="form-control col-span-2" {
                                        label class="label py-1" { span class="label-text" { "symbol query" } }
                                        input class="input input-bordered input-sm font-mono bg-base-200/30"
                                            name="q"
                                            placeholder="btc"
                                            // live search while typing (awesome)
                                            hx-trigger="keyup changed delay:250ms"
                                            hx-get="/ui/instruments/list"
                                            hx-target="#inst-list"
                                            hx-swap="innerHTML settle:150ms"
                                            hx-include="#inst-search";
                                    }

                                    div class="form-control col-span-2" {
                                        label class="label py-1" { span class="label-text" { "match mode" } }
                                        select class="select select-bordered select-sm bg-base-200/30"
                                            name="mode"
                                            // changing mode re-runs search immediately
                                            hx-trigger="change"
                                            hx-get="/ui/instruments/list"
                                            hx-target="#inst-list"
                                            hx-swap="innerHTML settle:150ms"
                                            hx-include="#inst-search" {
                                            option value="contains" selected { "contains" }
                                            option value="starts" { "starts_with" }
                                            option value="ends" { "ends_with" }
                                            option value="exact" { "exact" }
                                        }
                                    }

                                    div class="col-span-2 flex gap-2 pt-1" {
                                        button class="btn btn-sm btn-outline flex-1" type="submit" { "List 🔎" }
                                        button class="btn btn-sm btn-ghost" type="reset" { "Clear" }
                                    }
                                }

                                div id="inst-list" class="text-sm opacity-80" {
                                    "Results appear here."
                                }
                            }
                        }
                    }
                }
            }
        }
        .into_string(),
    )
}

pub async fn instruments_refresh(State(app): State<AppRuntime>) -> Html<String> {
    match app.refresh_instruments_registry().await {
        Ok(_) => Html(
            html! {
                div class="alert alert-success shadow-lg" {
                    div {
                        span class="font-semibold" { "Instruments refreshed successfully" }
                        span class="ml-2" { "✅" }
                    }
                }
            }
            .into_string(),
        ),
        Err(e) => Html(
            html! {
                div class="alert alert-error shadow-lg" {
                    div {
                        span class="font-semibold" { "Failed to refresh instruments" }
                        span class="ml-2" { "❌" }
                        div class="text-xs opacity-80 mt-1 font-mono" { (format!("{e}")) }
                    }
                }
            }
            .into_string(),
        ),
    }
}

#[derive(Debug, Deserialize)]
pub struct InstrumentsExistsForm {
    pub exchange: String,
    pub symbol: String,
}

pub async fn instruments_exists(
    State(app): State<AppRuntime>,
    Form(f): Form<InstrumentsExistsForm>,
) -> Html<String> {
    let exists = app.exists(&f.exchange, &f.symbol);

    if exists {
        Html(
            html! {
                div class="alert alert-success shadow-lg" {
                    div {
                        span class="font-semibold" { "Instrument exists" }
                        span class="ml-2 font-mono" { (format!("{} / {}", f.exchange, f.symbol)) }
                        span class="ml-2" { "✅" }
                    }
                }
            }
            .into_string(),
        )
    } else {
        Html(
            html! {
                div class="alert alert-error shadow-lg" {
                    div {
                        span class="font-semibold" { "Instrument NOT found" }
                        span class="ml-2 font-mono" { (format!("{} / {}", f.exchange, f.symbol)) }
                        span class="ml-2" { "❌" }
                    }
                }
            }
            .into_string(),
        )
    }
}

// Update your query struct too:
#[derive(Debug, Deserialize)]
pub struct InstrumentsListQuery {
    pub exchange: Option<String>,
    pub kind: Option<String>,
    pub q: Option<String>,    // search term (symbol)
    pub mode: Option<String>, // contains | starts | ends | exact
}

pub async fn instruments_list(
    State(app): State<AppRuntime>,
    Query(q): Query<InstrumentsListQuery>,
) -> Html<String> {
    // Require at least one filter to avoid dumping everything
    if q.exchange.is_none() && q.kind.is_none() {
        return Html(
            html! {
                div class="alert alert-warning shadow-lg" {
                    div {
                        span class="font-semibold" { "Provide at least one filter:" }
                        span class="ml-2 font-mono" { "exchange" }
                        span { " or " }
                        span class="font-mono" { "kind" }
                    }
                }
            }
            .into_string(),
        );
    }

    // Fetch instruments using the SAME logic as your JSON handler
    let mut items: Vec<InstrumentSpec> = match (q.exchange.as_deref(), q.kind.as_deref()) {
        (Some(ex), Some(kind_str)) => {
            let kind = match parse_instrument_kind_ui(kind_str) {
                Ok(k) => k,
                Err(e) => {
                    return Html(
                        html! {
                            div class="alert alert-error shadow-lg" {
                                div {
                                    span class="font-semibold" { "Bad kind filter" }
                                    div class="text-xs opacity-80 mt-1 font-mono" { (e) }
                                }
                            }
                        }
                        .into_string(),
                    );
                }
            };
            app.retrieve_instruments_by_exchange_kind(ex, kind)
        }
        (Some(ex), None) => app.retrieve_instruments_by_exchange(ex),
        (None, Some(kind_str)) => {
            let kind = match parse_instrument_kind_ui(kind_str) {
                Ok(k) => k,
                Err(e) => {
                    return Html(
                        html! {
                            div class="alert alert-error shadow-lg" {
                                div {
                                    span class="font-semibold" { "Bad kind filter" }
                                    div class="text-xs opacity-80 mt-1 font-mono" { (e) }
                                }
                            }
                        }
                        .into_string(),
                    );
                }
            };
            app.retrieve_instruments_by_kind(kind)
        }
        (None, None) => unreachable!(),
    };

    // Optional symbol search filter
    let search = q.q.as_deref().map(str::trim).unwrap_or("");
    if !search.is_empty() {
        let mode = q.mode.as_deref().unwrap_or("contains");
        items.retain(|inst| symbol_matches(&inst.symbol, search, mode));
    }

    // Cap rows for UI safety
    let total = items.len();
    let cap: usize = 200;
    let shown = total.min(cap);

    let out = html! {
        div class="space-y-3" {

            // Summary row (shows active filters)
            div class="flex flex-wrap gap-2 items-center justify-between" {
                div class="text-sm opacity-80" {
                    "Showing " (shown) " of " (total) " instruments"
                    @if !search.is_empty() {
                        span class="ml-2 badge badge-outline font-mono" {
                            "q=" (search)
                        }
                        span class="ml-2 badge badge-ghost font-mono" {
                            "mode=" (q.mode.as_deref().unwrap_or("contains"))
                        }
                    }
                }

                @if total > cap {
                    span class="badge badge-warning badge-outline" { "capped at 200" }
                } @else {
                    span class="badge badge-success badge-outline" { "ok" }
                }
            }

            div class="overflow-x-auto rounded-2xl border border-base-300 bg-base-100/20 backdrop-blur" {
                table class="table table-zebra w-full" {
                    thead class="sticky top-0 bg-base-200/70 backdrop-blur z-10" {
                        tr {
                            th { "Exchange" }
                            th { "Symbol" }
                            th { "Kind" }
                            th { "Qty Unit" }
                            th { "Contract Size" }
                        }
                    }
                    tbody {
                        @if total == 0 {
                            tr {
                                td colspan="5" class="opacity-70 py-6" {
                                    "No instruments match your filters."
                                }
                            }
                        } @else {
                            @for inst in items.iter().take(cap) {
                                tr {
                                    td class="opacity-90" { (inst.exchange) }
                                    td class="font-mono font-semibold" { (&inst.symbol) }
                                    td { span class="badge badge-outline" { (format!("{:?}", inst.kind)) } }
                                    td { span class="badge badge-ghost" { (format!("{:?}", inst.reported_qty_unit)) } }
                                    td class="font-mono opacity-80" {
                                        @match inst.contract_size {
                                            Some(v) => { (format!("{v:.8}")) }
                                            None => { "-" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    Html(out.into_string())
}

pub async fn capabilities_panel(State(app): State<AppRuntime>) -> Html<String> {
    let caps = app.stream_capabilities();
    let n = caps.len();

    let out = html! {
        div class="space-y-3" {
            div class="flex items-center justify-between" {
                h3 class="font-semibold" { "Capabilities 📡" }
                span class="badge badge-outline" { (n) " supported" }
            }

            @if n == 0 {
                div class="alert alert-warning shadow-lg" {
                    div {
                        span class="font-semibold" { "No capabilities returned." }
                        div class="text-xs opacity-80 mt-1 font-mono" {
                            "Check list_available_streams() / feature flags / init path."
                        }
                    }
                }
            } @else {
                div class="overflow-x-auto rounded-2xl border border-base-300 bg-base-100/20 backdrop-blur" {
                    table class="table table-zebra w-full text-sm" {
                        thead class="bg-base-200/70 backdrop-blur" {
                            tr {
                                th { "Exchange" }
                                th { "Kind" }
                                th { "Transport" }
                                th { "Note" }
                            }
                        }
                        tbody {
                            @for c in caps {
                                tr {
                                    td class="font-mono opacity-90" { (c.exchange.as_str()) }
                                    td { span class="badge badge-outline" { (format!("{:?}", c.kind)) } }
                                    td { span class="badge badge-ghost" { (format!("{:?}", c.transport)) } }
                                    td class="text-xs opacity-70" {
                                        @match c.note {
                                            Some(n) => { (n) }
                                            None => { "-" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    Html(out.into_string())
}
