# mini-fintickstreams

A high-performance, extensible **financial market data streaming service** written in **Rust**.

`mini-fintickstreams` orchestrates real-time and polling-based market data streams (trades, order books, funding, open interest, liquidations, etc.), with first-class support for **PostgreSQL / TimescaleDB**, **Redis**, and **Prometheus** observability.

It is designed to run reliably in long-lived environments (bare metal, Docker, Kubernetes) and to scale by composition rather than monolithic complexity.

---

## ✨ Key Features

- Multiple exchanges & stream types  
  - Binance Linear  
  - Hyperliquid Perpetuals  
  - Trades, L2 order books, funding, open interest, liquidations
- Multiple transports  
  - WebSocket (real-time)  
  - HTTP polling
- Strong persistence & messaging  
  - PostgreSQL / TimescaleDB for durable storage  
  - Redis for fan-out and real-time consumers
- Production-grade observability  
  - Prometheus metrics (`/metrics`)  
  - Structured logs via `tracing`
- Dynamic runtime control  
  - Add / remove streams at runtime  
  - Per-stream knobs (DB writes, Redis publishes, batching, etc.)
- Designed for extensibility  
  - Clear separation of runtime, dependencies, metrics, and API  
  - New exchanges / stream types plug in cleanly  
  - Optional components (DB, Redis, metrics) via feature flags

---

## 🧱 Architecture Overview

- AppRuntime  
  - Owns shared state, dependencies, and metrics
- Stream workers  
  - One task per `(exchange, symbol, kind, transport)`
- HTTP API (Axum)  
  - Control plane (health, streams, knobs, instruments)
- Metrics server  
  - Prometheus exposition format
- External systems  
  - PostgreSQL / TimescaleDB  
  - Redis

All components are intentionally **loosely coupled**, making the system easy to extend and reason about.

---

## 🚀 Running Locally

By default, the API listens on port `8080`.

```bash
BASE=http://127.0.0.1:8080
```

---

## 🩺 Health Endpoints

```bash
curl -sS "$BASE/health/runtime" | jq .
curl -sS "$BASE/health/db" | jq .
curl -sS "$BASE/health/redis" | jq .
```

---

## 📡 Stream Capabilities

```bash
curl -sS "$BASE/streams/capabilities" | jq .
```

---

## 📋 Streams Overview

```bash
curl -sS "$BASE/streams" | jq .
curl -sS "$BASE/streams/count" | jq .
```

---

## ➕ Add Streams

Helper function:

```bash
add_stream () {
  curl -sS -X POST "$BASE/streams" \
    -H "Content-Type: application/json" \
    -d "$1"
  echo
}
```

### Binance Linear (BTCUSDT)

```bash
add_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"OpenInterest","transport":"HttpPoll"}' | jq .
add_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"Funding","transport":"HttpPoll"}' | jq .
add_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"Trades","transport":"Ws"}' | jq .
add_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"L2Book","transport":"Ws"}' | jq .
add_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"Liquidations","transport":"Ws"}' | jq .
```

### Hyperliquid Perpetuals (BTC)

```bash
add_stream '{"exchange":"HyperliquidPerp","symbol":"BTC","kind":"Trades","transport":"Ws"}' | jq .
add_stream '{"exchange":"HyperliquidPerp","symbol":"BTC","kind":"L2Book","transport":"Ws"}' | jq .
add_stream '{"exchange":"HyperliquidPerp","symbol":"BTC","kind":"FundingOpenInterest","transport":"Ws"}' | jq .
```

---

## 🔍 Inspect a Single Stream

```bash
curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/OpenInterest/HttpPoll" | jq .
curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/Funding/HttpPoll" | jq .
curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/Trades/Ws" | jq .
curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/L2Book/Ws" | jq .
curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/Liquidations/Ws" | jq .
```

```bash
curl -sS "$BASE/streams/HyperliquidPerp/BTC/Trades/Ws" | jq .
curl -sS "$BASE/streams/HyperliquidPerp/BTC/L2Book/Ws" | jq .
curl -sS "$BASE/streams/HyperliquidPerp/BTC/FundingOpenInterest/Ws" | jq .
```

---

## 🎛 Stream Knobs

Inspect knobs:

```bash
curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/Trades/Ws/knobs" | jq .
```

Patch knobs (example):

```bash
curl -sS -X PATCH "$BASE/streams/BinanceLinear/BTCUSDT/Trades/Ws/knobs" \
  -H "Content-Type: application/json" \
  -d '{"db_writes_enabled":true,"redis_publishes_enabled":true}' | jq .
```

---

## ➖ Remove Streams

Helper:

```bash
rm_stream () {
  curl -sS -X DELETE "$BASE/streams" \
    -H "Content-Type: application/json" \
    -d "$1"
  echo
}
```

Example:

```bash
rm_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"Trades","transport":"Ws"}' | jq .
```

## ⚠️ Disclaimer & Rate Limiting Notes

This project interacts with **live exchange APIs**. Use responsibly and ensure you understand each exchange’s **rate limits, fair-use policies, and terms of service** before running it in production.

### Built-in Rate Limiting

`mini-fintickstreams` includes **internal rate limiters** for supported exchanges, designed to protect against accidental overload and API bans:

- **Binance Linear**
  - HTTP and WebSocket interactions are guarded by runtime rate limiters
  - Designed to remain within documented exchange limits
- **Hyperliquid**
  - Primarily WebSocket-driven
  - Minimal REST usage during steady-state operation

### Observability & Safety

- Prometheus metrics expose runtime counters and gauges that allow you to:
  - Detect elevated error rates
  - Monitor stream health
  - Identify backpressure or throttling conditions
- You are encouraged to **alert on abnormal error or retry metrics** to detect when limits are being approached.

### Multi-Instance Considerations

- Running **multiple instances** of this service has implications:
  - **Binance Linear**
    - Rate limiters operate *per process*
    - Multiple instances will each maintain independent limits
    - Horizontal scaling requires external coordination if strict global limits are needed
  - **Hyperliquid**
    - Predominantly WebSocket-based
    - Less sensitive to REST rate limits in steady state
    - Running multiple instances is generally safe under typical usage patterns

### Final Responsibility

While safeguards are implemented, **ultimate responsibility lies with the operator**:
- Monitor metrics
- Understand exchange constraints
- Scale responsibly

The authors assume no liability for misuse or exchange-side enforcement actions.

