# 📡 Runtime HTTP API

This document describes the HTTP API exposed by the runtime service.

---

## 🛠️ General Information
- **Base URL:** `http://localhost:8080`
- **Format:** JSON
- **Stream ID:** Identified by `{exchange}/{symbol}/{kind}/{transport}`

---

## 🏥 Health Checks

### Runtime Health
`GET /health/runtime`
Check if the runtime is in a GREEN state.
- **Example:** `curl http://localhost:8080/health/runtime`
- **Response:** `{ "ok": true }`

### Database Health
`GET /health/db`
OK if enabled, initialized, and reachable.
- **Example:** `curl http://localhost:8080/health/db`
- **Response:** `{ "ok": true }`

### Redis Health
`GET /health/redis`
OK if enabled and can publish.
- **Example:** `curl http://localhost:8080/health/redis`
- **Response:** `{ "ok": true }`

---

## 🌊 Stream Management

### List Capabilities
`GET /streams/capabilities`
Lists supported (exchange, transport, kind) combinations.

### List Active Streams
`GET /streams`
**Query Params:** `exchange`, `symbol`, `kind`, `transport`
- **Example:** `curl "http://localhost:8080/streams?exchange=binance_linear"`

### Get Stream Count
`GET /streams/count`
- **Response:** `{ "count": 3 }`

### Get Specific Stream
`GET /streams/{exchange}/{symbol}/{kind}/{transport}`

### Start Stream
`POST /streams`
- **Body:** `{ "exchange": "...", "symbol": "...", "kind": "...", "transport": "..." }`
- **Example:** `curl -X POST http://localhost:8080/streams -H "Content-Type: application/json" -d '{"exchange":"binance_linear","symbol":"BTCUSDT","kind":"Trades","transport":"Ws"}'`

### Stop Stream
`DELETE /streams`
- **Body:** Same as POST.

---

## ⚙️ Stream Knobs (Live Tuning)

### View Knobs
`GET /streams/{exchange}/{symbol}/{kind}/{transport}/knobs`

### Update Knobs
`PATCH /streams/{exchange}/{symbol}/{kind}/{transport}/knobs`
- **Body Example:** `{ "flush_rows": 500, "disable_redis_publishes": true }`

---

## 🎹 Instruments Registry

- `GET /instruments`: List instruments (Filter: `exchange`, `kind`)
- `GET /instruments/count`: Get count (Filter: `exchange`)
- `GET /instruments/exists`: Check existence (`exchange`, `symbol`)
- `POST /instruments/refresh`: Reload the registry

---

## ⚠️ Error Handling

All errors return JSON: `{ "error": "message", "kind": "error_code" }`

- **400**: Invalid argument
- **404**: Not found
- **409**: Conflict
- **503**: Service Unhealthy
- **500**: Internal Error

---

## 📝 Notes
- Knob updates are live and non-blocking.
- Combined streams (e.g. `FundingOpenInterest`) are preferred where available.
- Health checks reflect admission logic for the service.
