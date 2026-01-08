# 📡 Runtime HTTP API

This document describes the HTTP API exposed by the runtime service.

The API allows you to:
- Inspect runtime, DB, and Redis health
- Discover supported stream capabilities
- Start and stop streams
- Inspect stream status and counts
- Tune running streams dynamically via knobs
- Inspect and refresh the instruments registry

Base URL:
http://localhost:8080

All requests and responses use JSON unless otherwise stated.

==================================================
HEALTH
==================================================

GET /health/runtime
Checks whether the runtime is in a GREEN state.

Example:
curl http://localhost:8080/health/runtime

Response:
{ "ok": true }

--------------------------------------------------

GET /health/db
DB is OK if enabled, initialized, and healthy.

Example:
curl http://localhost:8080/health/db

Response:
{ "ok": true }

--------------------------------------------------

GET /health/redis
Redis is OK if enabled and can publish.

Example:
curl http://localhost:8080/health/redis

Response:
{ "ok": true }

==================================================
STREAM CAPABILITIES
==================================================

GET /streams/capabilities
Lists all supported (exchange, transport, kind) combinations.

Example:
curl http://localhost:8080/streams/capabilities

Response example:
[
  {
    "exchange": "binance_linear",
    "transport": "Ws",
    "kind": "Trades",
    "note": null
  },
  {
    "exchange": "hyperliquid_perp",
    "transport": "Ws",
    "kind": "FundingOpenInterest",
    "note": "combined OI+Funding stream; request this instead of OI/Funding"
  }
]

==================================================
STREAMS – CONTROL & STATUS
==================================================

GET /streams
Lists all active streams.

Optional query parameters:
- exchange
- symbol
- kind
- transport

Example:
curl "http://localhost:8080/streams?exchange=binance_linear"

Response:
[
  {
    "id": "binance_linear:BTCUSDT:Trades:Ws",
    "status": "Running",
    "exchange": "binance_linear",
    "symbol": "BTCUSDT",
    "kind": "Trades",
    "transport": "Ws"
  }
]

--------------------------------------------------

GET /streams/count
Returns number of active streams.

Example:
curl http://localhost:8080/streams/count

Response:
{ "count": 3 }

--------------------------------------------------

GET /streams/{exchange}/{symbol}/{kind}/{transport}
Returns status and spec for a single stream.

Example:
curl http://localhost:8080/streams/binance_linear/BTCUSDT/Trades/Ws

Response:
{
  "id": "binance_linear:BTCUSDT:Trades:Ws",
  "status": "Running",
  "spec": {
    "exchange": "binance_linear",
    "symbol": "BTCUSDT",
    "kind": "Trades",
    "transport": "Ws"
  }
}

--------------------------------------------------

POST /streams
Starts a new stream.

Request body:
{
  "exchange": "binance_linear",
  "transport": "Ws",
  "kind": "Trades",
  "symbol": "BTCUSDT"
}

Example:
curl -X POST http://localhost:8080/streams \
  -H "Content-Type: application/json" \
  -d '{ "exchange":"binance_linear","transport":"Ws","kind":"Trades","symbol":"BTCUSDT" }'

Response:
"ok"

--------------------------------------------------

DELETE /streams
Stops and removes a stream.

Request body:
{
  "exchange": "binance_linear",
  "transport": "Ws",
  "kind": "Trades",
  "symbol": "BTCUSDT"
}

Example:
curl -X DELETE http://localhost:8080/streams \
  -H "Content-Type: application/json" \
  -d '{ "exchange":"binance_linear","transport":"Ws","kind":"Trades","symbol":"BTCUSDT" }'

Response:
"ok"

==================================================
STREAM KNOBS (LIVE TUNING)
==================================================

Streams are identified by:
{exchange}/{symbol}/{kind}/{transport}

--------------------------------------------------

GET /streams/{exchange}/{symbol}/{kind}/{transport}/knobs
Returns current knob values.

Example:
curl http://localhost:8080/streams/binance_linear/BTCUSDT/Trades/Ws/knobs

Response:
{
  "knobs": {
    "disable_db_writes": false,
    "disable_redis_publishes": false,
    "flush_rows": 1000,
    "flush_interval_ms": 1000,
    "chunk_rows": 1000,
    "hard_cap_rows": 5000
  }
}

--------------------------------------------------

PATCH /streams/{exchange}/{symbol}/{kind}/{transport}/knobs
Applies partial knob updates.

Request body example:
{
  "flush_rows": 500,
  "flush_interval_ms": 250,
  "disable_redis_publishes": true
}

Example:
curl -X PATCH http://localhost:8080/streams/binance_linear/BTCUSDT/Trades/Ws/knobs \
  -H "Content-Type: application/json" \
  -d '{ "flush_rows":500,"flush_interval_ms":250,"disable_redis_publishes":true }'

==================================================
INSTRUMENTS REGISTRY
==================================================

GET /instruments
Lists instruments (filtered).

Query parameters:
- exchange
- kind

Example:
curl "http://localhost:8080/instruments?exchange=binance_linear"

--------------------------------------------------

GET /instruments/count
Returns number of instruments matching filters.

Example:
curl "http://localhost:8080/instruments/count?exchange=binance_linear"

Response:
{ "count": 412 }

--------------------------------------------------

GET /instruments/exists
Checks if an instrument exists.

Query parameters:
- exchange
- symbol

Example:
curl "http://localhost:8080/instruments/exists?exchange=binance_linear&symbol=BTCUSDT"

Response:
{ "exists": true }

--------------------------------------------------

POST /instruments/refresh
Reloads the instruments registry.

Example:
curl -X POST http://localhost:8080/instruments/refresh

Response:
"ok"

==================================================
ERROR FORMAT
==================================================

All errors are returned as JSON.

Example:
{
  "error": "stream not found",
  "kind": "stream_not_found"
}

Common HTTP status codes:
400 invalid argument
404 not found
409 conflict
429 rate limited
503 runtime / DB / Redis unhealthy
500 internal error

==================================================
NOTES
==================================================

- Streams are uniquely identified by (exchange, symbol, kind, transport)
- Some exchanges expose combined streams (e.g. FundingOpenInterest)
- Health endpoints reflect admission logic
- Knob updates are applied live and non-blocking
- Instruments refresh is an administrative operation

END OF DOCUMENT

