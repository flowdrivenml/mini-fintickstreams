BASE=http://127.0.0.1:8080


curl -sS "$BASE/health/runtime" | jq .
curl -sS "$BASE/health/db" | jq .
curl -sS "$BASE/health/redis" | jq .


curl -sS "$BASE/streams/capabilities" | jq .


curl -sS "$BASE/streams" | jq .
curl -sS "$BASE/streams/count" | jq .


add_stream () {
  curl -sS -X POST "$BASE/streams" \
    -H "Content-Type: application/json" \
    -d "$1"
  echo
}

add_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"OpenInterest","transport":"HttpPoll"}' | jq .
add_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"Funding","transport":"HttpPoll"}' | jq .
add_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"Trades","transport":"Ws"}' | jq .
add_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"L2Book","transport":"Ws"}' | jq .
add_stream '{"exchange":"BinanceLinear","symbol":"BTCUSDT","kind":"Liquidations","transport":"Ws"}' | jq .


add_stream '{"exchange":"HyperliquidPerp","symbol":"BTC","kind":"Trades","transport":"Ws"}' | jq .
add_stream '{"exchange":"HyperliquidPerp","symbol":"BTC","kind":"L2Book","transport":"Ws"}' | jq .
add_stream '{"exchange":"HyperliquidPerp","symbol":"BTC","kind":"FundingOpenInterest","transport":"Ws"}' | jq .


curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/OpenInterest/HttpPoll" | jq .
curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/Funding/HttpPoll" | jq .
curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/Trades/Ws" | jq .
curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/L2Book/Ws" | jq .
curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/Liquidations/Ws" | jq .


curl -sS "$BASE/streams/HyperliquidPerp/BTC/Trades/Ws" | jq .
curl -sS "$BASE/streams/HyperliquidPerp/BTC/L2Book/Ws" | jq .
curl -sS "$BASE/streams/HyperliquidPerp/BTC/FundingOpenInterest/Ws" | jq .


curl -sS "$BASE/streams/BinanceLinear/BTCUSDT/Trades/Ws/knobs" | jq .
L2Book
curl -sS -X PATCH "$BASE/streams/BinanceLinear/BTCUSDT/Trades/Ws/knobs" \
  -H "Content-Type: application/json" \
  -d '{"db_writes_enabled":true,"redis_publishes_enabled":true}' | jq .


rm_stream () {
  curl -sS -X DELETE "$BASE/streams" \
    -H "Content-Type: application/json" \
    -d "$1"
  echo
}


