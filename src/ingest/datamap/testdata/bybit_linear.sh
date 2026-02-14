#!/usr/bin/env bash
set -euo pipefail

# =========================================
# Bybit Linear testdata generator
# =========================================

# Always write into the folder where this script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTDIR="$SCRIPT_DIR"

SYMBOL="${SYMBOL:-BTCUSDT}"

API_BASE_URL="https://api.bybit.com"
WS_BASE_URL="wss://stream.bybit.com/v5/public/linear"

# Bybit topics
TOPIC_DEPTH="orderbook.1000.${SYMBOL}"
TOPIC_TRADES="publicTrade.${SYMBOL}"
TOPIC_TICKERS="tickers.${SYMBOL}"          # OI + funding fields live here
TOPIC_LIQ="allLiquidation.${SYMBOL}"       # more reliable than liquidation.<symbol>

# How many WS messages per file
WS_N="${WS_N:-5}"

# Timeouts (seconds)
# Fast streams should complete quickly; liquidations can be slow.
WS_TIMEOUT_FAST="${WS_TIMEOUT_FAST:-15}"
WS_TIMEOUT_LIQ="${WS_TIMEOUT_LIQ:-180}"

# -------- Helpers --------
need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing dependency: $1" >&2
    return 1
  }
}

mkdir -p "$OUTDIR"

if ! need_cmd curl; then
  echo "Please install curl (usually already installed)." >&2
  exit 1
fi

if ! command -v websocat >/dev/null 2>&1; then
  cat >&2 <<'EOF'
Missing dependency: websocat

Install (recommended binary method):
  sudo curl -L https://github.com/vi/websocat/releases/latest/download/websocat.x86_64-unknown-linux-musl \
    -o /usr/local/bin/websocat
  sudo chmod +x /usr/local/bin/websocat

Then re-run this script.
EOF
  exit 1
fi

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

rest_get() {
  local path="$1"
  local outfile="$2"
  echo "[REST] GET ${path} -> ${outfile}"
  curl -sS "${API_BASE_URL}${path}" \
    -H "Accept: application/json" \
    -o "${outfile}"
}

# Capture N WS messages into a JSONL file.
# Waits until WS_N messages arrive OR until timeout hits (to avoid hanging forever).
ws_capture_jsonl() {
  local topic="$1"
  local outfile="$2"
  local req_id="$3"
  local timeout_s="$4"

  echo "[WS] topic=${topic} -> ${outfile} (need ${WS_N} messages, timeout ${timeout_s}s)"

  set +o pipefail

  timeout "${timeout_s}s" bash -c '
    req_id="$1"
    topic="$2"
    ws_url="$3"
    n="$4"
    outfile="$5"

    {
      # initial subscribe
      printf "{\"req_id\":\"%s\",\"op\":\"subscribe\",\"args\":[\"%s\"]}\n" "$req_id" "$topic"

      # heartbeat ping every 20 seconds
      while true; do
        sleep 20
        printf "{\"op\":\"ping\"}\n"
      done
    } | websocat -t "$ws_url" 2>/dev/null | head -n "$n" > "$outfile"
  ' _ "$req_id" "$topic" "$WS_BASE_URL" "$WS_N" "$outfile" || true

  set -o pipefail

  if [[ ! -s "$outfile" ]]; then
    echo "WS capture produced empty file: $outfile" >&2
    return 1
  fi
}


# # -------- REST snapshots --------
# rest_get "/v5/market/time" "${OUTDIR}/BybitLinearPingSnapshot.json"
# rest_get "/v5/market/time" "${OUTDIR}/BybitLinearServerTimeSnapshot.json"
# rest_get "/v5/market/instruments-info?category=linear" "${OUTDIR}/BybitLinearExchangeInfoSnapshot.json"
# rest_get "/v5/market/orderbook?category=linear&symbol=${SYMBOL}&limit=1000" "${OUTDIR}/BybitLinearDepthSnapshot.json"
#
# # -------- WS samples (5 messages each) --------
# # Do fast streams first, liquidation last (since it may take longest).
# ws_capture_jsonl "$TOPIC_DEPTH"   "${OUTDIR}/BybitLinearWsDepthUpdate.jsonl"      "depth-$(timestamp)"   "$WS_TIMEOUT_FAST"
# ws_capture_jsonl "$TOPIC_TRADES"  "${OUTDIR}/BybitLinearWsTrade.jsonl"            "trades-$(timestamp)"  "$WS_TIMEOUT_FAST"
# ws_capture_jsonl "$TOPIC_TICKERS" "${OUTDIR}/BybitLinearWsOIFundingUpdate.jsonl"  "tickers-$(timestamp)" "$WS_TIMEOUT_FAST"
#
# Liquidations last
ws_capture_jsonl "$TOPIC_LIQ"     "${OUTDIR}/BybitLinearWsLiquidation.jsonl"      "liq-$(timestamp)"     "$WS_TIMEOUT_LIQ"

echo ""
echo "Done. Files written to: ${OUTDIR}"
ls -1 "${OUTDIR}" | sed 's/^/  - /'

