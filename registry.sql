CREATE SCHEMA IF NOT EXISTS mini_fintickstreams;

-- Optional: make unqualified names resolve here first for this session
SET search_path = mini_fintickstreams, public;

CREATE TABLE IF NOT EXISTS mini_fintickstreams.stream_registry (
  stream_id   text PRIMARY KEY,

  exchange    text NOT NULL,
  instrument  text NOT NULL,
  kind        text NOT NULL,
  transport   text NOT NULL,

  enabled     boolean NOT NULL DEFAULT true,

  -- knobs
  disable_db_writes       boolean NOT NULL DEFAULT false,
  disable_redis_publishes boolean NOT NULL DEFAULT false,
  flush_rows              integer NOT NULL DEFAULT 1000 CHECK (flush_rows > 0),
  flush_interval_ms       bigint  NOT NULL DEFAULT 1000 CHECK (flush_interval_ms > 0),
  chunk_rows              integer NOT NULL DEFAULT 1000 CHECK (chunk_rows > 0),
  hard_cap_rows           integer NOT NULL DEFAULT 5000 CHECK (hard_cap_rows > 0),

  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT exchange_valid CHECK (exchange IN ('binance_linear','hyperliquid_perp', 'bybit_linear')),
  CONSTRAINT transport_valid CHECK (transport IN ('ws','api')),
  CONSTRAINT kind_valid CHECK (kind IN (
    'Trades','L2Book','Ticker','Funding','OpenInterest','Liquidations','FundingOpenInterest'
  ))
);

CREATE UNIQUE INDEX IF NOT EXISTS stream_registry_dedupe
ON mini_fintickstreams.stream_registry (exchange, instrument, kind, transport);

CREATE INDEX IF NOT EXISTS stream_registry_enabled_idx
ON mini_fintickstreams.stream_registry (enabled)
WHERE enabled = true;

