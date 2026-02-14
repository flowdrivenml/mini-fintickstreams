-- One-time (per database)
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE OR REPLACE FUNCTION create_exchange_hypertables(
  p_exchange TEXT,
  p_retention          INTERVAL DEFAULT INTERVAL '3 months',
  p_compress_after     INTERVAL DEFAULT INTERVAL '6 hours',
  p_chunk_trades       INTERVAL DEFAULT INTERVAL '1 day',
  p_chunk_depth        INTERVAL DEFAULT INTERVAL '6 hours',
  p_chunk_oi           INTERVAL DEFAULT INTERVAL '1 day',
  p_chunk_funding      INTERVAL DEFAULT INTERVAL '30 days',
  p_chunk_liquidations INTERVAL DEFAULT INTERVAL '1 day',
  p_create_indexes     BOOLEAN  DEFAULT FALSE
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  sch TEXT := 'ex_' || lower(regexp_replace(p_exchange, '[^a-zA-Z0-9_]+', '_', 'g'));
BEGIN
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', sch);

  ---------------------------------------------------------------------------
  -- TRADES
  ---------------------------------------------------------------------------
  EXECUTE format($SQL$
    CREATE TABLE IF NOT EXISTS %I.trades (
      time        TIMESTAMPTZ NOT NULL,
      symbol      TEXT        NOT NULL,
      side        SMALLINT    NOT NULL,   -- 0=buy, 1=sell
      price_i     BIGINT      NOT NULL,   -- scaled integer price (e.g. *1e4)
      qty_i       BIGINT      NOT NULL,   -- scaled integer qty   (e.g. *1e8)
      trade_id    BIGINT      NULL,
      is_maker    BOOLEAN     NULL
    );
  $SQL$, sch);

  EXECUTE format(
    'SELECT create_hypertable(%L, %L, chunk_time_interval => %L::interval, if_not_exists => TRUE);',
    sch||'.trades', 'time', p_chunk_trades
  );

  IF p_create_indexes THEN
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.trades (symbol, time DESC);', sch||'_trades_sym_time', sch);
  END IF;

  ---------------------------------------------------------------------------
  -- DEPTH DELTAS (row per price-level change)
  ---------------------------------------------------------------------------
  EXECUTE format($SQL$
    CREATE TABLE IF NOT EXISTS %I.depth_deltas (
      time        TIMESTAMPTZ NOT NULL,
      symbol      TEXT        NOT NULL,
      side        SMALLINT    NOT NULL,   -- 0=bid, 1=ask
      price_i     BIGINT      NOT NULL,   -- scaled integer price
      size_i      BIGINT      NOT NULL,   -- size at price (0 = delete)
      seq         BIGINT      NULL
    );
  $SQL$, sch);

  EXECUTE format(
    'SELECT create_hypertable(%L, %L, chunk_time_interval => %L::interval, if_not_exists => TRUE);',
    sch||'.depth_deltas', 'time', p_chunk_depth
  );

  IF p_create_indexes THEN
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.depth_deltas (symbol, time DESC);', sch||'_depth_sym_time', sch);
  END IF;

  ---------------------------------------------------------------------------
  -- OPEN INTEREST
  ---------------------------------------------------------------------------
  EXECUTE format($SQL$
    CREATE TABLE IF NOT EXISTS %I.open_interest (
      time        TIMESTAMPTZ NOT NULL,
      symbol      TEXT        NOT NULL,
      oi_i        BIGINT      NOT NULL
    );
  $SQL$, sch);

  EXECUTE format(
    'SELECT create_hypertable(%L, %L, chunk_time_interval => %L::interval, if_not_exists => TRUE);',
    sch||'.open_interest', 'time', p_chunk_oi
  );

  IF p_create_indexes THEN
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.open_interest (symbol, time DESC);', sch||'_oi_sym_time', sch);
  END IF;

  ---------------------------------------------------------------------------
  -- FUNDING
  ---------------------------------------------------------------------------
  EXECUTE format($SQL$
    CREATE TABLE IF NOT EXISTS %I.funding (
      time          TIMESTAMPTZ NOT NULL,
      symbol        TEXT        NOT NULL,
      funding_rate  BIGINT NOT NULL,
      funding_time  TIMESTAMPTZ NULL
    );
  $SQL$, sch);

  EXECUTE format(
    'SELECT create_hypertable(%L, %L, chunk_time_interval => %L::interval, if_not_exists => TRUE);',
    sch||'.funding', 'time', p_chunk_funding
  );

  IF p_create_indexes THEN
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.funding (symbol, time DESC);', sch||'_funding_sym_time', sch);
  END IF;

  ---------------------------------------------------------------------------
  -- LIQUIDATIONS
  ---------------------------------------------------------------------------
  EXECUTE format($SQL$
    CREATE TABLE IF NOT EXISTS %I.liquidations (
      time        TIMESTAMPTZ NOT NULL,
      symbol      TEXT        NOT NULL,
      side        SMALLINT    NOT NULL,   -- your convention
      price_i     BIGINT      NULL,
      qty_i       BIGINT      NOT NULL,
      liq_id      BIGINT      NULL
    );
  $SQL$, sch);

  EXECUTE format(
    'SELECT create_hypertable(%L, %L, chunk_time_interval => %L::interval, if_not_exists => TRUE);',
    sch||'.liquidations', 'time', p_chunk_liquidations
  );

  IF p_create_indexes THEN
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.liquidations (symbol, time DESC);', sch||'_liq_sym_time', sch);
  END IF;

  ---------------------------------------------------------------------------
  -- COMPRESSION SETTINGS (warehouse-first)
  -- Segment by symbol (and side for depth) + order by time DESC
  ---------------------------------------------------------------------------
  EXECUTE format($SQL$
    ALTER TABLE %I.trades SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'symbol',
      timescaledb.compress_orderby   = 'time DESC'
    );
  $SQL$, sch);

  EXECUTE format($SQL$
    ALTER TABLE %I.depth_deltas SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'symbol,side',
      timescaledb.compress_orderby   = 'time DESC'
    );
  $SQL$, sch);

  EXECUTE format($SQL$
    ALTER TABLE %I.open_interest SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'symbol',
      timescaledb.compress_orderby   = 'time DESC'
    );
  $SQL$, sch);

  EXECUTE format($SQL$
    ALTER TABLE %I.funding SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'symbol',
      timescaledb.compress_orderby   = 'time DESC'
    );
  $SQL$, sch);

  EXECUTE format($SQL$
    ALTER TABLE %I.liquidations SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'symbol',
      timescaledb.compress_orderby   = 'time DESC'
    );
  $SQL$, sch);

  ---------------------------------------------------------------------------
  -- POLICIES (idempotent): add only if missing
  ---------------------------------------------------------------------------
  -- helper: add compression policy if not present
  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_compression'
      AND hypertable_schema = sch
      AND hypertable_name = 'trades'
  ) THEN
    EXECUTE format('SELECT add_compression_policy(%L, %L::interval);', sch||'.trades', p_compress_after);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_compression'
      AND hypertable_schema = sch
      AND hypertable_name = 'depth_deltas'
  ) THEN
    EXECUTE format('SELECT add_compression_policy(%L, %L::interval);', sch||'.depth_deltas', p_compress_after);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_compression'
      AND hypertable_schema = sch
      AND hypertable_name = 'open_interest'
  ) THEN
    EXECUTE format('SELECT add_compression_policy(%L, %L::interval);', sch||'.open_interest', p_compress_after);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_compression'
      AND hypertable_schema = sch
      AND hypertable_name = 'funding'
  ) THEN
    EXECUTE format('SELECT add_compression_policy(%L, %L::interval);', sch||'.funding', p_compress_after);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_compression'
      AND hypertable_schema = sch
      AND hypertable_name = 'liquidations'
  ) THEN
    EXECUTE format('SELECT add_compression_policy(%L, %L::interval);', sch||'.liquidations', p_compress_after);
  END IF;

  -- helper: add retention policy if not present
  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_retention'
      AND hypertable_schema = sch
      AND hypertable_name = 'trades'
  ) THEN
    EXECUTE format('SELECT add_retention_policy(%L, %L::interval);', sch||'.trades', p_retention);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_retention'
      AND hypertable_schema = sch
      AND hypertable_name = 'depth_deltas'
  ) THEN
    EXECUTE format('SELECT add_retention_policy(%L, %L::interval);', sch||'.depth_deltas', p_retention);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_retention'
      AND hypertable_schema = sch
      AND hypertable_name = 'open_interest'
  ) THEN
    EXECUTE format('SELECT add_retention_policy(%L, %L::interval);', sch||'.open_interest', p_retention);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_retention'
      AND hypertable_schema = sch
      AND hypertable_name = 'funding'
  ) THEN
    EXECUTE format('SELECT add_retention_policy(%L, %L::interval);', sch||'.funding', p_retention);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_retention'
      AND hypertable_schema = sch
      AND hypertable_name = 'liquidations'
  ) THEN
    EXECUTE format('SELECT add_retention_policy(%L, %L::interval);', sch||'.liquidations', p_retention);
  END IF;

END;
$$;

-- Example:
-- Warehouse-first defaults: compress after 6 hours, depth chunks 6 hours, retain 3 months, NO indexes.
SELECT create_exchange_hypertables('hyperliquid_perp');
SELECT create_exchange_hypertables('binance_linear');
SELECT create_exchange_hypertables(
  p_exchange           => 'bybit_linear',
  p_retention          => INTERVAL '3 days',
  p_compress_after     => INTERVAL '1 hour',
  p_chunk_trades       => INTERVAL '12 hours',
  p_chunk_depth        => INTERVAL '6 hours',
  p_chunk_oi           => INTERVAL '1 day',
  p_chunk_funding      => INTERVAL '1 day',
  p_chunk_liquidations => INTERVAL '12 hours'
);

