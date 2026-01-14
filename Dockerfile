FROM rust:1.88 as builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY . .
RUN cargo build --release --locked

# ---- runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     ca-certificates tini libssl3 libgcc-s1 \
  && rm -rf /var/lib/apt/lists/*

# Non-root
RUN useradd -r -u 10001 -g root mini \
  && mkdir -p /etc/mini-fintickstreams \
  && chown -R 10001:0 /etc/mini-fintickstreams

# This directory is expected to be bind-mounted from the host:
#   -v "$PWD/src/config:/etc/mini-fintickstreams:ro"
VOLUME ["/etc/mini-fintickstreams"]

COPY --from=builder /app/target/release/mini-fintickstreams /usr/local/bin/mini-fintickstreams

# --------------------------
# Runtime env defaults
# --------------------------
ENV RUST_BACKTRACE=1

# Single config version selector
ENV MINI_FINTICKSTREAMS_CONFIG_VERSION=1

# Explicit per-version config paths (pointing into the mounted volume)
ENV MINI_FINTICKSTREAMS_API_CONFIG_PATH_1=/etc/mini-fintickstreams/api.toml \
    MINI_FINTICKSTREAMS_APP_CONFIG_PATH_1=/etc/mini-fintickstreams/app.toml \
    MINI_FINTICKSTREAMS_TIMESCALE_CONFIG_PATH_1=/etc/mini-fintickstreams/timescale_db.toml \
    MINI_FINTICKSTREAMS_PROMETHEUS_CONFIG_PATH_1=/etc/mini-fintickstreams/prometheus.toml \
    MINI_FINTICKSTREAMS_REDIS_CONFIG_PATH_1=/etc/mini-fintickstreams/redis.toml \
    MINI_FINTICKSTREAMS_EXCHANGE_CONFIG_PATH_1_BINANCE_LINEAR=/etc/mini-fintickstreams/binance_linear.toml \
    MINI_FINTICKSTREAMS_EXCHANGE_CONFIG_PATH_1_HYPERLIQUID_PERP=/etc/mini-fintickstreams/hyperliquid_perp.toml

WORKDIR /app
USER 10001

ENTRYPOINT ["/usr/bin/tini","--","/usr/local/bin/mini-fintickstreams"]
CMD ["--config","env","--shutdown-action","none"]


