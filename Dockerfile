# ---- build stage ----
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

COPY --from=builder /app/target/release/mini-fintickstreams /usr/local/bin/mini-fintickstreams

# --------------------------
# Runtime env defaults
# --------------------------
ENV RUST_BACKTRACE=1

# Optional: keep a single version value to align with your *_PATH_{version} pattern
# (Set to the version you want as the default suffix used by your deployment.)
ENV MINI_FINTICKSTREAMS_CONFIG_VERSION=1

# Optional: explicitly set default config paths for that version.
# These match your in-code defaults, but setting them here makes it obvious and
# allows override via `docker run -e ...` or Kubernetes env.
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

