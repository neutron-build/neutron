# Nucleus container image — multi-stage build from source.
#
#   docker build -t ghcr.io/neutron-build/nucleus:latest .
#
# Defaults to binding 0.0.0.0 so sibling containers (e.g. observe) can reach
# it over a Docker network. A non-loopback bind with no auth requires
# NUCLEUS_ALLOW_NO_AUTH=1 (dev) or a password via NUCLEUS_PASSWORD.
FROM rust:1-bookworm AS builder
WORKDIR /build

# zstd + mimalloc (default `server` feature) compile C, so we need a toolchain.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release --bin nucleus \
    && strip target/release/nucleus

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/nucleus /usr/local/bin/nucleus

VOLUME ["/data"]
EXPOSE 5432 5433 5434

ENTRYPOINT ["nucleus"]
CMD ["start", "--host", "0.0.0.0", "--port", "5432", "--data", "/data"]
