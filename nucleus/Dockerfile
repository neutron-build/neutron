# syntax=docker/dockerfile:1.7
#
# Nucleus container image — multi-stage build from source.
#
#   docker buildx build -t ghcr.io/neutron-build/nucleus:latest ./nucleus
#
# Requires a BuildKit-capable builder (docker buildx, buildah/podman). The
# `--mount=type=cache` lines below are not understood by the legacy docker
# builder; there is no non-BuildKit fallback, by choice — a source build of
# this crate without a warm target cache is 20+ minutes every time.
#
# For release images, prefer `Dockerfile.dist`, which packages an
# already-built binary and therefore cross-builds arm64 without QEMU.
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

# The binary is copied out inside the same RUN because `target/` is a cache
# mount and does not survive into the image layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    cargo build --release --locked --bin nucleus \
    && strip target/release/nucleus \
    && cp target/release/nucleus /nucleus

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Fixed uid/gid so a host bind-mount can be chowned ahead of time.
RUN groupadd --system --gid 10001 nucleus \
    && useradd --system --uid 10001 --gid nucleus \
        --home-dir /data --shell /usr/sbin/nologin nucleus \
    && mkdir -p /data \
    && chown nucleus:nucleus /data

COPY --from=builder /nucleus /usr/local/bin/nucleus

USER 10001:10001
WORKDIR /data
VOLUME ["/data"]

# 5432 pgwire · 5433 cluster · 5434 replication · 6379 RESP
EXPOSE 5432 5433 5434 6379

# LIVENESS ONLY. `nucleus status` opens a TCP connection and reports success;
# it does not authenticate, does not run a query, and therefore does not prove
# the engine can serve reads. A readiness probe needs credentials — use
# `nucleus shell -c 'SELECT 1'` from an orchestrator that has them.
# START PERIOD IS SIZED FOR RECOVERY, NOT FOR STARTUP. The engine restores its
# catalog and replays its log BEFORE it binds the port, and how long that takes
# scales with the data directory. Measured 2026-08-31 on the first real upgrade
# ever performed: a 28 GB instance logged "Restored 64 table(s) from catalog"
# almost immediately and then took 3m15s to reach "Listening on 0.0.0.0:5432" —
# working the whole time, at 14-18% CPU.
#
# At the previous --start-period=20s that instance was marked UNHEALTHY about
# 110 seconds before it finished, and the failure mode of that is not cosmetic:
# an orchestrator configured to replace unhealthy containers kills a database
# that is mid-recovery, which restarts recovery, which is killed again. The
# bigger the database, the more certain the loop — so the default has to cover
# the slow case, and a probe that fails for longer than necessary during
# startup costs nothing, while a database that is never allowed to finish
# recovering costs everything.
#
# 15m is deliberately generous rather than fitted to the one measurement above;
# `--retries` still catches an engine that dies after the window.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15m --retries=3 \
    CMD ["nucleus", "status", "--host", "127.0.0.1:5432"]

ENTRYPOINT ["nucleus"]
CMD ["start", "--host", "0.0.0.0", "--port", "5432", "--data", "/data"]
