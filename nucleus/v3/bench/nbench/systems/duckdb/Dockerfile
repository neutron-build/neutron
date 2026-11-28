# DuckDB CLI builder (card NB-3 Work 4: the embedded TPC-H row's engine).
#
# Build-time fetch with hash verification — the sqlite builder precedent
# (systems/sqlite/Dockerfile). The DuckDB CLI release artifact for the
# pinned version is downloaded from GitHub releases and verified against
# the sha256 upstream publishes on the release page (recorded at pin
# time, 2026-11-27, from the v2.0.1 asset listing; the download was
# re-measured byte-identical at pin time). The card's Open point
# expected upstream to publish no hash for the CLI artifact — for
# v2.0.1's assets GitHub lists the sha256, so this pin is
# upstream-anchored rather than fetch-time-recorded.
#
# The tpch extension is INSTALLED at build time and LOADED offline at
# run time (no network fetch at test time, card NB-3 Limits): the driver
# runs THIS image, whose /root/.duckdb carries the baked extension,
# while the duckdb binary itself lives in the shared artifacts volume
# (/opt/nb, copied out by workloads/embedded-duckdb-tpch1/prepare.sh —
# the sqlite row's shape).
#
# Base image is pinned by digest in image.txt; ARG values are the
# upstream release coordinates.
FROM docker.io/library/debian:12@sha256:2c037a04925515fdd6ea85ea14a682d0e79931f5e9f5d07b6dbfc6ba12f9e858

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends wget ca-certificates tar time \
 && rm -rf /var/lib/apt/lists/*

ARG DUCKDB_VERSION=2.0.1
ARG DUCKDB_URL=https://github.com/duckdb/duckdb/releases/download/v2.0.1/duckdb-cli-linux-amd64.tar.gz
# sha256 as published on the v2.0.1 release page (verified 2026-11-27):
ARG DUCKDB_SHA256=b676268be4d39e4e41a904442c9796c9914e645cc085c7505f048f2c992b5eca

RUN mkdir -p /opt/nb /tmp/build \
 && cd /tmp/build \
 && wget -q "${DUCKDB_URL}" \
 && test "$(sha256sum duckdb-cli-linux-amd64.tar.gz | cut -d' ' -f1)" = "${DUCKDB_SHA256}" \
 && tar -xzf duckdb-cli-linux-amd64.tar.gz \
 && mv duckdb /opt/nb/duckdb \
 && chmod +x /opt/nb/duckdb \
 && rm -rf /tmp/build

# Bake the tpch extension at build time: dbgen (init) and the 22
# queries (run, digest) load it offline from the image. The run's
# extension directory is version-keyed (v2.0.1), matching the binary
# from the same build.
RUN /opt/nb/duckdb -c "INSTALL 'tpch'"

CMD ["/bin/sh", "-c", "echo 'builder image: run /opt/nb/duckdb; baked tpch extension lives in /root/.duckdb'"]
