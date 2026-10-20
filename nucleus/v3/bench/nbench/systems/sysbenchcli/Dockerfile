# sysbench client image (load generator for oltp-sysbench-pg17).
#
# Debian's sysbench package is compiled with the PostgreSQL driver
# (Alpine's is not — verified 2026-10-18: alpine sysbench 1.0.20 rejects
# --pgsql-host). sysbench itself is GPL-2.0 software run as a standalone
# tool — not linked into anything Nucleus ships — and GPL does not
# restrict publishing benchmark results of the database under test.
FROM docker.io/library/debian:12@sha256:2c037a04925515fdd6ea85ea14a682d0e79931f5e9f5d07b6dbfc6ba12f9e858

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends sysbench \
 && rm -rf /var/lib/apt/lists/*

# Record the exact tool version into the image history for provenance.
RUN sysbench --version
