# YCSB 0.17.0 client image (load generator for row oltp-ycsb-pg17,
# card NB-2: "YCSB A–F over pgwire" — the §5.1 OLTP row that §8.1's
# B-0-F7 ruling recorded as missing from B-0's scope).
#
# Hash provenance — the sqlite builder's honesty pattern, with one
# honest difference: sqlite.org publishes SHA3-256 hashes upstream, so
# the sqlite builder verifies against the vendor. YCSB publishes NO
# hash for its release tarball, and Maven Central publishes sha1
# alongside the jar — neither gives an upstream sha256. The sha256s
# below were therefore computed at pin time (2026-11-27, card NB-2
# RESULT records them) from the artifacts fetched by this very build
# recipe: they pin the bits the image builds with, exactly like an
# image digest, and claim nothing about upstream integrity. The sha1
# Maven Central does publish for the jar (93b508727bf4af3db27059dc592
# d90da68ecee8d) was cross-checked at pin time.
#
# Base image pinned by digest in the FROM line (B-0's image.txt style;
# recorded 2026-11-27 from `podman inspect --format
# '{{index .RepoDigests 0}}'`). Both artifacts are fetched at image
# build time, never at test time.
FROM docker.io/library/eclipse-temurin:17-jre-jammy@sha256:7a93c4f0b26a0bcae4d8737198e3e8dbd5c2759da91863852a120a700f7c50f0

ARG YCSB_VERSION=0.17.0
ARG YCSB_TARBALL_SHA256=d80d8b67c4b1da9c7d9dc0d40cdd9a651f0c0f44e580aea00fb3f86cffb6f5cc
ARG PGJDBC_VERSION=42.7.6
ARG PGJDBC_JAR_SHA256=f2a1cc0352dd5e5c6f65dbadff27bee00c32e432c6ad030d07447f8a5983c42a

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends wget \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /tmp/build \
 && cd /tmp/build \
 && wget -q "https://github.com/brianfrankcooper/YCSB/releases/download/${YCSB_VERSION}/ycsb-${YCSB_VERSION}.tar.gz" \
 && echo "${YCSB_TARBALL_SHA256}  ycsb-${YCSB_VERSION}.tar.gz" | sha256sum -c - \
 && wget -q "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PGJDBC_VERSION}/postgresql-${PGJDBC_VERSION}.jar" \
 && echo "${PGJDBC_JAR_SHA256}  postgresql-${PGJDBC_VERSION}.jar" | sha256sum -c - \
 && tar -xzf "ycsb-${YCSB_VERSION}.tar.gz" -C /opt \
 && mv "postgresql-${PGJDBC_VERSION}.jar" "/opt/ycsb-${YCSB_VERSION}/jdbc-binding/lib/" \
 && ln -sfn "/opt/ycsb-${YCSB_VERSION}" /opt/ycsb \
 && rm -rf /tmp/build

# Record the tool versions into the image history for provenance. YCSB
# 0.17.0 has no --version flag; the extracted release directory name is
# the release string (the /opt/ycsb symlink resolves to it). The
# launcher used by the row is bin/ycsb.sh — the POSIX sh one the same
# tarball ships; bin/ycsb is a Python 2 script (print >> syntax) and no
# current base image carries a Python 2.
RUN java -version 2>&1 \
 && basename "$(readlink -f /opt/ycsb)" \
 && ls "/opt/ycsb-${YCSB_VERSION}/jdbc-binding/lib/postgresql-${PGJDBC_VERSION}.jar"
