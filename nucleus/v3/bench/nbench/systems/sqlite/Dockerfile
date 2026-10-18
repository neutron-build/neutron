# SQLite speedtest1 builder (card B-0 Work item 3: "SQLite speedtest1
# (built from the amalgamation inside a container)").
#
# The SQLite source tarball is fetched at build time from sqlite.org and
# verified against the sha256 published on sqlite.org/download.html, so
# the binary is traceable to the exact upstream release.
#
# Base image is pinned by digest in image.txt; ARG values are the
# upstream release coordinates.
FROM docker.io/library/alpine:3.20@sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e

RUN apk add --no-cache build-base wget unzip openssl

ARG SQLITE_YEAR=2026
ARG SQLITE_VER=3540100
# sqlite.org's download page publishes SHA3-256 hashes (not SHA-256);
# verified against `openssl dgst -sha3-256` on 2026-10-18.
ARG SQLITE_SHA3=877c6f4996ce7adb368686bce21638de9e3d91534832ad3d9d119037d1bc22ad

RUN mkdir -p /opt/nb /tmp/build \
 && cd /tmp/build \
 && wget -q "https://sqlite.org/${SQLITE_YEAR}/sqlite-src-${SQLITE_VER}.zip" \
 && echo "${SQLITE_SHA3}  sqlite-src-${SQLITE_VER}.zip" > checksum.txt \
 && test "$(openssl dgst -sha3-256 -r sqlite-src-${SQLITE_VER}.zip | cut -d' ' -f1)" = "${SQLITE_SHA3}" \
 && unzip -q "sqlite-src-${SQLITE_VER}.zip" \
 && cd "sqlite-src-${SQLITE_VER}" \
 && cc -O2 -o /opt/nb/speedtest1 tool/speedtest1.c sqlite3.c \
      -I. -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_SHA3 \
      -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_OMIT_LOAD_EXTENSION \
      -lm \
 && cc -O2 -o /opt/nb/sqlite3 shell.c sqlite3.c \
      -I. -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_SHA3 \
      -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_OMIT_LOAD_EXTENSION \
      -lm \
 && rm -rf /tmp/build

# The workload driver runs the binary from the shared volume this image
# populates (see workloads/embedded-sqlite-speedtest1/driver.sh).
CMD ["/bin/sh", "-c", "echo 'builder image: copy /opt/nb from the volume'"]
