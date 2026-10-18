# SQLite speedtest1 builder (card B-0 Work item 3: "SQLite speedtest1
# (built from the amalgamation inside a container)").
#
# Two artifacts are fetched at build time from sqlite.org and each is
# verified against the SHA3-256 hash sqlite.org/download.html publishes
# (sqlite.org publishes SHA3-256, not SHA-256; verified with
# `openssl dgst -sha3-256` on 2026-10-17):
#   - sqlite-amalgamation-3540100.zip  -> sqlite3.c, sqlite3.h, shell.c
#     (the amalgamation; the src tree does not ship a prebuilt sqlite3.c)
#   - sqlite-src-3540100.zip           -> test/speedtest1.c
#     (speedtest1.c lives at test/ in the source tree)
#
# Base image is pinned by digest in image.txt; ARG values are the
# upstream release coordinates.
FROM docker.io/library/alpine:3.20@sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e

RUN apk add --no-cache build-base wget unzip openssl

ARG SQLITE_YEAR=2026
ARG SQLITE_VER=3540100
ARG AMALG_SHA3=92281c86e2957f9b9305890f9ac04204caa7b677cc868dc326d942689df28e30
ARG SRC_SHA3=877c6f4996ce7adb368686bce21638de9e3d91534832ad3d9d119037d1bc22ad

RUN mkdir -p /opt/nb /tmp/build \
 && cd /tmp/build \
 && wget -q "https://sqlite.org/${SQLITE_YEAR}/sqlite-amalgamation-${SQLITE_VER}.zip" \
 && wget -q "https://sqlite.org/${SQLITE_YEAR}/sqlite-src-${SQLITE_VER}.zip" \
 && test "$(openssl dgst -sha3-256 -r sqlite-amalgamation-${SQLITE_VER}.zip | cut -d' ' -f1)" = "${AMALG_SHA3}" \
 && test "$(openssl dgst -sha3-256 -r sqlite-src-${SQLITE_VER}.zip | cut -d' ' -f1)" = "${SRC_SHA3}" \
 && unzip -q "sqlite-amalgamation-${SQLITE_VER}.zip" \
 && unzip -q "sqlite-src-${SQLITE_VER}.zip" \
 && AMALG="sqlite-amalgamation-${SQLITE_VER}" \
 && SRC="sqlite-src-${SQLITE_VER}" \
 && cc -O2 -o /opt/nb/speedtest1 "${SRC}/test/speedtest1.c" "${AMALG}/sqlite3.c" \
      -I"${AMALG}" -DSQLITE_THREADSAFE=0 \
      -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_OMIT_LOAD_EXTENSION \
      -lm \
 && cc -O2 -o /opt/nb/sqlite3 "${AMALG}/shell.c" "${AMALG}/sqlite3.c" \
      -I"${AMALG}" -DSQLITE_THREADSAFE=0 \
      -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_OMIT_LOAD_EXTENSION \
      -lm \
 && rm -rf /tmp/build

# The workload driver runs the binary from the shared volume this image
# populates (see workloads/embedded-sqlite-speedtest1/driver.sh).
CMD ["/bin/sh", "-c", "echo 'builder image: copy /opt/nb from the volume'"]
