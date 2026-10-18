# sysbench client image (load generator for oltp-sysbench-pg17).
#
# sysbench itself is GPL-2.0 software run as a standalone tool — it is
# not linked into anything Nucleus ships, and GPL does not restrict
# publishing benchmark results of the *database* under test.
# Alpine's community repository packages sysbench 1.0.20 with the
# PostgreSQL driver; the base image is pinned by digest.
FROM docker.io/library/alpine:3.20@sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e

RUN apk add --no-cache sysbench libpq

# Record the exact tool version into the image history for provenance.
RUN sysbench --version
