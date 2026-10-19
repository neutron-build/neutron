#!/usr/bin/env bash
# One-time preparation for the SIFT1M vector rows: fetch the official
# ann-benchmarks SIFT1M file and convert it to the fvecs/ivecs layout the
# harness reads (the dataset the ann-benchmarks project itself uses for
# this benchmark; the texmex original is unreachable from this network,
# see the row README).
#
# The download is NOT trust-on-first-use: the sha256 and byte size below
# are pinned reference values, and a file that does not match both is
# rejected (gate nv3-B-0-gate-1 finding 7). Provenance of the pin: this
# exact file (fetched 2026-10-17) converts to exactly 1,000,000 base
# vectors and 10,000 queries of 128 dimensions (516,000,000 and
# 5,160,000 bytes of fvecs — length-checked by the harness reader), and
# the loaded content is independently pinned by the committed pgvector
# init digest (workloads/vector-sift1m-pgvector/expected/init.txt).
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

DATA=${NB_DATA:?}/sift
mkdir -p "$DATA"
H5=$DATA/sift-128-euclidean.hdf5
PYIMG=docker.io/library/python:3.12-slim
PIN_SHA256=dd6f0a6ed6b7ebb8934680f861a33ed01ff33991eaee4fd60914d854a0ca5984
PIN_SIZE=525128288

verify_h5() {
  local got
  got=$(sha256sum "$H5" | cut -d' ' -f1)
  local size
  size=$(stat -c %s "$H5")
  if [ "$got" != "$PIN_SHA256" ] || [ "$size" != "$PIN_SIZE" ]; then
    echo "SIFT1M hdf5 does not match the pinned reference:" >&2
    echo "  sha256 $got (want $PIN_SHA256)" >&2
    echo "  size   $size (want $PIN_SIZE)" >&2
    echo "Delete $H5 and re-run, or re-pin consciously with new provenance." >&2
    return 1
  fi
}

if [ -f "$DATA/base.fvecs" ] && [ -f "$DATA/query.fvecs" ] \
   && [ -f "$DATA/groundtruth.ivecs" ]; then
  # converted data exists; if the source hdf5 is still around it must
  # still match the pin (a swapped file must never convert silently)
  if [ -f "$H5" ]; then verify_h5; fi
  exit 0
fi

if [ ! -f "$H5" ]; then
  nb-podman run --rm --network host -v "$DATA:/data:Z" \
    docker.io/library/alpine:3.20 sh -c \
    "apk add --no-cache wget >/dev/null 2>&1 && \
     wget -q -O /data/sift-128-euclidean.hdf5.part \
       http://ann-benchmarks.com/sift-128-euclidean.hdf5 && \
     mv /data/sift-128-euclidean.hdf5.part /data/sift-128-euclidean.hdf5"
fi

verify_h5
sha256sum "$H5" > "$DATA/sha256.txt"

nb-podman run --rm --network host -v "$DATA:/data:Z" "$PYIMG" sh -c '
  pip install --quiet --no-cache-dir h5py numpy 2>/dev/null || \
    pip install --quiet --no-cache-dir --break-system-packages h5py numpy
  python3 - <<"EOF"
import h5py, numpy as np
f = h5py.File("/data/sift-128-euclidean.hdf5", "r")
def wvecs(path, arr, dtype):
    with open(path, "wb") as o:
        for row in arr:
            o.write(np.int32(len(row)).tobytes())
            o.write(np.asarray(row, dtype=dtype).tobytes())
wvecs("/data/base.fvecs", f["train"][:], "<f4")
wvecs("/data/query.fvecs", f["test"][:], "<f4")
wvecs("/data/groundtruth.ivecs", f["neighbors"][:], np.int32)
print("converted", f["train"].shape[0], "base,",
      f["test"].shape[0], "queries")
EOF
'
