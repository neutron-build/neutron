#!/usr/bin/env bash
# One-time preparation for the SIFT1M vector rows: fetch the official
# ann-benchmarks SIFT1M file and convert it to the fvecs/ivecs layout the
# harness reads (the dataset the ann-benchmarks project itself uses for
# this benchmark; the texmex original is unreachable from this network,
# see the row README).
#
# The hdf5 is kept (sha256 recorded) alongside the converted files.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

DATA=${NB_DATA:?}/sift
mkdir -p "$DATA"
H5=$DATA/sift-128-euclidean.hdf5
PYIMG=docker.io/library/python:3.12-slim

if [ -f "$DATA/base.fvecs" ] && [ -f "$DATA/query.fvecs" ] \
   && [ -f "$DATA/groundtruth.ivecs" ]; then
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
