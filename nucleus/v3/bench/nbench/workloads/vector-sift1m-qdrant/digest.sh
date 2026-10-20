#!/usr/bin/env bash
# Result-set digest for vector-sift1m-qdrant (PLAN-v3 §5.2 rule 4).
#
# Strict at init and final: point count plus a hash of a fixed sample of
# stored vectors read back through the API. The dataset is pinned by the
# prepare step, so any difference means the loaded data changed.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

"${NB_BIN:?}" vec-qdrant-digest \
  --host 127.0.0.1 --port "${NB_PORT:?}" --collection nbench-sift
