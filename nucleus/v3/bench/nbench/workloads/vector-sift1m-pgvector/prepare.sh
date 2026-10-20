#!/usr/bin/env bash
# One-time preparation for vector-sift1m-pgvector: same SIFT1M dataset
# as the qdrant row (shared fetch/convert step; see that prepare.sh).
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

exec bash "${NB_HARNESS}/workloads/vector-sift1m-qdrant/prepare.sh"
