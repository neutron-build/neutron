#!/usr/bin/env sh
# Linux validation run for Nucleus. Most development/testing happens on macOS;
# this exercises the engine on the production target (Linux) where the thread
# scheduler, filesystem (ext4/xfs), and tooling (ThreadSanitizer) differ — the
# places scheduler-dependent concurrency bugs and durability bugs hide.
#
# RAM-aware: full-scale fuzzing and ThreadSanitizer are memory-hungry and are
# skipped on small boxes (a 4 GB host will OOM). Use a >=8 GB VM for the heavy
# phases.
#
# Usage (from the repo's nucleus/ dir):  sh scripts/linux-vm-test.sh
set -eu

cd "$(dirname "$0")/.."   # nucleus/
FEATURES="server"
JOBS="${CARGO_BUILD_JOBS:-2}"          # cap parallel codegen on small boxes
export CARGO_BUILD_JOBS="$JOBS"

mem_mb=$(awk '/MemTotal/{print int($2/1024)}' /proc/meminfo 2>/dev/null || echo 0)
echo "==> host: $(uname -srm); cores=$(nproc 2>/dev/null||echo ?); mem=${mem_mb}MB; cargo jobs=$JOBS"

# ── Phase 1: library test suite (debug) ─────────────────────────────────────
echo "==> [1/4] cargo test --lib --features $FEATURES"
cargo test --lib --features "$FEATURES" 2>&1 | tail -5

# ── Phase 2: integration regressions (the concurrency/constraint/crash ones) ─
echo "==> [2/4] key regression tests"
cargo test --features "$FEATURES" \
  --test concurrent_lost_update_regression \
  --test concurrent_rmw_regression \
  --test ssi_write_skew_regression \
  --test read_committed_regression \
  --test concurrent_unique_constraint_regression \
  --test concurrency_schema_constraints_probe \
  --test disk_recovery_regression \
  --test disk_recovery_dml_regression \
  --test crash_utf8_overflow_regression \
  2>&1 | grep -E "test result|FAILED|error\[" || true

# ── Phase 3: probe suite (scale by RAM) ─────────────────────────────────────
if [ "$mem_mb" -ge 8000 ]; then
  echo "==> [3/4] PROBE_SCALE=full sh scripts/probe.sh  (>=8GB: full scale)"
  PROBE_SCALE=full sh scripts/probe.sh 2>&1 | tail -40
else
  echo "==> [3/4] sh scripts/probe.sh  (ci scale; <8GB RAM — full scale would OOM)"
  sh scripts/probe.sh 2>&1 | tail -40
fi

# ── Phase 4: ThreadSanitizer on the concurrency tests (Linux + nightly only) ─
# TSan directly detects data races in the MVCC engine — the strongest check for
# the begin/commit/SIREAD/unique-reservation concurrency code. Needs nightly and
# the rust-src component; very memory-hungry, so gated on RAM.
if [ "$mem_mb" -lt 8000 ]; then
  echo "==> [4/4] SKIP ThreadSanitizer (needs >=8GB RAM; this host has ${mem_mb}MB)"
elif ! rustup toolchain list 2>/dev/null | grep -q nightly; then
  echo "==> [4/4] SKIP ThreadSanitizer (no nightly toolchain: 'rustup toolchain install nightly')"
else
  echo "==> [4/4] ThreadSanitizer on concurrency tests (nightly)"
  RUSTFLAGS="-Zsanitizer=thread" \
  cargo +nightly test -Zbuild-std --target "$(uname -m)-unknown-linux-gnu" \
    --features "$FEATURES" \
    --test concurrent_rmw_regression \
    --test concurrency_schema_constraints_probe \
    2>&1 | grep -E "test result|data race|WARNING: ThreadSanitizer|FAILED" || true
fi

echo "==> done."
