#!/usr/bin/env bash
#
# Linux-only memory-safety + leak check for Nucleus.
#
# macOS can't do this: LeakSanitizer is Linux-only, valgrind doesn't run on
# modern macOS, and miri can't execute the threaded/FFI/async engine. So the
# host audit could only verify "no RSS growth under load". This script closes
# that gap by running the leak-prone tests under AddressSanitizer +
# LeakSanitizer, which detect real leaks, use-after-free, and buffer overflows.
#
# Run it on any Linux box (Proxmox node, dev laptop) or in CI:
#     bash nucleus/scripts/linux-leak-check.sh
#     FULL=1 bash nucleus/scripts/linux-leak-check.sh   # also instrument the full lib suite (slow)
#
# Exit code is non-zero if any sanitizer check fails.
set -uo pipefail

# 1. Guard: Linux only. On anything else, no-op success so it's safe to wire
#    into a cross-platform script without breaking macOS developers.
if [ "$(uname -s)" != "Linux" ]; then
  echo "linux-leak-check: requires Linux (LeakSanitizer is Linux-only); skipping on $(uname -s)."
  exit 0
fi

# 2. Resolve the sanitizer target triple from the host arch.
case "$(uname -m)" in
  x86_64)        TARGET=x86_64-unknown-linux-gnu ;;
  aarch64|arm64) TARGET=aarch64-unknown-linux-gnu ;;
  *) echo "linux-leak-check: unsupported arch $(uname -m)"; exit 1 ;;
esac

# 3. Sanitizers are a nightly -Z feature. Ensure nightly + the std target exist.
if ! command -v rustup >/dev/null 2>&1; then
  echo "linux-leak-check: rustup not found; install Rust via rustup first."
  exit 1
fi
rustup toolchain list | grep -q nightly || rustup toolchain install nightly --profile minimal
rustup target add --toolchain nightly "$TARGET" >/dev/null 2>&1 || true

# 4. Work from the nucleus crate root regardless of where we were invoked.
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.." || exit 1

export RUSTFLAGS="-Zsanitizer=address"
export RUSTDOCFLAGS="-Zsanitizer=address"
# detect_leaks=1: LeakSanitizer at exit. halt_on_error=0: report every issue,
# not just the first. A leak/UAF inside Nucleus code makes the test process exit
# non-zero, which fails the run.
export ASAN_OPTIONS="detect_leaks=1:halt_on_error=0:detect_stack_use_after_return=1"
# Suppress known-benign leaks from third-party/system libs (none yet — add lines
# like `leak:somelib` here if a dependency reports a false positive).
export LSAN_OPTIONS="suppressions=$PWD/scripts/lsan.supp:print_suppressions=0"
export RUST_BACKTRACE=1

fail=0
run() {
  echo ""
  echo "=== ASan+LSan: cargo +nightly test $* ==="
  cargo +nightly test --release --target "$TARGET" --features server "$@" || fail=1
}

# Leak-prone surface: heavy alloc/free under concurrency, durability replay, and
# scale. ASan instrumentation is slow, so target the churny tests by default.
run --test concurrent_stress -- --ignored   # 50+ concurrent clients, mixed R/W
run --test extreme_stress    -- --ignored   # version churn, GC, crash-recover cycles
run --test crash_recovery                   # WAL replay across restarts
run --test stress_test       -- --ignored   # large inserts / scans / joins
run --test scale_load        -- --ignored   # 1M-row table

# Optional: instrument the whole unit-test suite for broad UAF coverage (slow).
if [ "${FULL:-0}" = "1" ]; then
  run --lib
fi

echo ""
if [ "$fail" -eq 0 ]; then
  echo "linux-leak-check: PASSED — no leaks / memory errors detected under ASan+LSan."
else
  echo "linux-leak-check: FAILED — sanitizer reported issues (see output above)."
fi
exit "$fail"
