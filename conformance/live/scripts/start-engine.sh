#!/usr/bin/env bash
# Starts a Nucleus for the live conformance suite, the same way everywhere.
#
#   sh conformance/live/scripts/start-engine.sh [port] [datadir]
#
# Every SDK's CI job and every developer should boot the engine through this
# script rather than inventing their own invocation, because two of the three
# things it does are non-obvious and both have already cost a red run.
set -euo pipefail

PORT="${1:-55432}"
DATA="${2:-/tmp/nucleus-live}"
REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
BIN="${NUCLEUS_BIN:-$REPO/nucleus/target/release/nucleus}"

if [ ! -x "$BIN" ]; then
  echo "no nucleus binary at $BIN" >&2
  echo "build one: (cd nucleus && cargo build --release --bin nucleus --features server)" >&2
  exit 1
fi

rm -rf "$DATA"
mkdir -p "$DATA"

# Nucleus refuses writes below a 3% free-disk watermark. That is correct for a
# production server and wrong for a conformance run, which writes a few
# megabytes: on any developer machine that happens to be near-full, the entire
# suite fails with DiskFullError and reads as a conformance failure. A run that
# fails for a reason unrelated to what it tests is worse than no run.
cat > "$DATA/nucleus.toml" <<'TOML'
[storage]
disk_warn_free_pct = 0.5
disk_readonly_free_pct = 0.1
disk_min_free_mb = 128
TOML

# NOT 5432 or 5433. The server binds its own cluster transport on 5433,
# replication on 5434 and RESP on 6379, so serving SQL on 5433 makes it collide
# with itself -- which is exactly how the SDK-live workflow failed its first run.
"$BIN" start --data "$DATA" --config "$DATA/nucleus.toml" --port "$PORT" \
  > "$DATA/server.log" 2>&1 &
echo $! > "$DATA/server.pid"

for _ in $(seq 1 60); do
  if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
    echo "nucleus up on 127.0.0.1:$PORT (data=$DATA, pid=$(cat "$DATA/server.pid"))"
    exit 0
  fi
  sleep 1
done

echo "nucleus did not come up on port $PORT" >&2
tail -40 "$DATA/server.log" >&2
exit 1
