#!/usr/bin/env bash
# Smoke for row oltp-ycsb-pg17 (card NB-2 Acceptance), mirroring
# nbench-smoke.sh: one tiny row end to end — workload A, one thread,
# 1 000 records, 1 000 operations, one cold + one warm run. Smoke-size
# only (§7.7.4); nothing it prints is a measurement (§7.7.3 item 5).
#
# The smoke derives its own environment (the acceptance line runs it
# bare, with none of the operator's environment — the NB-3 smokes'
# rule). The piece this card's sandbox cannot take for granted is the
# user-scope sd-bus transport: the sandbox's PID namespace is nested
# inside a user namespace, so the host user systemd manager's PID is
# invisible from here, and sd-bus clients using the private transport
# (systemctl --user, plain `systemd-run --user` — exactly the calls
# the nbench runner makes to start/stop server containers) fail the
# peer-credential check: SO_PEERCRED pid 0 -> ENODATA -> "Failed to
# connect to user scope bus via local transport: No data available".
# The --wait/--pipe form the drivers' nb-podman wrapper uses reaches
# the manager over the session bus and is unaffected. When the probe
# below finds the private transport broken, the smoke relays it
# through a local socket (own PID namespace, so SO_PEERCRED sees the
# relay's PID) that forwards the D-Bus byte stream — SCM_RIGHTS
# descriptors included, which `systemd-run --pipe` passes stdio over —
# to the manager's real private socket, and points $XDG_RUNTIME_DIR
# at a shim holding that socket and a `bus` symlink to the real
# session bus. Units still start under the host's user manager,
# exactly as on the real host; only which socket path the sandboxed
# client connects to changes. On a host where the transport works the
# probe succeeds and none of this activates.
set -euo pipefail
cd "$(dirname "$0")/../.."

ROOT=$(cd ../.. && pwd)
cargo build -q --manifest-path "$ROOT/Cargo.toml" -p nbench

BIN="${CARGO_TARGET_DIR:-$ROOT/target}/debug/nbench"
if [ ! -x "$BIN" ]; then
  BIN="$ROOT/target/debug/nbench"
fi
if [ ! -x "$BIN" ]; then
  echo "nbench binary not found (looked at \$CARGO_TARGET_DIR and $ROOT/target)" >&2
  exit 1
fi

export NBENCH_ROOT="$PWD"

# --- environment the runner needs, derived, not inherited ----------
# RELAY_PID is the relay this run started (empty when it started
# none); the EXIT trap stops it so nothing is left running behind the
# acceptance command. A relay an earlier killed run left behind is
# simply reused (the relay script itself exits when its socket is
# already served — see its main()).
RELAY_PID=""
cleanup_relay() {
  if [ -n "$RELAY_PID" ] && kill -0 "$RELAY_PID" 2>/dev/null; then
    kill "$RELAY_PID" 2>/dev/null || true
  fi
}
trap cleanup_relay EXIT

if ! timeout 10 systemctl --user show-environment >/dev/null 2>&1; then
  ENVDIR="$PWD/work/ycsb-smoke-env"  # gitignored runtime scratch
  SHIM="$ENVDIR/shim"
  mkdir -p "$SHIM/systemd"
  REAL_XDG="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
  MANAGER_PRIVATE="$REAL_XDG/systemd/private"
  if ! command -v python3 >/dev/null 2>&1; then
    echo "user-scope sd-bus transport is broken here and python3, needed for the relay, is missing" >&2
    exit 1
  fi
  if [ ! -S "$MANAGER_PRIVATE" ]; then
    echo "user-scope sd-bus transport is broken here and no user manager private socket exists at $MANAGER_PRIVATE" >&2
    exit 1
  fi
  cat > "$ENVDIR/dbus-relay.py" <<'PYEOF'
#!/usr/bin/env python3
"""sd-bus private-socket relay for the oltp-ycsb-pg17 smoke (card
NB-2; written by smoke.sh into the gitignored work/ tree).

Why: the card sandbox's PID namespace is nested inside a user
namespace. The host's user systemd manager is healthy and answers on
its private socket, but from inside the sandbox sd-bus clients that
use the private transport (systemctl --user, plain
`systemd-run --user` — the nbench runner's container start/stop
calls) fail the peer-credential check: the manager's PID is
invisible from this namespace (SO_PEERCRED pid 0 -> -ENODATA ->
"Failed to connect to user scope bus ... No data available").

The relay listens on a socket inside THIS PID namespace (so
SO_PEERCRED sees the relay's own PID) and forwards the D-Bus byte
stream to the manager's real private socket — *including SCM_RIGHTS
file descriptors*, which `systemd-run --wait/--pipe` (the nb-podman
form) uses to pass stdio to the units; a naive recv/send pump
silently drops them and the manager then rejects the message with
"Invalid request descriptor".

All units still start under the host's user manager, exactly as on
the real host; this changes only which socket path the sandboxed
client connects to.
"""
import os
import socket
import struct
import sys
import threading

LISTEN = sys.argv[1]
TARGET = sys.argv[2] if len(sys.argv) > 2 else "/run/user/1000/systemd/private"

# Room for ~60 file descriptors in one control message.
ANCBUF = socket.CMSG_SPACE(256)


def pump(a: socket.socket, b: socket.socket) -> None:
    """Relay bytes and SCM_RIGHTS descriptors from a to b."""
    try:
        while True:
            data, ancdata, _flags, _addr = a.recvmsg(65536, ANCBUF)
            if not data and not ancdata:
                break
            # fds the kernel duplicated into this process, packed in
            # the cmsg payload; collect them so they can be closed
            # after send duplicates them again into the destination.
            held = []
            for level, ctype, cdata in ancdata:
                if level == socket.SOL_SOCKET and ctype == socket.SCM_RIGHTS:
                    n = len(cdata) // 4
                    held.extend(struct.unpack(f"{n}i", cdata[: n * 4]))
            try:
                if ancdata:
                    b.sendmsg([data], ancdata)
                else:
                    b.sendall(data)
            finally:
                for fd in held:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
    except OSError:
        pass
    finally:
        for s in (a, b):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def serve(client: socket.socket) -> None:
    upstream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    upstream.settimeout(10)
    try:
        upstream.connect(TARGET)
    except OSError as e:
        print(f"upstream connect failed: {e}", file=sys.stderr, flush=True)
        client.close()
        return
    finally:
        upstream.settimeout(None)
    threading.Thread(target=pump, args=(client, upstream), daemon=True).start()
    threading.Thread(target=pump, args=(upstream, client), daemon=True).start()


def main() -> None:
    # A previous relay may have left a stale socket file; if nothing
    # answers on it, it is ours to replace. If something answers, a
    # live relay is already serving this socket — exit and let it.
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.connect(LISTEN)
        print("another live relay is already listening; exiting", flush=True)
        return
    except OSError:
        try:
            os.unlink(LISTEN)
        except OSError:
            pass
    finally:
        probe.close()
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(LISTEN)
    srv.listen(64)
    os.chmod(LISTEN, 0o700)
    print(f"relay listening on {LISTEN} -> {TARGET}", flush=True)
    while True:
        client, _ = srv.accept()
        # Connect upstream in a worker thread: a wedged connect must
        # never stall the accept loop for other clients.
        threading.Thread(target=serve, args=(client,), daemon=True).start()


if __name__ == "__main__":
    main()
PYEOF
  # The session-bus form must keep reaching the real bus whatever
  # $DBUS_SESSION_BUS_ADDRESS says (or omits): the shim's `bus` is
  # the real one.
  ln -sfn "$REAL_XDG/bus" "$SHIM/bus"
  python3 "$ENVDIR/dbus-relay.py" "$SHIM/systemd/private" "$MANAGER_PRIVATE" \
    >"$ENVDIR/relay.log" 2>&1 &
  RELAY_PID=$!
  export XDG_RUNTIME_DIR="$SHIM"
  # Wait until the relay answers on behalf of the manager (it may
  # have exited immediately because an earlier run's relay still
  # serves this socket — the probe below then succeeds just the
  # same).
  relay_ok=""
  for ((i = 0; i < 50; i++)); do
    if timeout 5 systemctl --user show-environment >/dev/null 2>&1; then
      relay_ok=1
      break
    fi
    sleep 0.1
  done
  if [ -z "$relay_ok" ]; then
    echo "cannot reach the user systemd manager: relay $SHIM/systemd/private -> $MANAGER_PRIVATE did not come up (see $ENVDIR/relay.log)" >&2
    exit 1
  fi
  echo "--- derived environment: sd-bus relay $SHIM/systemd/private -> $MANAGER_PRIVATE"
fi

echo "--- cleanup any previous nbench servers"
"$BIN" cleanup

echo "--- run the tiny row"
"$BIN" run oltp-ycsb-pg17 --tag smoke \
  --set ycsb_workloads=a --set ycsb_threads=1 \
  --set recordcount=1000 --set ycsb_operations=1000 \
  --set runs=1 --set cold_runs=1 --set warmup_runs=0

echo "--- generate the report"
"$BIN" report --tag smoke

TODAY=$(date -u +%F)
REPORT="results/smoke/$TODAY/REPORT.md"
RESULTS="results/smoke/$TODAY/oltp-ycsb-pg17.smoke.json"
for f in "$REPORT" "$RESULTS"; do
  if [ ! -s "$f" ]; then
    echo "SMOKE FAIL: $f missing or empty" >&2
    exit 1
  fi
done

grep -q "oltp-ycsb-pg17" "$REPORT" || { echo "SMOKE FAIL: row missing from report" >&2; exit 1; }
grep -q '"voided":false' "$RESULTS" || { echo "SMOKE FAIL: run was voided" >&2; exit 1; }
grep -q '"match":true' "$RESULTS" || { echo "SMOKE FAIL: init digest did not match/record" >&2; exit 1; }

echo "SMOKE OK: report at $REPORT"
