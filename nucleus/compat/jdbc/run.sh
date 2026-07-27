#!/bin/sh
# JDBC compatibility harness — boots a release Nucleus on an ephemeral port,
# runs Main.java through the pgjdbc driver, tears down. Exit non-zero on FAIL.
#
# Usage:
#   sh run.sh              # build release nucleus + run
#   sh run.sh --no-build   # reuse existing target/release/nucleus
#
# Toolchain: any JDK (javac+java). The pgjdbc jar is downloaded into lib/
# on first run (gitignored). SKIPs (exit 0) if no JDK or the jar can't be
# fetched offline.
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
NUCLEUS_DIR=$(cd "$HERE/../.." && pwd)
BIN="$NUCLEUS_DIR/target/release/nucleus"
PGJDBC_VERSION=42.7.7
JAR="$HERE/lib/postgresql-$PGJDBC_VERSION.jar"

BUILD=1
for a in "$@"; do
    case "$a" in
        --no-build) BUILD=0 ;;
        *) echo "unknown flag: $a" >&2; exit 2 ;;
    esac
done

# Resolve a JDK: PATH first, then Homebrew's unlinked openjdk.
JAVAC=$(command -v javac || true)
JAVA=$(command -v java || true)
if [ -z "$JAVAC" ] || ! "$JAVAC" -version >/dev/null 2>&1; then
    for cand in /opt/homebrew/opt/openjdk/bin /usr/local/opt/openjdk/bin; do
        if [ -x "$cand/javac" ]; then JAVAC="$cand/javac"; JAVA="$cand/java"; break; fi
    done
fi
if [ -z "$JAVAC" ] || ! "$JAVAC" -version >/dev/null 2>&1; then
    echo "jdbc: no usable JDK — SKIP"
    exit 0
fi

if [ ! -f "$JAR" ]; then
    mkdir -p "$HERE/lib"
    echo "== jdbc: downloading pgjdbc $PGJDBC_VERSION =="
    curl -sfL -o "$JAR.part" \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/$PGJDBC_VERSION/postgresql-$PGJDBC_VERSION.jar" \
        && mv "$JAR.part" "$JAR"
    if [ ! -f "$JAR" ]; then
        echo "jdbc: pgjdbc download failed (offline?) — SKIP"
        exit 0
    fi
fi

if [ "$BUILD" = 1 ]; then
    echo "== building release nucleus =="
    (cd "$NUCLEUS_DIR" && cargo build --release 2>&1 | tail -1) || exit 1
fi
[ -x "$BIN" ] || { echo "missing $BIN" >&2; exit 1; }

TMP=$(mktemp -d)
cleanup() {
    if [ -f "$TMP/server.pid" ]; then
        kill "$(cat "$TMP/server.pid")" 2>/dev/null
        wait "$(cat "$TMP/server.pid")" 2>/dev/null
    fi
    rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

echo "== jdbc: compiling Main.java =="
"$JAVAC" -d "$TMP/classes" "$HERE/Main.java" || exit 1

port=$(python3 - <<'EOF'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
EOF
)
mkdir -p "$TMP/data"
"$BIN" start --port "$port" --host 127.0.0.1 --no-tls --data "$TMP/data" \
    >"$TMP/server.log" 2>&1 &
echo $! > "$TMP/server.pid"
i=0
while [ $i -lt 100 ]; do
    if python3 -c "import socket;socket.create_connection(('127.0.0.1',$port),0.2).close()" 2>/dev/null; then break; fi
    i=$((i+1)); sleep 0.1
done

echo "== jdbc: running (port $port) =="
JDBC_URL="jdbc:postgresql://127.0.0.1:$port/nucleus" NUCLEUS_TEST_CANCEL=1 \
    "$JAVA" -cp "$TMP/classes:$JAR" Main
rc=$?
if [ $rc -ne 0 ]; then
    echo "-- server.log tail --"
    tail -20 "$TMP/server.log"
    echo "jdbc: FAIL"
else
    echo "jdbc: PASS"
fi
exit $rc
