#!/bin/sh
# Differential binary-COPY harness: the same seed data exported/imported with
# COPY ... (FORMAT binary) must round-trip PG17 -> Nucleus, Nucleus -> PG17,
# and Nucleus -> Nucleus (incl. a column subset); a truncated stream must be
# rejected. SKIPs (exit 0) when PostgreSQL 17 is unavailable.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
NUCLEUS_DIR=$(cd "$HERE/../.." && pwd)
NUC="$NUCLEUS_DIR/target/release/nucleus"
for pgbin in /opt/homebrew/opt/postgresql@17/bin /usr/local/opt/postgresql@17/bin /usr/lib/postgresql/17/bin; do
    [ -x "$pgbin/initdb" ] && PATH="$pgbin:$PATH" && break
done
command -v initdb >/dev/null 2>&1 || { echo "copybinary: no postgresql — SKIP"; exit 0; }
[ -x "$NUC" ] || { echo "missing $NUC (cargo build --release first)" >&2; exit 1; }
W=$(mktemp -d)
PG_PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
NUC_PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
cleanup() {
    pg_ctl -D "$W/pgdata" stop -m immediate >/dev/null 2>&1
    [ -f "$W/nuc.pid" ] && kill "$(cat $W/nuc.pid)" 2>/dev/null
    rm -rf "$W"
}
trap cleanup EXIT INT TERM

initdb -D "$W/pgdata" -A trust -U nucleus --locale=C --encoding=UTF8 >/dev/null 2>&1
pg_ctl -D "$W/pgdata" -o "-p $PG_PORT -c listen_addresses=127.0.0.1" -l "$W/pg.log" start >/dev/null
createdb -h 127.0.0.1 -p $PG_PORT -U nucleus nucleus 2>/dev/null

mkdir -p "$W/data"
"$NUC" start --port $NUC_PORT --host 127.0.0.1 --no-tls --data "$W/data" >"$W/nuc.log" 2>&1 &
echo $! > "$W/nuc.pid"
sleep 1.2

DDL="CREATE TABLE bt (id INT PRIMARY KEY, big BIGINT, t TEXT, b BOOLEAN, f DOUBLE PRECISION, n NUMERIC(14,4), ts TIMESTAMP, d DATE, raw BYTEA)"
SEED="INSERT INTO bt VALUES
 (1, 9007199254740993, 'plain', true, 2.5, 12345678.9012, '2026-07-23 14:30:45.123456', '1999-12-31', '\\x00ff5c'),
 (2, -1, E'tab\\there \"quoted''\", NULL row next', false, -0.001, -0.5001, '2000-01-01 00:00:00', '2000-01-01', '\\x'),
 (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 (4, 0, 'unicode ✓ é', true, 1e100, 99999999.9999, '1970-06-15 23:59:59', '2026-02-28', '\\xdeadbeef')"

PSQL_PG="psql -h 127.0.0.1 -p $PG_PORT -U nucleus -d nucleus -qtA"
PSQL_NU="psql -h 127.0.0.1 -p $NUC_PORT -U nucleus -d nucleus -qtA"

$PSQL_PG -c "$DDL" -c "$SEED" >/dev/null || { echo "PG seed failed"; exit 1; }
$PSQL_NU -c "$DDL" -c "$SEED" >/dev/null || { echo "Nucleus seed failed"; exit 1; }

FAIL=0

echo "== 1. PG-produced binary file loads into Nucleus =="
$PSQL_PG -c "\\copy bt TO '$W/pg.bin' (FORMAT binary)" >/dev/null || { echo "pg export failed"; exit 1; }
$PSQL_NU -c "CREATE TABLE bt_in (id INT PRIMARY KEY, big BIGINT, t TEXT, b BOOLEAN, f DOUBLE PRECISION, n NUMERIC(14,4), ts TIMESTAMP, d DATE, raw BYTEA)" >/dev/null
$PSQL_NU -c "\\copy bt_in FROM '$W/pg.bin' (FORMAT binary)" || { echo "FAIL: nucleus binary import"; FAIL=1; }
$PSQL_PG -c "SELECT * FROM bt ORDER BY id" > "$W/expected.txt"
$PSQL_NU -c "SELECT * FROM bt_in ORDER BY id" > "$W/got_import.txt"
diff "$W/expected.txt" "$W/got_import.txt" > "$W/diff1.txt" || { echo "FAIL: imported rows differ"; cat "$W/diff1.txt"; FAIL=1; }

echo "== 2. Nucleus-produced binary file loads into PG =="
$PSQL_NU -c "\\copy bt TO '$W/nuc.bin' (FORMAT binary)" || { echo "FAIL: nucleus binary export"; FAIL=1; }
$PSQL_PG -c "CREATE TABLE bt_in (id INT PRIMARY KEY, big BIGINT, t TEXT, b BOOLEAN, f DOUBLE PRECISION, n NUMERIC(14,4), ts TIMESTAMP, d DATE, raw BYTEA)" >/dev/null
$PSQL_PG -c "\\copy bt_in FROM '$W/nuc.bin' (FORMAT binary)" || { echo "FAIL: pg import of nucleus binary"; FAIL=1; }
$PSQL_PG -c "SELECT * FROM bt_in ORDER BY id" > "$W/got_export.txt"
diff "$W/expected.txt" "$W/got_export.txt" > "$W/diff2.txt" || { echo "FAIL: exported rows differ"; cat "$W/diff2.txt"; FAIL=1; }

echo "== 3. Nucleus->Nucleus round trip with column subset =="
$PSQL_NU -c "\\copy bt (t, id) TO '$W/subset.bin' (FORMAT binary)" || { echo "FAIL: subset export"; FAIL=1; }
$PSQL_NU -c "CREATE TABLE bt_sub (t TEXT, id INT)" >/dev/null
$PSQL_NU -c "\\copy bt_sub (t, id) FROM '$W/subset.bin' (FORMAT binary)" || { echo "FAIL: subset import"; FAIL=1; }
got=$($PSQL_NU -c "SELECT count(*) FROM bt_sub")
[ "$got" = "4" ] || { echo "FAIL: subset count $got != 4"; FAIL=1; }
pg_sub=$($PSQL_PG -c "SELECT t FROM bt WHERE id=2")
nu_sub=$($PSQL_NU -c "SELECT t FROM bt_sub WHERE id=2")
[ "$pg_sub" = "$nu_sub" ] || { echo "FAIL: subset text mismatch: [$nu_sub] vs [$pg_sub]"; FAIL=1; }

echo "== 4. Malformed stream fails loudly =="
head -c 30 "$W/pg.bin" > "$W/trunc.bin"
if $PSQL_NU -c "CREATE TABLE bt_bad (id INT PRIMARY KEY, big BIGINT, t TEXT, b BOOLEAN, f DOUBLE PRECISION, n NUMERIC(14,4), ts TIMESTAMP, d DATE, raw BYTEA)" -c "\\copy bt_bad FROM '$W/trunc.bin' (FORMAT binary)" 2>/dev/null; then
    echo "FAIL: truncated stream accepted"; FAIL=1
fi

[ "$FAIL" = 0 ] && echo "copybin: PASS" || echo "copybin: FAIL"
exit $FAIL
