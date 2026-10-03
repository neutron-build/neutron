#!/usr/bin/env bash
# Throwaway Dovecot (+ Postfix for out-of-band delivery) for the live IMAP
# oracle in imap/live_test.go. No real accounts are involved: the one user,
# oracle@test.local, exists only inside the container.
#
#   mail/scripts/live-imap.sh up     build + start, wait until seeded
#   mail/scripts/live-imap.sh env    print the variables the tests read
#   mail/scripts/live-imap.sh down   remove the container
#   mail/scripts/live-imap.sh test   up, run the live tests with -race (a skip
#                                    fails), down
#
# Locally:   eval "$(mail/scripts/live-imap.sh env)" && (cd mail && go test -race ./imap/...)
#
# Override the container CLI (CONTAINER_CLI=podman), the name, or the host
# ports (LIVE_IMAP_PORT, LIVE_SMTP_PORT) through the environment. Ports are
# published on loopback only; the test dialer refuses plaintext elsewhere.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fixture="$here/../testdata/dovecot"
cli="${CONTAINER_CLI:-docker}"
name="${LIVE_IMAP_NAME:-neutron-mail-live-imap}"
image="neutron-mail-live-imap:local"
imap_port="${LIVE_IMAP_PORT:-13143}"
smtp_port="${LIVE_SMTP_PORT:-13025}"

up() {
  "$cli" rm -f "$name" >/dev/null 2>&1 || true
  "$cli" build -q -t "$image" "$fixture" >/dev/null
  "$cli" run -d --name "$name" \
    -p "127.0.0.1:${imap_port}:143" -p "127.0.0.1:${smtp_port}:25" \
    "$image" >/dev/null

  # Ready means: seeded, IMAP greets, SMTP greets.
  for _ in $(seq 1 150); do
    if "$cli" exec "$name" test -e /tmp/seeded 2>/dev/null \
      && greets "$imap_port" '* OK' && greets "$smtp_port" '220'; then
      echo "live IMAP fixture ready: imap=127.0.0.1:${imap_port} smtp=127.0.0.1:${smtp_port}" >&2
      return 0
    fi
    sleep 0.4
  done
  echo "live IMAP fixture did not become ready; container log:" >&2
  "$cli" logs "$name" >&2 || true
  return 1
}

# greets <port> <prefix>: true when the server's first line starts with prefix.
greets() {
  local line
  line="$( (exec 3<>"/dev/tcp/127.0.0.1/$1" && IFS= read -r -t 2 line <&3 && printf '%s' "$line") 2>/dev/null || true)"
  [[ "$line" == "$2"* ]]
}

down() { "$cli" rm -f "$name" >/dev/null 2>&1 || true; }

print_env() {
  echo "export NEUTRON_MAIL_TEST_IMAP=127.0.0.1:${imap_port}"
  echo "export NEUTRON_MAIL_TEST_SMTP=127.0.0.1:${smtp_port}"
  echo "export NEUTRON_MAIL_TEST_IMAP_SEEDED=1"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  env) print_env ;;
  test)
    trap 'rc=$?; [ "$rc" -ne 0 ] && "$cli" logs "$name" >&2; down; exit "$rc"' EXIT
    up
    eval "$(print_env)"
    # The point is that these stop skipping, so a skip is a failure here.
    out="$(mktemp)"
    (cd "$here/.." && go test -race -count=1 -v -run 'TestLive' ./imap/...) 2>&1 | tee "$out"
    if [ "${PIPESTATUS[0]}" -ne 0 ]; then rm -f "$out"; exit 1; fi
    if grep -q -- '--- SKIP' "$out"; then
      echo "live IMAP tests skipped against the fixture; they must all run" >&2
      rm -f "$out"; exit 1
    fi
    rm -f "$out"
    ;;
  *) echo "usage: $0 up|down|env|test" >&2; exit 2 ;;
esac
