#!/bin/sh
# Starts Postfix (SMTP in, LMTP out), seeds the mailbox once Dovecot answers,
# and runs Dovecot in the foreground as the container's main process.
set -eu
postfix start
( until doveadm user oracle@test.local >/dev/null 2>&1; do sleep 0.2; done
  /usr/local/bin/seed.sh && touch /tmp/seeded ) &
exec dovecot -F
