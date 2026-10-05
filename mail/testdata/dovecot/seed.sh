#!/bin/sh
# Seeds oracle@test.local with a fixed starting mailbox, written straight into
# the Maildir so flags can be set exactly (Maildir encodes them in the file
# name: S seen, F flagged, D draft).
#
#   INBOX    5 messages: 3 unread, 1 read, 1 flagged+unread, 1 flagged+read
#   Drafts   1 draft          (special-use \Drafts)
#   Sent     1 sent message   (special-use \Sent)
#   Trash    1 message        (special-use \Trash)
#   Junk, Archive exist and are empty
set -eu

USER_NAME=oracle@test.local
MAILDIR=/var/mail/vhome/$USER_NAME/Maildir

for box in Drafts Sent Trash Junk Archive; do
  doveadm mailbox create -u "$USER_NAME" "$box" 2>/dev/null || true
done
doveadm mailbox subscribe -u "$USER_NAME" Drafts Sent Trash Junk Archive

n=0
put() { # put <dir: "" for INBOX or ".Name"> <flags> <from> <to> <subject> <body>
  n=$((n + 1))
  dir="$MAILDIR/$1/cur"
  [ -d "$dir" ] || dir="$MAILDIR/cur"
  file="$dir/$((1700000000 + n)).seed$n.mail.test.local:2,$2"
  {
    printf 'From: %s\r\nTo: %s\r\nSubject: %s\r\n' "$3" "$4" "$5"
    printf 'Message-ID: <seed-%s@test.local>\r\n' "$n"
    printf 'Date: Mon, 01 Jun 2026 1%s:00:00 +0000\r\n' "$n"
    printf 'MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n%s\r\n' "$6"
  } >"$file"
  chown vmailer:vmailer "$file"
}

me=$USER_NAME
put ""        ""   "alice@test.local" "$me" "seed unread one"        "unread body one"
put ""        ""   "bob@test.local"   "$me" "seed unread two"        "unread body two"
put ""        ""   "carol@test.local" "$me" "seed unread three"      "unread body three"
put ""        "S"  "dave@test.local"  "$me" "seed read"              "read body"
put ""        "F"  "erin@test.local"  "$me" "seed flagged unread"    "flagged unread body"
put ""        "FS" "frank@test.local" "$me" "seed flagged read"      "flagged read body"
put ".Drafts" "DS" "$me" "alice@test.local" "seed draft"             "draft body"
put ".Sent"   "S"  "$me" "bob@test.local"   "seed sent"              "sent body"
put ".Trash"  "S"  "gina@test.local"    "$me" "seed trash"           "trash body"

# Let Dovecot index what was dropped in behind its back.
doveadm force-resync -u "$USER_NAME" '*'
