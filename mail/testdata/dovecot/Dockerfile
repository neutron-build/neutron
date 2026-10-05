# Disposable mail server for the live IMAP oracle (imap/live_test.go).
#
# Dovecot is the IMAP server under test. Postfix sits in front of it as the
# out-of-band delivery path the oracle's deliver() helper expects (plain SMTP
# in, LMTP to Dovecot out), so mail reaches the mailbox the way it does in
# production rather than through the adapter being tested.
#
# Alpine 3.20 is pinned on purpose: it ships Dovecot 2.3, whose config syntax
# differs from the 2.4 that newer Alpine releases carry.
FROM alpine:3.20

RUN apk add --no-cache dovecot dovecot-lmtpd postfix \
 && adduser -D -u 5000 -h /var/mail/vhome vmailer \
 && mkdir -p /var/mail/vhome && chown vmailer:vmailer /var/mail/vhome

COPY dovecot.conf /etc/dovecot/dovecot.conf
COPY users /etc/dovecot/users
COPY postfix-main.cf /etc/postfix/main.cf
COPY vmailbox /etc/postfix/vmailbox
COPY entrypoint.sh seed.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/seed.sh

EXPOSE 143 25
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
