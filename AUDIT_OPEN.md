# Open audit items

Unresolved findings for this repository from the ChatGPT-led audit series
(2026-09-09 through 2026-09-11, passes 1-5; register: teploy-neutron-lullmail
expanded audit). Every P0/P1 finding was fixed and verified earlier; the
P2 tail and the nucleus residual were worked on 2026-09-11.

Open items: none.

## Resolved 2026-09-11 (this run)

All 18 P2 items plus the nucleus residual were verified still present in
current source and fixed. No item was ALREADY FIXED and none was DEFERRED
against a recorded decision.

| Item | Resolution | Commit |
|---|---|---|
| neutron-02 | FIXED — explicit `upgrade` fails when the update check fails | fec68499 |
| neutron-03 | FIXED — cmd-context propagation, documented `--timeout`, strict count parse | 9b9852b7 |
| neutron-05 | FIXED — migration name slugs from a conservative charset, path-like names rejected | 9b9852b7 |
| neutron-06 | FIXED — numeric version ordering (files, status, rollback frontier) | 9b9852b7 |
| neutron-07 | FIXED — status is the union of DB records and local files, missing flagged | 9b9852b7 |
| neutron-08 | FIXED — ZIP extraction for Windows assets + tar.gz/zip fixtures | 5e126cf1 |
| neutron-10 | FIXED — SemVer 2.0.0 precedence; prerelease offered only to prerelease installs; build metadata ignored | 5e126cf1 |
| neutron-11 | FIXED — context-aware bounded downloads, byte caps, status checks, fsynced temps | 5e126cf1 |
| neutron-12 | FIXED — context-aware SMTP dial/deadlines/cancellation-driven close | b9d10536 |
| neutron-13 | FIXED — addresses via net/mail.Address.String + control-char rejection | b9d10536 |
| neutron-16 | FIXED — bearer attached only to explicitly allowed HTTPS origins (all redirect hops) | e4e2c8fc |
| neutron-18 | FIXED — Vary parsed fully; `*` and unrepresented fields never cached | 52e714b4 |
| neutron-19 | FIXED (default contract) — request no-cache bypasses read; response no-cache/max-age=0 not stored; max-age/s-maxage bound the stored TTL. The separately-proposed named opt-in override was deliberately NOT added: responses without freshness metadata already take the middleware TTL, and no route asked for stronger-than-origin behavior | 52e714b4 |
| neutron-20 | FIXED — Unwrap/Flush/Hijack forwarded; flushed/hijacked responses non-cacheable and uncaptured | 52e714b4 |
| neutron-22 | FIXED — exact mount root normalized to the "/" subrequest (one namespace per mount) | 70241b91 |
| neutron-23 | FIXED — problem+json rewrite only for genuine unmatched/method-mismatch outcomes (mux.Handler pattern check); application 404/405 pass through | 70241b91 |
| neutron-14 | FIXED — pathname-index TTL only ever extended (never shortened below a live member) | 14dceb36 |
| neutron-15 | FIXED — invalidation claims the index via atomic RENAME; concurrent writers stay indexed | 14dceb36 |
| nucleus-residual-01 | FIXED — execute_parsed/execute_prepared route through execute_statements_dispatch | 456e4526 |

Out-of-repo note: Lullmail's vendored copies of the send.go / bearer-transport
blobs (flagged in neutron-12/13/16 as affected consumers) are NOT fixed here —
that is a separate repo and needs its own sync.
