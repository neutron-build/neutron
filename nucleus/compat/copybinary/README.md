# Binary COPY differential harness

Verifies `COPY ... WITH (FORMAT binary)` against a real PostgreSQL 17 in both
directions with the same seed data (ints, bigint, text with quotes/tabs/
unicode, bool, float8 incl. 1e100, numeric, timestamp, date, bytea, and an
all-NULL row):

1. A PG-produced binary file loads into Nucleus; `SELECT *` output matches
   PG's byte-for-byte.
2. A Nucleus-produced binary file loads into PG; contents match.
3. Nucleus→Nucleus round trip with a named column subset (projection and
   reordering).
4. A truncated stream is rejected loudly (22P04), not partially applied.

```sh
sh run.sh    # needs postgresql@17 on PATH or in the usual Homebrew prefix
```

SKIPs (exit 0) without PostgreSQL. Result 2026-07-23: PASS.

Scope: binary COPY is a wire-protocol feature — the embedded/simple inline
`COPY FROM` path rejects `FORMAT binary` explicitly. Types without a binary
encoding (vector, interval, arrays) fail loudly rather than corrupting.
