# The Zig executor is built and registered

`src/main.zig` implements the full spec vocabulary against the in-repo Zig
client. It is registered in `runner/run.mjs` via `run.sh`, which resolves a
Zig 0.15 toolchain. This file records why it was once unregistrable, because
the toolchain constraint is the kind of thing that comes back.

## History: the blocker (2026-08-16 — 2026-08-19)

The executor was first written while the SDK targeted Zig 0.14 and Zig 0.16
was redesigning the standard library around an explicit `Io` parameter
(`std.net` → `std.Io.net`, `std.io` → `std.Io`, `std.crypto.random` moved,
`std.os.environ`/`std.posix.getenv` → `std.process.Init.environ_map`):

- Zig 0.14 could not link on this machine at all (`undefined symbol:
  __availability_version_check` against the macOS 26 / Xcode 26 SDK).
- Zig 0.16 could not compile the SDK (layers 1–3 used 0.14 spellings
  throughout).
- The executor itself had been written against the 0.16 `std.process.Init`
  proposal, so it built on neither.

So no toolchain could run it, and it was deliberately not registered.

## What unblocked it

The SDK landed on **Zig 0.15.2** (brew keg-only `zig@0.15` — see
`zig/README.md` and `zig/build.zig.zon`): 0.14-era APIs still present
(`std.net`, `std.time.nanoTimestamp`), but builds and links on current macOS.
On 2026-08-19 the executor was ported from the 0.16 proposal APIs to 0.15:

| 0.16 proposal (old) | 0.15 (now) |
|---|---|
| `pub fn main(init: std.process.Init)` | `pub fn main()` + arena |
| `init.environ_map.get(...)` | `std.posix.getenv(...)` |
| `std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(n))` | `std.fs.cwd().readFileAlloc(alloc, path, n)` |
| `std.json.Stringify.value(v, opts, arrayListWriter)` | `std.json.Stringify.valueAlloc(alloc, v, opts)` |

Three client-surface drifts were also reconciled (the executor predated
them): `streams.xack` now takes `(stream, group, id_ms, id_seq)` and the
executor splits the spec's `"<ms>-<seq>"` id; `datalog.clear()` is global in
the Zig client (no predicate argument — see the comment in `main.zig`;
cross-SDK drift on scoped clears will surface in the matrix); `cdc.cdcRead`
takes only the offset (no limit — same drift-matrix story).

## 2026-08-19: first live run, and what it took to go 42/42

The executor was ported to *compile*, and it had never exchanged a byte with
the engine. The first live matrix run failed 19 cases; the worklist below is
what was actually wrong, in causal order. Everything was measured against the
engine (psql) before being changed — the engine was never at fault.

1. **PgClient.execute() desynced the connection** (SDK, layer2). It read ONE
   TCP segment and returned without waiting for ReadyForQuery, so whenever
   the response was split the next query() on that pooled connection decoded
   the stale tail as its own answer — returning the health check's `1` or
   nothing. A probe (execute("SELECT 1") + query(...) ×200) reproduced it
   110/200 times. Fixed: drain to ReadyForQuery, and keep messages that
   straddle read boundaries instead of overwriting them.
2. **NucleusClient.execute() returned a dangling slice** (SDK). PgClient.query
   returns its QueryResult by value, so scalar() pointed into a dead frame;
   ids held across steps turned to garbage. Fixed: results and command tags
   are duped into the client's allocator.
3. **Four engine-surface mistakes in the SDK** (all measured): KV_SET with
   ttl 0 expires the key immediately, so no-TTL sets must use the two-arg
   form; graph node/edge ids are integers (GRAPH_ADD_EDGE rejects strings);
   CDC_READ requires (after, limit); DATALOG_CLEAR/IMPORT_GRAPH require the
   predicate. The old `datalog.clear()` was NOT global — the engine rejects
   the zero-argument call, so the "cross-SDK drift" note above was wrong.
4. **Vector SQL generators could never execute** (SDK): INSERT wrote a
   `vector` column no schema has (every SDK uses `embedding`), and search
   called VECTOR_DISTANCE with the table name and no FROM clause.
5. **Missing runtime surface** (SDK): document countIn/pathIn wrappers and
   the filter-based find/findOne/updateWhere/deleteWhere (DOC_QUERY answers
   ids, not docs — those ops are compositions, as in every other SDK);
   TS_RANGE and a windowed aggregate.

Executor-side: mappings for the ops above, spec args actually passed through
(streams bounds/counts, graph direction), raw-text binding so ids round-trip
as what the engine said, per-run fixture nonces ("unique across runs" — a
case-index-only seed made run two collide with run one's tables), KV
t/f→bool and ZRANGE/HGETALL pair decoding, and vector count/search via the
public SQL builders (this client's QueryResult keeps only each row's first
column, so a k-match search is honestly a list of matched ids).

## Running

```sh
NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres \
    sh run.sh
```

Exit codes: 0 all cases behaved as the spec says, 1 otherwise. Refuses to
run (exit 1, no report) when `NEUTRON_TEST_DATABASE_URL` is unset. Without a
live engine the executor cannot be scored locally; `runner/run.mjs` in CI is
where the zig column is produced.
