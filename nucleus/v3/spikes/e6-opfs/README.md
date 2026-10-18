# E6 OPFS spike (card X-6): persistence, multi-tab, durability after a tab kill

Measures the browser half of decision **N1** (PLAN-v3 §4 E6, backing the
`OpfsEnv` design of §3.2): a wasm32 crate persists **pages and a WAL**
through OPFS **sync access handles in a dedicated Worker**, survives a
**tab kill after a flush**, and coordinates **tabs through Web Locks**.

Engines tested: **Chromium headless shell 156, Firefox 157, WebKit 27.2**
(Playwright 1.64.1). Safari proper runs on the owner's Mac, separately.

> **Rework round (verdict X-6, 2026-10-18).** The first round's "browser
> SIGKILL" was a silent no-op (`launchPersistentContext` exposes no
> browser PID, so Playwright's `process()` was `null` and the kill
> degraded to a graceful close), and the WAL was checked by length only.
> Fixed per the verdict: kills are now **real SIGKILLs by PID** (found
> via a `/proc` cmdline scan on the run's unique profile directory,
> present in browser argv on all three engines), every kill records
> `{ pids, signaled, remaining, killed }` in `results.json`, a kill
> scenario that killed nothing is an **error**, and the WAL is verified
> **byte-for-byte** against the expected records in T1/T2/T4. All numbers
> below are from the reworked harness (full run, 2026-10-18).

## Layout

| Path | What it is |
|---|---|
| `src/lib.rs` | The spike crate: `open`/`write_page`/`flush`/`read_page`/`append_wal`/`wal_len`/`read_wal` exports, the page layout and the page-integrity oracle (unit-tested on the host). |
| `harness/worker.js` | Worker that loads the wasm, performs the unavoidably-async OPFS handle acquisition, then owns all sync I/O. |
| `harness/index.html` | Page bridge: per-database workers plus the Web Lock session API. |
| `harness/run.mjs` | Playwright driver: serves `harness/` on `127.0.0.1`, runs T1–T5 per engine under hard timeouts, SIGKILLs browsers by PID (on kill, on timeout, and at exit), writes `harness/results.json`. |
| `harness/pkg/`, `harness/results.json` | Generated (gitignored): wasm-bindgen glue and the last run's raw numbers. |

## Building and running

```bash
# 1. Build the crate (acceptance command 1)
cd nucleus/v3 && cargo build -q --release --target wasm32-unknown-unknown -p e6-opfs

# 2. Run all engines, quick datasets (acceptance command 2; ~1.5 min)
cd nucleus/v3/spikes/e6-opfs/harness
PLAYWRIGHT_BROWSERS_PATH=$HOME/.cache/nv3-target/playwright/browsers \
  timeout 900 node run.mjs --quick

# Full datasets (the numbers below): same command without --quick.
```

`run.mjs` regenerates `harness/pkg/` with `~/.cargo/bin/wasm-bindgen
--target web` whenever the `.wasm` is newer than the glue, starts
`python3 -m http.server 0 --bind 127.0.0.1`, and always stops it (also on
failure and Ctrl-C). Every scenario runs under a hard timeout
(`--step-timeout-ms`, default 120 s quick / 300 s full); on timeout the
browser is SIGKILLed by the same PID scan and the scenario is recorded as
`status: "timeout"`. Exit code 0 means every engine produced a record for
every test — a browser *failing* a durability test is an experiment
result, not a harness error.

**How a kill works** (verdict X-6 R1): `context.browser()?.process?.()`
is `null` for `launchPersistentContext` in all three engines, so the
harness finds the browser itself — it scans `/proc/*/cmdline` for the
run's unique profile directory, which every engine puts in the browser's
argv (Chromium `--user-data-dir=`, Firefox `-profile`, WebKit's
`pw_run.sh` wrapper and MiniBrowser both), SIGKILLs every match, and
waits for the processes to leave `/proc` before recording
`killed: true`. A kill scenario where nothing was killed is recorded as
an error, never as a pass.

## API note (judgment call)

The card asks for `open(name)`. OPFS handle *acquisition*
(`navigator.storage.getDirectory()`, `createSyncAccessHandle()`) is
promise-based, and the card forbids dependencies beyond
wasm-bindgen/js-sys/web-sys — so there is no `wasm-bindgen-futures` to
await from sync Rust. The Rust export is therefore

```rust
open(name, pages_handle, wal_handle)
```

with `worker.js` doing the async boot and handing over the ready
`FileSystemSyncAccessHandle`s. Everything after that — page writes,
reads, WAL appends, flushes — is synchronous Rust on the worker thread.
This "JS boot, Rust I/O" split is the candidate shape for `OpfsEnv`
(PLAN-v3 §3.2). Three further exports beyond the card's list put the
write path and the oracles in the same code: `page_pattern(n)` /
`check_page(n, bytes)` (a torn, short, zero-filled or wrong-offset page
is reported with its first bad byte; unit-tested in `src/lib.rs`) and
`read_wal()` (the log's bytes, so WAL *content* is verified after a
reopen, never length alone — verdict X-6 R3).

Page layout: page `n` is 8 KiB (format default, PLAN-v3 §3.3) at byte
offset `n * 8192`; the WAL is append-only, its frontier seeded from the
file size at open.

Known caveat deferred to D-5b (per the verdict, non-blocking): a failed
`WalCursor::append` advances the frontier before the write is known to
have landed, so a failed append leaves the cursor past a hole. Tolerable
in the spike; the production `OpfsEnv` must not do this.

## Results (full run, 2026-10-18, x64 linux, 16 cpus, Node v24.22.0)

Command: `node run.mjs` (no `--quick`), datasets: T1 1000 pages, T2/T3/T4
300 pages, T5 200 samples. Raw JSON: `harness/results.json` — note any
later run (including the acceptance `--quick`) overwrites it; the table
below is the committed record of the full run.

| | T1 persist 1000 pages + reload | T2 tab kill | T2 browser SIGKILL (real PIDs) | T3 unflushed, then SIGKILL (real PIDs) | T4 second tab waits | WAL content | flush p50/p99 | 8 KiB write p50/p99 |
|---|---|---|---|---|---|---|---|---|
| **Chromium** (headless shell 156) | pass | ok | ok, killed 1 pid | 300/300 pages + WAL record, killed 1 pid | yes (432 ms), granted +1 ms after release | exact (T1 525 B, T2 21 B, T4 34 B) | 0.3 / 1.1 ms | 0.0 / 0.2 ms |
| **Firefox** (157) | pass | ok | ok, killed 1 pid | 300/300 pages + WAL record, killed 1 pid | yes (453 ms), granted +1 ms after release | exact | 0.0 / 2 ms | 0.0 / 1 ms |
| **WebKit** (27.2) | pass | ok | ok, killed 2 pids | 300/300 pages + WAL record, killed 2 pids | yes (423 ms), granted +1 ms after release | exact | 0.0 / 1 ms | 0.0 / 1 ms |

Details worth keeping:

* **T2**: data written and `flush()`ed survives both `page.close({
  runBeforeUnload: false})` with sync handles still open, and a **real
  SIGKILL** of the browser processes (Chromium 1 pid, Firefox 1 pid,
  WebKit 2 pids — its `pw_run.sh` wrapper and MiniBrowser; each kill is
  recorded as `{ pids, signaled, remaining: [], killed: true }`) with the
  browser relaunched on the same profile — 0 pages missing, 0 corrupt,
  and the WAL byte-exact (21 B record) in all three engines.
* **T3**: with **no flush before a real SIGKILL**, all 300 pages and the
  20 B WAL record survived in all three engines (content verified
  byte-for-byte). Best-effort data surviving process death is expected —
  see the durability section; it is *not* evidence that flush can be
  skipped.
* **T4**: while tab A held the sync handles, tab B's open was rejected by
  every engine — Chromium ("Access Handles cannot be created if there is
  another open Access Handle…"), Firefox ("No modification allowed"),
  WebKit ("The object is in an invalid state"). With the Web Lock held by
  A, B's `hold_lock` did not settle within the 400 ms probe (waited
  423–453 ms); it was granted 0–1 ms after A released. B then read A's
  flushed half intact (0 missing / 0 corrupt of 150), wrote its own half,
  and a fresh tab verified all 300 pages and the **byte-exact** WAL
  (34 B: A's record then B's).
* **T5** measures only the wasm→browser call (`performance.now()` in the
  worker, pattern generation excluded). Chromium's flush p50 of ~0.3 ms
  matches this machine's fsync floor; Firefox/WebKit flushes and all
  sub-ms writes round to 0.0 at ms resolution — treat them as
  "sub-millisecond", not as literal zeros. p99 is jitter-prone (a
  previous full run put Chromium's flush p99 at 8.4 ms on a single
  outlier); p50, not p99, is the stable comparison point at this sample
  size.

## Oracles and mutation evidence

Re-run against the reworked harness (quick datasets, all engines):

| Mutant | Caught by |
|---|---|
| `append_wal` writes zeros of the right length | **T1/T2/T4 fail** via the WAL byte-for-byte check (`first_bad: 0`, right length) — UNCAUGHT before R3 |
| `write_page` writes page `n` at page `n+1`'s offset | **T1/T2/T4 fail** (page-integrity oracle; WAL still exact, so the two oracles are independent) |
| Web Lock name randomised per tab | **T4 fails** (tab B's lock settles in ~3 ms; it never waits) |
| `pattern_bad_index` accepts anything | **4 unit tests** (`pattern_detects_*`) |
| `flush()` is a no-op | **UNCAUGHT — inherent, and honest**: a kill is process death, and the OS page cache survives process death, so this harness cannot demonstrate that flush is load-bearing. Recorded here rather than hidden; see the decision section. |

## Per-browser durability semantics (documented per the decision rule)

* **What a kill means.** A tab kill or browser SIGKILL is *process death*,
  not power loss: the OS page cache survives the process. T3 confirms
  that even **unflushed** writes survived a real SIGKILL in all three
  engines (300/300 pages + the unflushed WAL record, content verified).
  `flush()` is the barrier that matters against OS crash or power loss,
  which a Playwright harness cannot simulate; the flush-time numbers
  above (Chromium p50 ≈ 0.3 ms) are the cost of that barrier.
* **Chromium / Firefox / WebKit agree** on the semantics this spike needs:
  one sync access handle per file per origin (contended opens reject),
  flushed writes durable across tab and browser death, `get_size` at
  reopen reflects exactly what survived, and Web Locks hand off across
  tabs within ~1 ms.
* **WebKit caveat (from the card's environment smoke test, 2026-10-17)**:
  main-thread `createWritable()` failed with "UnknownError: operation
  failed for an unknown transient reason" and the browser hung on close.
  This spike uses the worker `createSyncAccessHandle` path exclusively —
  which worked for every test, including persistence across a real
  WebKit process kill (2 pids SIGKILLed, relaunched on the same profile)
  — and every WebKit step ran under a hard timeout with a PID-based
  SIGKILL on overrun. `OpfsEnv` must stay worker-only on WebKit.
* **Safari proper** runs on the owner's Mac, separately (card X-6
  environment note); WebKit 27.2 is the proxy measured here.

## Decision rule (PLAN-v3 §4 E6)

> Data that was committed and flushed survives a tab kill in all three
> browsers; per-browser durability is documented.

**PASS.** Committed-and-flushed data survived both the tab kill and a
real, PID-verified browser SIGKILL in Chromium, Firefox and WebKit
(T1, T2; WAL byte-exact); per-browser durability semantics are documented
above. No finding re-opens N1's browser claim.

**What PASS does and does not show** (verdict X-6 R4): every kill in
this harness is *process death*, which the OS page cache survives — so
PASS shows that committed-and-flushed data survives the death of the tab
and of the whole browser process, **not** that `flush()` is
load-bearing. A no-op-`flush` build of this exact harness passes every
test (see the mutation table); flush's guarantee only becomes observable
under OS crash or power loss, which no browser harness can simulate.
`OpfsEnv` must keep flush-before-commit-ack as the durability barrier
regardless of T3's benign outcome, and Safari proper remains untested
here (owner's Mac).
