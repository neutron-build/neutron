# Neutron — Adoption Findings

The single log of problems found by **building real products on Neutron**,
rather than by reading its docs or its tests. One file, one numbering scheme.

Add to this whenever building on Neutron makes you work around something. An
entry is worth writing even if you worked around it — the workaround is the
evidence. Every entry is verified against installed code and carries a
reproduction; claims that could not be verified say so.

Sources so far: the Omni Analyst marketing-site port (2026-07-25/26, a 20-route
Astro site with a 28-post blog moved onto Neutron TypeScript), Omni Analyst v2
(the Python + Preact dogfood), the mail connector, and Teploy Observe.

## How this file is numbered

Findings are **`A-###`** (adoption). Engine and roadmap work in
`_internal/OPEN_WORK.md` is **`N#`**. These were previously both "N", so `N10`
(60 unchecked `DATABASE_COMPLETION` items) and `N-010` (Migrator rollback) were
unrelated things one hyphen apart. Old ids appear in commit messages and branch
names; they map straight across:

`N-001…N-011` → `A-001…A-011`. Entries promoted from the former
`docs/NEUTRON_GAPS.md` (merged into this file 2026-08-08) are `A-012` onward.

**Adjudication:**

| Tag | Means |
|---|---|
| `REAL-BUG` | Neutron misbehaves |
| `SPEC-GAP` | No supported way to do an ordinary thing |
| `DOC-GAP` | Docs or templates disagree with shipped code |

A fourth category — reporter error — is deliberately excluded. Several candidate
"bugs" turned out to be correct behaviour on inspection and were dropped rather
than filed; they are listed under "Not filed" so they are not re-raised.

**Status key:** `OPEN` · `PART` (partially landed) · `FIXED` · `DEFERRED`
(deliberate, with a reason) · `WONTFIX`.

When you close something, update the status **here** and in
`_internal/OPEN_WORK.md` §A. This file previously carried no statuses at all,
so `A-010` still read as open and still told adopters to work around a
capability that had already shipped.

---

## Index

Statuses verified against source on 2026-08-08.

| # | Finding | Area | Sev | Tag | Status |
|---|---|---|---|---|---|
| A-001 | Python `TestClient` is async-only; no sync facade | Python | HIGH | `SPEC-GAP` | FIXED `c9af596` |
| A-002 | Job queue is in-process only; its persistence does nothing | Python | HIGH | `REAL-BUG` | FIXED `d73d649` |
| A-003 | `entry.html` always `""` for markdown; docs + template still use it | TS content | HIGH | `DOC-GAP` | FIXED `4d9b0b0` |
| A-004 | No 404 / not-found convention | TS routing | MED | `SPEC-GAP` | FIXED `68df2c6` |
| A-005 | Docs import paths do not match the published package | TS docs | LOW | `DOC-GAP` | FIXED `4d9b0b0` |
| A-006 | No public way to get rendered markdown as a string | TS content | MED | `SPEC-GAP` | FIXED `4d9b0b0` |
| A-007 | react-compat is the first adopter question and the answer is a hedge | TS docs | HIGH | `DOC-GAP` | **OPEN** |
| A-008 | `/health` reports `disconnected` for a healthy Postgres | Python | MED | `REAL-BUG` | FIXED `a5f66d5` |
| A-009 | Editable install exposes a top-level `tests` package | Python | LOW | `REAL-BUG` | **DEFERRED** |
| A-010 | `Migrator` parses `-- DOWN` but can never run it | Python | MED | `SPEC-GAP` | FIXED `a7a7d86` |
| A-011 | `useLocation` silently returns `/` during SSR | TS routing | HIGH | `REAL-BUG` | **OPEN** |
| A-012 | Rust Nucleus client had no table-attached FTS | Rust SDK | MED | `SPEC-GAP` | FIXED |
| A-013 | `neutron-oauth` could not redeem a refresh token | Rust SDK | HIGH | `REAL-BUG` | FIXED `c06d027` |
| A-014 | English stemmer: singular and plural never match | Nucleus FTS | HIGH | `REAL-BUG` | **OPEN** |
| A-015 | `nucleus start` has no `--config` flag | Nucleus CLI | LOW | `SPEC-GAP` | **PART** |
| A-016 | Self-referential symlink in the repo root | Repo | LOW | `REAL-BUG` | FIXED |
| A-017 | `target/debug` grows without bound | Repo | LOW | `SPEC-GAP` | **OPEN** |
| A-018 | No SDK client retried a serialization failure (40001) | All SDKs | HIGH | `SPEC-GAP` | FIXED `6fd69e8`, `13d5cfd`, `f6b22b4` |
| A-019 | Client navigation paid a round-trip on a cold click | TS routing | MED | `SPEC-GAP` | FIXED `78411c5` |
| A-020 | Static-route `middleware` never runs when a prebuilt file exists | TS routing | HIGH | `REAL-BUG` | **OPEN** |

Open: A-007, A-011, A-014, A-017, A-020, plus A-015 partial and A-009 deferred.

---

# Open

## A-011 — `useLocation` silently returns `/` during SSR; no `RouterContext` provider on the server
**TypeScript / routing · HIGH · `REAL-BUG` · OPEN**

`useLocation()` reads `RouterContext` (`client/hooks.js:65-68`). That provider is
mounted **only** in the client hydrate path — `client/hydrate.js:226` wraps the
tree in `RouterContext.Provider`. The server renderer (`core/render-app-route.js`)
never mounts it: it composes layouts with
`h(layoutModule.default, { data: loaderData[layoutRoute.id] }, element)` and no
router provider anywhere in the chain.

So on the server every consumer falls through to the `createContext` default,
which is `{ routeId: "", pathname: "/", search: "", params: {} }`
(`client/hooks.js:19-23`). `useLocation().pathname` is therefore **always `"/"`
during SSR**, on every route, with no warning.

The failure is silent and direction-dependent, which is what makes it costly: a
layout that branches on pathname renders the *home-route* branch server-side and
the correct branch after hydration. Reproduction from Omni Analyst v2 — a layout
that renders bare chrome on `/login`:

```tsx
const { pathname } = useLocation();
const isPublic = pathname === "/login";
if (isPublic) return <main>{children}</main>;   // never taken on the server
return <><TopNav/><StatusRail/>{children}</>;   // always taken instead
```

`curl /login` returned the full nine-link application nav plus the status rail,
which then vanished on hydration. The same defect makes `useSearchParams` (same
context, `hooks.js:69-72`) return an empty `URLSearchParams` server-side.

`useParams` is unaffected in practice only because route params are also threaded
through `loaderData`; anything reading them from context alone has the same hole.

Fix: mount `RouterContext.Provider` in `renderAppRoute` around the composed
element, with the values it already has — it computes the matched route, the
pathname and the params to run loaders, so nothing new needs deriving. That makes
the hook isomorphic and removes the class of bug entirely.

Failing that, the hooks should throw (or warn loudly) when read without a
provider, rather than returning a plausible-looking default. A wrong `"/"` is
worse than a crash because it renders successfully.

Workaround for adopters: give the layout a `loader` that returns
`new URL(request.url).pathname` and prefer it over the hook when
`typeof window === "undefined"`. This is what Omni Analyst v2 now does
(`ui/src/routes/_layout.tsx`).

## A-020 — static-route `middleware` does not run when a prebuilt file exists
**TypeScript / routing + static adapter · HIGH · `REAL-BUG` · OPEN**

Found while closing A-019, and not the same thing as that item.

`server/index.ts` serves `staticHtmlCache` for a GET whenever
`isStaticRoute(match)` — and that check (`server/index.ts:911`) is still
`match.route.file.includes("_layout") || match.route.config.mode === "static"`.
It returns at line 587, before `renderAppRoute`, which is the only place route
`middleware` and `globalMiddleware` are invoked.

So a route that declares both:

    export const config = { mode: "static" };
    export const middleware = requireAuth;

is prerendered by SSG (`render-static.ts` only skips `mode !== "static"`) and
then served from the prebuilt file with `requireAuth` never running. The author
gets no error and no warning; the page is simply public.

Options: refuse to prerender a static route that exports `middleware`, refuse
to serve the cached file for one, or reject the combination at build time with
a clear message. The last is probably right — the combination is more likely a
misunderstanding than an intent, and silently downgrading either half is worse
than failing the build.

## A-014 — English stemmer: singular and plural of the same noun never match
**Nucleus / FTS · HIGH · `REAL-BUG` · OPEN**

**Where:** `nucleus/src/fts/mod.rs`, `pub fn stem` (the `-ed`/`-ly`/`-er`
branch is still at line 379 as described).

The stemmer applies its rules as a mutually exclusive if/else chain, and the
`-er` comparative rule fires before the plural rule can be reached for the
singular form:

```
"numbers" -> ends with 's'  -> plural rule  -> "number"
"number"  -> ends with "er" -> -er rule     -> "numb"
```

So the two forms of one noun stem to different terms and never match each
other. Reproduced directly over the wire:

```sql
CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT);
INSERT INTO t VALUES (1, 'Quarterly numbers'), (2, 'the number four');

SELECT id FROM t WHERE body @@ 'numbers';  -- {1}      correct
SELECT id FROM t WHERE body @@ 'number';   -- {2}      misses row 1
SELECT id FROM t WHERE body @@ 'numb';     -- {2}      proves "number" -> "numb"
```

The `-er` rule is meant for comparatives ("faster" -> "fast") but is applied to
every word of five or more characters ending in `-er`, which is an enormous set
of ordinary English nouns: **user, server, folder, order, customer, member,
header, provider, filter, owner, number, partner, manager**. For mail search
alone that breaks folder/folders, order/orders, customer/customers.

The `-ly`, `-ed`, and `-est` rules in the same branch have the same shape and
are worth auditing together (`-ed`: "seed" -> "se"; `-ly`: "reply" -> "rep").

A real Porter/Snowball implementation applies measure conditions (only strip a
suffix when the remaining stem has enough syllables) rather than a bare length
check. Either adopt `rust-stemmers`, or gate each rule on a measure function.

**Worked around** in `mail/store.go` with a characterisation test
(`TestIntegrationSearchMatchesOnWordsNotSubstrings`) that pins current
behaviour and fails loudly once this is fixed.

## A-007 — react-compat is the first question adopters ask, and the answer is a hedge
**TypeScript / docs + positioning · HIGH · `DOC-GAP` · OPEN**

`src/config.ts:4` defines exactly two runtimes:

```ts
export type NeutronRuntime = "preact" | "react-compat";
```

`react-compat` aliases `react`, `react-dom` and the JSX runtimes to Preact
equivalents (`config.ts:137-152`). React is not a dependency of the framework
and no config hook can swap the renderer. That design is defensible.

The gap is that `docs/react-compat.md` answers the highest-stakes adoption
question with *"usually works — verify per package in app context."* That moves
all compatibility risk onto the evaluator, at evaluation time, on their own
codebase — and they are typically holding a Radix-heavy React app when they ask.

CI already runs a dual-runtime lane (`pnpm run ci:runtime-compat`) against
`@neutron/playground`. That proves the **framework** works in both modes. It
proves nothing about the **ecosystem**, which is what is actually being asked.

**Suggested:** extend that existing lane into a published compatibility matrix —
mount the top ~30 React libraries under `preact/compat` and report pass/fail per
package per version. Hard end of the distribution first: Radix primitives
(dialog, select, popover, dropdown-menu, tooltip, tabs), framer-motion, TanStack
Query and Table, react-hook-form, recharts, nivo, react-syntax-highlighter,
react-day-picker, react-force-graph.

That turns the weakest page in the docs into a differentiator — nobody publishes
this — at a fraction of the cost of a second runtime. See the roadmap note below
for why a second runtime is not recommended.

## A-017 — `target/debug` grows without bound
**Repo hygiene · LOW · `SPEC-GAP` · OPEN**

`nucleus/target/debug` had reached **60 GB** — every profile, every dependency,
incremental artifacts, and a separate test binary per `probe_*`, `fuzz`,
`bench`, `compete`, and `stress` target. Nothing is wrong with it as such, but
nothing prunes it either, and it silently became the largest thing on the disk.

Worth either a documented `cargo clean` cadence or a CI job that reports it.
Both `nucleus/target` and `rust/target` were cleared manually on 2026-08-07
(7 GB), which is the second time this has been handled by hand.

## A-015 — `nucleus start` has no `--config` flag
**Nucleus / CLI · LOW · `SPEC-GAP` · PART**

`src/config/mod.rs` has `Config::load(path)` and `from_toml`, and the config
struct covers real knobs — `disk_readonly_free_pct`, `disk_min_free_mb`,
`buffer_pool_size_mb`. The original finding was that `nucleus start` exposed
none of it and never read a config file, so the only way to change a documented
setting was to edit source and rebuild.

**How this was hit:** the dev machine's disk fell below the 3% watermark and
Nucleus correctly went read-only. The documented fix is "raise
`storage.disk_readonly_free_pct`", and there was no way to do that. Worked
around with `--memory`, which meant the store integration suite validated the
executor but never the disk storage path.

**Partially resolved.** `main.rs:512` now loads `<data-dir>/nucleus.toml` at
startup and `NucleusConfig::load` overlays `NUCLEUS_*` env vars, so a documented
setting is reachable without a rebuild. What remains is the flag itself: there
is no `--config <path>`, so the file must live at that one derived location and
one server cannot be pointed at an arbitrary config. Small, and only matters for
automation that wants config outside the data directory.

## A-009 — the editable install exposes a top-level `tests` package
**Python · LOW · `REAL-BUG` · DEFERRED (deliberate, see below)**

Installing the Python tier from a path checkout —

```toml
[tool.uv.sources]
neutron-py = { path = "../Neutron/python", editable = true }
```

— puts Neutron's own `python/tests/` on `sys.path` as a top-level `tests`
module. A consuming application with the conventional `tests/` directory then
finds Neutron's:

```
tests/test_skeleton.py:7: in <module>
    from tests.conftest import TEST_DATABASE_URL
E   ImportError: cannot import name 'TEST_DATABASE_URL' from 'tests.conftest'
    (/…/Neutron/python/tests/conftest.py)
```

The app's own `tests/conftest.py` is shadowed. Editable path installs are the
normal setup for anyone dogfooding or contributing, so this is hit early.

Cause — and it is *not* the wheel's package set, which is correctly scoped to
`packages = ["neutron"]`. Hatchling's default editable mode writes a `.pth`
containing the **project root**:

```
$ cat .venv/lib/python3.12/site-packages/_editable_impl_neutron_py.pth
/Users/…/Neutron/python

$ python -c "import tests; print(tests.__file__)"
/Users/…/Neutron/python/tests/__init__.py
```

So every top-level directory next to `neutron/` — currently `tests/`, and
anything added later — becomes importable in the consuming app. `packages`
governs the built wheel; it does not constrain the editable path entry.

**Deferred, not forgotten.** The obvious fix — `dev-mode-exact = true` under
`[tool.hatch.build.targets.wheel]` — emits an `editables` redirector that the
consuming venv must have installed, and without it `neutron` stops importing
altogether. That is a worse failure than the shadowing, so it was left as-is
rather than half-applied; the reasoning is recorded in `python/pyproject.toml`
next to the setting. Closing it needs either `editables` declared as a runtime
dependency or `tests/` moved out of the project root.

Workaround for adopters: don't import across your own `tests` package; use
fixtures instead.

---

# Fixed

## A-001 — Python `TestClient` is async-only; no sync facade
**Python · HIGH · `SPEC-GAP` · FIXED `c9af596`**

`neutron/test/__init__.py` was an `httpx.AsyncClient` over `ASGITransport`, used
as `async with` + `await client.get(...)`. Starlette and FastAPI's `TestClient`
is synchronous (an `httpx.Client` driven through an anyio portal).

Every FastAPI codebase migrating to Neutron therefore had to convert its entire
test suite to `async def` + `await` + anyio markers. For the application that
surfaced this, that is ~84 test files — roughly doubling the migration surface
for zero product benefit. FastAPI migration is plausibly Neutron Python's
largest inflow, and this taxed all of it.

**Resolved:** `SyncTestClient` ships alongside `TestClient` — an `httpx.Client`
driving the same `ASGITransport` through an anyio blocking portal, which the
client owns and tears down.

## A-002 — the job queue is in-process only, and its persistence does nothing
**Python · HIGH · `REAL-BUG` · FIXED `d73d649`**

Upgraded from MEDIUM/`needs verification` on 2026-07-27 after a full read of
`neutron/jobs/queue.py` (342 lines). The original entry was a grep; every
suspicion it raised was confirmed, and the durability problem was worse than
suspected.

| Line | Finding |
|---|---|
| 81 | `self._queue: asyncio.Queue[str]` — the work queue is **in-process**. Two workers in two processes hold two independent queues. No `SKIP LOCKED`, no claim, no lease, no visibility timeout. |
| 105–133 | `_persist_job` writes `_neutron_jobs`, but **nothing ever reads the table back**. There is no recovery path. |
| 58–62 | The class docstring says passing a `db` makes jobs "persist across restarts". Given the above this is false: after a restart `self._jobs` and `self._queue` are empty and every pending row is orphaned permanently. |
| 100–103, 132–133 | `_ensure_db` and `_persist_job` both end in `except Exception: pass`. A persistence outage is silent — the queue reports healthy while writing nothing. |
| 250–258 | `_scheduler_loop` fires on a `time.localtime()` match then `sleep(60)`, with no leader election. Two instances double-fire every cron entry, and a slow iteration can skip a minute entirely. |
| 213–216 | A job whose `scheduled_at` is in the future is re-`put` on the queue followed by `sleep(0.1)`. A backlog of delayed jobs busy-spins every worker. |

**Why it mattered.** This was the one finding that blocked the Omni Analyst v2
rebuild. That system's scheduler dispatches agents against a ranked queue of
coverage gaps — it *is* a distributed work queue, replacing a Celery + Redis
deployment. Adopting Neutron's queue as it stood was a durability regression,
and the docstring meant an adopter would not know until they lost a restart's
worth of work.

**Resolved:** `_neutron_jobs` gained `lease_owner` / `lease_expires_at`; claims
go through `FOR UPDATE SKIP LOCKED` so N workers claim concurrently; a crashed
worker is detected by lease expiry rather than a heartbeat table, so a hard kill
recovers; and the scheduler elects a single firer through a Postgres advisory
lock. A follow-up (`d431c09`) fixed a dead worker stranding its job and a deploy
re-running it.

## A-003 — `entry.html` is always `""` for markdown; docs and the docs template still use it
**TypeScript / content collections · HIGH · `DOC-GAP` (migration debt) · FIXED `4d9b0b0`**

Not a coding error. Markdown rendering is lazy *by design* — the OOM fix merged
as `fix/content-lazy-render-oom` deliberately stopped memoizing rendered markup
onto long-lived cached entries, because a static build would otherwise
accumulate an entire collection's HTML in memory. That change is correct.

What did not follow it: `src/content/index.ts` set `html: ""` and exposed real
markup only via the non-enumerable `render()`, but

1. `typescript/docs/content-collections.md:46` still listed
   `html` — *"rendered HTML for markdown/MDX files"*.
2. `packages/create-neutron/templates/docs/src/routes/docs/[...slug].tsx` still
   used it **twice** — `extractToc(entry.html)` and `html: entry.html`.

So the shipped docs starter rendered empty document bodies and an empty table of
contents. Anyone running `npm create neutron` with the docs template hit this
immediately.

**Reproduction:**

```
slug: alternative-data | sourceType: markdown
body len: 15262 | html len: 0
```

Downstream, a site built "successfully" — 45 pages, zero errors — as 28 posts of
`<div class="post-body"></div>`. The failure was silent: no warning, no error,
build reports success.

**Resolved:** the lazy behaviour stays. The docs template now uses the render
path and carries an inline comment stating `entry.html` is always `""` for
markdown, so the next person does not re-introduce it.

## A-006 — no public way to obtain rendered markdown as a string
**TypeScript / content collections · MEDIUM · `SPEC-GAP` · FIXED `4d9b0b0`**

Followed from A-003. `render()` returns `{ Content }`, a Preact component. Loader
data must be serializable, so a static route could not pass `Content` through to
its component. Nothing public returned the string — the only direct access was the
private, non-enumerable `__lazyMarkup`, forcing this in every static content
route:

```ts
const { Content } = await entry.render();
const html = renderToString(h(Content, {}));
```

A full `preact-render-to-string` round-trip to recover a string the renderer
already produced.

**Resolved:** `renderEntry()` is exported from `content/index.ts:310`, so lazy
rendering stays intact without a component round-trip.

Second-order, still true and worth knowing: `Content` wraps output in its own
`<div>`, so consumer CSS cannot use direct-child selectors — `.post-body > * + *`
silently matches nothing.

## A-004 — no 404 / not-found convention
**TypeScript / routing + static adapter · MEDIUM · `SPEC-GAP` · FIXED `68df2c6`**

There was no documented convention for a not-found page, neither shipped template
included one (`templates/marketing`, `templates/docs`), and the static adapter
emitted no `404.html` — `dist/` after `build --preset static` contained only
`index.html` and per-route directories, and `dist/.neutron-static-policy.json`
had no 404 entry. A static site on any host that serves `404.html` had no
supported way to provide one.

**Resolved:** `not-found.tsx`, deepest-wins per directory, rendered through the
layout chain.

## A-005 — docs import paths do not match the published package
**TypeScript / docs · LOW · `DOC-GAP` · FIXED `4d9b0b0`**

`docs/content-collections.md` showed `from "neutron/content"` and
`docs/react-compat.md` showed `from "neutron"`. The published package is
`@neutron-build/core`; `templates/docs/src/content/config.ts` correctly used
`@neutron-build/core/content`. Copy-pasting from the docs failed to resolve.

## A-008 — `/health` reports `nucleus: "disconnected"` for a healthy Postgres connection
**Python · MEDIUM · `REAL-BUG` · FIXED `a5f66d5`**

`neutron/app.py` computed the health payload's `nucleus` field from feature
detection:

```python
if db is None:                                  nucleus = "unconfigured"
elif getattr(getattr(db, "features", None), "is_nucleus", False):
                                                nucleus = "connected"
else:                                           nucleus = "disconnected"
```

Running against plain PostgreSQL — an explicitly supported mode, since the
pitch is that Nucleus speaks the Postgres wire protocol so "any Postgres client
connects" — a fully healthy connection reported `"disconnected"`. Observed on
`timescale/timescaledb:2.17.2-pg17`.

The field conflated "this backend is not Nucleus" with "the database is
unreachable". Any monitor keyed on it pages for a healthy system, and the one
genuine outage case was indistinguishable from the normal case. It also left
`"disconnected"` unreachable in practice, since nothing ever probed the
connection.

**Resolved:** the field now reflects the *health* of the dependency —
`_dependency_reachable(db)` is awaited, an unreachable database reports
`disconnected` **and** downgrades `status` to `degraded`. Feature detection
(Nucleus versus plain Postgres) is `FRAMEWORK_CONTRACT` §1 and deliberately not
this endpoint.

## A-010 — `Migrator` parses `-- DOWN` sections but can never run them
**Python · MED · `SPEC-GAP` · FIXED `a7a7d86`**

`neutron.nucleus.migrate.Migrator` loaded each `NNN_name.sql` file and split it
on `-- DOWN` into `Migration.up` and `Migration.down` (`migrate.py:88-94`), and
the docstring on `migrate()` even said *"Optionally include a `-- DOWN` marker
to separate up/down SQL."* But there was **no `rollback` / `downgrade` method on
`Migrator`** — only `migrate()` and `run_migrations()`, both running `up`
exclusively. The `Migration.down` field was dead.

Observed in Omni Analyst v2 (the dogfood): ~10 of 29 migration files carried a
populated `-- DOWN` section (`DROP TABLE ...`, `DROP INDEX ...`), written in good
faith because the loader's split invites them. They looked operational and were
not. An adopter who rolled a migration forward expecting the documented down path
had no rollback — only `pg_restore` from a backup.

Filed `SPEC-GAP` rather than `REAL-BUG` because nothing *broke*; the gap was that
an advertised capability (the `-- DOWN` marker, named in the public docstring)
had no supporting API.

**Resolved (2026-08-06):** `Migrator.rollback(migrations, target_version)` and
`rollback_dir(dir, target_version)` landed in `nucleus/migrate.py`. They walk
applied migrations above `target_version` newest-first, running each `down` and
deleting its `_neutron_migrations` row in one transaction (schema and record can
never disagree). A migration with an empty `down` raises `ValueError` rather
than skip — skipping would leave its changes present while later rows are
removed. The `-- DOWN` sections across both repos are now operational.

**Follow-up still open:** a CLI surface (`neutron migrate:down`) is deferred —
the CLI's `migrate` command uses `NucleusClient.migrate`, a separate path.

## A-012 — Rust Nucleus client had no table-attached FTS
**Rust SDK · MEDIUM · `SPEC-GAP` · FIXED**

**Where:** `rust/crates/neutron-nucleus/src/models/fts.rs`

The client only spoke the doc-id sidecar API (`FTS_INDEX`, `FTS_SEARCH`), which
returns `(doc_id, score)` pairs rather than rows — not joinable, not
filterable, not covered by row-level security. The table-attached index shipped
in `1bb99cc` and the client never caught up.

**Resolved:** added `create_index`, `drop_index`, `matches` (`@@`), and `bm25`,
which return real primary keys from real tables. The document-store methods stay
for corpora with no table behind them and for fuzzy search, which the
table-attached index does not yet expose; the module doc now says which to
reach for.

Table and column names interpolate into DDL, where bind parameters are not
allowed, so identifiers are validated and rejected rather than quoted — quoting
a hostile identifier still lets it terminate the quote. Tested against the
obvious escapes.

## A-013 — `neutron-oauth` could not redeem a refresh token
**Rust SDK · HIGH · `REAL-BUG` · FIXED `c06d027`**

**Where:** `rust/crates/neutron-oauth/src/token.rs`
**Hit while:** building the mail connector, which needs long-lived provider access.
**Cost:** would have made every OAuth integration die at the first token expiry.

`TokenResponse` parsed `refresh_token` but nothing could redeem one — there was
only `exchange_code`.

**Resolved:** added `refresh_access_token`, which also carries the existing token
forward when a provider omits it on refresh (Google issues one only on first
consent, so a naive implementation drops the credential), and maps
`invalid_grant` to a distinct `RefreshRejected` error rather than a confusing
missing-field parse failure.

## A-018 — no SDK client retried a serialization failure (40001)
**All SDKs + contract · HIGH · `SPEC-GAP` · FIXED `6fd69e8`, `13d5cfd`, `f6b22b4`**

Found 2026-07-30. A serializable transaction can now fail with **SQLSTATE 40001**
on the shipping engine — that was new. Before R6 the disk engine refused
`SERIALIZABLE` outright, so 40001 only ever came from the in-memory MVCC engine
and effectively never reached an application. Strict 2PL with wait-die means a
younger transaction is killed on conflict, and `lock_timeout` adds **55P03
`lock_not_available`** as a second new error class.

Grepping every SDK (`go/`, `python/`, `typescript/`, `rust/`, `elixir/`, `zig/`,
`julia/`) for `40001` returned **zero hits**. No client had retry-on-
serialization-failure logic, and `FRAMEWORK_CONTRACT.md` did not mention
isolation levels at all. Every SDK's transaction helper surfaced a retryable
conflict to application code as a hard error.

**Resolved across all seven SDKs.** Each ships `isSerializationFailure` /
`isLockNotAvailable` classification (by SQLSTATE, never by message text) plus a
retry helper with jittered exponential backoff — `go/nucleus/retry.go`,
`python/neutron/nucleus/retry.py`,
`typescript/packages/neutron-nucleus/src/retry.ts`,
`rust/crates/neutron-nucleus/src/retry.rs`, `elixir/lib/nucleus/retry.ex`,
`zig/src/nucleus/retry.zig`, `julia/src/retry.jl`.

Go needed the most: it had `Begin`/`Commit`/`Rollback` but no managed helper at
all, so there was nowhere for retry to live. `client.WithTx(ctx, opts, fn)` also
guarantees rollback on panic, which matters more than usual here because an
abandoned exclusive lock blocks every other serializable transaction on that
table until the session drops.

All treat **55P03 as non-retryable** — the holder is still there, so retrying
spins against a lock that is not moving — and each has a test asserting a lock
timeout is attempted exactly once. Backoff is full-jitter because under wait-die
the younger transaction loses every round, so a fixed backoff can starve it
indefinitely.

`FRAMEWORK_CONTRACT.md` §"Isolation levels" now specifies the retry contract:
re-run the **entire** transaction body on `40001`/`25P02` with bounded attempts,
and that `SERIALIZABLE` on Nucleus's disk engine is table-level 2PL, so a hot
table serializes (see `nucleus/docs/MODEL_SEMANTICS.md`).

## A-019 — client navigation paid a round-trip on a cold click
**TypeScript / routing · MEDIUM · `SPEC-GAP` · FIXED `78411c5`**

`d76888e` first fixed the prefetch subsystem, which never ran, wrote to a cache
nothing read, served post-mutation data as a prefetch forever, and had a
cache-hit path that a stale in-flight fetch could overwrite. With prefetch
working, a link the user hovers or scrolls past is warm before the click.

What remained: a *cold* click — a link never in the viewport, never hovered,
chosen straight from the keyboard, or clicked within 65ms — still fetched, including
for a `mode: "static"` route with no loader whose component was already in the
bundle. On a high-RTT deployment that is 200-400ms of waiting for a payload whose
only useful contents are `__head__` and `__css__`.

**Resolved in a different shape than originally specced**, and the analysis that
preceded it was wrong twice — both corrections are worth keeping:

1. **The middleware-bypass objection was wrong.** `globalMiddleware` is never
   registered as app-level middleware — it is only passed into `renderAppRoute` —
   and the static-HTML cache hit at `server/index.ts:586` returns before
   `renderAppRoute` runs. A static route with a prebuilt file is already served
   with no middleware at all, so its content is already public and there was
   nothing for a client shortcut to bypass. (That is A-020, which is a defect in
   its own right.)
2. **Local rendering was not the fast option.** A prerendered document is already
   painted when the click lands; a local render still has to run Preact, apply
   the head and load CSS after it. So the build-time head, the manifest CSS list
   and the stale-head risk were all cost for a slower result.

The fix is therefore to NOT intercept static targets and let the browser
prerender them — needing none of the four build-pipeline changes originally
specced (per-route `hasLoader`/`mode` in the client route table, build-time head
emission, middleware-reachability marking, and the conditional skip).

## A-016 — self-referential symlink in the repo root
**Repo · LOW · `REAL-BUG` · FIXED**

An untracked symlink at the repo root (`Neutron -> /Users/tyler/Documents/Code
Projects/Neutron`) pointed at the repo root, making the tree infinitely deep for
anything that walked it recursively, and showing as `?? Neutron` in every
`git status`. Removed; verified absent 2026-08-08.

---

# Notes that are not findings

Recorded so they are not re-raised as bugs.

**Nucleus's disk watermark did its job.** It refused writes at 2.6% free with a
precise error naming the setting to change and the current values. That is
exactly right behaviour; the only gap was that the setting was unreachable
(A-015).

**Table-attached FTS works, indexed and unindexed.** An earlier draft claimed
`@@` and `BM25()` were broken over pgwire. They are not. The checked-out
`target/release/nucleus` binary was built the day *before* the FTS merge, so
every query was hitting an engine that predated the feature.

```sql
SELECT id FROM t WHERE body @@ 'quarterly';              -- works with no index
CREATE INDEX t_fts ON t USING FTS (body);
SELECT id, BM25(body,'quarterly') FROM t WHERE body @@ 'quarterly';  -- works
```

**The lesson is worth keeping:** a stale `target/release` binary is
indistinguishable from a missing feature, and it cost real time here. Anything
testing engine behaviour should either rebuild first or assert the binary is
newer than the feature commit. `nucleus version` reporting a build timestamp
and git SHA would make this self-evident — arguably the actual gap.

**FTS requires an integer PRIMARY KEY for the index, and that is documented.**
`mail_messages` is keyed on `(account_id, id)` — text, because message identity
comes from provider IDs and Message-ID headers rather than being minted
locally. So it gets `@@` matching but no index and no `BM25` ranking. That is
the documented trade, not a defect; worth revisiting only if mail search
becomes slow enough to justify a surrogate key.

**Lazy markdown rendering itself.** A deliberate, well-commented memory
optimisation. The finding was the stale field and template (A-003), not the
laziness.

**`runtime: "react-compat"` being unsupported for internals-dependent
libraries.** Documented honestly and correctly scoped; the gap is verification
coverage (A-007), not the design.

---

# Roadmap note — a real React runtime is not recommended

Recorded because the question recurs and the reasoning is easy to lose.

**The pool it targets is not unblocked by it.** Developers who want "real React"
are overwhelmingly Next.js users, and their applications depend on RSC, server
actions, app-router conventions, `next/image` and `next/font`. Supporting
React-the-library without those is still not drop-in — Neutron would absorb
React's full maintenance burden and still not be a migration target.

**It dissolves the differentiator.** The benchmark claims (~2.7x Next.js,
~1.7x Astro) rest partly on Preact's ~3KB runtime against React's ~45KB. Running
real React makes Neutron another React meta-framework competing with Next, Remix
and TanStack Start on their terms.

**React is now framework-coupled and moving** — RSC, server actions, the
compiler, `use`. Supporting it means indefinitely tracking whatever React
decides framework integration means.

**And the surface doubles**: two SSR paths, two hydration models, two benchmark
suites, two doc sets.

**Higher-leverage alternative** if the Next.js migration pool becomes a
priority: a codemod (route conventions, `getServerSideProps`/RSC handlers to
loaders, `next/link` to the Neutron equivalent). The barrier there is conversion
effort, not runtime compatibility.

**Suggested positioning:** *Neutron runs Preact. Most React libraries work —
here is the tested list. If you need React internals or RSC, use Next.*
