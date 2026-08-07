# Neutron — Adoption Findings

Issues found by building a real application on Neutron, rather than by reading
the docs. Sourced from the Omni Analyst marketing-site port (2026-07-25/26),
which moved a 20-route Astro site with a 28-post blog onto Neutron TypeScript.

Every entry is verified against installed code (`@neutron-build/core` 0.1.7) and
carries a reproduction. Claims that could not be verified say so.

**Adjudication:**

| Tag | Means |
|---|---|
| `REAL-BUG` | Neutron misbehaves |
| `SPEC-GAP` | No supported way to do an ordinary thing |
| `DOC-GAP` | Docs or templates disagree with shipped code |

A fourth category — reporter error — is deliberately excluded. Several candidate
"bugs" turned out to be correct behaviour on inspection and were dropped rather
than filed.

---

## N-003 — `entry.html` is always `""` for markdown; docs and the docs template still use it
**TypeScript / content collections · HIGH · `DOC-GAP` (migration debt)**

Not a coding error. Markdown rendering is lazy *by design* — the OOM fix merged
as `fix/content-lazy-render-oom` deliberately stopped memoizing rendered markup
onto long-lived cached entries, because a static build would otherwise
accumulate an entire collection's HTML in memory. That change is correct.

What did not follow it: `src/content/index.ts` sets `html: ""` (lines 597, 696)
and exposes real markup only via the non-enumerable `render()`, but

1. `typescript/docs/content-collections.md:46` still lists
   `html` — *"rendered HTML for markdown/MDX files"*.
2. `packages/create-neutron/templates/docs/src/routes/docs/[...slug].tsx` still
   uses it **twice** — `extractToc(entry.html)` (line 25) and
   `html: entry.html` (line 37).

So the shipped docs starter renders empty document bodies and an empty table of
contents. Anyone running `npm create neutron` with the docs template hits this
immediately.

**Reproduction:**

```
slug: alternative-data | sourceType: markdown
body len: 15262 | html len: 0
```

Downstream, a site built "successfully" — 45 pages, zero errors — as 28 posts of
`<div class="post-body"></div>`. The failure is silent: no warning, no error,
build reports success.

**Fix:** the lazy behaviour should stay. Update the docs to describe `html` as
empty for markdown (or remove the field), and switch the docs template to
`render()`.

## N-006 — no public way to obtain rendered markdown as a string
**TypeScript / content collections · MEDIUM · `SPEC-GAP`**

Follows from N-003. `render()` returns `{ Content }`, a Preact component. Loader
data must be serializable, so a static route cannot pass `Content` through to
its component. Nothing public returns the string — the only direct access is the
private, non-enumerable `__lazyMarkup`.

Workaround currently required in a static content route:

```ts
const { Content } = await entry.render();
const html = renderToString(h(Content, {}));
```

A full `preact-render-to-string` round-trip to recover a string the renderer
already produced.

**Suggested:** export the markup step (e.g. `renderEntry(entry) -> { html }`)
so lazy rendering stays intact without forcing a component round-trip.

Second-order: `Content` wraps output in its own `<div>`, so consumer CSS cannot
use direct-child selectors — `.post-body > * + *` silently matches nothing.
Worth documenting even if the wrapper stays.

## N-007 — react-compat is the first question adopters ask, and the answer is a hedge
**TypeScript / docs + positioning · HIGH · `DOC-GAP`**

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

## N-004 — no 404 / not-found convention
**TypeScript / routing + static adapter · MEDIUM · `SPEC-GAP`**

No documented convention for a not-found page, neither shipped template includes
one (`templates/marketing`, `templates/docs`), and the static adapter emits no
`404.html` — `dist/` after `build --preset static` contains only `index.html`
and per-route directories. `dist/.neutron-static-policy.json` has no 404 entry.

A static site on any host that serves `404.html` has no supported way to provide
one.

## N-005 — docs import paths do not match the published package
**TypeScript / docs · LOW · `DOC-GAP`**

`docs/content-collections.md` shows `from "neutron/content"` and
`docs/react-compat.md` shows `from "neutron"`. The published package is
`@neutron-build/core`; `templates/docs/src/content/config.ts` correctly uses
`@neutron-build/core/content`. Copy-pasting from the docs fails to resolve.

## N-001 — Python `TestClient` is async-only; no sync facade
**Python · HIGH · `SPEC-GAP`**

`neutron/test/__init__.py` is an `httpx.AsyncClient` over `ASGITransport`, used
as `async with` + `await client.get(...)`. Starlette and FastAPI's `TestClient`
is synchronous (an `httpx.Client` driven through an anyio portal).

Every FastAPI codebase migrating to Neutron must therefore convert its entire
test suite to `async def` + `await` + anyio markers. For the application that
surfaced this, that is ~84 test files — roughly doubling the migration surface
for zero product benefit. FastAPI migration is plausibly Neutron Python's
largest inflow, and this taxes all of it.

**Suggested:** a sync `TestClient` facade using an anyio portal, as Starlette
does. Small, and it removes the single largest obstacle to FastAPI migration.

## N-002 — the job queue is in-process only, and its persistence does nothing
**Python · HIGH · `REAL-BUG`**

Upgraded from MEDIUM/`needs verification` on 2026-07-27 after a full read of
`neutron/jobs/queue.py` (342 lines). The original entry was a grep; every
suspicion it raised is confirmed, and the durability problem is worse than
suspected.

| Line | Finding |
|---|---|
| 81 | `self._queue: asyncio.Queue[str]` — the work queue is **in-process**. Two workers in two processes hold two independent queues. No `SKIP LOCKED`, no claim, no lease, no visibility timeout. |
| 105–133 | `_persist_job` writes `_neutron_jobs`, but **nothing ever reads the table back**. There is no recovery path. |
| 58–62 | The class docstring says passing a `db` makes jobs "persist across restarts". Given the above this is false: after a restart `self._jobs` and `self._queue` are empty and every pending row is orphaned permanently. |
| 100–103, 132–133 | `_ensure_db` and `_persist_job` both end in `except Exception: pass`. A persistence outage is silent — the queue reports healthy while writing nothing. |
| 250–258 | `_scheduler_loop` fires on a `time.localtime()` match then `sleep(60)`, with no leader election. Two instances double-fire every cron entry, and a slow iteration can skip a minute entirely. |
| 213–216 | A job whose `scheduled_at` is in the future is re-`put` on the queue followed by `sleep(0.1)`. A backlog of delayed jobs busy-spins every worker. |

`tests/test_jobs.py` (188 lines) has no test for restart recovery, multi-worker
claiming, or durability, so none of this is guarded.

**Why it matters.** This is the one finding that blocks the Omni Analyst v2
rebuild. That system's scheduler dispatches agents against a ranked queue of
coverage gaps — it *is* a distributed work queue, and it is replacing a
Celery + Redis deployment. Adopting Neutron's queue as it stands is a
durability regression, and the docstring means an adopter will not know until
they lose a restart's worth of work.

**Fix sketch.** Postgres claim via `UPDATE … WHERE id IN (SELECT … FOR UPDATE
SKIP LOCKED)`, a lease column with heartbeat and reclaim-on-expiry, read-back
of pending rows on boot, an advisory lock so exactly one scheduler fires cron,
and surfacing persistence errors instead of swallowing them. Until then the
docstring should say in-memory-only.

---

## N-008 — `/health` reports `nucleus: "disconnected"` for a healthy Postgres connection
**Python · MEDIUM · `REAL-BUG`**

`neutron/app.py:145-162` computes the health payload's `nucleus` field:

```python
if db is None:                                  nucleus = "unconfigured"
elif getattr(getattr(db, "features", None), "is_nucleus", False):
                                                nucleus = "connected"
else:                                           nucleus = "disconnected"
```

Running against plain PostgreSQL — an explicitly supported mode, since the
pitch is that Nucleus speaks the Postgres wire protocol so "any Postgres client
connects" — a fully healthy connection reports `"disconnected"`.

Reproduction: point `NucleusClient.connect()` at stock Postgres, boot, and
`GET /health` returns `{"status":"ok","nucleus":"disconnected","version":…}`.
Observed on `timescale/timescaledb:2.17.2-pg17`.

The field conflates "this backend is not Nucleus" with "the database is
unreachable". Any monitor keyed on it pages for a healthy system, and the one
genuine outage case is indistinguishable from the normal case. Three states
are being squeezed into a field that needs four — suggest `"postgres"` (or
`"connected (non-nucleus)"`) for a healthy non-Nucleus backend, leaving
`"disconnected"` to mean unreachable.

---

## N-009 — the editable install exposes a top-level `tests` package
**Python · LOW · `REAL-BUG`**

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

Fix: set `dev-mode-exact = true` under `[tool.hatch.build.targets.wheel]` so the
editable install maps the package directly instead of exposing the source root.
Dropping `tests/__init__.py` would not be sufficient on its own — `tests/` would
remain importable as a namespace package.

Workaround for adopters: don't import across your own `tests` package; use
fixtures instead.

---

## N-010 — `Migrator` parses `-- DOWN` sections but can never run them
**Python · MED · `SPEC-GAP`**

`neutron.nucleus.migrate.Migrator` loads each `NNN_name.sql` file and splits it
on `-- DOWN` into `Migration.up` and `Migration.down` (`migrate.py:88-94`), and
the docstring on `migrate()` even says *"Optionally include a `-- DOWN` marker
to separate up/down SQL."* But **there is no `rollback` / `downgrade` method on
`Migrator`** — only `migrate()` and `run_migrations()`, and both run `up`
exclusively (`run_migrations` executes `m.up`; `m.down` is never referenced
outside the loader). The `Migration.down` field is dead.

Observed in Omni Analyst v2 (the dogfood): ~10 of 29 migration files carry a
populated `-- DOWN` section (`DROP TABLE ...`, `DROP INDEX ...`), written in good
faith because the loader's split invites them. They look operational and are
not. An adopter who rolls a migration forward expecting the documented down path
exists has no rollback — only `pg_restore` from a backup.

This is a `SPEC-GAP` rather than `REAL-BUG` because nothing *breaks*; the gap is
that an advertised capability (the `-- DOWN` marker, named in the public
docstring) has no supporting API.

Fix: add `Migrator.rollback(target_version)` — load migrations, for each applied
version above `target` in descending order, run `down` inside a transaction and
delete the `_neutron_migrations` row. Pair with a `neutron migrate:down` CLI
surface. Until then, `Migration.down` should either be removed (so the loader
stops implying a capability that isn't there) or the docstring corrected to
state that `-- DOWN` is documentation-only.

Workaround for adopters: treat `-- DOWN` as human-readable notes; rely on DB
backups (`pg_dump`) for rollback, as Omni Analyst does.

**Resolved (2026-08-06):** `Migrator.rollback(migrations, target_version)` and
`rollback_dir(dir, target_version)` landed in `nucleus/migrate.py`. They walk
applied migrations above `target_version` newest-first, running each `down` and
deleting its `_neutron_migrations` row in one transaction (schema and record can
never disagree). A migration with an empty `down` raises `ValueError` rather
than skip -- skipping would leave its changes present while later rows are
removed. The `-- DOWN` sections across both repos are now operational. A CLI
surface (`neutron migrate:down`) is deferred: the CLI's `migrate` command uses
`NucleusClient.migrate`, a separate path; wiring rollback through it is a
follow-up.

---

## Roadmap note — a real React runtime is not recommended

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

---

## N-011 — `useLocation` silently returns `/` during SSR; no `RouterContext` provider on the server
**TypeScript / routing · HIGH · `REAL-BUG`**

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

---

## Not filed

Candidates investigated and dropped, recorded so they are not re-raised:

- **Lazy markdown rendering itself.** A deliberate, well-commented memory
  optimisation. The finding is the stale field and template (N-003), not the
  laziness.
- **`runtime: "react-compat"` being unsupported for internals-dependent
  libraries.** Documented honestly and correctly scoped; the gap is verification
  coverage (N-007), not the design.
