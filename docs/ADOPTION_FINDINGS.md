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

## Not filed

Candidates investigated and dropped, recorded so they are not re-raised:

- **Lazy markdown rendering itself.** A deliberate, well-commented memory
  optimisation. The finding is the stale field and template (N-003), not the
  laziness.
- **`runtime: "react-compat"` being unsupported for internals-dependent
  libraries.** Documented honestly and correctly scoped; the gap is verification
  coverage (N-007), not the design.
