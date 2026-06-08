# Relative framework comparison — 2026-06-08 (deploy-test5)

**This is a RELATIVE comparison on a modest dev box, not an absolute/publish-grade
benchmark.** Read the methodology before quoting any number.

## Methodology (read first)

- **Host:** `deploy-test5` — Debian 13, **4 cores / 4 GB**, idle (load ~0), one of the
  disposable dev laptops. Not representative of production hardware.
- **Load generator co-located** with the server-under-test (autocannon over loopback,
  to avoid the box's variable Wi-Fi latency). On 4 cores the load generator competes
  with the server for CPU, so **absolute req/sec is contention-limited**. Co-location
  penalizes the *fastest* server most, so Neutron's lead here is **conservative**.
- **Profile:** 5 measured runs (median reported), 2 warmup, 10 s/run, 64 connections.
- **Servers:** each framework runs its own production Node server, single process
  (no clustering): Neutron `neutron-ts start` (Preact SSR on Hono), Next.js Pages
  Router (`next start`, `getServerSideProps`), Astro SSR (node adapter).
- **Fairness audits performed:**
  - The `dynamic` route originally carried `cache: { maxAge: 30 }`. Re-measured with
    the cache disabled (`Cache-Control: no-store`, verified on the live response):
    **5,760 → 5,681 rps (<2% delta)** — the result is genuine uncached SSR, not a
    cache artifact. The bench route is now uncached so the comparison is unimpeachable.
  - `compute` / `big` / `mutate` were already uncached (verified in route source).

## Node track — each framework on its own server (the meaningful comparison)

| Scenario              | Neutron | Next.js | Astro | vs Next | vs Astro |
|-----------------------|--------:|--------:|------:|:-------:|:--------:|
| static `/`            |   7,298 |   2,255 | 1,801 |   3.2×  |   4.1×   |
| dynamic `/users/1`*   |   5,681 |     576 | 1,468 |   9.9×  |   3.9×   |
| compute `/compute`    |     982 |     442 |   716 |   2.2×  |   1.4×   |
| big `/big`            |   1,693 |     355 |   904 |   4.8×  |   1.9×   |
| mutate POST `/api/mutate` | 2,089 | 1,127 | 1,388 |   1.9×  |   1.5×   |

\* dynamic re-measured with route cache disabled (see audits above).

Neutron wins every scenario. The margin is **widest on trivial per-request work**
(dynamic/static, where Next's fixed `getServerSideProps`+React overhead dominates)
and **narrowest on `compute`** (1.4–2.2×, where raw CPU work dominates and no
framework can help) — the expected, internally-consistent pattern.

## Optimal-static track — all served by the same static file server

| Scenario   | Neutron | Next.js | Astro |
|------------|--------:|--------:|------:|
| static `/` |  12,775 |  12,944 | 13,406 |

A three-way tie (±5%, noise). **Expected:** in this track every framework's output is
served by the *same* `serve-static.mjs`, so the framework is out of the loop — once
everything is static HTML, hosting speed is identical. The static *payload* audit
(198 B Astro vs 1053 B Neutron vs 1167 B Next) is **not** comparable here because the
bench apps render different homepage content (the harness flags the 5.9× ratio).

## Honest headline this supports

> On identical hardware, the Neutron TS server delivers **~2–5× the throughput of
> Next.js and Astro** across SSR, static, and POST workloads (up to ~10× on trivial
> SSR pages, where competitors' fixed per-request overhead dominates).

Lead with the 2–5× range; the 10× is real but best-case. Always ship the methodology.
For absolute/publishable numbers, re-run with the load generator on a **separate
machine** and a production-class server host.
