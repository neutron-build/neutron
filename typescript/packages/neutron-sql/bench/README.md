# neutron-sql gates

Measurement scripts that keep the ORM's performance and integrity budgets
enforceable. They run against the built package (`pnpm build`) and are not
part of the published tarball.

| Script | Needs | What it gates |
|---|---|---|
| `orm-gate.mjs` | `NEUTRON_TEST_DATABASE_URL` (disposable PostgreSQL) | 100-parent relation pages (0/2/20 children on two edges) and depth-3 reads: results equal to hand-written SQL and to the pinned `drizzle-orm`, one statement per read (client logger) and one server transaction per read (`pg_stat_database`; it counts transactions, so it independently catches an autocommit N+1 only), no sequential scan anywhere under a relation edge (to-one lookups included) on indexed fixtures, server time within `serverRatioVsHandMax` of the hand-written statement, streaming early-exit release, absolute p50 ceilings |
| `typecheck-gate.mjs` | nothing | 100-table, depth-3 consumer project type-checked against `dist/*.d.ts`: type and instantiation counts against the recorded baseline + 20% (per TypeScript version) |

```sh
# from typescript/packages/neutron-sql, after pnpm build
NEUTRON_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres \
  node --expose-gc bench/orm-gate.mjs --profile ci --gate
node bench/typecheck-gate.mjs --gate

# full profile (two data scales, 100 iterations), compared with the
# reference machine's run — only meaningful on that machine
node --expose-gc bench/orm-gate.mjs --profile full --compare bench/baseline.json
```

CI (typescript.yml, neutron-sql live job) runs both gates with `--gate`.
What CI enforces does not depend on the runner: equality, statement counts,
plan shape, the same-run server-time ratio, type/instantiation counts, and
absolute p50 ceilings set far above the reference numbers. The numbers are
regression signals, not publishable benchmarks.

`budgets.json` holds every threshold with its reasoning. A failing gate is
investigated before any threshold or baseline changes; re-record a baseline
only from a measured run and say why in the commit.

The Studio counterpart is `studio/scripts/render-bench.mjs` (the real served
UI in headless Chrome: virtualization DOM bound, time to painted rows,
scroll frame times, heap after a 10,000-row result).
