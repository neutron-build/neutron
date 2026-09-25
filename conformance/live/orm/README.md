# ORM capability probes

Probes that state PostgreSQL behaviours `@neutron-build/sql`, the CLI
introspector and Studio depend on, run through the ORM's own `pg` and
`postgres.js` adapters against the engine behind `NEUTRON_TEST_DATABASE_URL`.
The measured result for Nucleus, with upstream defect reproducers, is
[`ORM_CONFORMANCE.md`](ORM_CONFORMANCE.md).

| file | role |
|---|---|
| `probes.mjs` | the probes; each encodes one PostgreSQL behaviour |
| `run.mjs` | runner: `--control`, `--check <file>`, `--write <file>`, `--driver`, `--only`, `--out` |
| `capabilities.nucleus.json` | recorded Nucleus verdicts per probe and driver (canonical) |
| `report.mjs` | regenerates the measured tables in `ORM_CONFORMANCE.md` from the JSON; `--check` fails when stale |
| `reproducers.sql`, `upstream/` | plain-SQL engine reproducers, PostgreSQL as the oracle |
| `x01-nucleus-leg.mjs`, `x02-nucleus-leg.mjs` | per-card legs that start an engine binary and record verdicts (`x02` exits non-zero on any failed verdict) |

Rules:

- `--control` against PostgreSQL must report every probe `supported`. A probe
  that fails there tests the probe, not the engine.
- Never hand-edit a status. After an engine change, re-record with
  `--write` against the new build, run `report.mjs`, and review the diff.
- `--check` fails on any status change in either direction.
