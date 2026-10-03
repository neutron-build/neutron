The Python sync and async ORM connectors accept an explicitly selected
`profile='nucleus-relational-rc-v1-candidate'` for qualification. The default
`postgres-direct` path continues to reject Nucleus before handing off a client.
The candidate requires exact startup and SQL version reports, records immutable
capabilities and `package_enabled=False`, and never infers engine identity from
PostgreSQL wire compatibility.

This entry point admits a narrow generated point CRUD grammar and six scalar
metadata families. Metadata checks cover the entire table, including columns
omitted from a projection or write. UTC-aware datetime and explicit JSONB
parameters are allowed. Unsupported types, raw SQL, Query algebra, functions,
comments/multiple statements, unbound scans, catalog discovery and cursor streams
fail before dispatch. RETURNING admission happens before the implicit transaction
opens. Owned transaction and savepoint APIs remain available for qualification;
the exact binary still must prove their semantics. These guards are a finite
operation contract, not a security boundary against callers modifying private
connection fields.

Run the existing endpoint unit suite and strict typecheck on compute-2, then
qualify both freshly installed clients against a recorded source/config/binary
with PostgreSQL controls. `python-admission-contract.json` names the bounded
contract and outstanding gates. It does not retarget the historical assessment
in `profile.json`, certify the package, or complete TypeScript/Go admission or
NP03/NP05's mandatory wider numeric/cursor work.

The bounded installed-consumer native gate requires both owned endpoints, a
fresh package root, and the exact coordinator-run Nucleus binary. It provisions
isolated schemas with raw psycopg, checks sync/async point CRUD, SQL NULL versus
JSON null, UTC microseconds, savepoint recovery, outer rollback, conservative
refusals and unchanged committed rows against PostgreSQL, then removes fixtures.
Cleanup failures make the evidence fail. Credentials never appear in reports.

```sh
# Coordinator supplies URLs as PG_OWNED_URL and NUCLEUS_OWNED_URL.
# Use the fresh consumer interpreter; pass recorded paths/hash, never URLs.
python conformance/polyglot/nucleus/python_admission_native.py \
  --postgres-url-env PG_OWNED_URL --nucleus-url-env NUCLEUS_OWNED_URL \
  --package-root /path/to/fresh/consumer/site-packages \
  --binary-file /path/to/exact/nucleus --binary-sha256 RECORDED_SHA256 \
  --report /path/to/evidence/python-nucleus-finite.json
```

The probe hashes its own source, Python executable and imported ORM modules. The
binary hash verifies the provided file; the coordinator must bind that file to
the running process and endpoint. This remains characterization of these finite
facts and leaves `package_enabled=False`.
