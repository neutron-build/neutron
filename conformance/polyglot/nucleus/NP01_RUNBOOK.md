# NP01 native qualifier runbook

Authored, not executed. The three NP01 finite-admission qualifiers
(`ts_admission_native.mjs`, `go-admission-native/main.go`,
`python_admission_native.py`) are run through one orchestrator,
`run_np01_qualifiers.py`. It needs Linux and the Python standard library only,
and it never reads the engine binary's source or configuration. A passing
combined report is a finite characterization of the NP01 point CRUD profile
against one Nucleus binary and one PostgreSQL oracle. It is not certification,
PostgreSQL parity or a timing result, and the package stays disabled
(`packageEnabled: false`).

## What the orchestrator does

1. Validates its inputs. A bad flag, missing file or unset variable exits `2`
   before anything is started and writes no report.
2. Starts the exact Nucleus binary on free loopback ports with a temporary data
   directory and a random bootstrap password (`NUCLEUS_ALLOW_INSECURE_AUTH=1`,
   `--no-tls`), the same way `run_nucleus_authority.py` does.
3. Proves the running image is that binary (SHA-256 of `/proc/<pid>/exe`) and
   that the loopback LISTEN socket is a descriptor of that process.
4. Runs one qualifier. The PostgreSQL oracle URL reaches it only through the
   environment variable whose NAME you pass; the Nucleus URL is composed in
   memory from the random password and handed over as
   `NEUTRON_NP01_NUCLEUS_URL`. Neither URL is ever written to disk or the report.
5. Always stops the engine process group and removes its data directory, then
   writes one combined JSON report.

## Environment variable names

| Name | Meaning |
|---|---|
| `PG_OWNED_URL` (any plain name, passed with `--postgres-url-env`) | URL of an owned, disposable PostgreSQL oracle. The qualifier creates and drops one uniquely named schema. Python compares timestamps as UTC instants, so the server `TimeZone` does not matter. |
| `NEUTRON_NP01_NUCLEUS_URL` | Set by the orchestrator for the qualifier only. Do not export it yourself. |

`NODE_PATH`, `NODE_OPTIONS`, `PYTHONPATH`, `PYTHONHOME` and `PYTHONSTARTUP` are
removed from the qualifier environment so they cannot redirect resolution of
the installed artifact.

## Cheap self-check

```sh
python3 -I conformance/polyglot/nucleus/run_np01_qualifiers.py --self-test-redaction
```

Starts nothing; exits `0` only when the redaction helper removes a sample
password and URL.

## TypeScript

Build the package from the revision under test and install it into a fresh
consumer outside the checkout, with both drivers beside it:

```sh
cd /checkout/typescript && pnpm install --frozen-lockfile
cd packages/neutron-sql && pnpm run build
mkdir -p /work/ts-pack && pnpm pack --pack-destination /work/ts-pack
mkdir -p /work/ts-consumer && cd /work/ts-consumer && npm init -y >/dev/null
npm install /work/ts-pack/neutron-build-sql-*.tgz pg postgres
```

Run:

```sh
export PG_OWNED_URL='<owned PostgreSQL URL>'
python3 -I /checkout/conformance/polyglot/nucleus/run_np01_qualifiers.py \
  --language typescript --binary /path/to/exact/nucleus \
  --package-root /work/ts-consumer/node_modules/@neutron-build/sql \
  --postgres-url-env PG_OWNED_URL --report /path/to/evidence/np01-typescript.json
```

`--node` selects the Node executable (default: `node` on `PATH`). The package
root must be inside a `node_modules` directory and contain `dist/profile.js`.

## Python

```sh
python3 -m venv /work/py-venv
/work/py-venv/bin/pip install '/checkout/python[orm]'
```

The install must be non-editable: the qualifier refuses ORM modules that are not
under the package root.

```sh
export PG_OWNED_URL='<owned PostgreSQL URL>'
python3 -I /checkout/conformance/polyglot/nucleus/run_np01_qualifiers.py \
  --language python --binary /path/to/exact/nucleus \
  --venv /work/py-venv --postgres-url-env PG_OWNED_URL \
  --report /path/to/evidence/np01-python.json
```

The package root defaults to the venv `purelib`; override with `--package-root`.
The qualifier runs under `bin/python -I`.

## Go

Create an outside consumer module that points at the checkout and build the
qualifier without `-trimpath` (the qualifier proves its `orm` sources from
build-time source paths):

```sh
mkdir -p /work/go-consumer && cd /work/go-consumer
go mod init example.invalid/np01consumer
cp /checkout/conformance/polyglot/nucleus/go-admission-native/main.go .
go mod edit -require=github.com/neutron-build/neutron/go@v0.0.0 \
  -replace=github.com/neutron-build/neutron/go=/checkout/go
go mod tidy
go build -o go-admission-native .
```

Run:

```sh
export PG_OWNED_URL='<owned PostgreSQL URL>'
python3 -I /checkout/conformance/polyglot/nucleus/run_np01_qualifiers.py \
  --language go --binary /path/to/exact/nucleus \
  --module-root /checkout/go --go-consumer-dir /work/go-consumer \
  --postgres-url-env PG_OWNED_URL --report /path/to/evidence/np01-go.json
```

`--go-binary` overrides the default `<consumer>/go-admission-native`. The
consumer directory must be outside the module root, and its `main.go` must be
byte-identical to the checked-in qualifier.

## Common options

`--qualifier-timeout SECONDS` (default 900, minimum 30) is the hard limit for
the qualifier child; engine start-up is limited to 90 seconds. The exit code is
`0` for a passing report, `1` for a failing one and `2` for rejected arguments.

## Reading the report

The runner prints one line (`status`, `language`, `binarySha256`). The report
holds:

- `status`: `pass` only when all of the following hold: the qualifier exited `0`
  without timing out; its embedded report says `pass`, `package_enabled` false,
  the NP01 profile and the same binary hash; `attestation.processBound` is
  true; `runningImageSha256` equals `binarySha256`; the engine process group
  was stopped; and the data directory was removed. Any `runnerFailure` forces
  `fail`.
- `binarySha256`, `runningImageSha256`, `runningImageSha256AfterQualifier`,
  `runnerSha256`, `qualifierSha256` and `qualifierProvenance` (the installed
  package root, or for Go the consumer file hashes and the built executable).
- `attestation`: `processBound`, `imageMatchesBinary`,
  `listenerOwnedByProcess`. It binds only this run's engine process. Engine
  source, build and configuration provenance, and the identity of the
  PostgreSQL oracle, must be recorded separately.
- `qualifierExit`, `qualifierTimedOut`, `qualifierReport` (the qualifier's own
  report, redacted) and `qualifierLogTail` (bounded and redacted).
- `engineLogTail` (bounded; lines mentioning a password are dropped),
  `engineStopped`, `engineExitCode` and `engineDataDirRemoved`.

On a failing run read `runnerFailure`, then `qualifierReport.failure`
(and `cleanupFailure`), then the log tails. If `engineStopped` or
`engineDataDirRemoved` is false, check for leftover `neutron-np01-*` temporary
directories and engine processes by hand.

## Known limits

- Killing the orchestrator with SIGKILL cannot run cleanup; SIGINT, SIGTERM and
  SIGHUP do.
- The listener check reads `/proc/net/tcp` for `127.0.0.1` and assumes the
  engine holds its own listening socket; an engine that hands the socket to a
  child process fails the check by design.
- The qualifiers exercise only point CRUD over `int4`, `int8`, `bool`, `text`,
  `jsonb` and `timestamptz` (UTC), nested savepoint recovery, rollback and
  refusal of operations outside the profile. Nothing else about Nucleus is
  claimed.
