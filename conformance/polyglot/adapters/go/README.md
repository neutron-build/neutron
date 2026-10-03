This adapter builds a standalone consumer of the actual Go module source archive
outside the repository, with `GOWORK=off`, a private local snapshot replacement
and pinned dependencies from the module's go.mod/go.sum. It uses production
`orm.Select`, Decimal, time.Time and explicit JSON null handling. It contains no
fixture creation, expected rows, raw SQL observations or oracle implementation.

From the integrated repository root, on the execution box only:

```sh
python3 conformance/polyglot/adapters/go/qualify.py
```

The existing runner/oracle must be present, psycopg installed, Go1.26+ available,
and `NEUTRON_TEST_DATABASE_URL` privately set to the disposable PostgreSQL
profile. The runner owns isolated fixtures and cleanup. The script preserves an
outside-origin temporary directory containing a deterministic module archive,
all extracted source and consumer files, built binary, toolchain/build identity,
artifact hashes, manifest and result. Diagnostics remain bounded and never print
a native database/driver error or URL. No credentials are copied into artifacts.

The result is explicitly an archived-module scalar-read qualification. The
local replacement points to the archived snapshot rather than the origin. It
does not establish a public Go module release, public registry installation,
write/transaction/association conformance or general polyglot parity. Those
remain independent gates. NaN/Infinity, arrays and arbitrary custom codecs are
unsupported by this initial adapter's production scalar package.
