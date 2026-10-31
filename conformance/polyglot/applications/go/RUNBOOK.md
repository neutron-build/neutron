# GO-APP native comparison runbook (compute-2, coordinator only)

Status: authored. Nothing here has been executed. The original-native harness,
the converted module and every script were written and statically checked only
(`gofmt`, `py_compile`, the inventory and source verifiers). No Go build, test or
vet has run on any of it, so compile errors in the harness or converted module
are possible and must be reported as such, not worked around by weakening a check.

The comparison is the unchanged public go-admin application (pin `386ddeae`,
core v2.11.0 at `004376f3`) against the bounded Neutron conversion in
`converted/`. The original is ground truth: a disagreement means the converted
code, or the authored harness, is wrong; it never means the original is.

## Preconditions

- Serialized compute-2 lease. No compute-1. No ssh from any script here.
- Go toolchain **>= 1.27.1** as a private alternate peer, for BOTH sides. The
  whole upstream `go.mod` requires 1.27.1; the SDK profile Go 1.26.6 cannot run
  the unchanged application. Do not edit the upstream `go` directive, `go.mod`
  or `go.sum`. Below, `GO127` is that binary.
- Disposable PostgreSQL reachable through the environment variable
  `NEUTRON_GO_APP_DATABASE_URL` (a postgres URI). Set it privately; never echo it,
  put it in a command line, a log, a report or a transcript. The role must be able
  to `CREATE SCHEMA`/`DROP SCHEMA`. Each test creates and drops its own schema.
- `SDK_ROOT`: the `go/` directory of the **integrated, qualified SDK artifact** to
  convert against (it must contain the typed LIKE, handle-owned deferred graph,
  request-owned HTTP Scope and qualified-catalog commits). Record its git HEAD.
- Network or a populated module cache for `go mod tidy` of the converted module.
  The original is built with `-mod=readonly` from its own untouched `go.sum`.

## 1. Static verification (any machine, no Go)

```sh
cd conformance/polyglot/applications/go
python3 verify_source.py            # 48 frozen upstream files, bytes + SHA256
python3 verify_inventory.py --gofmt # authored-file manifest, no upstream copies,
                                    # fail-not-skip tests, scenario + oracle, gofmt
python3 scenario_oracle.py > /dev/null
```

`verify_inventory.py --update` is an authoring step; the coordinator only
verifies. A changed authored file without a manifest update fails verification.

## 2. Original: prepare and run (exact commands)

```sh
EVID=/fresh/private/evidence/go-app-NN        # must not exist
python3 prepare_original.py "$EVID/original"
mkdir -p "$EVID/transcripts/original" "$EVID/transcripts/converted" "$EVID/logs"
cd "$EVID/original/go-admin"
```

`prepare_original.py` fetches both public repositories at the exact commits,
checks HEAD identities, re-verifies every frozen byte (including the SysRole,
SysMenu, SysApi and `role_dept.go` sources the baseline now needs), and adds only
three authored files to `app/admin/apis`: `neutron_corpus_native_test.go`,
`neutron_corpus_scenario_test.go` and `neutron_corpus_scenario.json`. Their
hashes are in `reconstruction.json`.

The baseline schema is the original application's own GORM AutoMigrate of
`SysDept`, `SysUser` and **`SysRole`**; the pinned `SysRole` many2many
(`sys_role_dept`, joinForeignKey `role_id`, joinReferences `dept_id`) creates
`sys_role_dept` as a NOT NULL composite-key join table with foreign keys to
`sys_role` and `sys_dept`, and recursively `sys_menu`, `sys_menu_api_rule`,
`sys_api`, `sys_role_menu`. The unchanged application's deployment-time
`cmd/migrate` tables (FK-less standalone `SysRoleDept`) are a different DDL and
are not what this baseline compares.

```sh
export NEUTRON_GO_APP_TRANSCRIPT_DIR="$EVID/transcripts/original"   # optional; required for the comparison
"$GO127" test -mod=readonly ./app/admin/models -run TestEncrypt -count=1 -v
"$GO127" test -mod=readonly ./app/admin/apis -run 'TestUpdate_|TestNeutronCorpusOriginal' -count=1 -v
```

(`NEUTRON_GO_APP_DATABASE_URL` is read from the environment by name only.)

Expected PASS lines, with no `--- SKIP` and no `--- FAIL`:

- models: `TestEncryptLeavesAnAlreadyHashedPasswordAlone`,
  `TestEncryptHashesAPlaintextPassword` (+ `BeforeCreate`, `BeforeUpdate`),
  `TestEncryptIgnoresAnEmptyPassword`.
- apis: `TestUpdate_CannotEscalatePrivilegeThroughAnotherUsersRecord`,
  `TestUpdate_SelfEditCannotChangePrivilegedFields` (original SQLite references,
  unchanged), `TestNeutronCorpusOriginalPostgresUserServiceAndAuthorization`,
  `TestNeutronCorpusOriginalPostgresScopeMatrix`,
  `TestNeutronCorpusOriginalPostgresOperations`.

Transcripts written: `authorization.json`, `scope-matrix.json`, `operations.json`.

## 3. Converted: stage, resolve, run

```sh
cd /path/to/conformance/polyglot/applications/go
python3 prepare_converted.py --original "$EVID/original" --sdk-root "$SDK_ROOT" "$EVID/converted"
cd "$EVID/converted"
GOFLAGS= "$GO127" mod tidy                       # records the dependency drift below
"$GO127" list -m all > "$EVID/logs/converted-modules.txt"
sha256sum go.mod go.sum
export NEUTRON_GO_APP_TRANSCRIPT_DIR="$EVID/transcripts/converted"
"$GO127" test -mod=readonly . -run TestNeutronCorpusConverted -count=1 -v
```

`prepare_converted.py` copies `service.go`, `api.go` and the converted test,
renders `go.mod` with `replace go-admin` (the prepared original, unmodified) and
`replace github.com/neutron-build/neutron/go` (`SDK_ROOT`), renders the shared
scenario helper and scenario, and seeds `go.sum` from the original and SDK sums.
Dependencies are resolved by one MVS over go-admin's graph and the SDK's, so the
converted build may use newer versions than either pin (for example pgx 5.10.0
from go-admin against the SDK's 5.7.2). That is permitted but must be recorded:
keep `converted-modules.txt` and the `go.mod`/`go.sum` hashes in the evidence.

Expected PASS lines: `TestNeutronCorpusConvertedPostgresScopeMatrix`,
`...Operations`, `...UserServiceAndAuthorization`, `...HTTPBoundary`.
`...UserServiceAndAuthorization` additionally asserts HTTP 403 for the Casbin-
denied other-user update and 200 for the self edit, through the request-owned
`ormhttp` Scope with the application's own Casbin enforcer (empty policy).

Same-machine caveat: the converted module links the original `go-admin` models,
DTOs and `core` sources by `replace`; only database access in the service layer
is Neutron. GORM and Casbin's GORM adapter remain solely for baseline DDL and the
application-owned policy store.

## 4. Compare

```sh
python3 compare_transcripts.py --original "$EVID/transcripts/original" --converted "$EVID/transcripts/converted"
```

or run phases 2-4 in one go (still compute-2 only):

```sh
python3 run_comparison.py --evidence-dir "$EVID" --sdk-root "$SDK_ROOT" --go "$GO127"
```

`run_comparison.py` redacts the database URL value from stored logs and refuses a
non-fresh evidence directory or a toolchain older than 1.27.1.

## Pass, partial and fail

**Pass** requires all of:

1. Section 1 static checks succeed on the exact checkout under test.
2. Both original `go test` commands exit 0 with every expected test passing and no
   skip, and the converted `go test` exits 0 likewise.
3. `compare_transcripts.py` prints `"status": "pass"`: all three transcripts exist
   on both sides; original and converted transcripts are identical; the
   scope-matrix equals the independent oracle (`scenario_oracle.py`) on both
   sides; the stated operation and authorization invariants hold on both sides.
4. Evidence records: SDK `git rev-parse HEAD` and cleanliness, original
   `reconstruction.json`, `converted-staging.json`, resolved converted
   `go.mod`/`go.sum` hashes, `go list -m all`, transcript hashes, Go version,
   and the test logs.

**Partial** (never reported as pass): a transcript is missing on a side; only a
subset of tests ran; converted tests pass but the comparison was not executed;
`NEUTRON_GO_APP_TRANSCRIPT_DIR` was not set. Report exactly which parts ran.

**Fail**: any test failure; a transcript difference; an oracle mismatch; an
invariant violation. Triage: an original-side failure first questions the authored
harness or the environment (the original application is not edited); a
converted-only failure is a conversion or SDK defect. An oracle-only mismatch
(original equals converted but differs from the oracle) means re-read the oracle
against the frozen sources before concluding anything about either side.

**Blocked / not run** (not a verdict on the application): toolchain older than
1.27.1, `go mod tidy` or compile failure, database unavailable. A compile failure
in authored harness code is a harness defect to fix in source, not to bypass.

## What a pass would and would not mean

It would establish equivalence on the bounded scenarios: the five upstream data
scopes plus invalid scope, department zero and nil permission (converted fails
closed where the original would panic on nil, untested in the original), search
(contains/exact/department-path/order/pagination), Get/GetSelf/GetProfile, Insert,
Update (privileged self fields, zero-value skipping, credential preservation),
soft-delete Remove, Casbin-gated cross-user update, and rollback on a real bcrypt
hook failure, each against native PostgreSQL snapshots.

It would not qualify the whole go-admin product: not Gin/JWT, `UpdateAvatar`,
`UpdateStatus`, `ResetPwd`, `UpdatePwd`, batch-id Get, the remaining admin
services, the UI, or Casbin policy evaluation (application-owned). It is also not
a GORM parity claim.

## Known conversion differences (all listed so they are not silent)

- `LEFT JOIN sys_dept ... dept_path LIKE` is a semi-join (`dept_id IN (SELECT ...)`),
  equivalent only because `sys_dept.dept_id` is a primary key.
- Custom-scope subquery omits the original `LEFT JOIN`'s possible NULL member.
  Equivalent in a WHERE context only; not for a NOT IN.
- Exact `roleId`/`postId` filters parse the string client-side; non-numeric text
  errors in both, with different error text.
- HTTP responses (status/body) differ from gin's; DB effects are what is compared.
- No `SELECT ... FOR UPDATE`: the original `First` does not lock either.
- GORM `Updates` write-back of unchanged `create_by`/`update_by`/`created_at` is not
  reproduced (identical values, no observable difference outside concurrency).
- `default:"1"` DTO tags are not applied when decoding JSON (the harness always
  sends `status`).
