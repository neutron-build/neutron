# GO-APP: pinned go-admin application conversion

This is the public application's selected user/service/authorization corpus,
not a renamed ORM unit fixture. Its original code remains under `upstream`:
go-admin commit `386ddeae8207684d0df572a49ef180b700ddda74` and the exact
`go-admin-core/v2 v2.11.0` dependency at commit
`004376f3b940c291c67cc666e527c83f435ac787`. The original go-admin MIT and go-admin-core Apache-2.0 license files
remain beside the source. `source-manifest.json` records all ten selected
corpus files and their frozen byte counts/SHA256; the dependency manifest
records the additional original model/scope/DTO sources inspected for extraction.

Status: original source is frozen and inspected. Unchanged application
reconstruction, PostgreSQL baseline, Neutron conversion and independent native
acceptance are still required. Source inspection is not an execution result.

`python3 verify_source.py` verifies the selected and dependency bytes without
executing Go. `python3 prepare_original.py /fresh/private/evidence/original`
fetches the full public repositories at the exact commits into a fresh directory,
checks HEAD identities and compares every frozen byte to the full trees. It does
not rewrite dependencies, install a toolchain, or execute tests. Reconstructing
full trees ensures original package dependencies are present rather than making
an invented "baseline" by deleting files that fail to build.

The original go-admin and core modules require **Go 1.27.1**. The SDK's Go 1.26.6
verification profile cannot execute that unchanged application. Use a separately
recorded Go >=1.27.1 profile for both sides of this app conversion, keeping its
original go.mod/go.sum (GORM 1.31.2, PostgreSQL driver 1.6.2, core 2.11.0) intact.
Do not lower the go directive or upgrade dependencies to make reconstruction
appear to pass. Original password and anti-privilege-escalation tests remain
unchanged behavior references; the latter use SQLite and need additional
PostgreSQL equivalents. The coordinator runs all native builds/tests serially.

## Manual conversion contract

| Original boundary | Neutron conversion and retained behavior |
|---|---|
| SysUser Encrypt / BeforeCreate / BeforeUpdate | Preserve the bcrypt.Cost valid-hash guard, empty password semantics and bcrypt failure propagation; explicit hook workflows own rollback. |
| AfterFind | Preserve DeptIds/PostIds/RoleIds materialization after each loaded user. |
| Preload("Dept") | Explicit user-to-department relation and bounded batch eager loading; missing Department remains absent. |
| Permission scope | Keep all five upstream create_by-based policies, including custom-role and department-tree subqueries; unknown/invalid department scopes fail closed. Scope policy remains separate from RLS. |
| GetPage | Apply identical search/permission predicates to count and paged rows; remove pagination for count. Retain original supported DTO fields and explicit ordering. |
| GetSelf versus Get | Self lookup uses token identity without data-permission filtering; general lookup retains Permission. Translate no-row behavior at the application boundary. |
| API Update | Retain the upstream explicit authorization check when body userId differs from token caller; ordinary self edit cannot change roleId/deptId/status. Authentication and Casbin policy evaluation remain application-owned. |
| Service Update | Lock/read the scoped user, preserve privileged self fields, omit password/salt assignments, and require exactly one updated row. Do not turn present zero into accidental omission without recording original GORM struct update semantics. |
| ModelTime / ControlBy | Inspect the exact core schema before translating timestamps/deletion/creator/updater fields. Soft deletion is asserted only if the embedded native schema confirms it. |

Acceptance must compare unchanged original application behavior, converted
behavior, and independently queried PostgreSQL snapshots for password/salt
preservation, department/ID association results, page/count scopes, unauthorized
role/status mutations, hook failure rollback and record-not-found/RowsAffected
translation. Root evidence must identify source/toolchain/artifact/native hashes.
This bounded conversion does not replace the whole go-admin product, its Gin/JWT
runtime, every service operation, its frontend, or its Casbin policy engine.


The preparer adds only `original-native/neutron_corpus_native_test.go` to the full
original API test package; every frozen product file and original test stays
byte-identical. That added native harness runs the actual original models,
service `GetPage`, `Preload`/`AfterFind`, password hooks and API `Update`, including
Casbin's original ordinary-role denial. It independently queries PostgreSQL for
credential preservation, privilege fields, zero-live millisecond deletion and
transaction rollback after the real bcrypt failure. Its environment is a private
`NEUTRON_GO_APP_DATABASE_URL` PostgreSQL URI; the harness does not print it.

Run under the original Go >=1.27.1 toolchain in the reconstructed go-admin tree:

```sh
go test -mod=readonly ./app/admin/models -run TestEncrypt -count=1 -v
go test -mod=readonly ./app/admin/apis -run 'TestUpdate_|TestNeutronCorpusOriginal' -count=1 -v
```

These are author commands pending coordinator execution, not pass claims. The
native harness requires PostgreSQL and fails rather than skipping when its URL
is absent. Original SQLite anti-privilege tests remain in the same test package.
ModelTime's actual core contract is now frozen: `soft_delete.DeletedAt` with
`softDelete:milli`, an unsigned zero-live marker stored natively as bigint, plus
CreatedAt/UpdatedAt and creator/updater fields. It is not nullable datetime soft
deletion. The native harness confirms this schema and behavior before conversion.
