Card: A-1a
Class: impl
Card-gate: P
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code session a9a687ba-c885-4d0f-9be1-553aa452ac51 (T3 Code review sub-agent, independent P gate of A-1a)
Date: 2026-11-15
Gate-exit: GATE EXIT=0
Commit: b6d2dcea5781f7395a51c9ebbf1c3b0b50956043
Mutants: SHARED-AS-EXCLUSIVE -> mem_locks_shared_readers_coexist, SHARED-IGNORES-WRITER -> mem_locks_shared_excludes_writer, STD-SHARED-UNLOCKED -> std_locks_shared_excludes_writer (caught at the shared-holder-vs-writer direction, not the predicted reader direction), DROP-KEEPS-LOCK -> mem_lock_released_on_unlock_and_drop, READ-HANDLE-WRITES -> mem_read_handle_refuses_writes, RENAME-REPLACES -> std_namespace_ops, DOTDOT-ALLOWED -> mem_invalid_paths_rejected + std_invalid_paths_rejected + path_rejects_dotdot, READ-ZERO-FILLS -> mem_read_write_roundtrip, SYNC-DIR-NOOP -> std_sync_dir_root_and_subdir, G1 SYNC-DIR-NO-FSYNC UNCAUGHT (card-stated limit: needs a power cut, A-1b/LazyFS), G2 MEM-UNLOCK-KEEPS-SHARED UNCAUGHT (A-1a-F2), G3 MEM-RAND-UNSEEDED-AFTER-FIRST UNCAUGHT (A-1a-F2), G4 STD-SYNC-ALL-FAILS-ON-RW UNCAUGHT (A-1a-F2), G5 MEM-RENAME-REPLACES -> mem_namespace_ops, G6 STD-EXCL-AS-SHARED -> std_locks_exclusive_excludes_all, G7 STD-SYNC-DATA-NOOP UNCAUGHT (needs a power cut, A-1b), G8 MEM-WRITE-NO-EXTEND -> mem_read_write_roundtrip, G9 MEM-CREATENEW-EXISTS-AS-NOTFOUND UNCAUGHT (A-1a-F2), G10 MEM-LOCK-CONFLICT-AS-READONLY UNCAUGHT (A-1a-F2)
Followups: A-1a-F1, A-1a-F2, A-1a-F3

## Gate run

Scaffold's `fresh-gate.sh`, extracted from a `--no-local --depth 1` clone of
nucleus/v3-scaffold (scaffold id f3ffe88c84806e24ebbdad4751837bc4ba4244b5),
run once on the full commit id, CARGO_TARGET_DIR=~/.cache/nv3-target/gate-A-1a.
The `fresh-gate:`, `STEP` and final lines; the literal last line is `GATE EXIT=0`,
stderr empty:

```
fresh-gate: commit b6d2dcea5781f7395a51c9ebbf1c3b0b50956043
fresh-gate: base f3ffe88c84806e24ebbdad4751837bc4ba4244b5
fresh-gate: checks passed (4789 files hashed in 3.93 s)
fresh-gate: 2 file(s) matched through their eol conversion
STEP test EXIT=0
STEP clippy EXIT=0
STEP fmt EXIT=0
STEP deny EXIT=0
STEP wasm32 EXIT=0
STEP core-paths-selftest EXIT=0
STEP enforcement-tests EXIT=0
GATE EXIT=0
```

## Scope

`git diff nucleus/v3-scaffold...b6d2dcea --stat`: 10 files, all under
`nucleus/v3/crates/nucleus-env/` (Cargo.toml, src/{lib,env,error,mem,path,std_env}.rs,
tests/{common/mod,mem_env,std_env}.rs), +2161/-5. All inside Touch only; no
protected path; no new dependency beyond `thiserror = { workspace = true }`.

## What was checked and holds

- **StdEnv durability (review point 1).** `sync_data` = `File::sync_data`
  (fdatasync), `sync_all` = `File::sync_all` (fsync), `sync_dir` = open the
  directory read-only and fsync it, `CreateNew` = `create_new(true)`
  (O_CREAT|O_EXCL), `rename` = `std::fs::rename` (rename(2), atomic) after a
  `symlink_metadata` no-overwrite check (card-sanctioned; the race is excluded
  by N11's single owner, commented). Every platform error is returned, mapped
  (NotFound, AlreadyExists, StorageFull/QuotaExceeded -> NoSpace, everything else
  Io with op/path/kind/message); none is swallowed. `read_at` loops on short
  reads and retries Interrupted; `write_at` is `write_all_at`. `create_dir`,
  `CreateNew`, `rename`, `remove` and `set_len` make no durability claim of
  their own, matching E-DURABLE. I found no place where StdEnv reports success
  for an operation that it should have made durable for a directory or file it
  was really given; the one near-miss (`sync_dir` of a *file* path returns Ok)
  is in A-1a-F3.
- **Locks (point 2).** std `try_lock`/`try_lock_shared`/`unlock` are flock
  (LOCK_NB) on Linux: per open file description, so two descriptors of one
  process conflict (the conformance tests could not pass under fcntl
  semantics, which never conflict within a process), they conflict across
  processes, they work on an O_RDONLY descriptor in both modes, and close
  releases. That is the right choice for S-STORE §14.1 (the HTML comment's
  fcntl/O_RDONLY problem is exactly what flock avoids). Exclusive vs shared,
  several shared holders, release on unlock and on drop, and non-blocking
  `Locked` are tested on both backends; variants verified by a scratch probe
  on both backends.
- **Error paths (point 3).** No `unsafe`; no non-test `unwrap`/`expect`/`panic!`/
  `unreachable!` (the only `unwrap_or` is a saturating conversion). MemEnv's
  poisoned mutex maps to `Io{Other}`; `now`/`rand` fall back to 0 on poison
  (documented).
- **wasm32 (point 4).** `std_env` is `#[cfg(unix)]` (module and re-export);
  only `src/std_env.rs` names `std::fs`, `std::os`, `Instant`; MemEnv uses
  `thread::yield_now` only. The gate's wasm32 step is green.
- **Card mutants (point 5).** All nine caught on re-application in a scratch
  copy of the commit; every mutant reverted, scratch diff clean after each.
- **Trait additions (point 6).** All kept as sourced: `create_dir` (S-STORE §2
  `wal/`), `sync_dir` and `lock_shared` (S-2-F2-F1; §12.5, §14.1), `rename`
  (§12.1 recycling; E4 `rename`), `remove` (C-T0 v8 §2.1; E4 `unlink`),
  `exists` (E4's G3 probe, which also argues for `Ok(false)`-not-error), and
  `list_dir` (E4 `wal_names`; bytewise sort suits 16-hex segment names);
  `is_empty` for clippy. Same-directory-only and never-replace `rename`: keep.
- **API for A-1b / A-6 / A-7 (point 7).** No signature change is forced. Two
  doc-level rules are missing (handle lifetime across rename/remove, A-1a-F1;
  file/directory mismatches, A-1a-F3); both are additive. Note for OpfsEnv:
  OPFS fixes the lock mode when the sync access handle is created, so a
  backend there must defer creating the access handle until `lock_*` or first
  I/O; workable without a trait change.

## Findings

1. **MemEnv handles name a path, not a file (major; A-1a-F1).** `MemFile`
   looks up `state.files[self.path]` on every call, and the lock state moves
   with the data on `rename`. Scratch probe on the commit:
   - lock leak: a handle holds `lock_exclusive` on `wal/a`; `rename(wal/a, wal/b)`;
     drop the handle; a new handle's `lock_exclusive` on `wal/b` ->
     `MEM Err(Locked { path: "wal/b" })`, `STD Ok(())`. E-LOCK's "dropping the
     handle releases its lock" is broken in MemEnv;
   - stale handle writes into a successor file: handle `u` on `wal/c`,
     `remove(wal/c)`, `CreateNew(wal/c)` writes "new", `u.write_at(0,"ZZZ")` ->
     MemEnv's new file reads "ZZZ", StdEnv's reads "new" (silent cross-file
     write in MemEnv);
   - after a rename the old handle gets `NotFound` in MemEnv and keeps working
     in StdEnv.
   The trait does not say what a handle does after its file is renamed or
   removed, so the two backends implement different contracts here. E4's
   SimDisk used a `FileId` (inode-like) model, which A-1b must carry over (§10
   item 5). Not blocking for A-1a: nothing consumes the trait yet, and the one
   locked file (the page file) is never renamed; but SimEnv must not inherit
   this.
2. **The conformance suite does not check error variants (major; A-1a-F2).**
   `err_is` in `tests/common/mod.rs` accepts any of NotFound / AlreadyExists /
   Locked / ReadOnly / InvalidPath whenever the path matches, so the checks
   that the card words as "gives `AlreadyExists`", "gives `Locked`", "gives
   `ReadOnly`" accept the wrong variant: G9 (CreateNew on an existing file
   returns NotFound) and G10 (a lock conflict returns ReadOnly) pass every
   test. The code itself returns the right variants on both backends (scratch
   probe), but A-1b re-runs this suite on SimEnv, and §14.1's read-only open
   distinguishes `Locked` from other failures. Further gaps: no test unlocks a
   *shared* lock (G2: `unlock` leaving the shared holder passes; the card's
   `locks_shared_excludes_writer` says "after it unlocks", the code drops it);
   no test ever calls `sync_data`/`sync_all` on a writable handle (G4: StdEnv's
   `sync_all` failing on every RW handle passes); `mem_rand_is_seeded` builds a
   new `MemEnv` per draw, so it compares the first value eight times, not the
   first eight values (G3: a seed-independent second draw passes).
3. **StdEnv/MemEnv divergences on file/directory mismatches and edges (minor;
   A-1a-F3).** Scratch probe, MEM vs STD:
   - `sync_dir("page")` (a file): `NotFound` vs `Ok(())` (StdEnv fsyncs the
     file). A caller that passes `wal/<seg>.log` instead of `wal` gets Ok with
     no directory sync on StdEnv; MemEnv/SimEnv would catch it, so it is not
     a blocking durability lie, but StdEnv should refuse a non-directory;
   - `open("wal", Read)` (a directory): `Io{Other}` vs `Ok` handle;
   - `exists("page/x")` / `open("page/x", _)` under a file: `Ok(false)` /
     `NotFound` vs `Io{NotADirectory}` (E-NAMESPACE says a missing path is
     `Ok(false)`, never an error);
   - `rename("d","e")` of a directory: `Io{Other}` vs `Ok` (the trait says it
     moves a file);
   - `rename(missing, existing)`: `NotFound` vs `AlreadyExists` (check order);
   - `write_at(100, b"")` on a 3-byte file: MemEnv length becomes 100, StdEnv
     stays 3 (MemEnv resizes to `off` for an empty write);
   - `read_at(1<<63, ..)`: `Ok(0)` vs `Io{InvalidInput}` (E-IO says 0 past EOF).
   Nits: `path::check`'s doc lists `sync_dir` and `list_dir` among its users
   (they use `check_dir`); StdEnv `rename` reports every failure against
   `from`.
4. **Process anomalies, reported verbatim, not defects of the code.**
   - The card's Notes say "a Fable 5.2 or Opus 5.5 delegate implements this
     card, not the GLM runner"; the runner log says `[A-1a] attempt 1:
     opencode zai-coding-plan/glm-5.3 effort max` (PLAN §9's row says "S impl").
   - The worker's RESULT reports its own gate.sh run as `STEP
     enforcement-tests EXIT=1; GATE EXIT=1` (two `runner e2e` rows, K and BG,
     in its container), while the runner log records `[A-1a] PASS: cd
     nucleus/v3 && bash scripts/gate.sh`. The fresh gate above is the result
     this verdict relies on.

## Follow-ups

All three fold into **A-1b**, the next card that touches
`crates/nucleus-env/` (owner rule 2026-11-14: fold, no chains). None fits
A-1a-B, which is the API-surface lint under `scripts/`.

- **A-1a-F1 (fold into A-1b): handle identity.** Give MemEnv (and so SimEnv)
  file identity separate from the name, like E4's `FileId`: a directory entry
  maps a name to a file id, a handle holds the file id, and lock state lives
  on the file; or, if A-1b prefers, refuse `rename`/`remove` of a file with an
  open handle loudly in MemEnv/SimEnv. Either way add one E-* doc clause to
  the trait saying what a handle does after its file is renamed or removed
  (OPFS cannot move a file with an open sync access handle, so "callers close
  handles before rename/remove; Mem/Sim refuse" may be the portable rule), and
  a conformance check for it, including the lock-after-rename-and-drop case
  above. Must land before SimEnv's rename-loss model is built on MemEnv's state.
- **A-1a-F2 (fold into A-1b): conformance suite strength.** Make `err_is` take
  the expected variant and use it in every check; add `unlock` of a shared
  lock (then a writer's `lock_exclusive` succeeds); call `sync_data` and
  `sync_all` on a `ReadWrite`/`CreateNew` handle and `sync_dir` after them and
  require `Ok`; fix `mem_rand_is_seeded` to draw eight values from one `MemEnv`.
  Re-run G2, G3, G4, G9, G10 from this verdict; each must be caught. A-1b's
  SimEnv run of the suite then inherits the stronger checks.
- **A-1a-F3 (fold into A-1b): mismatch and edge rules.** Add to the trait
  docs what each method does on a file/directory mismatch (open, sync_dir,
  list_dir, remove, rename, exists under a file) and pin one behaviour on all
  backends: StdEnv's `sync_dir` refuses a non-directory (check the opened fd's
  metadata), `open` refuses a directory, `exists` under a file is `Ok(false)`,
  `rename` refuses a directory source, and one check order for
  `rename(missing, existing)`. MemEnv's zero-length `write_at` must not
  extend the file; StdEnv's `read_at` beyond `i64::MAX` returns 0. Fix the
  two doc nits. Add conformance checks for each.
