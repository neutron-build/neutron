//! Backend-generic conformance checks for `nucleus-env` (card A-1a
//! Work 6; strengthened in card A-1b-0 Work 2 after verdict A-1a's
//! finding 2: the expected error variant is part of every check).
//!
//! Each check runs on a fresh, empty namespace and uses only the root
//! and one subdirectory named `wal` — S-STORE §2's layout — so A-1b's
//! SimEnv, whose namespace is that layout, can run the same checks.
//!
//! The helpers return `Result<(), String>` rather than panicking: they
//! run outside `#[test]` functions, where the workspace's test lint
//! exemptions do not reach. Each test binary (`mem_env`, `std_env`)
//! uses a subset of these checks, so unused ones are allowed below.

#![allow(dead_code)]
// Each test binary uses a subset of the checks in this module.

use nucleus_env::{Env, EnvError, File, OpenMode};
use std::time::Duration;

/// The path-carrying `EnvError` variants a check can require (A-1a-F2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Want {
    /// [`EnvError::NotFound`].
    NotFound,
    /// [`EnvError::AlreadyExists`].
    AlreadyExists,
    /// [`EnvError::Locked`].
    Locked,
    /// [`EnvError::ReadOnly`].
    ReadOnly,
    /// [`EnvError::InvalidPath`].
    InvalidPath,
}

/// Requires `got` to be `Err` of the wanted variant with `want_path`
/// (A-1a-F2: the variant is part of the contract, not only the path).
fn err_is(
    got: Result<(), EnvError>,
    want: Want,
    want_path: &str,
    line: &str,
) -> Result<(), String> {
    let matched = match &got {
        Err(EnvError::NotFound { path }) if want == Want::NotFound => path == want_path,
        Err(EnvError::AlreadyExists { path }) if want == Want::AlreadyExists => path == want_path,
        Err(EnvError::Locked { path }) if want == Want::Locked => path == want_path,
        Err(EnvError::ReadOnly { path }) if want == Want::ReadOnly => path == want_path,
        Err(EnvError::InvalidPath { path, .. }) if want == Want::InvalidPath => path == want_path,
        _ => false,
    };
    if matched {
        Ok(())
    } else {
        Err(format!(
            "{line}: expected {want:?} for {want_path:?}, got {got:?}"
        ))
    }
}

/// Requires `got` to be `Err(EnvError::Io { kind, .. })` with exactly
/// `kind` (E-KIND's refusals; A-1a-F3).
fn io_kind_is(
    got: Result<(), EnvError>,
    kind: std::io::ErrorKind,
    line: &str,
) -> Result<(), String> {
    match got {
        Err(EnvError::Io { kind: k, .. }) if k == kind => Ok(()),
        other => Err(format!(
            "{line}: expected Io {{ kind: {kind:?} }}, got {other:?}"
        )),
    }
}

/// **E-OPEN.** `CreateNew` on an existing file gives `AlreadyExists`;
/// `Read` and `ReadWrite` on a missing file, and `CreateNew` under a
/// missing `wal/`, give `NotFound`.
pub fn open_modes<E: Env>(env: &E) -> Result<(), String> {
    let f = env
        .open("page", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew page: {e:?}"))?;
    if f.is_empty()
        .map_err(|e| format!("is_empty on a fresh file: {e:?}"))?
    {
        drop(f);
    } else {
        return Err("a fresh CreateNew file is not empty".to_string());
    }
    err_is(
        env.open("page", OpenMode::CreateNew).map(|_| ()),
        Want::AlreadyExists,
        "page",
        "CreateNew on an existing file",
    )?;
    err_is(
        env.open("missing", OpenMode::Read).map(|_| ()),
        Want::NotFound,
        "missing",
        "Read on a missing file",
    )?;
    err_is(
        env.open("missing", OpenMode::ReadWrite).map(|_| ()),
        Want::NotFound,
        "missing",
        "ReadWrite on a missing file",
    )?;
    err_is(
        env.open("wal/seg", OpenMode::CreateNew).map(|_| ()),
        Want::NotFound,
        "wal/seg",
        "CreateNew under a missing wal/",
    )?;
    Ok(())
}

/// **E-IO.** Bytes written read back; a read crossing end-of-file is
/// short and counts only real bytes, and a read past it returns 0; a
/// write past the end extends `len`; `set_len` grows with zeros and
/// shrinks; `is_empty` follows `len`.
pub fn read_write_roundtrip<E: Env>(env: &E) -> Result<(), String> {
    env.create_dir("wal")
        .map_err(|e| format!("create_dir wal: {e:?}"))?;
    let f = env
        .open("wal/data", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew wal/data: {e:?}"))?;

    f.write_at(0, b"hello world")
        .map_err(|e| format!("write_at 0: {e:?}"))?;
    let mut buf = [0u8; 5];
    let n = f
        .read_at(0, &mut buf)
        .map_err(|e| format!("read_at 0: {e:?}"))?;
    if n != 5 || buf != *b"hello" {
        return Err(format!(
            "read back wrote {n} bytes {buf:?}, wanted b\"hello\""
        ));
    }

    // Crossing end-of-file: short, and only the real bytes.
    let mut buf = [0xFFu8; 20];
    let n = f
        .read_at(6, &mut buf)
        .map_err(|e| format!("read_at 6: {e:?}"))?;
    if n != 5 || buf[..5] != *b"world" || buf[5..] != [0xFF; 15] {
        return Err(format!(
            "cross-EOF read gave {n} bytes {buf:?}, wanted b\"world\" and the tail untouched"
        ));
    }
    // At and past end-of-file: 0.
    for off in [11, 100] {
        let n = f
            .read_at(off, &mut buf)
            .map_err(|e| format!("read_at {off}: {e:?}"))?;
        if n != 0 {
            return Err(format!("read_at {off} past EOF gave {n}, wanted 0"));
        }
    }

    // A write past the end extends the file; the gap reads as zeros.
    f.write_at(20, b"XY")
        .map_err(|e| format!("write_at 20: {e:?}"))?;
    let len = f.len().map_err(|e| format!("len: {e:?}"))?;
    if len != 22 {
        return Err(format!(
            "len after a write past the end is {len}, wanted 22"
        ));
    }
    let mut gap = [1u8; 9];
    let n = f
        .read_at(11, &mut gap)
        .map_err(|e| format!("read_at gap: {e:?}"))?;
    if n != 9 || gap != [0u8; 9] {
        return Err(format!("the gap reads as {n} bytes {gap:?}, wanted zeros"));
    }

    // set_len grows with zeros, and shrinks.
    f.set_len(30).map_err(|e| format!("set_len 30: {e:?}"))?;
    let mut tail = [1u8; 1];
    let n = f
        .read_at(29, &mut tail)
        .map_err(|e| format!("read_at 29: {e:?}"))?;
    if n != 1 || tail != [0u8; 1] {
        return Err("grown bytes are not zero".to_string());
    }
    f.set_len(4).map_err(|e| format!("set_len 4: {e:?}"))?;
    let len = f.len().map_err(|e| format!("len: {e:?}"))?;
    if len != 4 {
        return Err(format!("len after set_len(4) is {len}, wanted 4"));
    }
    let mut buf = [0u8; 8];
    let n = f
        .read_at(0, &mut buf)
        .map_err(|e| format!("read_at 0: {e:?}"))?;
    if n != 4 || buf[..4] != *b"hell" {
        return Err(format!(
            "after set_len(4) the file reads {n} bytes {buf:?}, wanted b\"hell\""
        ));
    }

    // is_empty follows len.
    f.set_len(0).map_err(|e| format!("set_len 0: {e:?}"))?;
    if !f.is_empty().map_err(|e| format!("is_empty: {e:?}"))? {
        return Err("is_empty is false at len 0".to_string());
    }
    Ok(())
}

/// **E-OPEN.** On a `Read` handle, `write_at`, `set_len`, `sync_data`
/// and `sync_all` each give `ReadOnly`, and the content is unchanged.
pub fn read_handle_refuses_writes<E: Env>(env: &E) -> Result<(), String> {
    let w = env
        .open("page", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew page: {e:?}"))?;
    w.write_at(0, b"abc")
        .map_err(|e| format!("write_at: {e:?}"))?;
    drop(w);

    let r = env
        .open("page", OpenMode::Read)
        .map_err(|e| format!("Read page: {e:?}"))?;
    err_is(
        r.write_at(0, b"x"),
        Want::ReadOnly,
        "page",
        "write_at on a Read handle",
    )?;
    err_is(
        r.set_len(0),
        Want::ReadOnly,
        "page",
        "set_len on a Read handle",
    )?;
    err_is(
        r.sync_data(),
        Want::ReadOnly,
        "page",
        "sync_data on a Read handle",
    )?;
    err_is(
        r.sync_all(),
        Want::ReadOnly,
        "page",
        "sync_all on a Read handle",
    )?;
    drop(r);

    let w = env
        .open("page", OpenMode::ReadWrite)
        .map_err(|e| format!("ReadWrite page: {e:?}"))?;
    let len = w.len().map_err(|e| format!("len: {e:?}"))?;
    let mut buf = [0u8; 3];
    let n = w
        .read_at(0, &mut buf)
        .map_err(|e| format!("read_at: {e:?}"))?;
    if len != 3 || n != 3 || buf != *b"abc" {
        return Err(format!(
            "after refused writes the file is {len} bytes, read {buf:?}, wanted b\"abc\""
        ));
    }
    Ok(())
}

/// **E-NAMESPACE.** `create_dir` twice; `exists` for files and the
/// directory; `list_dir` sorted and including `wal`; rename inside
/// `wal/`; rename onto an existing name; cross-directory rename; a
/// missing source; `remove`.
pub fn namespace_ops<E: Env>(env: &E) -> Result<(), String> {
    env.create_dir("wal")
        .map_err(|e| format!("create_dir wal: {e:?}"))?;
    err_is(
        env.create_dir("wal"),
        Want::AlreadyExists,
        "wal",
        "create_dir twice",
    )?;

    let f = env
        .open("page", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew page: {e:?}"))?;
    drop(f);
    for (path, want) in [("page", true), ("wal", true), ("missing", false)] {
        let got = env
            .exists(path)
            .map_err(|e| format!("exists {path}: {e:?}"))?;
        if got != want {
            return Err(format!("exists({path:?}) is {got}, wanted {want}"));
        }
    }

    let f = env
        .open("wal/000.log", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew wal/000.log: {e:?}"))?;
    drop(f);
    let root = env
        .list_dir("")
        .map_err(|e| format!("list_dir '': {e:?}"))?;
    if root != ["page", "wal"] {
        return Err(format!(
            "list_dir('') is {root:?}, wanted [\"page\", \"wal\"]"
        ));
    }
    let wal = env
        .list_dir("wal")
        .map_err(|e| format!("list_dir wal: {e:?}"))?;
    if wal != ["000.log"] {
        return Err(format!("list_dir(wal) is {wal:?}, wanted [\"000.log\"]"));
    }

    // Rename inside wal/ (S-STORE §12.1's recycling rename).
    env.rename("wal/000.log", "wal/001.log")
        .map_err(|e| format!("rename in wal/: {e:?}"))?;
    let (old, new) = (
        env.exists("wal/000.log")
            .map_err(|e| format!("exists: {e:?}"))?,
        env.exists("wal/001.log")
            .map_err(|e| format!("exists: {e:?}"))?,
    );
    if old || !new {
        return Err(format!(
            "after the rename, old exists {old}, new exists {new}"
        ));
    }

    // Rename onto an existing name is AlreadyExists, never a replace.
    for (path, bytes) in [("wal/a", b"AAA"), ("wal/b", b"BBB")] {
        let f = env
            .open(path, OpenMode::CreateNew)
            .map_err(|e| format!("CreateNew {path}: {e:?}"))?;
        f.write_at(0, bytes)
            .map_err(|e| format!("write_at {path}: {e:?}"))?;
        drop(f);
    }
    err_is(
        env.rename("wal/a", "wal/b"),
        Want::AlreadyExists,
        "wal/b",
        "rename onto an existing name",
    )?;
    for (path, want) in [("wal/a", b"AAA".as_slice()), ("wal/b", b"BBB".as_slice())] {
        let f = env
            .open(path, OpenMode::Read)
            .map_err(|e| format!("Read {path}: {e:?}"))?;
        let mut buf = [0u8; 3];
        let n = f
            .read_at(0, &mut buf)
            .map_err(|e| format!("read_at {path}: {e:?}"))?;
        if n != 3 || buf.as_slice() != want {
            return Err(format!(
                "{path} reads {buf:?} after the refused rename, wanted {want:?}"
            ));
        }
    }

    // A cross-directory rename is InvalidPath.
    match env.rename("page", "wal/page") {
        Err(EnvError::InvalidPath { .. }) => {}
        other => {
            return Err(format!(
                "cross-directory rename gave {other:?}, wanted InvalidPath"
            ));
        }
    }
    // A missing source is NotFound.
    err_is(
        env.rename("wal/ghost", "wal/x"),
        Want::NotFound,
        "wal/ghost",
        "rename from a missing source",
    )?;

    // remove, then exists is false.
    env.remove("wal/a")
        .map_err(|e| format!("remove wal/a: {e:?}"))?;
    if env
        .exists("wal/a")
        .map_err(|e| format!("exists wal/a: {e:?}"))?
    {
        return Err("wal/a still exists after remove".to_string());
    }
    Ok(())
}

/// **E-PATH.** `""` for `open`, plus `"/abs"`, `"a//b"`, `"../x"`,
/// `"wal/./x"`, `"a\b"` and `"a\0b"`: each gives `InvalidPath` from
/// `open`, `exists`, `remove`, `rename` and `create_dir`. `""` is
/// accepted by `sync_dir` and `list_dir`.
pub fn invalid_paths_rejected<E: Env>(env: &E) -> Result<(), String> {
    env.create_dir("wal")
        .map_err(|e| format!("create_dir wal: {e:?}"))?;
    let bad = ["", "/abs", "a//b", "../x", "wal/./x", "a\\b", "a\0b"];
    for p in bad {
        let ctx = format!("path {p:?}");
        let is_invalid = |got: Result<(), EnvError>| match got {
            Err(EnvError::InvalidPath { .. }) => Ok(()),
            other => Err(format!("{ctx}: got {other:?}, wanted InvalidPath")),
        };
        is_invalid(env.open(p, OpenMode::Read).map(|_| ()))?;
        is_invalid(env.open(p, OpenMode::ReadWrite).map(|_| ()))?;
        is_invalid(env.open(p, OpenMode::CreateNew).map(|_| ()))?;
        is_invalid(env.exists(p).map(|_| ()))?;
        is_invalid(env.remove(p))?;
        is_invalid(env.create_dir(p))?;
        is_invalid(env.rename(p, "wal/x"))?;
        is_invalid(env.rename("wal/x", p))?;
        if !p.is_empty() {
            // Only the root is accepted by the directory-only methods.
            is_invalid(env.sync_dir(p))?;
            is_invalid(env.list_dir(p).map(|_| ()))?;
        }
    }
    // "" names the root, and sync_dir and list_dir accept it (E-PATH).
    env.sync_dir("")
        .map_err(|e| format!("sync_dir root: {e:?}"))?;
    env.list_dir("")
        .map_err(|e| format!("list_dir root: {e:?}"))?;
    Ok(())
}

/// **E-DURABLE's directory half** (S-2-F2-F1; S-STORE §12.5: "its
/// `wal/` directory entry synced through `Env`"). `sync_dir("")` and
/// `sync_dir("wal")` are `Ok` after a create and a rename;
/// `sync_dir("missing")` is `NotFound`. The durability effect is
/// A-1b's to test: only SimEnv has a power cut.
pub fn sync_dir_root_and_subdir<E: Env>(env: &E) -> Result<(), String> {
    env.create_dir("wal")
        .map_err(|e| format!("create_dir wal: {e:?}"))?;
    let f = env
        .open("wal/000.log", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew wal/000.log: {e:?}"))?;
    drop(f);
    env.rename("wal/000.log", "wal/001.log")
        .map_err(|e| format!("rename: {e:?}"))?;
    env.sync_dir("")
        .map_err(|e| format!("sync_dir root after a create and a rename: {e:?}"))?;
    env.sync_dir("wal")
        .map_err(|e| format!("sync_dir wal after a create and a rename: {e:?}"))?;
    err_is(
        env.sync_dir("missing"),
        Want::NotFound,
        "missing",
        "sync_dir of a missing directory",
    )?;
    Ok(())
}

/// **E-DURABLE's calls on writable handles** (A-1a-F2, mutant G4's
/// substrate). `sync_data`, `sync_all` and `sync_dir` are `Ok` on a
/// `CreateNew` and a `ReadWrite` handle, around a `set_len` and a
/// further write. Their durability effect is A-1b's to test, because
/// only SimEnv has a power cut; here the check is that they succeed.
pub fn sync_on_writable_handles<E: Env>(env: &E) -> Result<(), String> {
    env.create_dir("wal")
        .map_err(|e| format!("create_dir wal: {e:?}"))?;
    let f = env
        .open("wal/seg", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew wal/seg: {e:?}"))?;
    f.write_at(0, b"abc")
        .map_err(|e| format!("write_at wal/seg: {e:?}"))?;
    f.sync_data()
        .map_err(|e| format!("sync_data on a CreateNew handle: {e:?}"))?;
    f.sync_all()
        .map_err(|e| format!("sync_all on a CreateNew handle: {e:?}"))?;
    f.set_len(8).map_err(|e| format!("set_len 8: {e:?}"))?;
    f.sync_all()
        .map_err(|e| format!("sync_all after set_len: {e:?}"))?;
    drop(f);

    let f = env
        .open("wal/seg", OpenMode::ReadWrite)
        .map_err(|e| format!("ReadWrite wal/seg: {e:?}"))?;
    f.write_at(8, b"d")
        .map_err(|e| format!("write_at 8: {e:?}"))?;
    f.sync_data()
        .map_err(|e| format!("sync_data on a ReadWrite handle: {e:?}"))?;
    f.sync_all()
        .map_err(|e| format!("sync_all on a ReadWrite handle: {e:?}"))?;
    drop(f);

    env.sync_dir("wal")
        .map_err(|e| format!("sync_dir wal: {e:?}"))?;
    env.sync_dir("")
        .map_err(|e| format!("sync_dir root: {e:?}"))?;

    let r = env
        .open("wal/seg", OpenMode::Read)
        .map_err(|e| format!("Read wal/seg: {e:?}"))?;
    let mut buf = [0u8; 9];
    let n = r
        .read_at(0, &mut buf)
        .map_err(|e| format!("read_at wal/seg: {e:?}"))?;
    if n != 9 || buf != *b"abc\0\0\0\0\0d" {
        return Err(format!(
            "wal/seg reads {n} bytes {buf:?}, wanted b\"abc\\0\\0\\0\\0\\0d\""
        ));
    }
    Ok(())
}

/// **E-LOCK.** A `ReadWrite` handle holds the exclusive lock; a second
/// handle's `lock_exclusive` and a third, `Read`, handle's
/// `lock_shared` both give `Locked` at once.
pub fn locks_exclusive_excludes_all<E: Env>(env: &E) -> Result<(), String> {
    let f = env
        .open("page", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew page: {e:?}"))?;
    drop(f);
    let w1 = env
        .open("page", OpenMode::ReadWrite)
        .map_err(|e| format!("ReadWrite page: {e:?}"))?;
    w1.lock_exclusive()
        .map_err(|e| format!("lock_exclusive: {e:?}"))?;
    let w2 = env
        .open("page", OpenMode::ReadWrite)
        .map_err(|e| format!("ReadWrite page: {e:?}"))?;
    err_is(
        w2.lock_exclusive(),
        Want::Locked,
        "page",
        "lock_exclusive beside an exclusive holder",
    )?;
    let r = env
        .open("page", OpenMode::Read)
        .map_err(|e| format!("Read page: {e:?}"))?;
    err_is(
        r.lock_shared(),
        Want::Locked,
        "page",
        "lock_shared beside an exclusive holder",
    )?;
    Ok(())
}

/// **E-LOCK, the several-readers choice.** Two `Read` handles both
/// take `lock_shared` (flock semantics; S-STORE §14.1 leaves this to
/// the lock mode's choice).
pub fn locks_shared_readers_coexist<E: Env>(env: &E) -> Result<(), String> {
    let f = env
        .open("page", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew page: {e:?}"))?;
    drop(f);
    let r1 = env
        .open("page", OpenMode::Read)
        .map_err(|e| format!("Read page: {e:?}"))?;
    r1.lock_shared()
        .map_err(|e| format!("first lock_shared: {e:?}"))?;
    let r2 = env
        .open("page", OpenMode::Read)
        .map_err(|e| format!("Read page: {e:?}"))?;
    r2.lock_shared()
        .map_err(|e| format!("second lock_shared: {e:?}"))?;
    Ok(())
}

/// **E-LOCK; S-STORE §14.1's "No writer", in both directions.** A
/// `Read` handle holding the shared lock makes a writer's
/// `lock_exclusive` give `Locked`. After it unlocks and a writer holds
/// the exclusive lock, a new `Read` handle's `lock_shared` gives
/// `Locked`.
pub fn locks_shared_excludes_writer<E: Env>(env: &E) -> Result<(), String> {
    let f = env
        .open("page", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew page: {e:?}"))?;
    drop(f);
    // A shared holder excludes a writer.
    let r = env
        .open("page", OpenMode::Read)
        .map_err(|e| format!("Read page: {e:?}"))?;
    r.lock_shared().map_err(|e| format!("lock_shared: {e:?}"))?;
    let w = env
        .open("page", OpenMode::ReadWrite)
        .map_err(|e| format!("ReadWrite page: {e:?}"))?;
    err_is(
        w.lock_exclusive(),
        Want::Locked,
        "page",
        "lock_exclusive beside a shared holder",
    )?;
    // Released by `unlock` — not by dropping the reader (A-1a-F2: the
    // unlock of a shared lock is the thing under test; `r` stays alive
    // to the end of the check) — the writer takes it; a new reader is
    // then excluded.
    r.unlock()
        .map_err(|e| format!("unlock of a shared lock: {e:?}"))?;
    w.lock_exclusive()
        .map_err(|e| format!("lock_exclusive after the reader unlocked: {e:?}"))?;
    let r2 = env
        .open("page", OpenMode::Read)
        .map_err(|e| format!("Read page: {e:?}"))?;
    err_is(
        r2.lock_shared(),
        Want::Locked,
        "page",
        "lock_shared beside an exclusive holder",
    )?;
    Ok(())
}

/// **E-LOCK.** After `unlock`, and separately after dropping the
/// holder, another handle's `lock_exclusive` succeeds.
pub fn lock_released_on_unlock_and_drop<E: Env>(env: &E) -> Result<(), String> {
    let f = env
        .open("page", OpenMode::CreateNew)
        .map_err(|e| format!("CreateNew page: {e:?}"))?;
    drop(f);
    // unlock releases, and is Ok when the handle holds none.
    let w1 = env
        .open("page", OpenMode::ReadWrite)
        .map_err(|e| format!("ReadWrite page: {e:?}"))?;
    w1.lock_exclusive()
        .map_err(|e| format!("lock_exclusive: {e:?}"))?;
    w1.unlock().map_err(|e| format!("unlock: {e:?}"))?;
    w1.unlock()
        .map_err(|e| format!("unlock with no lock held: {e:?}"))?;
    let w2 = env
        .open("page", OpenMode::ReadWrite)
        .map_err(|e| format!("ReadWrite page: {e:?}"))?;
    w2.lock_exclusive()
        .map_err(|e| format!("lock_exclusive after unlock: {e:?}"))?;
    drop(w2);
    // Dropping the holder releases its lock.
    let w3 = env
        .open("page", OpenMode::ReadWrite)
        .map_err(|e| format!("ReadWrite page: {e:?}"))?;
    w3.lock_exclusive()
        .map_err(|e| format!("lock_exclusive: {e:?}"))?;
    drop(w3);
    let w4 = env
        .open("page", OpenMode::ReadWrite)
        .map_err(|e| format!("ReadWrite page: {e:?}"))?;
    w4.lock_exclusive()
        .map_err(|e| format!("lock_exclusive after the holder dropped: {e:?}"))?;
    Ok(())
}

/// **E-LOCK; mutant RO-LOCK-FILE's substrate.** `list_dir("")` and
/// `list_dir("wal")` are identical before and after each lock mode:
/// S-STORE §14.1's "the lock creates and writes no file".
pub fn lock_creates_no_file<E: Env>(env: &E) -> Result<(), String> {
    env.create_dir("wal")
        .map_err(|e| format!("create_dir wal: {e:?}"))?;
    for path in ["page", "wal/seg"] {
        let f = env
            .open(path, OpenMode::CreateNew)
            .map_err(|e| format!("CreateNew {path}: {e:?}"))?;
        drop(f);
    }
    let before_root = env
        .list_dir("")
        .map_err(|e| format!("list_dir '': {e:?}"))?;
    let before_wal = env
        .list_dir("wal")
        .map_err(|e| format!("list_dir wal: {e:?}"))?;
    let r = env
        .open("page", OpenMode::Read)
        .map_err(|e| format!("Read page: {e:?}"))?;
    r.lock_shared().map_err(|e| format!("lock_shared: {e:?}"))?;
    let after_shared_root = env
        .list_dir("")
        .map_err(|e| format!("list_dir '': {e:?}"))?;
    let after_shared_wal = env
        .list_dir("wal")
        .map_err(|e| format!("list_dir wal: {e:?}"))?;
    let w = env
        .open("wal/seg", OpenMode::ReadWrite)
        .map_err(|e| format!("ReadWrite wal/seg: {e:?}"))?;
    w.lock_exclusive()
        .map_err(|e| format!("lock_exclusive: {e:?}"))?;
    let after_excl_root = env
        .list_dir("")
        .map_err(|e| format!("list_dir '': {e:?}"))?;
    let after_excl_wal = env
        .list_dir("wal")
        .map_err(|e| format!("list_dir wal: {e:?}"))?;
    if after_shared_root != before_root
        || after_shared_wal != before_wal
        || after_excl_root != before_root
        || after_excl_wal != before_wal
    {
        return Err(format!(
            "a lock changed the namespace: root {before_root:?} -> shared {after_shared_root:?} / exclusive {after_excl_root:?}; wal {before_wal:?} -> shared {after_shared_wal:?} / exclusive {after_excl_wal:?}"
        ));
    }
    Ok(())
}

/// **E-CLOCK/RAND.** `now` never decreases; after `sleep(1 ms)`, `now`
/// has grown by at least 1 ms; two `rand` calls differ.
pub fn clock_and_rand<E: Env>(env: &E) -> Result<(), String> {
    let t0 = env.now();
    let t1 = env.now();
    if t1 < t0 {
        return Err(format!("now went backwards: {t0:?} then {t1:?}"));
    }
    env.sleep(Duration::from_millis(1));
    let t2 = env.now();
    if t2.saturating_sub(t1) < Duration::from_millis(1) {
        return Err(format!(
            "sleep(1ms) advanced now by only {:?}",
            t2.saturating_sub(t1)
        ));
    }
    let (a, b) = (env.rand(), env.rand());
    if a == b {
        return Err(format!("two rand calls both returned {a}"));
    }
    Ok(())
}
