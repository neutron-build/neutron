//! C-T6e3 item 3: the online-build crash matrix — crashes at **every
//! phase of the §7 ladder** with a concurrent `synchronous_commit = off`
//! writer, reopened through `Db::open` (whose cleanup runs before the
//! handle returns) and, for CONCURRENTLY builds, completed with
//! `resume_build`.
//!
//! The scenes park the real driver ([`create_index_online`]) at a
//! phase-boundary hook — after T0 (DeleteOnly committed), after T1
//! (WriteOnly), after the fence, and before the publish (the chunked
//! ingest done) — then the `Off` writer commits (from the test thread,
//! or from the hook itself where the phase demands it), and the crash
//! cuts the unsynced tail at **every** `keep` in `0..=unsynced_len()`
//! (the t6c_crash pattern: a probe build measures the deterministic
//! tail, each cell rebuilds the scene).
//!
//! One scene per ingest chunk — "after each ingest chunk (chunk size 2
//! via `BuildHooks`)": `BuildHooks` has no per-chunk callback (a seam
//! this card cannot add, `src/` being off-limits; see the card report),
//! so the scene parks the driver after T1 and hand-runs T3's pieces on
//! the test thread — the same §7 step-5/6 primitives the driver calls
//! (the §4 scan at the registered `S`, the §6 entry keys, sorted, then
//! the **real** `Core::ingest_versions` for the first k chunks of 2) —
//! then crashes with k of the chunks durable. The completion is the
//! real driver's: `resume_build` re-runs the fence and the whole of T3
//! over whatever survived.
//!
//! What every cell asserts, from the surviving committed set (the
//! reopened store's `/sys/txn` records) — never from scheduling:
//!
//! - the CONCURRENTLY index survived `Db::open`'s cleanup in its parked
//!   state (DeleteOnly after T0, WriteOnly after T1/fence/ingest
//!   chunks), and the plain variant's was dropped before the handle
//!   returned (§7 Failure, §9.5);
//! - amcheck is clean before the resume — I-IDX-DO over the DeleteOnly
//!   scene, I-IDX-WO over every WriteOnly one: whatever survived of
//!   the `Off` writer's cut footprint agrees with its rows;
//! - `resume_build` completes, publishes (`built_at > 0`), amcheck is
//!   clean (I-IDX), `index_scan` equals the full scan, and both equal
//!   the model built from the surviving records;
//! - the sweep saw both outcomes of the scan-seen writer's commit
//!   (keep=0 loses its record, keep=n keeps it) in every variant.
//!
//! The G8 shape is the point of the before-publish and chunk scenes: a
//! `sync off` writer the build's scan saw commits at `c <= S`; §7 step
//! 6's **WAL sync** is what makes its record durable before any entry
//! derived from it, so no cut can leave the durably-ingested entry
//! without its row. Mutant G8 (the sync skipped): a low `keep` drops
//! the writer's record while the ingested entry survives — the boot
//! sweep discards its row, and amcheck reports the dangling entry
//! (I-IDX-WO before the resume, I-IDX after it).

mod t6b_support;

use std::ops::Bound;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use t6b_support::build_rig::BuildRig;
use t6b_support::{i32_d, i32_v, i_entry_key, idx_def, ok};

use nucleus_catalog::{
    create_index_online, resume_build, BuildHooks, IndexDef, IndexState, ScannedRow, Stmt,
};
use nucleus_codec::row::{ColumnDef, Datum, TableSchema};
use nucleus_codec::{KeyColumn, KeyType, Value};
use nucleus_txn::catalog::{index_ranges, IdxRow, IdxStorage};
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::jobs::{BootSweep, JobOutcome};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::read::{scan, NoSsi};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Ts, TxnId};

/// Everything a thread may wait on is bounded by this; a hang is a
/// failure, never a pass.
const BOUND: Duration = Duration::from_secs(30);

/// The concurrent writer's flavour this rig runs on.
type Db = nucleus_catalog::Db<t6b_support::SharedFault>;

/// The baseline rows: `(id, a)`, `id` the int4 pk, `a` the indexed
/// int4 (`a = 10 + id`, so the entry keys are legible).
const ROWS: [(i32, i32); 6] = [(1, 11), (2, 12), (3, 13), (4, 14), (5, 15), (6, 16)];

/// The `Off` writer's rows: id 7 is the scan-seen one (committed at
/// `c <= S`, from the fence hook or the test thread), id 8 the
/// post-ingest one (never scanned).
const W_SCAN: (i32, i32) = (7, 70);
const W_LATE: (i32, i32) = (8, 80);

// ---- fixtures ----------------------------------------------------------------

/// `CREATE TABLE t (id int4 PRIMARY KEY, a int4)` — the indexed column
/// is attnum 2, int-valued so the §6 entry key is hand-computable (the
/// `i_entry_key` oracle).
fn a_schema() -> TableSchema {
    ok(TableSchema::new(
        vec![
            ColumnDef {
                attnum: 1,
                name: "id".into(),
                ty: KeyType::Int4,
                not_null: true,
                dropped: false,
                missing: None,
            },
            ColumnDef {
                attnum: 2,
                name: "a".into(),
                ty: KeyType::Int4,
                not_null: false,
                dropped: false,
                missing: None,
            },
        ],
        vec![1],
    ))
}

/// The non-unique index definition on `a`; the driver overrides state
/// and built_at.
fn a_def() -> IndexDef {
    idx_def(2, false, IndexState::DeleteOnly)
}

/// The table plus one committed `On` row per [`ROWS`] entry, each its
/// own txn.
fn table_a(rig: &BuildRig) -> u64 {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let (oid, _) = ok(rig.db.create_table(&st, b"mx3", &a_schema()));
    ok(rig.commit_rc(t));
    for &(id, a) in &ROWS {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(id), i32_d(a)]));
        ok(rig.commit_rc(t));
    }
    oid
}

/// One `synchronous_commit = off` insert of `(id, a)` in its own txn,
/// with **no** resolver pass: its intents, its `/sys/txn` record and
/// whatever a build's ingest resolved of it all join the unsynced tail
/// the crash cuts. Returns the writer's id.
fn off_insert(db: &Arc<Db>, oid: u64, id: i32, a: i32) -> TxnId {
    let t = db.core.begin(Isolation::ReadCommitted);
    let out = t.id;
    let st = ok(Stmt::new(&t, db.core.visible_ts()));
    let h = ok(db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(db.insert_with(&st, &h, &[i32_d(id), i32_d(a)]));
    ok(db.core.commit(t, SyncCommit::Off));
    out
}

/// A hook that signals it arrived and then parks forever: the build
/// thread stays inside its phase (its committed prefix survives even
/// `crash(0)` — every build commit is `SyncCommit::On`), and never
/// touches the store again, so the crashed state is exactly the parked
/// phase's.
fn park_hook(tx: mpsc::Sender<()>) -> Option<Box<dyn Fn() + Send + Sync>> {
    Some(Box::new(move || {
        let _ = tx.send(());
        loop {
            thread::park();
        }
    }))
}

/// The boot sweep driven to `Finished` plus one resolver pass — the
/// reopen sequence that settles every older-epoch intent (§7.2).
fn sweep(rig: &BuildRig) {
    let mut sweep = BootSweep::new(Arc::clone(rig.core()));
    // The walk is over a finite store; a bound only turns a livelock
    // into a test failure.
    for _ in 0..100_000 {
        match ok(sweep.run_once()) {
            JobOutcome::Finished => break,
            JobOutcome::Progress | JobOutcome::Idle => continue,
        }
    }
    ok(Resolver::run_once(rig.core()));
}

// ---- the hand-run T3 pieces (the per-chunk scene) ------------------------------

/// The index row's entry-storage id.
fn isid_of(row: &IdxRow) -> u64 {
    match row.storage {
        IdxStorage::Id(isid) => isid,
        IdxStorage::Range(..) => panic!("an online-built index has an id storage"),
    }
}

/// §7 steps 4-6 on the test thread, with the driver parked after T1:
/// register `S`, scan every row live at `S` through the same §4 read
/// path the driver's scan uses, compute each row's §6 entry key
/// (non-unique `/i/`: the encoded `a` and the row key, empty payload),
/// sort by stored key, and ingest the first `chunks` chunks of 2
/// through the **real** `Core::ingest_versions` — its span resolution
/// and its WAL sync are the behaviours under crash test. Returns the
/// registered `S` and the number of chunks ingested.
fn hand_ingest(rig: &BuildRig, sid: u64, isid: u64, chunks: usize) -> (Ts, usize) {
    let guard = rig.core().registry.take_snapshot();
    let s = guard.ts();
    let ctx = ReadCtx {
        txn: TxnId {
            epoch: rig.core().epoch(),
            n: u64::MAX,
        },
        snapshot: s,
        stmt_seq: 0,
    };
    let lo = nucleus_catalog::table_lo(sid);
    let mut hi = lo.clone();
    hi.push(0xFF); // §5: chunks are never rows
    let view = rig.core().open_view();
    let rows = ok(scan(
        rig.core(),
        &view,
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        &ctx,
        &mut NoSsi,
    )
    .collect::<Result<Vec<_>, _>>());
    drop(view);
    let schema = a_schema();
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = rows
        .iter()
        .map(|(row_key, bytes)| {
            let live = ok(schema.decode_row(bytes));
            let a = match live.get(1) {
                Some(Some(Datum::Inline(Value::Int4(a)))) => *a,
                other => panic!("a datum: {other:?}"),
            };
            (
                i_entry_key(isid, &[KeyColumn::asc(KeyType::Int4)], &[i32_v(a)], row_key).to_vec(),
                Vec::new(),
            )
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let ranges: Vec<(Vec<u8>, Vec<u8>)> = index_ranges(isid);
    let mut done = 0usize;
    for part in entries.chunks(2) {
        if done == chunks {
            break;
        }
        let mut chunk = part.iter().map(|(k, v)| (k.clone(), v.clone()));
        ok(rig.core().ingest_versions(&guard, &ranges, &mut chunk));
        done += 1;
    }
    drop(guard);
    (s, done)
}

// ---- the survivors and the model ------------------------------------------------

/// Whether each given txn's `/sys/txn` commit record the reopened store
/// still holds (read before the sweep, which may truncate released
/// records — the t6c_crash discipline).
fn writer_survived(rig: &BuildRig, ids: &[TxnId]) -> Vec<bool> {
    use nucleus_txn::encoding::{parse_sys_txn_key, sys_txn_prefix, sys_txn_prefix_end};
    let view = rig.core().open_view();
    let mut present: Vec<TxnId> = Vec::new();
    for row in view.scan(
        (
            Bound::Included(sys_txn_prefix().as_slice()),
            Bound::Excluded(sys_txn_prefix_end().as_slice()),
        ),
        false,
    ) {
        let (k, _) = ok(row);
        if let Some(id) = parse_sys_txn_key(&k) {
            present.push(id);
        }
    }
    ids.iter().map(|id| present.contains(id)).collect()
}

/// The model: the baseline rows plus every scene writer whose record
/// survived, by id.
fn model_rows(writers: &[(bool, (i32, i32))]) -> Vec<(i32, i32)> {
    let mut out: Vec<(i32, i32)> = ROWS.to_vec();
    for &(survived, row) in writers {
        if survived {
            out.push(row);
        }
    }
    out.sort();
    out
}

/// A full scan of `oid` at the latest state, as sorted `(id, a)` pairs.
fn scan_a(rig: &BuildRig, oid: u64) -> Vec<(i32, i32)> {
    let t = rig.rc();
    let st = rig.stmt(&t, rig.core().visible_ts());
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st, &h));
    ok(rig.commit_rc(t));
    rows_a(&rows)
}

/// `index_scan` over the whole index at the latest state.
fn iscan_all(rig: &BuildRig, idx_oid: u64) -> Vec<(i32, i32)> {
    let t = rig.rc();
    let st = rig.stmt(&t, rig.core().visible_ts());
    let rows = ok(rig
        .db
        .index_scan(&st, idx_oid, Bound::Unbounded, Bound::Unbounded));
    ok(rig.commit_rc(t));
    rows_a(&rows)
}

fn rows_a(rows: &[ScannedRow]) -> Vec<(i32, i32)> {
    let mut out = Vec::with_capacity(rows.len());
    for (_, live) in rows {
        let id = match live.first() {
            Some(Some(Datum::Inline(Value::Int4(n)))) => *n,
            other => panic!("id datum: {other:?}"),
        };
        let a = match live.get(1) {
            Some(Some(Datum::Inline(Value::Int4(n)))) => *n,
            other => panic!("a datum: {other:?}"),
        };
        out.push((id, a));
    }
    out.sort();
    out
}

fn amcheck(rig: &BuildRig, oid: u64, where_: &str) {
    let report = rig.amcheck(oid);
    assert!(
        report.is_clean(),
        "{where_}: amcheck: {:?}",
        report.violations
    );
}

// ---- the scenes ----------------------------------------------------------------

/// Where the driver is parked when the crash lands (the card's phase
/// list; the per-chunk scene parks at AfterT1 and hand-runs T3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    AfterT0,
    AfterT1,
    AfterFence,
    BeforePublish,
}

/// One scene's outcome: everything the cells need that was built before
/// the crash.
struct Scene {
    rig: BuildRig,
    oid: u64,
    /// The `Off` writers, in model order, with their rows.
    writers: Vec<TxnId>,
}

/// Builds one scene deterministically: the table and its `On` rows, the
/// driver parked at `phase` (CONCURRENTLY unless `plain`), and the
/// scene's `Off` writers committed at their exact spots:
///
/// - AfterT0/AfterT1/AfterFence: the scan-seen writer commits from the
///   **test thread** while the driver is parked (DeleteOnly-aware for
///   AfterT0 — its insert maintains no entry; WriteOnly-aware
///   otherwise);
/// - BeforePublish: the scan-seen writer commits from the
///   **after_fence hook** (before T3 registers `S`, so the scan at `S`
///   sees its row — the G8 shape), and the late writer commits from the
///   before_publish hook just before it parks (its row is never
///   scanned; its footprint is the tail the cuts hit under the correct
///   code).
fn build_scene(phase: Phase, plain: bool) -> Scene {
    let rig = BuildRig::new();
    let oid = table_a(&rig);

    let mut hooks = BuildHooks::default();
    let (tx, rx) = mpsc::channel::<()>();
    // The hook-driven writers' ids, handed to the test thread once the
    // park signal proves their hooks ran.
    let scan_id: Arc<Mutex<Option<TxnId>>> = Arc::new(Mutex::new(None));
    let late_id: Arc<Mutex<Option<TxnId>>> = Arc::new(Mutex::new(None));
    match phase {
        Phase::AfterT0 => hooks.after_t0 = park_hook(tx),
        Phase::AfterT1 => hooks.after_t1 = park_hook(tx),
        Phase::AfterFence => hooks.after_fence = park_hook(tx),
        Phase::BeforePublish => {
            let db = Arc::clone(&rig.db);
            let slot = Arc::clone(&scan_id);
            hooks.after_fence = Some(Box::new(move || {
                let id = off_insert(&db, oid, W_SCAN.0, W_SCAN.1);
                *slot.lock().expect("the scan-writer slot") = Some(id);
            }));
            let db = Arc::clone(&rig.db);
            let slot = Arc::clone(&late_id);
            hooks.before_publish = Some(Box::new(move || {
                let id = off_insert(&db, oid, W_LATE.0, W_LATE.1);
                *slot.lock().expect("the late-writer slot") = Some(id);
                let _ = tx.send(());
                loop {
                    thread::park();
                }
            }));
        }
    }

    let db = Arc::clone(&rig.db);
    thread::spawn(move || {
        let _ = create_index_online(&db, oid, &a_def(), !plain, &hooks);
    });
    match rx.recv_timeout(BOUND) {
        Ok(()) => {}
        Err(_) => panic!("the build never reached its {phase:?} park"),
    }

    // The writers, in model order: the phase's test-thread writer, or
    // the hook-driven ones (their slots are filled — the park signal
    // follows their commits).
    let writers = match phase {
        Phase::BeforePublish => vec![
            scan_id
                .lock()
                .expect("the scan-writer slot")
                .expect("the fence hook committed the scan-seen writer"),
            late_id
                .lock()
                .expect("the late-writer slot")
                .expect("the publish hook committed the late writer"),
        ],
        _ => vec![off_insert(&rig.db, oid, W_SCAN.0, W_SCAN.1)],
    };
    Scene { rig, oid, writers }
}

/// The writers' rows, in [`Scene::writers`] order.
fn writer_rows(phase: Phase) -> Vec<(i32, i32)> {
    match phase {
        Phase::BeforePublish => vec![W_SCAN, W_LATE],
        _ => vec![W_SCAN],
    }
}

/// One matrix cell: rebuild the scene, crash keeping `keep` unsynced
/// batches, reopen through `Db::open`, and check everything the card
/// names. Returns whether each writer's record survived.
fn cell(phase: Phase, keep: usize) -> Vec<bool> {
    let where_ = format!("{phase:?} / keep {keep}");
    let scene = build_scene(phase, false);
    let n = scene.rig.kv.unsynced_len();
    assert!(n > 0, "{where_}: the scene must build an unsynced tail");
    let keep = keep.min(n);
    let Scene { rig, oid, writers } = scene;
    let rig = rig.crash_reopen(keep);

    // The surviving committed set, before the sweep can truncate.
    let survived = writer_survived(&rig, &writers);
    // Db::open's cleanup kept the CONCURRENTLY index in its parked state.
    let idxs = rig.idxs_of(oid);
    assert_eq!(idxs.len(), 1, "{where_}: the CONCURRENTLY index survived");
    let idx_oid = idxs[0].oid;
    let def = rig.def_of(idx_oid);
    let want_state = match phase {
        Phase::AfterT0 => IndexState::DeleteOnly,
        Phase::AfterT1 | Phase::AfterFence | Phase::BeforePublish => IndexState::WriteOnly,
    };
    assert_eq!(def.state, want_state, "{where_}: the parked state");
    assert!(def.concurrently, "{where_}: the def carries the flag");

    sweep(&rig);
    amcheck(&rig, oid, &where_);
    let model: Vec<(bool, (i32, i32))> = survived.iter().copied().zip(writer_rows(phase)).collect();
    let want = model_rows(&model);
    assert_eq!(scan_a(&rig, oid), want, "{where_}: scan vs the model");

    // The remedy (§7): resume completes over the crashed prefix and
    // publishes, and the index agrees with the table.
    assert_eq!(
        ok(resume_build(&rig.db, idx_oid, &BuildHooks::default())),
        idx_oid,
        "{where_}: resume_build completes"
    );
    let def = rig.def_of(idx_oid);
    assert_eq!(def.state, IndexState::Public, "{where_}");
    assert!(def.built_at > 0, "{where_}: built_at is the resumed S");
    amcheck(&rig, oid, &where_);
    assert_eq!(
        iscan_all(&rig, idx_oid),
        scan_a(&rig, oid),
        "{where_}: index_scan == scan after the resume"
    );
    assert_eq!(scan_a(&rig, oid), want, "{where_}: the model, still");
    survived
}

/// The per-chunk scene: the driver parked after T1, `chunks` chunks of
/// the ingest hand-run through the real `Core::ingest_versions`, the
/// late `Off` writer committed after the ingest (its footprint is the
/// tail the cuts hit under the correct code — the scan-seen writer's
/// was made durable by the ingest's WAL sync, which is exactly what
/// mutant G8 skips), then the crash at `keep`.
fn chunk_scene(chunks: usize) -> (Scene, Ts, usize) {
    let scene = build_scene(Phase::AfterT1, false);
    let idx = scene.rig.idxs_of(scene.oid).remove(0);
    let isid = isid_of(&idx);
    let sid = scene
        .rig
        .rel_row(scene.oid)
        .expect("the rel row")
        .storage_id;
    let (s, done) = hand_ingest(&scene.rig, sid, isid, chunks);
    assert_eq!(done, chunks, "every requested chunk ingested");
    let late = off_insert(&scene.rig.db, scene.oid, W_LATE.0, W_LATE.1);
    let mut writers = scene.writers.clone();
    writers.push(late);
    (
        Scene {
            rig: scene.rig,
            oid: scene.oid,
            writers,
        },
        s,
        done,
    )
}

/// One per-chunk matrix cell.
fn chunk_cell(chunks: usize, keep: usize) -> Vec<bool> {
    let where_ = format!("chunks {chunks} / keep {keep}");
    let (scene, _s, _done) = chunk_scene(chunks);
    let n = scene.rig.kv.unsynced_len();
    assert!(n > 0, "{where_}: the scene must build an unsynced tail");
    let keep = keep.min(n);
    let Scene { rig, oid, writers } = scene;
    let rig = rig.crash_reopen(keep);

    let survived = writer_survived(&rig, &writers);
    let idxs = rig.idxs_of(oid);
    assert_eq!(idxs.len(), 1, "{where_}: the CONCURRENTLY index survived");
    let idx_oid = idxs[0].oid;
    assert_eq!(
        rig.def_of(idx_oid).state,
        IndexState::WriteOnly,
        "{where_}: the publish never ran"
    );

    sweep(&rig);
    amcheck(&rig, oid, &where_);
    let model: Vec<(bool, (i32, i32))> = survived.iter().copied().zip([W_SCAN, W_LATE]).collect();
    let want = model_rows(&model);
    assert_eq!(scan_a(&rig, oid), want, "{where_}: scan vs the model");

    assert_eq!(
        ok(resume_build(&rig.db, idx_oid, &BuildHooks::default())),
        idx_oid,
        "{where_}: resume completes over the partial ingest"
    );
    assert_eq!(rig.def_of(idx_oid).state, IndexState::Public);
    amcheck(&rig, oid, &where_);
    assert_eq!(
        iscan_all(&rig, idx_oid),
        scan_a(&rig, oid),
        "{where_}: index_scan == scan after the resume"
    );
    assert_eq!(scan_a(&rig, oid), want, "{where_}: the model, still");
    survived
}

// ---- the tests ------------------------------------------------------------------

/// The matrix: every phase × every keep, plus every ingest-chunk
/// boundary (chunks of 2 over the 7 scanned rows — 6 baseline + the
/// scan-seen writer's — = 4 chunks), each cell a fresh deterministic
/// scene. Every sweep must reach both outcomes of the scan-seen
/// writer's commit: keep=0 loses its record, keep=n keeps it.
#[test]
fn t6e3_crash_every_phase_every_keep() {
    let phases = [
        Phase::AfterT0,
        Phase::AfterT1,
        Phase::AfterFence,
        Phase::BeforePublish,
    ];
    for phase in phases {
        // The probe measures the deterministic tail; each cell rebuilds.
        let probe = build_scene(phase, false);
        let n = probe.rig.kv.unsynced_len();
        drop(probe);
        assert!(n > 0, "{phase:?}: the scene must build an unsynced tail");
        // The both-outcomes pin names the writer whose footprint the
        // tail is: the test-thread writer for the parked phases, the
        // late one for BeforePublish — the scan-seen writer of that
        // scene is durable at every cut by construction (the ingest's
        // WAL sync precedes the crash; skipping it is mutant G8, which
        // the amchecks above catch as a dangling entry).
        let tail_writer = if phase == Phase::BeforePublish { 1 } else { 0 };
        let mut saw_all = false;
        let mut saw_none = false;
        let mut cells = 0usize;
        for keep in 0..=n {
            cells += 1;
            let survived = cell(phase, keep);
            if survived[tail_writer] {
                saw_all = true;
            } else {
                saw_none = true;
            }
        }
        assert_eq!(cells, n + 1, "{phase:?}: every keep in 0..={n} ran");
        assert!(
            saw_all && saw_none,
            "{phase:?}: the sweep (n = {n}) must reach both outcomes of the tail writer's commit"
        );
    }

    // The per-chunk cells: crash after each ingest chunk.
    let chunks = ROWS.len() / 2 + 1; // 7 rows scanned, chunks of 2
    for k in 1..=chunks {
        let (probe, _, _) = chunk_scene(k);
        let n = probe.rig.kv.unsynced_len();
        drop(probe);
        assert!(n > 0, "chunks {k}: the scene must build an unsynced tail");
        let mut saw_all = false;
        let mut saw_none = false;
        for keep in 0..=n {
            let survived = chunk_cell(k, keep);
            if survived[1] {
                saw_all = true;
            } else {
                saw_none = true;
            }
        }
        assert!(
            saw_all && saw_none,
            "chunks {k}: the sweep (n = {n}) must reach both outcomes of the late writer's commit"
        );
    }
}

/// The plain-build variant: a crashed plain build's non-Public index is
/// dropped by `Db::open`'s cleanup **before the handle returns** (§7
/// Failure, §9.5), the `Off` writer's fate still follows the surviving
/// records, and the table is left sound at every keep.
#[test]
fn t6e3_crash_plain_build_dropped_at_open() {
    let probe = build_scene(Phase::AfterT1, true);
    let n = probe.rig.kv.unsynced_len();
    drop(probe);
    assert!(n > 0, "the scene must build an unsynced tail");
    for keep in 0..=n {
        let scene = build_scene(Phase::AfterT1, true);
        let keep = keep.min(scene.rig.kv.unsynced_len());
        let Scene { rig, oid, writers } = scene;
        let rig = rig.crash_reopen(keep);
        assert!(
            rig.idxs_of(oid).is_empty(),
            "keep {keep}: Db::open dropped the crashed plain build"
        );
        let survived = writer_survived(&rig, &writers);
        sweep(&rig);
        amcheck(&rig, oid, "plain / dropped");
        let want = model_rows(&[(survived[0], W_SCAN)]);
        assert_eq!(scan_a(&rig, oid), want, "keep {keep}: scan vs the model");
    }
}
