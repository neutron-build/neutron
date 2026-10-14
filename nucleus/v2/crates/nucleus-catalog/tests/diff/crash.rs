//! C-T6c2 item 2: the DDL crash matrix (§7.2, §9.2) — the scene
//! builders, the surviving-commit model and the post-crash checks the
//! `t6c_crash` binary drives over the `t6b_support` hand-driven rig
//! (a `Fault<MemKv>` with the commit pipeline, SSI, the lock manager
//! and the catalog).
//!
//! Every scene is deterministic: a durable baseline (the table — and
//! for DROP COLUMN its dependent Public index, for the born-public and
//! rowid scenarios the no-PK table — plus the scenario's rows, all
//! committed `synchronous_commit = on`), then the action txn **begun
//! first**, the one concurrent committed DML txn under it (`on` in one
//! variant of the scene, `off` in the other, so its record and intents
//! join the unsynced tail the crash cuts), the action txn's statement
//! and its own `off` commit, and a resolver pass. A crash keeping any
//! prefix of the tail — every `keep` in `0..=unsynced_len()`, plus the
//! seeded draws — followed by the full reopen (Core, Ssi, LockManager,
//! `Catalog::open`) and the §7.2 sweep must then give:
//!
//! - the definition is old-or-new, exactly per the surviving committed
//!   set (an ALTER's def bump, TRUNCATE's storage move, the index rows
//!   that travel with a dropped column or a born-public CREATE INDEX);
//! - every acked `on` commit survived (I-DURABLE);
//! - every row decodes and amcheck is clean (I-ROWFMT, I-IDX);
//! - the model built from the surviving committed set — read from the
//!   reopened store's `/sys/txn` records — equals a full scan (rows by
//!   identity, and a born index's visible entries).
//!
//! One honest consequence of the WAL contract runs through two scenes:
//! a `Durability::Yes` write (the id counters TRUNCATE and CREATE INDEX
//! allocate inside the action txn) makes the whole unsynced queue
//! durable, so the concurrent `off` DML commit of those two scenes is
//! already durable by the action txn's statement. The matrix cuts
//! everything after it exactly as before; only the DML txn's fate stops
//! varying.
//!
//! Mutation evidence (binding, card item 3): **E5** — `drop_column`
//! dropping the dependent index with a `Core::write` outside the txn —
//! dies on the DropColumn scene's old-or-new assertion (a low `keep`
//! keeps the outside-the-txn delete but not the DDL's commit record:
//! the definition is old while the index row is gone). **E6** —
//! born-public entries written outside the txn — dies on the
//! CreateIndex scene's entry check (an entry without its row survives a
//! cut that loses the creating txn's commit).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use crate::t6b_support::{
    commit_off, decode_def, i32_d, i32_v, i_entry_key, idx_def, ok, pk_schema, row_key_of,
    rowid_schema, set_col, sweep, text_d, Rig,
};

use nucleus_catalog::amcheck::check_table;
use nucleus_catalog::{IndexDef, IndexState, Stmt, TOAST_CHUNK, TOAST_MARK, TOAST_THRESHOLD};
use nucleus_codec::row::{value_repr_eq, Datum, TableSchema};
use nucleus_codec::{Collation, KeyColumn, KeyType, Value};
use nucleus_txn::catalog::{table_range, IdxStorage};
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::read::{scan, NoSsi};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Ts, TxnId, TxnStatus};

/// The ADD COLUMN default the AddColumn scene plants (int4).
const ADD_DEFAULT: i32 = 77;

/// `CREATE TABLE mx_t (id int4 PRIMARY KEY, payload text, big text)` —
/// the LargeValues scene's wide table (C-T6d2 §5).
fn wide_pk_schema() -> TableSchema {
    use nucleus_codec::row::ColumnDef;
    let col = |attnum: u16, name: &str, ty: KeyType, not_null: bool| ColumnDef {
        attnum,
        name: name.into(),
        ty,
        not_null,
        dropped: false,
        missing: None,
    };
    ok(TableSchema::new(
        vec![
            col(1, "id", KeyType::Int4, true),
            col(
                2,
                "payload",
                KeyType::Text(Collation::C),
                false,
            ),
            col(3, "big", KeyType::Text(Collation::C), false),
        ],
        vec![1],
    ))
}

/// One large text of `len` bytes, all the letter `'a' + tag % 26` — the
/// scene's `big` values, one byte per char so the length is the byte
/// length (t6d_toast's `blob`).
fn big_text(tag: u8, len: usize) -> String {
    char::from(b'a' + (tag % 26)).to_string().repeat(len)
}

/// Which scene the matrix crashes inside (card item 2's list).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    AddColumn,
    /// With a dependent Public index on the dropped column.
    DropColumn,
    SetNotNull,
    Truncate,
    /// Born-public CREATE INDEX on an empty pre-existing table (§6.1
    /// case (b)): the creating txn's own uncommitted row is the one row
    /// that gets an entry.
    CreateIndex,
    /// A plain insert into the no-PK (hidden-rowid) table.
    InsertU,
    /// C-T6d2: the large-value scene (§5) — the action txn does a large
    /// insert, a large update (a replace: the old chunks tombstone) and
    /// a large delete over rows whose `big` values are chunked, with the
    /// concurrent DML txn committing a large insert of its own.
    LargeValues,
}

/// Where one cell crashes: an explicit prefix of the unsynced tail, or
/// `Fault::crash_seeded`'s draw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashAt {
    Keep(usize),
    Seeded(u64),
}

/// What one deterministic scene build captured.
#[derive(Debug, Clone)]
pub struct Scene {
    pub scenario: Scenario,
    pub t_oid: u64,
    pub u_oid: Option<u64>,
    /// DropColumn's dependent index, or CreateIndex's born-public one.
    pub idx_oid: Option<u64>,
    /// The born index's storage id (read pre-crash; the allocation is
    /// deterministic, so every cell rebuilds the same one).
    pub idx_isid: Option<u64>,
    /// The born index's `built_at` (§6.1 (b): the `S_e` its Share lock
    /// registered).
    pub idx_built_at: Option<u64>,
    /// `t`'s storage id before the action txn (TRUNCATE's "old").
    pub old_sid: u64,
    /// TRUNCATE's new storage id, allocated inside the action txn.
    pub new_sid: Option<u64>,
    /// Every txn the scene acked a `SyncCommit::On` commit of.
    pub acked_on: Vec<(TxnId, Ts)>,
    /// The concurrent DML txn: id, ts, its sync mode.
    pub b: (TxnId, Ts, SyncCommit),
    /// The action txn: id, ts (always committed `off`).
    pub a: (TxnId, Ts),
}

// ----- the scene ----------------------------------------------------------------

/// One committed `on` row `(id, payload)` in its own txn.
fn add_row_on(rig: &Rig, oid: u64, id: i32, payload: &str) -> (TxnId, Ts) {
    let t = rig.rc();
    let txn_id = t.id;
    let st = rig.stmt(&t, rig.core().visible_ts());
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st, &h, &[i32_d(id), text_d(payload)]));
    let ts = ok(rig.commit_rc(t));
    (txn_id, ts)
}

/// One committed `on` wide row `(id, payload, big)` in its own txn — the
/// LargeValues scene's baseline writer.
fn add_big_row_on(rig: &Rig, oid: u64, id: i32, payload: &str, tag: u8, len: usize) -> (TxnId, Ts) {
    let t = rig.rc();
    let txn_id = t.id;
    let st = rig.stmt(&t, rig.core().visible_ts());
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(
        &st,
        &h,
        &[i32_d(id), text_d(payload), text_d(&big_text(tag, len))],
    ));
    let ts = ok(rig.commit_rc(t));
    (txn_id, ts)
}

/// Builds one matrix scene (see the module docs for the shape).
pub fn build_scene(scenario: Scenario, b_sync: SyncCommit) -> (Rig, Scene) {
    let rig = Rig::new();
    let mut acked_on = Vec::new();
    // A registered reader lives for the whole scene (dropped before the
    // crash, and the hand-driven rig runs no resolver outside it): its
    // view counter holds §7.4 condition 2 back, so no `/sys/txn`
    // truncation delete ever joins the WAL. Record presence after the
    // crash is then a sound commit witness — without this, the
    // resolver's truncation deletes of earlier txns are flushed durable
    // by later `on` syncs (or applied write-through to the live store),
    // and a record can be legitimately gone although its commit
    // survived. (A registered *snapshot* is not enough: condition 2
    // compares view counters only.)
    let _reader = rig.core().open_view();

    // The durable baseline: `t`, plus the scenario's extras.
    let (t_oid, old_sid) = {
        let t = rig.rc();
        let txn_id = t.id;
        let st = rig.stmt(&t, rig.core().visible_ts());
        let schema = match scenario {
            Scenario::LargeValues => wide_pk_schema(),
            _ => pk_schema(),
        };
        let (oid, sid) = ok(rig.db.create_table(&st, b"mx_t", &schema));
        let ts = ok(rig.commit_rc(t));
        acked_on.push((txn_id, ts));
        (oid, sid)
    };
    let mut u_oid = None;
    let mut idx_oid = None;
    let mut idx_isid = None;
    let mut idx_built_at = None;
    match scenario {
        Scenario::AddColumn => {
            acked_on.push(add_row_on(&rig, t_oid, 1, "base"));
        }
        Scenario::LargeValues => {
            // Two toasted baseline rows (3 chunks each): the update's
            // and the delete's targets.
            acked_on.push(add_big_row_on(&rig, t_oid, 1, "one", 0, 20_000));
            acked_on.push(add_big_row_on(&rig, t_oid, 2, "two", 1, 20_000));
        }
        Scenario::DropColumn => {
            // The dependent Public index on the column about to be
            // dropped, committed `on` like the rest of the baseline.
            let isid = ok(rig.catalog().alloc_storage_id(rig.core()));
            let t = rig.rc();
            let txn_id = t.id;
            let s = rig.core().visible_ts();
            let (io, _) = ok(rig.catalog().catalog_create_index(
                &rig.ddl(&t, s),
                t_oid,
                IdxStorage::Id(isid),
                &ok(idx_def(2, false, IndexState::Public).encode()),
            ));
            let ts = ok(rig.commit_rc(t));
            acked_on.push((txn_id, ts));
            idx_oid = Some(io);
            idx_isid = Some(isid);
        }
        Scenario::Truncate => {
            acked_on.push(add_row_on(&rig, t_oid, 1, "r1"));
            acked_on.push(add_row_on(&rig, t_oid, 2, "r2"));
        }
        Scenario::CreateIndex | Scenario::InsertU => {
            let t = rig.rc();
            let txn_id = t.id;
            let st = rig.stmt(&t, rig.core().visible_ts());
            let (oid, _) = ok(rig.db.create_table(&st, b"mx_u", &rowid_schema()));
            let ts = ok(rig.commit_rc(t));
            acked_on.push((txn_id, ts));
            u_oid = Some(oid);
        }
        Scenario::SetNotNull => {}
    }

    // A: the action txn, begun before the concurrent DML txn's (the
    // t6b3b shape; it holds no lock until its statement).
    let a_txn = rig.rc();

    // B: the concurrent committed DML txn, committed while A is open.
    let b_txn = rig.rc();
    {
        let st = rig.stmt(&b_txn, rig.core().visible_ts());
        match scenario {
            // CREATE INDEX's emptiness scan forbids a committed row in
            // `t`, so this scene's DML txn writes the no-PK table; every
            // other scene's writes the DDL target.
            Scenario::CreateIndex => {
                let h = ok(rig.db.open_table(
                    &st,
                    u_oid.expect("the CreateIndex scene built u"),
                    RelLockMode::RowExclusive,
                ));
                ok(rig.db.insert_with(&st, &h, &[i32_d(901)]));
            }
            Scenario::InsertU => {
                let h = ok(rig.db.open_table(&st, t_oid, RelLockMode::RowExclusive));
                ok(rig.db.insert_with(&st, &h, &[i32_d(9), text_d("bee")]));
            }
            // C-T6d2: B's own large insert — a one-chunk value, so the
            // scene's surviving set is legible chunk-wise at every cut.
            Scenario::LargeValues => {
                let h = ok(rig.db.open_table(&st, t_oid, RelLockMode::RowExclusive));
                ok(rig.db.insert_with(
                    &st,
                    &h,
                    &[i32_d(3), text_d("bee"), text_d(&big_text(2, 3000))],
                ));
            }
            _ => {
                let h = ok(rig.db.open_table(&st, t_oid, RelLockMode::RowExclusive));
                ok(rig.db.insert_with(&st, &h, &[i32_d(5), text_d("bee")]));
            }
        }
    }
    let b_id = b_txn.id;
    let b_ts = match b_sync {
        SyncCommit::On => {
            let ts = ok(rig.commit_rc(b_txn));
            acked_on.push((b_id, ts));
            ts
        }
        SyncCommit::Off => commit_off(&rig, b_txn),
    };

    // A: the action statement(s), then its own `off` commit — its
    // intents, record and resolutions land in the tail after B's.
    let mut new_sid = None;
    match scenario {
        Scenario::AddColumn => {
            let st = rig.stmt(&a_txn, rig.core().visible_ts());
            ok(rig.db.add_column(
                &st,
                t_oid,
                "note",
                KeyType::Int4,
                false,
                Some(Value::Int4(ADD_DEFAULT)),
            ));
        }
        Scenario::DropColumn => {
            let st = rig.stmt(&a_txn, rig.core().visible_ts());
            ok(rig.db.drop_column(&st, t_oid, 2));
        }
        Scenario::SetNotNull => {
            // The validating scan runs under the AccessExclusive and
            // sees B's committed non-NULL payload (§3).
            let st = rig.stmt(&a_txn, rig.core().visible_ts());
            ok(rig.db.set_not_null(&st, t_oid, 2, true));
        }
        Scenario::Truncate => {
            let s = rig.core().visible_ts();
            new_sid = Some(ok(rig
                .catalog()
                .catalog_truncate_table(&rig.ddl(&a_txn, s), t_oid)));
        }
        Scenario::CreateIndex => {
            // §6.1 (b): the Share-locked emptiness check passes (the
            // committed table is empty), so the index is born Public
            // with `built_at = S_e` and the txn's own row — inserted by
            // an earlier statement, invisible to the ghost scan — gets
            // its entry through this statement's write path.
            let st = rig.stmt(&a_txn, rig.core().visible_ts());
            let h = ok(rig.db.open_table(&st, t_oid, RelLockMode::RowExclusive));
            ok(rig.db.insert_with(&st, &h, &[i32_d(7), text_d("own")]));
            let st2 = rig.stmt(&a_txn, rig.core().visible_ts());
            let io = ok(rig
                .db
                .create_index(&st2, t_oid, &idx_def(2, false, IndexState::Public)));
            // The index row is the txn's own write — read it through the
            // txn's own next statement, whose handle carries the index.
            let st3 = rig.stmt(&a_txn, rig.core().visible_ts());
            let h = ok(rig.db.open_table(&st3, t_oid, RelLockMode::AccessShare));
            let ih = h
                .indexes
                .iter()
                .find(|i| i.oid == io)
                .expect("the creating txn sees its own index");
            idx_oid = Some(io);
            idx_isid = Some(ih.storage_id);
            idx_built_at = Some(ih.def.built_at);
        }
        Scenario::InsertU => {
            let st = rig.stmt(&a_txn, rig.core().visible_ts());
            let h = ok(rig.db.open_table(
                &st,
                u_oid.expect("the InsertU scene built u"),
                RelLockMode::RowExclusive,
            ));
            ok(rig.db.insert_with(&st, &h, &[i32_d(801)]));
        }
        // C-T6d2: the large insert, update and delete of one txn — the
        // insert's 3 chunks are written with the row, the update
        // replaces row 1's value (a fresh id; the old 3 chunks
        // tombstone), and the delete kills row 2 with its 3.
        Scenario::LargeValues => {
            let st = rig.stmt(&a_txn, rig.core().visible_ts());
            let h = ok(rig.db.open_table(&st, t_oid, RelLockMode::RowExclusive));
            ok(rig.db.insert_with(
                &st,
                &h,
                &[i32_d(4), text_d("four"), text_d(&big_text(3, 20_000))],
            ));
            let st2 = rig.stmt(&a_txn, rig.core().visible_ts());
            assert!(ok(rig.db.update(
                &st2,
                t_oid,
                &[i32_v(1)],
                &[set_col(3, text_d(&big_text(4, 20_000)))]
            )));
            let st3 = rig.stmt(&a_txn, rig.core().visible_ts());
            assert!(ok(rig.db.delete(&st3, t_oid, &[i32_v(2)])));
        }
    }
    let (a_id, a_ts) = (a_txn.id, commit_off(&rig, a_txn));
    // The resolver's writes (intent removals, version puts) join the
    // tail too: the matrix cuts between them as well.
    rig.settle();
    // The reader goes away before the crash (nothing runs between: the
    // hand-driven rig has no threads of its own).
    drop(_reader);

    (
        rig,
        Scene {
            scenario,
            t_oid,
            u_oid,
            idx_oid,
            idx_isid,
            idx_built_at,
            old_sid,
            new_sid,
            acked_on,
            b: (b_id, b_ts, b_sync),
            a: (a_id, a_ts),
        },
    )
}

// ----- the model of the surviving committed set -----------------------------------

/// A row as the model carries it: one value per live attnum of the
/// definition it was written under (every scene row is written at v1).
type MRow = Vec<Option<Value>>;

fn t_row(id: i32, payload: &str) -> MRow {
    vec![Some(Value::Int4(id)), Some(Value::Text(payload.into()))]
}

fn u_row(v: i32) -> MRow {
    vec![Some(Value::Int4(v))]
}

/// A wide row of the LargeValues scene, carrying its full `big` value
/// (the model stores whole values — the scan comparison detoasts).
fn wide_row(id: i32, payload: &str, tag: u8, len: usize) -> MRow {
    vec![
        Some(Value::Int4(id)),
        Some(Value::Text(payload.into())),
        Some(Value::Text(big_text(tag, len))),
    ]
}

/// The §5 chunk count a `big` value occupies (over the threshold →
/// `ceil(len / TOAST_CHUNK)`; else inline, no chunk).
fn chunks_of_big(v: &Value) -> usize {
    match v {
        Value::Text(s) if s.len() > TOAST_THRESHOLD => s.len().div_ceil(TOAST_CHUNK),
        _ => 0,
    }
}

/// The state the surviving committed set implies: `t`'s and `u`'s rows
/// by identity, the surviving definition's shape, and a born index's
/// expected visible entries.
#[derive(Debug, Clone, Default)]
pub struct CrashModel {
    t: BTreeMap<i32, MRow>,
    u: BTreeMap<i32, MRow>,
    /// The `note` column's default, when the AddColumn action survived.
    add_default: Option<i32>,
    /// The payload column is dropped (the DropColumn action survived).
    payload_dropped: bool,
    /// The born-public index committed: its one entry, by key.
    entries: Vec<nucleus_kv::Key>,
    /// The LargeValues scene: `t` is the wide 3-column table whose
    /// definition never changes, so rows render as stored.
    wide: bool,
}

/// The model of exactly the scene txns whose `/sys/txn` records the
/// reopened store still holds, applied in commit order (baseline, then
/// B, then A — the scene fixes their order).
pub fn model_of(scene: &Scene, a_in: bool, b_in: bool) -> CrashModel {
    let mut m = CrashModel::default();
    match scene.scenario {
        Scenario::AddColumn => {
            m.t.insert(1, t_row(1, "base"));
        }
        Scenario::LargeValues => {
            m.wide = true;
            m.t.insert(1, wide_row(1, "one", 0, 20_000));
            m.t.insert(2, wide_row(2, "two", 1, 20_000));
        }
        Scenario::Truncate => {
            m.t.insert(1, t_row(1, "r1"));
            m.t.insert(2, t_row(2, "r2"));
        }
        _ => {}
    }
    if b_in {
        match scene.scenario {
            Scenario::CreateIndex => {
                m.u.insert(901, u_row(901));
            }
            Scenario::InsertU => {
                m.t.insert(9, t_row(9, "bee"));
            }
            Scenario::LargeValues => {
                m.t.insert(3, wide_row(3, "bee", 2, 3000));
            }
            _ => {
                m.t.insert(5, t_row(5, "bee"));
            }
        }
    }
    if a_in {
        match scene.scenario {
            Scenario::AddColumn => m.add_default = Some(ADD_DEFAULT),
            Scenario::DropColumn => m.payload_dropped = true,
            Scenario::SetNotNull => {}
            Scenario::Truncate => m.t.clear(),
            Scenario::LargeValues => {
                // The replace moves row 1 to a fresh value, the insert
                // adds row 4, the delete removes row 2.
                m.t.insert(1, wide_row(1, "one", 4, 20_000));
                m.t.insert(4, wide_row(4, "four", 3, 20_000));
                m.t.remove(&2);
            }
            Scenario::CreateIndex => {
                m.t.insert(7, t_row(7, "own"));
                let isid = scene.idx_isid.expect("the scene captured the storage id");
                let row_key = row_key_of(scene.old_sid, &pk_schema(), &[Some(Value::Int4(7))]);
                m.entries = vec![i_entry_key(
                    isid,
                    &[KeyColumn::asc(KeyType::Text(Collation::C))],
                    &[Some(Value::Text("own".into()))],
                    &row_key,
                )];
            }
            Scenario::InsertU => {
                m.u.insert(801, u_row(801));
            }
        }
    }
    m
}

impl CrashModel {
    /// The rows a full scan of `t` must return, rendered under the
    /// surviving definition (§3's decode rule: a column the row's writer
    /// never had reads `missing`, else NULL).
    fn rendered_t(&self) -> BTreeMap<i32, MRow> {
        if self.wide {
            // The LargeValues definition never changes: rows render as
            // stored, `big` values whole.
            return self.t.iter().map(|(id, row)| (*id, row.clone())).collect();
        }
        self.t
            .iter()
            .map(|(id, row)| {
                let mut out = vec![row[0].clone()];
                if !self.payload_dropped {
                    out.push(row[1].clone());
                }
                if let Some(d) = self.add_default {
                    out.push(Some(Value::Int4(d)));
                }
                (*id, out)
            })
            .collect()
    }

    /// The rows a full scan of `u` must return, by their `v` identity.
    fn rendered_u(&self) -> BTreeMap<i32, MRow> {
        self.u.iter().map(|(v, row)| (*v, row.clone())).collect()
    }

    /// The number of large-value chunks the surviving rows' `big`
    /// pointers must name (C-T6d2: the no-orphan/no-dangling oracle's
    /// count form — amcheck's I-TOAST checks the same set exactly).
    pub fn chunks_expected(&self) -> usize {
        self.t
            .values()
            .map(|row| {
                row.get(2)
                    .and_then(|v| v.as_ref())
                    .map(chunks_of_big)
                    .unwrap_or(0)
            })
            .sum()
    }
}

// ----- the post-crash checks --------------------------------------------------------

/// Every persisted commit record the reopened store holds: the
/// surviving committed set. Read before the sweep, which may truncate
/// released records.
pub fn surviving_records(rig: &Rig) -> BTreeMap<TxnId, Ts> {
    use nucleus_txn::encoding::{parse_sys_txn_key, sys_txn_prefix, sys_txn_prefix_end};
    let view = rig.core().open_view();
    let lo = sys_txn_prefix();
    let hi = sys_txn_prefix_end();
    let mut out = BTreeMap::new();
    for row in view.scan(
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        false,
    ) {
        let (k, v) = ok(row);
        let id = parse_sys_txn_key(&k).unwrap_or_else(|| panic!("bad /sys/txn key {k:?}"));
        let mut b = [0u8; 8];
        b.copy_from_slice(&v);
        out.insert(id, Ts(u64::from_be_bytes(b)));
    }
    out
}

/// A full scan of `oid` through the reopened typed layer, keyed by the
/// first live column (the pk of `t`, the `v` of `u`): every row of a
/// table the scan returns has already decoded.
fn scan_by_identity(rig: &Rig, oid: u64) -> BTreeMap<i32, MRow> {
    let txn = rig.rc();
    let guard = rig.core().registry.take_snapshot();
    let st = rig.stmt(&txn, guard.ts());
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st, &h));
    drop(guard);
    ok(rig.commit_rc(txn));
    let mut out = BTreeMap::new();
    for (_key, datums) in rows {
        let mut vals = Vec::with_capacity(datums.len());
        let mut identity = None;
        for (i, datum) in datums.iter().enumerate() {
            let v = datum.as_ref().map(|d| match d {
                Datum::Inline(v) => v.clone(),
                // Impossible for these values; a marker that can never
                // equal a modeled one.
                Datum::Toast(_) => Value::Text("\u{0}TOAST".into()),
            });
            if i == 0 {
                identity = v.clone();
            }
            vals.push(v);
        }
        match identity {
            Some(Value::Int4(id)) => {
                assert!(out.insert(id, vals).is_none(), "duplicate identity {id}");
            }
            other => panic!("a scanned row's first column is not an int4 identity: {other:?}"),
        }
    }
    out
}

fn rows_eq(got: &MRow, want: &MRow) -> bool {
    got.len() == want.len()
        && got.iter().zip(want).all(|(g, w)| match (g, w) {
            (Some(a), Some(b)) => value_repr_eq(a, b),
            (None, None) => true,
            _ => false,
        })
}

fn assert_rows(where_: &str, got: BTreeMap<i32, MRow>, want: BTreeMap<i32, MRow>) {
    assert_eq!(
        got.len(),
        want.len(),
        "{where_}: the scan returned {got:?}, the model has {want:?}"
    );
    for (id, w) in &want {
        match got.get(id) {
            Some(g) => assert!(
                rows_eq(g, w),
                "{where_}: row {id} is {g:?}, the model has {w:?}"
            ),
            None => panic!("{where_}: the scan is missing the model's row {id}: {w:?}"),
        }
    }
}

/// One matrix cell: build the scene, crash (keeping `keep` unsynced
/// batches, or `crash_seeded`'s draw), reopen the whole stack, run the
/// resolver, and check everything item 2 names. Returns whether the
/// action txn's commit survived (the cell's "new" outcome).
pub fn cell(scenario: Scenario, b_sync: SyncCommit, at: CrashAt) -> bool {
    let where_ = format!("{scenario:?} / DML {:?} / {:?}", b_sync, at);
    let (rig, scene) = build_scene(scenario, b_sync);
    let n = rig.kv.unsynced_len();
    assert!(n > 0, "{where_}: the scene must build an unsynced tail");
    let keep = match at {
        CrashAt::Keep(keep) => keep,
        CrashAt::Seeded(seed) => ok(rig.kv.crash_seeded(seed)),
    };
    let rig2 = rig.crash_reopen(keep);

    // Every acked `on` commit survived the crash (I-DURABLE).
    for (id, ts) in &scene.acked_on {
        assert_eq!(
            ok(rig2.core().status.lookup_for_intent(*id)),
            TxnStatus::Committed(*ts),
            "{where_}: the acked On commit of {id:?} was lost (keep {keep})"
        );
    }

    // The surviving committed set, read from the store before the sweep
    // can truncate released records.
    let survivors = surviving_records(&rig2);
    let a_in = survivors.contains_key(&scene.a.0);
    let b_in = survivors.contains_key(&scene.b.0);

    // §7.2's boot sweep to completion, then a resolver pass.
    sweep(&rig2);

    // The definition is old-or-new, exactly per the surviving set.
    let rel = rig2
        .rel_row(scene.t_oid)
        .expect("the baseline def survived every cut (it is durable)");
    let def = decode_def(&rel.def);
    let want_version = u32::from(a_in) + 1;
    match scenario {
        Scenario::AddColumn => {
            assert_eq!(def.version, want_version, "{where_}");
            assert_eq!(
                def.columns.len(),
                if a_in { 3 } else { 2 },
                "{where_}: the added column travels with the commit"
            );
            if a_in {
                assert_eq!(
                    def.columns[2].missing,
                    Some(Value::Int4(ADD_DEFAULT)),
                    "{where_}: the fast default is in the def"
                );
            }
        }
        Scenario::DropColumn => {
            assert_eq!(def.version, want_version, "{where_}");
            assert_eq!(def.columns[1].dropped, a_in, "{where_}");
            let io = scene.idx_oid.expect("the DropColumn scene built the index");
            assert_eq!(
                rig2.idx_row(io).is_some(),
                !a_in,
                "{where_}: the dependent index must travel with the column"
            );
        }
        Scenario::SetNotNull => {
            assert_eq!(def.version, want_version, "{where_}");
            assert_eq!(def.columns[1].not_null, a_in, "{where_}");
        }
        Scenario::Truncate => {
            assert_eq!(
                def.version, 1,
                "{where_}: TRUNCATE moves the storage, not the def"
            );
            assert_eq!(
                rel.storage_id,
                if a_in {
                    scene.new_sid.expect("the scene captured the new storage")
                } else {
                    scene.old_sid
                },
                "{where_}: the storage is old-or-new, never anything else"
            );
        }
        Scenario::CreateIndex => {
            assert_eq!(
                def.version, 1,
                "{where_}: CREATE INDEX leaves the def version"
            );
            let io = scene
                .idx_oid
                .expect("the CreateIndex scene built the index");
            assert_eq!(
                rig2.idx_row(io).is_some(),
                a_in,
                "{where_}: the index row is old-or-new"
            );
            if a_in {
                let row = rig2.idx_row(io).expect("checked just above");
                let idx = IndexDef::decode(&row.def).expect("the surviving def decodes");
                assert_eq!(idx.state, IndexState::Public, "{where_}");
                assert_eq!(
                    idx.built_at,
                    scene.idx_built_at.expect("the scene captured built_at"),
                    "{where_}: born public at S_e"
                );
            }
        }
        Scenario::InsertU => assert_eq!(def.version, 1, "{where_}: no DDL in this scene"),
        Scenario::LargeValues => {
            assert_eq!(def.version, 1, "{where_}: no DDL in this scene");
            assert_eq!(
                rel.storage_id, scene.old_sid,
                "{where_}: the storage never moves"
            );
        }
    }

    // amcheck at a fresh snapshot of the reopened state: every row
    // decodes (I-ROWFMT) and every index agrees with its rows
    // (I-IDX/I-IDX-WO/I-IDX-DO).
    for oid in [Some(scene.t_oid), scene.u_oid].into_iter().flatten() {
        let guard = rig2.core().registry.take_snapshot();
        let report = ok(check_table(&rig2.db, oid, &guard));
        drop(guard);
        assert!(
            report.is_clean(),
            "{where_}: amcheck on {oid} after the crash: {:?}",
            report.violations
        );
    }

    // The model built from the surviving committed set equals a full
    // scan (and, for the born index, its visible entries).
    let model = model_of(&scene, a_in, b_in);
    assert_rows(
        &where_,
        scan_by_identity(&rig2, scene.t_oid),
        model.rendered_t(),
    );
    if let Some(u_oid) = scene.u_oid {
        assert_rows(&where_, scan_by_identity(&rig2, u_oid), model.rendered_u());
    }
    if scenario == Scenario::CreateIndex {
        let isid = scene.idx_isid.expect("the scene captured the storage id");
        let got: BTreeSet<nucleus_kv::Key> = rig2
            .scan_entries(isid)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let want: BTreeSet<nucleus_kv::Key> = model.entries.iter().cloned().collect();
        assert_eq!(
            got, want,
            "{where_}: the born index's visible entries must be exactly the model's"
        );
    }
    // C-T6d2: the LargeValues scene's no-orphan/no-dangling oracle, in
    // count form — every visible chunk of the storage belongs to a
    // surviving row's pointer, and every surviving row's pointer has its
    // chunks (amcheck's I-TOAST, run above, checks the same set exactly;
    // this counts it against the model independently of the checker).
    if scenario == Scenario::LargeValues {
        let want = model.chunks_expected();
        assert!(want > 0, "{where_}: every cut keeps a toasted row");
        let got = visible_chunk_count(&rig2, rel.storage_id);
        assert_eq!(
            got, want,
            "{where_}: visible chunks vs the surviving rows' pointers"
        );
    }
    a_in
}

/// The number of §5 chunks of storage `sid` visible at a fresh snapshot
/// of the reopened store, through the same read path amcheck uses.
fn visible_chunk_count(rig: &Rig, sid: u64) -> usize {
    let (lo, hi) = table_range(sid);
    let mut clo = lo.clone();
    clo.push(TOAST_MARK);
    let guard = rig.core().registry.take_snapshot();
    let ctx = ReadCtx {
        txn: TxnId {
            epoch: rig.core().epoch(),
            n: u64::MAX,
        },
        snapshot: guard.ts(),
        stmt_seq: 0,
    };
    let view = rig.core().open_view();
    let n = ok(scan(
        rig.core(),
        &view,
        (
            Bound::Included(clo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        &ctx,
        &mut NoSsi,
    )
    .collect::<Result<Vec<_>, _>>())
    .len();
    n
}
