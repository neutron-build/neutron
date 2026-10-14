//! The sequential reference model (C-T6c1a item 3, extended by C-T6c1b):
//! a `BTreeMap<Ts, State>` with `t`'s rows keyed by pk and `u`'s by their
//! unique `a` constant, plus `t`'s schema history by version. Committed
//! txns are replayed in `commit-ts` order; a replaying txn carries a
//! per-txn own-writes overlay so its statements can be checked at their
//! statement snapshots (state at `S` + overlay).
//!
//! Effects are taken from what the runner recorded the engine apply —
//! updates record a post-write read-back, so an RC statement whose row op
//! re-based on a newer committed version through EPQ (§5.2) still models
//! what later statements and the final state actually see. Whether a
//! write *should* have applied is C-T6c1b's write-effect oracle
//! (`check.rs::Effects`), which runs off [`Model::build_with`]'s replay
//! hook; C-T6c1a's checks are unchanged.
//!
//! DDL follows C-T6 §3 exactly: ADD COLUMN appends attnum `ncols + 1`
//! with the evaluated constant as `missing` (or none → NULL), and no row
//! is rewritten — old rows read the default only through the decode rule,
//! which [`Model::render`] mirrors by filling a column absent from the
//! row with the schema's `missing`, else NULL. DROP COLUMN sets `dropped`
//! and clears `not_null`; SET/DROP NOT NULL flips the flag; each applied
//! DDL bumps the version by one, and versions are assigned in commit
//! order (every def-changing DDL holds AccessExclusive, so they
//! serialize). TRUNCATE empties `t` (its storage id moves; the model is
//! storage-agnostic — only the engine sees storages, which is the point
//! of the differential).
//!
//! `u` rows are identified by their `a` value: write constants are unique
//! per write and `u` rows are never updated, so the mapping is stable.
//! The engine's rowid keys are learned from ScanU records (recorded as
//! returned, rowid embedded) and only used to resolve a DeleteU's target;
//! row existence and values always come from applying recorded writes,
//! never from the observations the checks compare against.

use std::collections::BTreeMap;

use nucleus_codec::Value;

use super::{Fate, History, OpDetail, OpKind, OpOut, OpRec, RowVals, TxnRec};

/// One column of `t`'s definition as the model evolves it.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaCol {
    pub attnum: u16,
    pub not_null: bool,
    pub dropped: bool,
    /// ADD COLUMN's evaluated constant (§3 `missing`), served for rows
    /// written before the attnum existed.
    pub missing: Option<Value>,
}

/// `t`'s definition at one version: `pk int8 PK` at attnum 1 plus the
/// evolved columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub version: u32,
    pub cols: Vec<SchemaCol>,
}

impl Schema {
    /// `t(pk int8 PK, a int8, b text, big text)`, version 1 (CREATE
    /// TABLE) — C-T6d2's fourth column carries the large values.
    pub fn initial() -> Schema {
        Schema {
            version: 1,
            cols: vec![
                SchemaCol {
                    attnum: 1,
                    not_null: true,
                    dropped: false,
                    missing: None,
                },
                SchemaCol {
                    attnum: 2,
                    not_null: false,
                    dropped: false,
                    missing: None,
                },
                SchemaCol {
                    attnum: 3,
                    not_null: false,
                    dropped: false,
                    missing: None,
                },
                SchemaCol {
                    attnum: 4,
                    not_null: false,
                    dropped: false,
                    missing: None,
                },
            ],
        }
    }

    /// The live columns, in attnum order.
    pub fn live(&self) -> impl Iterator<Item = &SchemaCol> {
        self.cols.iter().filter(|c| !c.dropped)
    }
}

/// `u(a int8, b text)` — no PK, hidden rowid; DDL never touches `u`, so
/// its live attnum list is a constant of the run.
pub const U_LIVE: [u16; 2] = [1, 2];

/// One modeled row: attnum → value (`None` = NULL). An attnum absent from
/// the map is a column the row's writer never had — rendered through the
/// decode rule (`missing`, else NULL), exactly as §3 prescribes for rows
/// older than a column.
pub type MRow = BTreeMap<u16, Option<Value>>;

/// The state after the txn committed at a given ts.
#[derive(Debug, Clone, PartialEq)]
pub struct State {
    /// `t`'s rows by pk.
    pub t: BTreeMap<i64, MRow>,
    /// `u`'s rows by their unique `a` value.
    pub u: BTreeMap<i64, MRow>,
    /// `t`'s current definition.
    pub schema: Schema,
    /// C-T6c1b: the uid of the txn whose committed write produced each
    /// live row (the read-from side of the SERIALIZABLE serialization
    /// graph). A row deleted or truncated has no entry — its last writer
    /// is tracked by the checker's own `last_write` maps instead.
    pub t_prov: BTreeMap<i64, u32>,
    /// The same, for `u`'s rows by their `a` value.
    pub u_prov: BTreeMap<i64, u32>,
}

impl State {
    fn new() -> State {
        State {
            t: BTreeMap::new(),
            u: BTreeMap::new(),
            schema: Schema::initial(),
            t_prov: BTreeMap::new(),
            u_prov: BTreeMap::new(),
        }
    }
}

/// The replayed run: states indexed by commit ts, `t`'s schema history by
/// version, and the rowid bindings learned from ScanU records.
#[derive(Debug, Clone)]
pub struct Model {
    snaps: BTreeMap<u64, State>,
    /// `schemas[v - 1]` is `t`'s definition at version `v`; versions are
    /// contiguous from 1 (one per applied def-changing DDL, in commit
    /// order).
    schemas: Vec<Schema>,
    /// `u` rowid (logical key) → the row's unique `a` value.
    pub u_bind: BTreeMap<Vec<u8>, i64>,
    /// The state after the last committed txn.
    pub final_state: State,
    /// Replay inconsistencies (a DeleteU whose target no ScanU ever
    /// showed, a schema-version discontinuity): surfaced as violations by
    /// the checker.
    pub anomalies: Vec<String>,
}

impl Model {
    /// Replays every committed txn of the history in commit-ts order,
    /// calling `hook` for every op of every committed txn **at its commit
    /// position**: `state` is the model state built from every txn that
    /// committed before this one (all earlier commit ts applied), `own`
    /// the txn's own-writes overlay as of just before this op, so the
    /// hook observes exactly what a statement that re-executed here would
    /// see. The hook runs before the op's effect is applied to `state`,
    /// `own` and the model; returning without mutating anything keeps the
    /// replay pure (C-T6c1b's write-effect, SET NOT NULL and
    /// SERIALIZABLE checks run here; nothing they do feeds back into the
    /// model).
    ///
    /// The replay also maintains the per-row provenance
    /// (`State::t_prov`/`u_prov`) and, per txn, the own-writes overlay.
    #[allow(clippy::type_complexity)] // the hook sees exactly the replay's per-op context
    pub fn build_with(
        h: &History,
        hook: &mut dyn FnMut(&Model, &State, &TxnRec, usize, &OpRec, &OwnWrites),
    ) -> Model {
        let mut model = Model {
            snaps: BTreeMap::new(),
            schemas: vec![Schema::initial()],
            u_bind: BTreeMap::new(),
            final_state: State::new(),
            anomalies: Vec::new(),
        };
        let mut state = State::new();
        // Commit ts are unique (one sequencer); uid breaks any tie a
        // degenerate history could hold.
        let mut ordered: Vec<(u64, u32, &TxnRec)> = h
            .txns
            .iter()
            .filter_map(|t| match t.fate {
                Fate::Committed(ts) => Some((ts, t.uid, t)),
                Fate::Failed(_) => None,
            })
            .collect();
        ordered.sort_by_key(|(ts, uid, _)| (*ts, *uid));
        for (ts, _, txn) in ordered {
            let mut own = OwnWrites::default();
            for (idx, op) in txn.ops.iter().enumerate() {
                hook(&model, &state, txn, idx, op, &own);
                model.apply(&mut state, txn, op);
                note_provenance(&mut state, txn, op, &model.u_bind);
                own.apply(op, &model.u_bind);
            }
            model.snaps.insert(ts, state.clone());
        }
        model.final_state = state;
        model
    }

    /// Applies one op's recorded effect to `state` (the commit-merge half
    /// of the overlay discipline; the checker runs the same rules over a
    /// txn-local overlay while it replays the txn's reads).
    fn apply(&mut self, state: &mut State, txn: &TxnRec, op: &super::OpRec) {
        let where_ = || {
            format!(
                "txn {} (s{} p{}.{}) op {:?}",
                txn.uid, txn.session, txn.prog, txn.attempt, op.kind
            )
        };
        match (&op.detail, &op.out) {
            (OpDetail::InsertT { pk, row }, OpOut::Ok) => {
                state.t.insert(*pk, to_mrow(row));
            }
            (
                OpDetail::UpdateSet {
                    pk,
                    new_row: Some(row),
                    ..
                },
                OpOut::Applied(true),
            ) => {
                state.t.insert(*pk, to_mrow(row));
            }
            // C-T6d2: the large-value writes carry their full read-back
            // rows, exactly as UpdateSet does — the model stores the
            // whole value, never just its length.
            (
                OpDetail::SetBig {
                    pk,
                    new_row: Some(row),
                    ..
                }
                | OpDetail::NullBig {
                    pk,
                    new_row: Some(row),
                    ..
                },
                OpOut::Applied(true),
            ) => {
                state.t.insert(*pk, to_mrow(row));
            }
            (OpDetail::DeleteT { pk }, OpOut::Applied(true)) => {
                state.t.remove(pk);
            }
            (OpDetail::InsertU { row }, OpOut::Ok) => match u_identity(row) {
                Some(a) => {
                    state.u.insert(a, to_mrow(row));
                }
                None => self.anomalies.push(format!(
                    "{}: InsertU row has no int8 value in u's first column",
                    where_()
                )),
            },
            (OpDetail::DeleteU { key: Some(key) }, OpOut::Applied(true)) => {
                match self.u_bind.get(key).copied() {
                    Some(a) => {
                        state.u.remove(&a);
                    }
                    None => self.anomalies.push(format!(
                        "{}: DeleteU of a rowid no ScanU recorded a binding for",
                        where_()
                    )),
                }
            }
            (OpDetail::ScanU, OpOut::RowsU(rows)) => {
                // Learn the rowid → a bindings (rows are never updated, so
                // a binding is stable once seen). Existence and values
                // still come only from applied writes; the checker
                // compares the observation separately.
                for (key, row) in rows {
                    if let Some(a) = u_identity(row) {
                        self.u_bind.insert(key.clone(), a);
                    }
                }
            }
            (OpDetail::AddColumn { name, default }, OpOut::Ok) => {
                let next_attnum =
                    u16::try_from(state.schema.cols.len() + 1).expect("the column count fits u16");
                state.schema.cols.push(SchemaCol {
                    attnum: next_attnum,
                    not_null: false,
                    dropped: false,
                    missing: default.map(Value::Int8),
                });
                self.bump(&mut state.schema, &where_(), name);
            }
            (OpDetail::DropColumn { attnum }, OpOut::Ok) => {
                match state.schema.cols.iter_mut().find(|c| c.attnum == *attnum) {
                    Some(col) => {
                        col.dropped = true;
                        col.not_null = false;
                        self.bump(&mut state.schema, &where_(), "drop");
                    }
                    None => self.anomalies.push(format!(
                        "{}: DROP COLUMN of attnum {} the model does not have",
                        where_(),
                        attnum
                    )),
                }
            }
            (OpDetail::SetNotNull { attnum }, OpOut::Ok) => {
                match state.schema.cols.iter_mut().find(|c| c.attnum == *attnum) {
                    Some(col) => {
                        col.not_null = true;
                        self.bump(&mut state.schema, &where_(), "set not null");
                    }
                    None => self.anomalies.push(format!(
                        "{}: SET NOT NULL on attnum {} the model does not have",
                        where_(),
                        attnum
                    )),
                }
            }
            (OpDetail::DropNotNull { attnum }, OpOut::Ok) => {
                match state.schema.cols.iter_mut().find(|c| c.attnum == *attnum) {
                    Some(col) => {
                        col.not_null = false;
                        self.bump(&mut state.schema, &where_(), "drop not null");
                    }
                    None => self.anomalies.push(format!(
                        "{}: DROP NOT NULL on attnum {} the model does not have",
                        where_(),
                        attnum
                    )),
                }
            }
            (OpDetail::Truncate { .. }, OpOut::Ok) => {
                state.t.clear();
            }
            // Failed ops, reads, no-op writes: no effect.
            _ => {}
        }
    }

    /// One applied def-changing DDL: version `+ 1` and a contiguous
    /// schema-history push.
    fn bump(&mut self, schema: &mut Schema, where_: &str, what: &str) {
        schema.version += 1;
        if schema.version as usize == self.schemas.len() + 1 {
            self.schemas.push(schema.clone());
        } else {
            self.anomalies.push(format!(
                "{where_}: {what} made schema version {} but {} versions exist",
                schema.version,
                self.schemas.len()
            ));
        }
    }

    /// The state after the last txn committed at ts `<= s` (rows cannot
    /// exist before the setup txn committed, so no floor reads empty).
    pub fn state_at(&self, s: u64) -> &State {
        self.snaps
            .range(..=s)
            .next_back()
            .map_or(empty(), |(_, st)| st)
    }

    /// `t`'s definition at version `v` (versions are contiguous from 1).
    pub fn schema_at(&self, v: u32) -> &Schema {
        self.schemas
            .get(usize::try_from(v).unwrap_or(0).saturating_sub(1))
            .unwrap_or(&self.schemas[0])
    }

    /// Renders a modeled row of `t` under the definition of version `v`
    /// (§3's decode rule: a value per live column; a column the row never
    /// had reads `missing`, else NULL — an explicit NULL stays NULL).
    pub fn render(&self, v: u32, row: &MRow) -> RowVals {
        self.schema_at(v)
            .live()
            .map(|c| match row.get(&c.attnum) {
                Some(value) => (c.attnum, value.clone()),
                None => (c.attnum, c.missing.clone()),
            })
            .collect()
    }

    /// Renders a modeled row of `u` (fixed definition: `a`, `b`).
    pub fn render_u(row: &MRow) -> RowVals {
        U_LIVE
            .iter()
            .map(|&attnum| (attnum, row.get(&attnum).cloned().flatten()))
            .collect()
    }
}

/// The pre-setup floor (no commit at or below the snapshot): empty
/// tables, initial schema. Built once; a static needs a const context.
static EMPTY: std::sync::OnceLock<State> = std::sync::OnceLock::new();

fn empty() -> &'static State {
    EMPTY.get_or_init(State::new)
}

/// The `a` identity of a `u` row.
fn u_identity(row: &RowVals) -> Option<i64> {
    match row.iter().find(|(attnum, _)| *attnum == U_LIVE[0]) {
        Some((_, Some(Value::Int8(a)))) => Some(*a),
        _ => None,
    }
}

fn to_mrow(row: &RowVals) -> MRow {
    row.iter().map(|(a, v)| (*a, v.clone())).collect()
}

/// Records `op`'s commit-position provenance in `state` (C-T6c1b), right
/// after [`Model::apply`] merges the op's rows — the same resolution rules
/// (a DeleteU's target comes from the rowid bindings the model has
/// learned). A delete removes the row's provenance; a TRUNCATE clears the
/// table's (the DDL session here is never SERIALIZABLE, so its uid could
/// never name a graph node).
fn note_provenance(
    state: &mut State,
    txn: &TxnRec,
    op: &super::OpRec,
    bind: &BTreeMap<Vec<u8>, i64>,
) {
    match (&op.detail, &op.out) {
        (super::OpDetail::InsertT { pk, .. }, super::OpOut::Ok) => {
            state.t_prov.insert(*pk, txn.uid);
        }
        (
            super::OpDetail::UpdateSet {
                pk,
                new_row: Some(_),
                ..
            },
            super::OpOut::Applied(true),
        ) => {
            state.t_prov.insert(*pk, txn.uid);
        }
        (super::OpDetail::DeleteT { pk }, super::OpOut::Applied(true)) => {
            state.t_prov.remove(pk);
        }
        (super::OpDetail::InsertU { row }, super::OpOut::Ok) => {
            if let Some(a) = u_identity(row) {
                state.u_prov.insert(a, txn.uid);
            }
        }
        (super::OpDetail::DeleteU { key: Some(key) }, super::OpOut::Applied(true)) => {
            if let Some(a) = bind.get(key) {
                state.u_prov.remove(a);
            }
        }
        (super::OpDetail::Truncate { .. }, super::OpOut::Ok) => {
            state.t_prov.clear();
        }
        _ => {}
    }
}

/// A txn-local overlay entry: the row a statement wrote, or a deletion.
#[derive(Debug, Clone, PartialEq)]
pub enum Overlay {
    Row(MRow),
    Gone,
}

/// The per-txn own-writes overlay: later statements of the txn read the
/// state at their snapshot *plus* these (§4: own writes with an earlier
/// statement's seq).
#[derive(Debug, Clone, Default)]
pub struct OwnWrites {
    pub t: BTreeMap<i64, Overlay>,
    pub u: BTreeMap<i64, Overlay>,
}

impl OwnWrites {
    /// Applies one op's recorded effect to the overlay — the same rules
    /// [`Model::apply`] merges at commit. `bind` resolves a DeleteU's
    /// rowid target (learned from the txn's earlier ScanU records, which
    /// replay before this op).
    pub fn apply(&mut self, op: &super::OpRec, bind: &BTreeMap<Vec<u8>, i64>) {
        match (&op.detail, &op.out) {
            (OpDetail::InsertT { pk, row }, OpOut::Ok) => {
                self.t.insert(*pk, Overlay::Row(to_mrow(row)));
            }
            (
                OpDetail::UpdateSet {
                    pk,
                    new_row: Some(row),
                    ..
                },
                OpOut::Applied(true),
            ) => {
                self.t.insert(*pk, Overlay::Row(to_mrow(row)));
            }
            (
                OpDetail::SetBig {
                    pk,
                    new_row: Some(row),
                    ..
                }
                | OpDetail::NullBig {
                    pk,
                    new_row: Some(row),
                    ..
                },
                OpOut::Applied(true),
            ) => {
                self.t.insert(*pk, Overlay::Row(to_mrow(row)));
            }
            (OpDetail::DeleteT { pk }, OpOut::Applied(true)) => {
                self.t.insert(*pk, Overlay::Gone);
            }
            (OpDetail::InsertU { row }, OpOut::Ok) => {
                if let Some(a) = u_identity(row) {
                    self.u.insert(a, Overlay::Row(to_mrow(row)));
                }
            }
            (OpDetail::DeleteU { key: Some(key) }, OpOut::Applied(true)) => {
                if let Some(a) = bind.get(key).copied() {
                    self.u.insert(a, Overlay::Gone);
                }
            }
            _ => {}
        }
    }

    /// The `t` row a read at the statement's snapshot should see for `pk`
    /// (overlay first, then the base state).
    pub fn t_row<'a>(&'a self, base: &'a State, pk: i64) -> Option<&'a MRow> {
        match self.t.get(&pk) {
            Some(Overlay::Row(row)) => Some(row),
            Some(Overlay::Gone) => None,
            None => base.t.get(&pk),
        }
    }

    /// The `u` row a read at the statement's snapshot should see for the
    /// row of identity `a` (overlay first, then the base state).
    pub fn u_row<'a>(&'a self, base: &'a State, a: i64) -> Option<&'a MRow> {
        match self.u.get(&a) {
            Some(Overlay::Row(row)) => Some(row),
            Some(Overlay::Gone) => None,
            None => base.u.get(&a),
        }
    }

    /// The `u` rows a read at the statement's snapshot should see: base
    /// rows plus the overlay's net writes.
    pub fn u_rows<'a>(&'a self, base: &'a State) -> BTreeMap<i64, &'a MRow> {
        let mut out: BTreeMap<i64, &'a MRow> = base.u.iter().map(|(a, r)| (*a, r)).collect();
        for (a, ov) in &self.u {
            match ov {
                Overlay::Row(row) => {
                    out.insert(*a, row);
                }
                Overlay::Gone => {
                    out.remove(a);
                }
            }
        }
        out
    }
}

/// Whether an op kind is DDL.
pub fn is_ddl(kind: OpKind) -> bool {
    matches!(
        kind,
        OpKind::AddColumn
            | OpKind::DropColumn
            | OpKind::SetNotNull
            | OpKind::DropNotNull
            | OpKind::Truncate
            | OpKind::DropBig
            | OpKind::AddBig
    )
}
