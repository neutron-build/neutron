//! C-T6e4fix2: NOT NULL binds by `not_null_since`, not by "any older
//! row". C-T6e4's carve-out cleared **every** NOT NULL flag whenever a
//! row's header version was below the def version, so any later
//! unrelated DDL (an ADD COLUMN) disarmed the online corruption check
//! for every older row — including columns NOT NULL since CREATE
//! TABLE and NOT NULL pk columns. The fix carries the era marker per
//! column on `ColumnDef`: a NULL bit on attnum `a` of a row at header
//! version `v` is XX001 iff `a.not_null && v >= a.not_null_since`, the
//! def version at which the constraint (last) came into force. The
//! rule's one home is `TableSchema::decode_row`;
//! `decode_row_at_era` is the DML/EPQ seam over it and amcheck's
//! I-ROWFMT decodes through the same function.
//!
//! The marker persists in the def bytes as §3's flags-gated
//! `be32(not_null_since)` word (bit4), elided exactly when it equals
//! the re-derivation (`version` for a NOT NULL column, 0 for a
//! nullable one) — the elision keeps the golden def corpus
//! (`tests/golden_row/defs.tsv`, outside this card's Touch-only list)
//! byte-identical while every DDL-producible schema round-trips with
//! its markers exact: a marker born at the def's own version is elided
//! and re-derived, and the first later DDL writes the word. Through
//! the catalog (each `open_table`, amcheck, the build scan decodes
//! `rel.def`) the era therefore survives unrelated DDL **and**
//! crash-reopens — the audit-probe class below.
//!
//! **Enforced-out half** (this card's ESCALATE): the spec letter — §2's
//! NULL clause and §3's decode rule restated to the per-column
//! `not_null_since` rule, §3's `col` layout gaining the marker word, a
//! changelog line naming the e4 over-correction — was authored for
//! `docs/C-T6-rows-schema-indexes.md` and reverted by the runner's
//! `nucleus/v2/docs/` protection (the fourth enforcement in this
//! lineage). The divergence is pinned by
//! `t6e3_regress.rs::t6e4_spec_divergence_is_pinned`.
//!
//! The scene pins, each asserting both the read path and amcheck:
//!
//! - `audit_probe_...`: the owner-tier audit's probe — SET NOT NULL
//!   (v2), an unrelated ADD COLUMN (v3), a planted v2 row with a NULL
//!   on the constrained column — XX001 on `get`/`scan`, on UPDATE's
//!   and DELETE's EPQ-base decode, and an I-ROWFMT finding in amcheck
//!   (CLEAR-ALL's catcher: the old any-`v < V` clear-all serves it).
//! - `create_time_...`: the over-correction's headline column — a NOT
//!   NULL **pk** column with a planted v1 NULL row after two unrelated
//!   ADD COLUMNs — likewise XX001 on the read and in amcheck.
//! - `pre_constraint_null_still_served_after_add_column`: the e4 legal
//!   scene (the pre-constraint NULL served at a snapshot below the
//!   delete + SET NOT NULL pair) still holds after a later ADD COLUMN
//!   (E4-REGRESS's catcher: a check made unconditional XX001s it).
//! - `drop_then_reset_not_null`: the marker's provenance at the helpers
//!   (NO-RESET's catcher: DROP resets, a re-SET takes the new version,
//!   a SET on an already-constrained column keeps its era), then the
//!   scene: a NULL row written while dropped is served below the re-SET
//!   (a stale marker makes the post-DROP def bytes undecodable — the
//!   word is written faithfully, never masked, for a live column), a
//!   NULL planted at the re-SET's own version is XX001
//!   (SINCE-OFF-BY-ONE's catcher — the `v >= since` boundary), and a
//!   legal-era NULL row visible under the constrained def is served
//!   and amcheck-clean (AMCHECK-OLD's catcher: a checker with its own
//!   strict rule reports it).
//! - `marker_persists_...`: the word's own contract — the exact
//!   round-trip of a marker installed below the current version, the
//!   three inconsistent encodings rejected (XX001), and the era's
//!   survival across a crash-reopen (the probe through reopened defs).

mod t6b_support;

use t6b_support::{
    i32_d, i32_v, ok, pk_schema, plant_live_version, row_key_of, set_col, text_d, Rig,
};

use nucleus_catalog::amcheck::check_table;
use nucleus_catalog::{DbError, Stmt};
use nucleus_codec::row::{ColumnDef, RowError, TableSchema};
use nucleus_codec::{Collation, KeyType};
use nucleus_txn::locks::RelLockMode;

/// The XX001 signature every corruption leg asserts: the NOT NULL
/// decode's own message and SQLSTATE.
fn assert_xx001(err: DbError) {
    assert!(
        matches!(
            &err,
            DbError::Row(RowError::Corrupt("NULL bit on a NOT NULL column"))
        ),
        "the NOT NULL corruption signature, not {err:?}"
    );
    assert_eq!(err.sqlstate(), "XX001");
}

/// A row-byte producer for eras that no legal writer can encode: the
/// given shape at `version` with every NOT NULL flag cleared (the
/// corruption oracle's primitive — header version and column layout
/// are the def's, the refusal is bypassed).
fn era_producer(schema: &TableSchema, version: u32) -> TableSchema {
    let mut era = schema.clone();
    era.version = version;
    for col in &mut era.columns {
        col.not_null = false;
    }
    era
}

/// `CREATE TABLE t (id int4 PRIMARY KEY, b int4 NOT NULL)` — the
/// drop/re-set fixture (`b`'s constraint is in force from v1).
fn nn_b_schema() -> TableSchema {
    ok(TableSchema::new(
        vec![
            ColumnDef {
                attnum: 1,
                name: "id".into(),
                ty: KeyType::Int4,
                not_null: true,
                not_null_since: 1,
                dropped: false,
                missing: None,
            },
            ColumnDef {
                attnum: 2,
                name: "b".into(),
                ty: KeyType::Int4,
                not_null: true,
                not_null_since: 1,
                dropped: false,
                missing: None,
            },
        ],
        vec![1],
    ))
}

/// A DDL statement in its own RC txn, committed: returns its commit ts.
fn committed_ddl(rig: &Rig, f: impl FnOnce(&Stmt<'_>) -> Result<(), DbError>) -> nucleus_txn::Ts {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    f(&st).unwrap_or_else(|e| panic!("the DDL failed: {e:?}"));
    rig.commit_rc(t).unwrap_or_else(|e| panic!("commit: {e:?}"))
}

/// The audit's probe (the card's Goal): SET NOT NULL installs the
/// constraint at v2, an unrelated ADD COLUMN moves the def to v3, and
/// a planted v2 row carries a NULL on the constrained column. The
/// persisted marker carries the era (2) under the v3 def, so the v2
/// row sits **at** the constraint's `not_null_since`: the NULL is
/// corruption on every path — `get`/`scan`, UPDATE's and DELETE's
/// EPQ-base decode, and amcheck's I-ROWFMT. (C-T6e4's clear-all and
/// the C-T6e4fix `v == V` letter both served it; at 64255d5d — before
/// e4 — the read XX001'd, but only while no unrelated DDL followed.)
#[test]
fn audit_probe_stray_null_at_the_constraints_era_is_xx001_on_every_path() {
    let rig = Rig::new();
    // t(id int4 PK, payload text) at v1, empty.
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, b"e4fix2probe", &pk_schema()));
    ok(rig.commit_rc(t0));
    // SET NOT NULL(payload) → v2 (the empty table validates trivially),
    // then the unrelated ADD COLUMN → v3 (the marker's word appears:
    // 2 ≠ 3, so the def persists the era exactly).
    let t_snn = committed_ddl(&rig, |st| rig.db.set_not_null(st, oid, 2, true));
    let t_add = committed_ddl(&rig, |st| {
        rig.db.add_column(st, oid, "x", KeyType::Int4, false, None)
    });
    assert!(t_add > t_snn);

    // The planted v2 row: header version 2 (the constraint's own era),
    // payload NULL — bytes no legal writer could produce post-SNN.
    let bytes = ok(era_producer(&pk_schema(), 2).encode_row(&[i32_d(2), None]));
    let row2 = row_key_of(sid, &pk_schema(), &[i32_v(2)]);
    plant_live_version(&rig, &row2, t_add, &bytes);

    // (1) The read path: the handle's marker is the constraint's own
    // version (2), not the def's (3) — the era survived the unrelated
    // DDL — and the stray NULL XX001s on both `get` and `scan`.
    let t_r = rig.rc();
    let s_r = rig.core().visible_ts();
    let st_g = rig.stmt(&t_r, s_r);
    let hg = ok(rig.db.open_table(&st_g, oid, RelLockMode::AccessShare));
    assert_eq!(hg.def_version, 3, "the handle carries the post-ADD def");
    assert_eq!(
        (true, 2),
        (
            hg.schema.columns[1].not_null,
            hg.schema.columns[1].not_null_since
        ),
        "the persisted marker names the constraint's own version"
    );
    assert_xx001(
        rig.db
            .get(&st_g, &hg, &[i32_v(2)])
            .expect_err("the stray v2 NULL under the v3 def is corruption"),
    );
    assert_xx001(
        rig.db
            .scan(&st_g, &hg)
            .expect_err("the scan hits the same decode"),
    );
    ok(rig.core().abort(t_r));

    // (2) UPDATE's EPQ base decode: the base row is the planted era
    // bytes, and the decode refuses them before any set applies.
    let t_u = rig.rc();
    let s_u = rig.core().visible_ts();
    let st_u = rig.stmt(&t_u, s_u);
    assert_xx001(
        rig.db
            .update(
                &st_u,
                oid,
                &[i32_v(2)],
                &[set_col(2, text_d("z")), set_col(3, i32_d(9))],
            )
            .expect_err("the update's base decode refuses the stray NULL"),
    );
    ok(rig.core().abort(t_u));

    // (3) DELETE's EPQ base decode: same base, same refusal.
    let t_d = rig.rc();
    let s_d = rig.core().visible_ts();
    let st_d = rig.stmt(&t_d, s_d);
    assert_xx001(
        rig.db
            .delete(&st_d, oid, &[i32_v(2)])
            .expect_err("the delete's base decode refuses the stray NULL"),
    );
    ok(rig.core().abort(t_d));

    // (4) amcheck over the corrupted state: an I-ROWFMT finding — the
    // checker decodes through the same rule, never a softer own copy.
    let g = rig.core().registry.take_snapshot();
    let report = ok(check_table(&rig.db, oid, &g));
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.contains("I-ROWFMT") && v.contains("NULL bit on a NOT NULL column")),
        "the stray NULL must be an I-ROWFMT finding:\n{}",
        report.violations.join("\n")
    );
    drop(g);
}

/// The over-correction's headline column: a NOT NULL **pk** column is
/// in force from v1 (`not_null_since` = 1 at CREATE TABLE), so no
/// amount of later unrelated DDL may serve a NULL on it — the planted
/// v1 row sits at the marker (1 == 1) and is corruption on the read
/// path and in amcheck, where the decode itself refuses it (the pk
/// key-invariant check is never even reached).
#[test]
fn create_time_not_null_pk_column_binds_across_unrelated_ddl() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, b"e4fix2pk", &pk_schema()));
    ok(rig.commit_rc(t0));
    // Two unrelated DDLs: the def is at v3, the id constraint unchanged
    // since v1 (the word persists 1).
    committed_ddl(&rig, |st| {
        rig.db.add_column(st, oid, "a", KeyType::Int4, false, None)
    });
    committed_ddl(&rig, |st| {
        rig.db
            .add_column(st, oid, "b", KeyType::Text(Collation::C), false, None)
    });

    // A v1 row with a NULL on the pk column, planted under the row key
    // a NULL id itself encodes to (so I-ROWFMT's key re-encode check is
    // not the thing on trial here — the NOT NULL clause is).
    let bytes = ok(era_producer(&pk_schema(), 1).encode_row(&[None, text_d("p")]));
    let row_null = row_key_of(sid, &pk_schema(), &[None]);
    plant_live_version(&rig, &row_null, rig.core().visible_ts(), &bytes);

    let t_r = rig.rc();
    let s_r = rig.core().visible_ts();
    let st_r = rig.stmt(&t_r, s_r);
    let hr = ok(rig.db.open_table(&st_r, oid, RelLockMode::AccessShare));
    assert_eq!(hr.def_version, 3, "two ADD COLUMNs moved the def");
    assert_eq!(
        (true, 1),
        (
            hr.schema.columns[0].not_null,
            hr.schema.columns[0].not_null_since
        ),
        "the create-time constraint's era survived both ADD COLUMNs"
    );
    assert_xx001(
        rig.db
            .scan(&st_r, &hr)
            .expect_err("a NULL on a create-time NOT NULL pk column is corruption"),
    );
    ok(rig.core().abort(t_r));

    let g = rig.core().registry.take_snapshot();
    let report = ok(check_table(&rig.db, oid, &g));
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.contains("I-ROWFMT") && v.contains("NULL bit on a NOT NULL column")),
        "the decode itself must refuse the pk-NULL row:\n{}",
        report.violations.join("\n")
    );
    drop(g);
}

/// The e4 legal scene stays legal under the corrected rule: a row's
/// pre-constraint NULL is served at a snapshot below the concurrent
/// delete + SET NOT NULL pair — here **also after a later ADD COLUMN**
/// moved the def to v3 under the reader, the exact shape the
/// over-correction got wrong (it served the audit probe's stray NULL
/// from the same def shape). A check made unconditional (E4-REGRESS)
/// XX001s the served row here.
#[test]
fn pre_constraint_null_still_served_after_a_later_add_column() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st0, b"e4fix2legal", &pk_schema()));
    ok(rig.commit_rc(t0));
    // The NULL row, committed below every later snapshot (v1 era).
    let t1 = {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(1), None]));
        ok(rig.commit_rc(t))
    };

    // The RR reader takes S at or below the NULL row's commit.
    let (rr, sguard, s) = rig.rr();
    assert!(t1 <= s, "S={s:?} at or below the NULL row's commit {t1:?}");

    // The concurrent pair above S: the delete (whose tombstone the
    // validating scan sees), then the SET NOT NULL (v2, since 2).
    let hd = rig.rc();
    let hs = rig.core().visible_ts();
    let hstd = rig.stmt(&hd, hs);
    assert!(
        ok(rig.db.delete(&hstd, oid, &[i32_v(1)])),
        "the row was live"
    );
    let t_del = ok(rig.commit_rc(hd));
    assert!(t_del > s, "the delete commits above S");
    let t_snn = committed_ddl(&rig, |st| rig.db.set_not_null(st, oid, 2, true));
    assert!(t_snn > t_del);
    // The unrelated ADD COLUMN (v3) — the reader's later statements
    // decode under it, with the era (2) persisted in the word.
    let t_add = committed_ddl(&rig, |st| {
        rig.db.add_column(st, oid, "x", KeyType::Int4, false, None)
    });
    assert!(t_add > t_snn);

    // The reads at S: the NULL row served, the added column NULL (no
    // default) — exactly the `[Int4, None, None]` shape the audit probe
    // returned clean under the over-correction, here from a legal row
    // (its era, v1, is below the constraint's 2).
    let st_g = rig.stmt(&rr, s);
    let hg = ok(rig.db.open_table(&st_g, oid, RelLockMode::AccessShare));
    assert_eq!(hg.def_version, 3, "the handle carries the post-ADD def");
    assert_eq!(
        (true, 2),
        (
            hg.schema.columns[1].not_null,
            hg.schema.columns[1].not_null_since
        ),
        "the era is the SET's version, not the def's"
    );
    assert_eq!(
        ok(rig.db.get(&st_g, &hg, &[i32_v(1)])),
        Some(vec![i32_d(1), None, None]),
        "the pre-constraint NULL row is served under the v3 def"
    );
    let st_c = rig.stmt(&rr, s);
    let rows = ok(rig.db.scan(&st_c, &hg));
    assert_eq!(rows.len(), 1, "one row visible at S");
    assert_eq!(rows[0].1, vec![i32_d(1), None, None]);

    // Amcheck at the pair's own snapshot sees neither the constrained
    // def nor the row (the catalog read runs at `at.ts()`); at the
    // latest state the row is a tombstone. Both stay clean — and the
    // era-rule half lives in the drop/re-set pin's planted leg below.
    rig.abort_txn(rr);
    drop(sguard);
    let g = rig.core().registry.take_snapshot();
    assert!(
        ok(check_table(&rig.db, oid, &g)).is_clean(),
        "amcheck after"
    );
    drop(g);
}

/// DROP NOT NULL resets the era marker: rows written while the
/// constraint was dropped may legally carry the NULL, so the re-SET's
/// `not_null_since` is the **new** version and those rows stay lenient —
/// while a NULL planted at the re-SET's own version is corruption. The
/// planted legal-era row (visible under the constrained def, byte-wise
/// indistinguishable from a legal pre-constraint row) must also keep
/// amcheck clean: the checker shares the read path's rule. The marker's
/// helper-level contract is pinned first (the persisted word carries it
/// through the catalog; a stale marker on a live nullable column is
/// written faithfully and makes the def bytes undecodable: NO-RESET's
/// catcher).
#[test]
fn drop_then_reset_not_null_scopes_the_era_to_the_reset() {
    // The marker's provenance, at the helpers: create-time NOT NULL
    // carries since 1; DROP NOT NULL resets it to 0; a re-SET takes the
    // DDL's new version; a SET on an already-constrained column keeps
    // the era it came into force.
    let v1 = nn_b_schema();
    assert_eq!(
        (true, 1),
        (v1.columns[1].not_null, v1.columns[1].not_null_since)
    );
    let v2 = ok(v1.set_not_null(2, false));
    assert_eq!(
        (false, 0),
        (v2.columns[1].not_null, v2.columns[1].not_null_since)
    );
    let v3 = ok(v2.set_not_null(2, true));
    assert_eq!(
        (true, 3),
        (v3.columns[1].not_null, v3.columns[1].not_null_since),
        "a re-SET after a DROP takes the newer version"
    );
    let v4 = ok(v3.set_not_null(2, true));
    assert_eq!(
        (true, 3),
        (v4.columns[1].not_null, v4.columns[1].not_null_since),
        "a SET on an already-constrained column keeps its era"
    );

    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, b"e4fix2reset", &nn_b_schema()));
    ok(rig.commit_rc(t0));
    // DROP NOT NULL(b) → v2 (marker reset to 0; a stale marker here
    // would make every later def decode XX001 — the word is written
    // faithfully for a live nullable column, never masked).
    committed_ddl(&rig, |st| rig.db.set_not_null(st, oid, 2, false));
    // The NULL row, written while the constraint was dropped (v2 era).
    let t1 = {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(1), None]));
        ok(rig.commit_rc(t))
    };

    // The RR reader at S; the concurrent pair above it (the delete whose
    // tombstone the re-SET's validating scan sees, then the re-SET → v3,
    // marker 3).
    let (rr, sguard, s) = rig.rr();
    assert!(t1 <= s);
    let hd = rig.rc();
    let hs = rig.core().visible_ts();
    let hstd = rig.stmt(&hd, hs);
    assert!(ok(rig.db.delete(&hstd, oid, &[i32_v(1)])));
    let t_del = ok(rig.commit_rc(hd));
    assert!(t_del > s);
    let t_rex = committed_ddl(&rig, |st| rig.db.set_not_null(st, oid, 2, true));
    assert!(t_rex > t_del);

    // The NULL row written while dropped is served at S: its era (v2) is
    // below the re-SET's marker (v3). NO-RESET (the drop keeping the old
    // marker) and E4-REGRESS (an unconditional check) both fail here.
    let st_g = rig.stmt(&rr, s);
    let hg = ok(rig.db.open_table(&st_g, oid, RelLockMode::AccessShare));
    assert_eq!(hg.def_version, 3, "the handle carries the re-SET def");
    assert_eq!(
        ok(rig.db.get(&st_g, &hg, &[i32_v(1)])),
        Some(vec![i32_d(1), None]),
        "a NULL written while the constraint was dropped is served below \
         the re-SET"
    );
    rig.abort_txn(rr);
    drop(sguard);
    let g0 = rig.core().registry.take_snapshot();
    assert!(
        ok(check_table(&rig.db, oid, &g0)).is_clean(),
        "amcheck after the pair"
    );
    drop(g0);

    // A legal-era NULL row (v2 bytes) visible under the constrained def:
    // byte-identical to what a legal history could not resurrect (the
    // re-SET validated the latest state), so the engine serves it and
    // amcheck — sharing `TableSchema::decode_row` — must stay clean.
    // AMCHECK-OLD (the checker keeping its own strict rule) reports it.
    let era2 = ok(era_producer(&nn_b_schema(), 2).encode_row(&[i32_d(4), None]));
    let row4 = row_key_of(sid, &nn_b_schema(), &[i32_v(4)]);
    plant_live_version(&rig, &row4, rig.core().visible_ts(), &era2);
    {
        let t_r = rig.rc();
        let s_r = rig.core().visible_ts();
        let st_r = rig.stmt(&t_r, s_r);
        let hr = ok(rig.db.open_table(&st_r, oid, RelLockMode::AccessShare));
        assert_eq!(
            ok(rig.db.get(&st_r, &hr, &[i32_v(4)])),
            Some(vec![i32_d(4), None]),
            "the legal-era NULL row is served under the re-SET def"
        );
        ok(rig.core().abort(t_r));
    }
    let g1 = rig.core().registry.take_snapshot();
    assert!(
        ok(check_table(&rig.db, oid, &g1)).is_clean(),
        "a legal-era NULL row is not an amcheck finding:\n{}",
        ok(check_table(&rig.db, oid, &g1)).violations.join("\n")
    );
    drop(g1);

    // A NULL planted at the re-SET's own version (v3 = marker 3): the
    // boundary row — `v >= since` binds, `v > since` would not
    // (SINCE-OFF-BY-ONE).
    let era3 = ok(era_producer(&nn_b_schema(), 3).encode_row(&[i32_d(3), None]));
    let row3 = row_key_of(sid, &nn_b_schema(), &[i32_v(3)]);
    plant_live_version(&rig, &row3, rig.core().visible_ts(), &era3);
    {
        let t_r = rig.rc();
        let s_r = rig.core().visible_ts();
        let st_r = rig.stmt(&t_r, s_r);
        let hr = ok(rig.db.open_table(&st_r, oid, RelLockMode::AccessShare));
        assert_xx001(
            rig.db
                .get(&st_r, &hr, &[i32_v(3)])
                .expect_err("a NULL at the re-SET's own version is corruption"),
        );
        ok(rig.core().abort(t_r));
    }
    let g2 = rig.core().registry.take_snapshot();
    let report = ok(check_table(&rig.db, oid, &g2));
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.contains("I-ROWFMT") && v.contains("does not decode")),
        "the planted-at-since NULL must be an I-ROWFMT finding:\n{}",
        report.violations.join("\n")
    );
    drop(g2);
}

/// Hand-assembled §3 def bytes, so the marker-word cases do not depend
/// on the encoder: one int4 column `c` (attnum 1, tag 03), flags byte
/// and the optional `be32` word the flags gate. `hidden` and `pk` make
/// it a one-column pk table (`hidden_rowid` 0, npk 1).
fn marker_def(version: u32, flags: u8, word: Option<u32>) -> Vec<u8> {
    let mut out = vec![0x54, 0x01];
    out.extend_from_slice(&version.to_be_bytes());
    out.push(0); // hidden_rowid: pk table
    out.extend_from_slice(&1u16.to_be_bytes()); // ncols
    out.extend_from_slice(&1u16.to_be_bytes()); // attnum
    out.extend_from_slice(&1u32.to_be_bytes()); // namelen
    out.push(b'c');
    out.push(0x03); // int4 tag
    out.push(flags);
    if let Some(w) = word {
        out.extend_from_slice(&w.to_be_bytes());
    }
    out.extend_from_slice(&1u16.to_be_bytes()); // npk
    out.extend_from_slice(&1u16.to_be_bytes()); // pk attnum
    out
}

/// The word's own contract: the marker installed below the current
/// version round-trips through the def bytes exactly (elided when it
/// equals the re-derivation — the golden corpus's every def — and
/// persisted when it does not), the three inconsistent encodings are
/// rejected as corruption (XX001), and a marker that deviates is never
/// silently canonicalized away. The final leg is the era's survival
/// across a crash-reopen: the SET NOT NULL's version stays named under
/// the later ADD COLUMN's def, so the audit probe XX001s on reopened
/// defs too — the shape a re-deriving decode served.
#[test]
fn marker_persists_across_the_def_bytes_and_reopens() {
    // The round-trip: SET NOT NULL (v2) then ADD COLUMN (v3) leaves the
    // constrained column's marker at 2 under a version-3 def — written
    // (2 ≠ 3) and read back exactly. A create-time marker (1 under a
    // version-1 def) is elided and re-derived: the golden corpus's
    // shapes, byte-identical to the pre-marker layout.
    let v3 = ok(pk_schema()
        .set_not_null(2, true) // v2: the constraint's era
        .and_then(|s| s.add_column("x", KeyType::Int4, false, None))); // v3
    assert_eq!(
        (true, 2),
        (v3.columns[1].not_null, v3.columns[1].not_null_since)
    );
    let back = TableSchema::decode(&ok(v3.encode())).expect("the def decodes");
    assert_eq!(v3.version, back.version);
    assert_eq!(
        (true, 2),
        (back.columns[1].not_null, back.columns[1].not_null_since),
        "the era survives the def bytes"
    );
    assert_eq!(
        (false, 0),
        (back.columns[2].not_null, back.columns[2].not_null_since),
        "the added nullable column carries no marker"
    );
    let v1_back = TableSchema::decode(&ok(pk_schema().encode())).expect("the v1 def decodes");
    assert_eq!(
        (false, 0),
        (
            v1_back.columns[1].not_null,
            v1_back.columns[1].not_null_since
        ),
        "an elided marker re-derives exactly"
    );

    // A stale marker on a live nullable column is written faithfully
    // (never masked): the def bytes a NO-RESET drop would produce are
    // themselves undecodable, so the broken state cannot persist
    // silently.
    let mut stale = ok(pk_schema().set_not_null(2, false)); // v2, nullable
    stale.columns[1].not_null_since = 1; // the state a no-reset drop keeps
    let stale_bytes = ok(stale.encode());
    assert!(
        matches!(
            TableSchema::decode(&stale_bytes),
            Err(RowError::Corrupt(
                "not_null_since word on a nullable column"
            ))
        ),
        "the stale marker is corruption in the bytes, not a silent mask"
    );

    // The three inconsistent encodings (§3's decode rejections), each
    // hand-assembled so the encoder is not on trial:
    // (a) `not_null && since == 0`.
    assert_eq!(
        Err(RowError::Corrupt(
            "not_null_since of zero on a NOT NULL column"
        )),
        TableSchema::decode(&marker_def(3, 0x01 | 0x10, Some(0)))
    );
    // (b) `!not_null && since != 0` (a nullable or dropped column
    // carries no era).
    assert_eq!(
        Err(RowError::Corrupt(
            "not_null_since word on a nullable column"
        )),
        TableSchema::decode(&marker_def(3, 0x10, Some(1)))
    );
    assert_eq!(
        Err(RowError::Corrupt(
            "not_null_since word on a nullable column"
        )),
        TableSchema::decode(&marker_def(3, 0x02 | 0x10, Some(1)))
    );
    // (c) `since > version`.
    assert_eq!(
        Err(RowError::Corrupt("not_null_since above the version")),
        TableSchema::decode(&marker_def(3, 0x01 | 0x10, Some(4)))
    );
    // The consistent encodings decode: a word below the version names
    // its era exactly, and the elided forms re-derive.
    let word = TableSchema::decode(&marker_def(3, 0x01 | 0x10, Some(2))).expect("decodes");
    assert_eq!(
        (true, 2),
        (word.columns[0].not_null, word.columns[0].not_null_since)
    );
    let elided = TableSchema::decode(&marker_def(3, 0x01, None)).expect("decodes");
    assert_eq!(
        (true, 3),
        (elided.columns[0].not_null, elided.columns[0].not_null_since)
    );

    // The reopen leg: SET NOT NULL (v2), ADD COLUMN (v3), crash-reopen —
    // the reopened def still names the constraint's version 2 (through
    // the word, not the re-derivation a pre-marker decode performed),
    // so the probe's planted v2 NULL row XX001s on the read path and is
    // an I-ROWFMT finding.
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, b"e4fix2reopen", &pk_schema()));
    ok(rig.commit_rc(t0));
    committed_ddl(&rig, |st| rig.db.set_not_null(st, oid, 2, true));
    committed_ddl(&rig, |st| {
        rig.db.add_column(st, oid, "x", KeyType::Int4, false, None)
    });
    let rig = rig.crash_reopen(0);
    let t_p = rig.rc();
    let s_p = rig.core().visible_ts();
    let st_p = rig.stmt(&t_p, s_p);
    let hp = ok(rig.db.open_table(&st_p, oid, RelLockMode::AccessShare));
    assert_eq!(hp.def_version, 3, "the reopened def is the v3 def");
    assert_eq!(
        (true, 2),
        (
            hp.schema.columns[1].not_null,
            hp.schema.columns[1].not_null_since
        ),
        "the reopened def names the SET's version through the word"
    );
    let probe = ok(era_producer(&pk_schema(), 2).encode_row(&[i32_d(2), None]));
    let row2 = row_key_of(sid, &pk_schema(), &[i32_v(2)]);
    plant_live_version(&rig, &row2, rig.core().visible_ts(), &probe);
    assert_xx001(rig.db.get(&st_p, &hp, &[i32_v(2)]).expect_err(
        "the probe's stray v2 NULL is corruption under the \
                     reopened v3 def",
    ));
    ok(rig.core().abort(t_p));
    let g = rig.core().registry.take_snapshot();
    let report = ok(check_table(&rig.db, oid, &g));
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.contains("I-ROWFMT") && v.contains("NULL bit on a NOT NULL column")),
        "the probe is an I-ROWFMT finding after the reopen:\n{}",
        report.violations.join("\n")
    );
    drop(g);
}
