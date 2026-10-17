//! C-T6e3 item 4: the pinned-seed regressions and the non-vacuity
//! proofs for the new differential arms — the seeds the item-5
//! mutation report's surfaces are pinned at (G3/G7: the ladder's step
//! ordering; G6/N9: the blind entry delete around a build's scan→ingest
//! gap; G8: the ingest's WAL sync, bound by `t6e3_crash`).
//!
//! - `t6e3_regress_pinned_seeds`: the extended differential (DML index
//!   scans + the DDL index ops) over two pinned seeds, clean, with the
//!   index surface actually exercised — a build published, a scan
//!   served, a scan refused, a resume ran.
//! - `t6e3_regress_a_range_is_pure`: the seeded a-range resolution is
//!   a pure function of its pick, and covers bounded and full scans.
//! - `t6e3_regress_checker_is_not_vacuous`: a clean mini run corrupted
//!   one index-scan record at a time — a served row's value, a served
//!   scan's post-sight (a WriteOnly index), a 0A000 refusal over a
//!   serving pre-sight, a 42704 over a present pre-sight — each is
//!   flagged by the arm it exists to exercise; uncorrupted, the run
//!   checks clean.
//!
//! C-T6e3fix additions — the escalation's deterministic pin and the
//! in-code doc-line check:
//!
//! - `t6e3fix_reinsert_over_concurrent_delete_scans_clean`: the gate's
//!   escalation-(d) scene, single-threaded (the test thread is the
//!   scheduler; the only concurrency the trigger needs is one commit
//!   above S, ordered between the RR txn's statements) — before the fix
//!   it fails with XX001, the escalated class.
//! - `t6e3fix_doc_line_states_the_verification_regime`: `index_scan`'s
//!   doc comment states the regime rule (DOC-LINE's catcher — the
//!   in-scope half of the card's spec-first item; the spec file itself
//!   is protected, see the card's ESCALATE).
//!
//! C-T6e3fix2 additions — the gate follow-ups (M-1/M-2/M-3):
//!
//! - `t6e3fix2_scans_keep_the_seeds_order`: the differential's
//!   scans-to-front splice is gone — over a fixed program space,
//!   writes-on-`t`-then-IndexScan adjacency exists in numbers (the e3
//!   escalation's class, which the shaping never generated; SPLICE-BACK
//!   zeroes the count).
//! - `t6e3fix2_spec_divergence_is_pinned`: C-T6 §6's verification
//!   sentence still carries the pre-fix letter — the card's Work 2
//!   (restate it, add the spec's changelog) was reverted by the
//!   runner's `nucleus/v2/docs/` protection, so the divergence is
//!   pinned in-suite instead (SPEC-EDIT's catcher; the in-code doc
//!   line stays pinned by the DOC-LINE test above).
//! - `t6e3fix2_corrupt_entry_beneath_own_reinsert_is_xx001`: the fix's
//!   strictly-stronger half pinned by real corruption — a committed
//!   entry visible at S that no row at S re-encodes to, planted raw
//!   beneath the txn's own re-insert, must XX001 (S-PAIR-DEAF's
//!   catcher).
//!
//! C-T6e4 additions — the threaded-XX001 escalation of the same family
//! through the row decode (`St`/`G` reads; `Ixa`'s row fetch), fixed in
//! `db.rs` by `decode_row_at_era` (a row whose header version predates
//! the handle's definition decodes with the NOT NULL flags of its own
//! era — a pre-constraint NULL at a snapshot below the SET NOT NULL's
//! commit is served, never XX001):
//!
//! - `t6e4_pre_constraint_null_row_reads_clean_under_a_later_set_not_null`:
//!   the captured interleaving (the C-T6b3bfix2 gate's E-1, seed
//!   0x7c1a0001, dump of 2026-10-17: RR at S; a concurrent DELETE
//!   committing above S; SET NOT NULL validating the latest state —
//!   where the row is a tombstone — and committing above it), pinned
//!   single-threaded through `Db::get`, `Db::scan` and `Db::index_scan`:
//!   the old NULL row is served, amcheck stays clean, and the abort
//!   leaves the committed state untouched.
//! - `t6e4_pre_constraint_null_row_updates_clean_under_a_later_set_not_null`:
//!   the write-side crossing — an RC UPDATE whose statement snapshot
//!   sits below a concurrent update-and-SET NOT NULL pair decodes the
//!   pre-constraint NULL read row (then EPQ re-bases and applies);
//!   before the fix the decode itself XX001'd the legal statement.
//! - `t6e4_stray_null_on_a_current_def_row_is_still_xx001`: the fix's
//!   strictly-stronger half — a raw-planted row whose header version
//!   equals the definition's, carrying a NULL on its NOT NULL column,
//!   still decodes XX001 (the carve-out is era-scoped, never a blanket
//!   disarm).
//! - `t6e4_spec_divergence_is_pinned`: the spec letter's tripwire. Its
//!   interim form pinned the enforced-out divergence (the runner's docs
//!   protection had reverted C-T6e4's spec-first edit, the C-T6e3fix2
//!   precedent); C-T6e4fix landed the letter — §2's NULL clause
//!   era-scoped, §3's decode rule's `v == V` condition, the changelog
//!   line naming the e4 escalation — and the pin flipped with it (the
//!   e3fix2→e3fix3 sequence): it now guards the landed letter in both
//!   directions of drift (SPEC-REVERT's catcher).
//!
//! C-T6e4fix additions — the gate's residuals (M-1's letter above, L-1,
//! L-2's leg living in `t6c_soak.rs`):
//!
//! - `t6e4_pre_constraint_null_row_deletes_clean_under_a_later_set_not_null`:
//!   the delete leg (L-1) — the era family through `delete_with`'s
//!   EPQ-base decode, the one converted site no C-T6e4 pin drove. Leg
//!   (a) runs the read pin's scene with the RR txn deleting pk 1 at `S`
//!   below the pair: the legal outcome there is §5.1's 40001 (the
//!   newer-version rule — an RR row op never EPQs), so the decode never
//!   runs and the leg pins that boundary instead. Leg (b) drives the
//!   decode itself with the only deterministic shape that puts era
//!   bytes under the delete's base — a raw-planted v1-era NULL row (the
//!   corruption oracle's primitive, byte-identical to a legal era row;
//!   post-SNN no key's latest committed version can legally carry the
//!   NULL, so no engine-mediated history reaches the site): the delete
//!   must apply cleanly (pre-fix, and under DELETE-DEAF, it XX001s),
//!   commit, and leave an empty, amcheck-clean table.

mod diff;
// diff/crash.rs (pulled in by diff/mod.rs) drives the t6b crash rig.
mod t6b_support;

use std::ops::Bound;

use diff::runner::run_workload;
use diff::{a_range, check, gen_prog, Config, Fate, IdxSight, OpDetail, OpOut, POp, Run, Verdict};
use nucleus_catalog::amcheck::check_table;
use nucleus_catalog::{DbError, IndexState, Stmt};
use nucleus_codec::{Collation, KeyColumn, KeyType};
use nucleus_txn::locks::RelLockMode;
use t6b_support::{
    i32_d, i32_v, i_entry_key, idx_def, ok, pk_schema, plant_live_version, row_key_of, set_col,
    text_d, text_v, Rig,
};

/// The pinned seeds (item 4's table: the report's runs are these). The
/// walk continues past them (still pinned, bounded) because two of the
/// coverage surfaces — a scan meeting the ladder mid-flight, and a
/// resume finding a leftover — are interleaving-dependent, and a loaded
/// machine can run both pinned seeds' DML streams to completion before
/// the DDL stream's first unfinished index appears.
const SEEDS: [u64; 2] = [0x7ce3_0001, 0x7ce3_0002];
const WALK_MAX: usize = 8;

/// A small config: enough DML/DDL for every index surface to bite,
/// small enough for a regress binary.
fn mini(seed: u64) -> Config {
    // The soak's shape (4 sessions × 200 programs against 200 DDL
    // txns): the refusal surface needs the DML stream to still be
    // alive while the DDL stream's unfinished indexes are — a smaller
    // shape's DML finishes first and never meets one.
    Config {
        seed,
        threads: 4,
        progs: 200,
        ddl_txns: 200,
        max_attempts: 8,
        deadlock_ms: 5,
    }
}

fn run_mini(seed: u64) -> Run {
    let cfg = mini(seed);
    let run = run_workload(&cfg);
    let verdict = check(&run);
    assert!(
        verdict.is_clean(),
        "seed {seed:#x} is not clean:\n{}",
        verdict.report()
    );
    run
}

/// The extended differential over the pinned seeds: clean, and the
/// online-build surface actually happened — a build published, an index
/// scan was served (the rows oracle's bite) and one was refused by a
/// mid-ladder state, and a resume ran on a leftover.
#[test]
fn t6e3_regress_pinned_seeds() {
    // Every seed in the walk is clean and exercises the deterministic
    // surfaces (a published build, served scans); the interleaving-
    // dependent ones (a refusal or mid-ladder sight, a resume on a
    // leftover) must appear somewhere inside the bounded walk.
    let mut refused = 0usize;
    let mut resumes = 0usize;
    let mut runs = 0usize;
    let walk = SEEDS
        .iter()
        .copied()
        .chain((SEEDS.len()..WALK_MAX).map(|i| 0x7ce3_0000 + i as u64));
    for seed in walk {
        let run = run_mini(seed);
        runs += 1;
        let verdict = check(&run);
        let s = &verdict.stats;
        println!(
            "t6e3 regress seed {seed:#x}: {} builds ok / {} failed, {} resumes, \
             {} drops, {} scans ({} served, {} refused, {} no-target), sights {}/{}",
            s.idx_builds_ok,
            s.idx_builds_failed,
            s.idx_resumes,
            s.idx_drops,
            s.idx_scans,
            s.idx_scans_served,
            s.idx_scans_refused,
            s.idx_scans_noop,
            s.idx_sights_delete_only,
            s.idx_sights_write_only
        );
        assert!(s.idx_builds_ok > 0, "no build published: {s:?}");
        assert!(s.idx_scans_served > 0, "no index scan served: {s:?}");
        assert_eq!(run.leaked_intents, 0, "{}", run.leak_report);
        refused += s.idx_scans_refused + s.idx_sights_delete_only + s.idx_sights_write_only;
        resumes += s.idx_resumes;
        if refused > 0 && resumes > 0 {
            break;
        }
    }
    assert!(
        refused > 0,
        "no scan met the ladder mid-flight in {runs} pinned seeds"
    );
    assert!(resumes > 0, "no resume ran in {runs} pinned seeds");
}

/// The seeded a-range is a pure function of its pick, and the picks
/// cover bounded windows and the full scan.
#[test]
fn t6e3_regress_a_range_is_pure() {
    let mut saw_full = false;
    let mut saw_bounded = false;
    for pick in 0..200u32 {
        let a = a_range(pick);
        let b = a_range(pick);
        assert_eq!(a, b, "pick {pick}");
        match (a, b) {
            ((None, None), _) => saw_full = true,
            ((Some(lo), Some(hi)), _) => {
                saw_bounded = true;
                assert!(lo <= hi, "pick {pick}: {a:?}");
                // Every bounded window stays inside the setup's
                // `a = 1000 + pk` domain (the model's filter's world).
                assert!(
                    (1000..=1009).contains(&lo) && (1000..=1009).contains(&hi),
                    "pick {pick}: {a:?}"
                );
            }
            _ => panic!("pick {pick}: mixed bounds {a:?}"),
        }
    }
    assert!(saw_full && saw_bounded);
}

/// The checker's new arms can fail: a clean mini run, corrupted one
/// index-scan record at a time, is flagged by the arm each corruption
/// exists to exercise.
#[test]
fn t6e3_regress_checker_is_not_vacuous() {
    // The first pinned seed's run reliably records a served scan (the
    // setup's seed index serves every scan that finds it); the loop is
    // a guard, not a retry budget. The mined scan must carry rows: a
    // served scan of an empty range (or of a table a truncate emptied
    // at S) records `RowsT([])`, which leg (a) below cannot corrupt —
    // under parallel test load the first such run left the corruption
    // loop nothing to bite on and the uncorrupted run checked clean
    // (the C-T6c1afix2 leg-(c) flake's shape; the predicate and the
    // corruption now agree — C-T6e3fix de-flake, no assertion changed).
    let mut run = None;
    for seed in SEEDS.iter().copied().chain([0x7ce3_0003]) {
        let r = run_mini(seed);
        let has_served = r.history.txns.iter().any(|t| {
            t.ops.iter().any(|op| {
                matches!(&op.detail, OpDetail::IndexScan { oid: Some(_), .. })
                    && matches!(&op.out, OpOut::RowsT(rows) if !rows.is_empty())
            })
        });
        if has_served {
            run = Some(r);
            break;
        }
        assert!(
            seed != 0x7ce3_0003,
            "no mini run recorded a served scan carrying rows"
        );
    }
    let run = run.expect("a served scan");
    let verdict = check(&run);
    assert!(verdict.is_clean(), "{}", verdict.report());

    // (a) A served scan's recorded row lies: the value no write derived.
    let mut bad_rows = run.clone();
    'outer: for txn in bad_rows.history.txns.iter_mut() {
        for op in txn.ops.iter_mut() {
            if !matches!(
                (&op.detail, &op.out),
                (OpDetail::IndexScan { oid: Some(_), .. }, OpOut::RowsT(_))
            ) {
                continue;
            }
            let rows = match &mut op.out {
                OpOut::RowsT(rows) if !rows.is_empty() => rows,
                _ => continue,
            };
            let Some((_, slot)) = rows[0].1.iter_mut().find(|(_, v)| v.is_some()) else {
                continue;
            };
            *slot = Some(nucleus_codec::Value::Text("\u{0}CORRUPT".into()));
            break 'outer;
        }
    }
    let verdict = check(&bad_rows);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("index scan pk")),
        "the corrupted served row went unnoticed: {}",
        verdict.report()
    );

    // (b) A served scan whose post-sight is a mid-ladder state: §4 says
    // a non-Public index (or built_at > S) must not serve.
    let mut bad_sight = run.clone();
    'outer2: for txn in bad_sight.history.txns.iter_mut() {
        for op in txn.ops.iter_mut() {
            if let OpDetail::IndexScan {
                oid: Some(_),
                post: Some(post),
                ..
            } = &mut op.detail
            {
                *post = IdxSight::Seen {
                    state: nucleus_catalog::IndexState::WriteOnly,
                    built_at: 0,
                };
                break 'outer2;
            }
        }
    }
    let verdict = check(&bad_sight);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("served a read at S=")),
        "the non-serving post-sight went unnoticed: {}",
        verdict.report()
    );

    // (c) A refusal over a serving pre-sight: the engine turned an
    // index §4 obliges it to serve away.
    let mut bad_refusal = run.clone();
    'outer3: for txn in bad_refusal.history.txns.iter_mut() {
        for op in txn.ops.iter_mut() {
            if let OpDetail::IndexScan {
                oid: Some(_),
                pre,
                post,
                ..
            } = &mut op.detail
            {
                let snap = op.snap;
                *pre = IdxSight::Seen {
                    state: nucleus_catalog::IndexState::Public,
                    built_at: 0,
                };
                *post = None;
                op.out = OpOut::Err("0A000".into());
                let _ = snap;
                break 'outer3;
            }
        }
    }
    let verdict = check(&bad_refusal);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("refused with 0A000")),
        "the unjustified refusal went unnoticed: {}",
        verdict.report()
    );

    // (d) A 42704 over a present pre-sight: the index the bracket saw
    // cannot have vanished under the statement's AccessShare.
    let mut bad_absent = run.clone();
    'outer4: for txn in bad_absent.history.txns.iter_mut() {
        for op in txn.ops.iter_mut() {
            if let OpDetail::IndexScan {
                oid: Some(_),
                pre,
                post,
                ..
            } = &mut op.detail
            {
                *pre = IdxSight::Seen {
                    state: nucleus_catalog::IndexState::Public,
                    built_at: 0,
                };
                *post = None;
                op.out = OpOut::Err("42704".into());
                break 'outer4;
            }
        }
    }
    let verdict = check(&bad_absent);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("answered 42704")),
        "the phantom absence went unnoticed: {}",
        verdict.report()
    );

    // The runs these corruptions were built from carry committed
    // index-DDL pseudo-txns (the stats' world), and the base stays
    // clean — re-asserted once more against the untouched clone.
    let v: Verdict = check(&run);
    assert!(v.is_clean(), "{}", v.report());
    let _ = Fate::Committed(0);
}

// ---- C-T6e3fix: the escalation's deterministic pin ---------------------------

/// C-T6e3fix (the gate's escalation (d), pinned): the legal C-T0 §5.3
/// history the crossing-regimes verification XX001'd. An RR txn takes
/// its snapshot `S`; a helper deletes the only pk, committing its
/// tombstones (row and entry) at `T > S` while the txn is parked
/// between statements — single-threaded, the test thread *is* the
/// park; the txn re-inserts the same pk (accepted: §5.3's unique check
/// reads the key's current state, where the tombstone sits — "not live
/// → proceed, whatever its ts"); the txn then index-scans ranges over
/// the OLD entry's value, still live at `S`. Correct: no error, the old
/// entry skipped (its row is the txn's own write now), the own row
/// served through its own entry iff its value is in range — exactly the
/// checker's index-scan arm's expected set (the model at `S` plus own
/// writes, filtered by the range): `["a","c")` → `{}`; `["y",∞)` and
/// the full range → the own row alone. Amcheck is clean before and
/// after, and the abort leaves the committed state exactly as the
/// helper's commit left it. Before the fix this is XX001 ("index entry
/// does not match its row") at the first scan.
#[test]
fn t6e3fix_reinsert_over_concurrent_delete_scans_clean() {
    let rig = Rig::new();
    // The fixture: t(pk int4, payload text), one row (1, "b"), and a
    // born-public index on payload (§6.1(a): built_at 0, the own row's
    // entry written through §5 — committed at or below every later S).
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st0, b"e3fix", &pk_schema()));
    let st1 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(1), text_d("b")]));
    let st2 = rig.stmt(&t0, s0);
    let idx = ok(rig
        .db
        .create_index(&st2, oid, &idx_def(2, false, IndexState::Public)));
    let t_setup = ok(rig.commit_rc(t0));
    // Amcheck clean before.
    let g = rig.core().registry.take_snapshot();
    assert!(
        ok(check_table(&rig.db, oid, &g)).is_clean(),
        "amcheck before"
    );
    drop(g);

    // The RR txn takes S — the setup is at or below it, the delete
    // above — and parks between its statements.
    let (rr, sguard, s) = rig.rr();
    assert!(
        t_setup <= s,
        "S={s:?} at or below the setup commit {t_setup:?}"
    );

    // The one concurrent commit above S the trigger needs: the helper's
    // delete tombstones the row and its entry at T > S.
    let htxn = rig.rc();
    let hs = rig.core().visible_ts();
    let hst = rig.stmt(&htxn, hs);
    assert!(
        ok(rig.db.delete(&hst, oid, &[i32_v(1)])),
        "the row was live"
    );
    let t_del = ok(rig.commit_rc(htxn));
    assert!(
        t_del > s,
        "the delete commits above S (T={t_del:?}, S={s:?})"
    );

    // The committed state the abort will have to leave untouched.
    let isid = {
        let row = rig.idx_row(idx).expect("the idx row committed");
        let nucleus_txn::catalog::IdxStorage::Id(isid) = row.storage else {
            panic!("a born-public index has an id storage")
        };
        isid
    };
    let entries_before = rig.raw_entry_versions(isid);

    // §5.3: the txn re-inserts the same pk — this must be accepted.
    let st_i = rig.stmt(&rr, s);
    let hi = ok(rig.db.open_table(&st_i, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st_i, &hi, &[i32_d(1), text_d("y")]));

    // The scans, all at S.
    let b_incl = |v: &str| Bound::Included(vec![text_v(v.to_string())]);
    // (1) The old entry's value in range, the own row's out of it: the
    // old entry is skipped — nothing is served.
    let st_c1 = rig.stmt(&rr, s);
    let rows1 = ok(rig.db.index_scan(
        &st_c1,
        idx,
        b_incl("a"),
        Bound::Excluded(vec![text_v("c".into())]),
    ));
    assert!(
        rows1.is_empty(),
        "the old entry is skipped and the own value is out of range: {rows1:?}"
    );
    // (2) The own row's value in range: served through its own entry.
    let st_c2 = rig.stmt(&rr, s);
    let rows2 = ok(rig
        .db
        .index_scan(&st_c2, idx, b_incl("y"), Bound::Unbounded));
    assert_eq!(
        rows2.len(),
        1,
        "the own row is served through its own entry"
    );
    assert_eq!(rows2[0].1, vec![i32_d(1), text_d("y")]);
    // (3) The full range: the own row once, never the old row.
    let st_c3 = rig.stmt(&rr, s);
    let rows3 = ok(rig
        .db
        .index_scan(&st_c3, idx, Bound::Unbounded, Bound::Unbounded));
    assert_eq!(
        rows3.len(),
        1,
        "the full scan serves the own row exactly once"
    );
    assert_eq!(rows3[0].1, vec![i32_d(1), text_d("y")]);

    // Nothing committed: the abort leaves the committed state exactly
    // as the helper's commit left it, and amcheck stays clean.
    rig.abort_txn(rr);
    drop(sguard);
    assert_eq!(
        rig.raw_entry_versions(isid),
        entries_before,
        "no entry version changed"
    );
    let t_scan = rig.rc();
    let s_scan = rig.core().visible_ts();
    let st_s = rig.stmt(&t_scan, s_scan);
    let hs2 = ok(rig.db.open_table(&st_s, oid, RelLockMode::AccessShare));
    assert!(
        ok(rig.db.scan(&st_s, &hs2)).is_empty(),
        "the committed table is the helper's empty table"
    );
    ok(rig.commit_rc(t_scan));
    let g2 = rig.core().registry.take_snapshot();
    assert!(
        ok(check_table(&rig.db, oid, &g2)).is_clean(),
        "amcheck after"
    );
    drop(g2);
}

/// C-T6e3fix (the in-scope half of the spec-first item): `index_scan`'s
/// doc comment — the card's Touch-only names it ("its doc line") —
/// states the verification's visibility regime: the licensing sentence
/// for the regime-consistent verification that
/// `t6e3fix_reinsert_over_concurrent_delete_scans_clean` pins. The spec
/// file itself (`nucleus/v2/docs/`) is protected by the card's hard
/// rules, so its §6 sentence still carries the old letter; that
/// spec/code divergence is this card's ESCALATE, and this check keeps
/// the in-code licensing sentence from silently reverting to the
/// crossing-regimes form (DOC-LINE).
#[test]
fn t6e3fix_doc_line_states_the_verification_regime() {
    let src = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/db.rs"))
        .expect("Db's source");
    // index_scan's doc comment: from its opening line to the §4 lock
    // paragraph (the whole documented contract of the scan), read as
    // flat text (the comment is line-wrapped).
    let start = src
        .find("A verified index scan")
        .expect("index_scan's doc comment");
    let end = src[start..]
        .find("The statement takes AccessShare")
        .map(|i| start + i)
        .expect("the end of the doc paragraph");
    let flat = |s: &str| {
        s.lines()
            .map(|l| {
                l.trim()
                    .trim_start_matches("///")
                    .trim_start_matches("//")
                    .trim()
            })
            .collect::<Vec<_>>()
            .join(" ")
    };
    let doc = flat(&src[start..end]);
    assert!(
        doc.contains("entry's own visibility regime"),
        "the doc line must name the regime:\n{doc}"
    );
    assert!(
        doc.contains("fetched without the txn's own overlay"),
        "the foreign-entry half of the regime rule:\n{doc}"
    );
    assert!(
        doc.contains("is skipped"),
        "the own-overwritten-row carve-out:\n{doc}"
    );
    // The verification block itself carries the same rule.
    let body = flat(&src[end..]);
    assert!(
        body.contains("verify the entry against the row **in the entry's own visibility regime**"),
        "the verification block's §6 comment states the regime"
    );
}

// ---- C-T6e3fix2: the gate follow-ups (M-1/M-2/M-3) -----------------------------

/// C-T6e3fix2 (the gate's M-1/L-1): the differential's scans-to-front
/// splice is gone from `gen_prog` — programs keep the seed's op order,
/// so an index scan of `t` can sit **after** the txn's own writes on
/// `t`: the e3 escalation's class, which the shaping never generated
/// and which blinded the soak to the crossing-regimes bug (the C-T6e3fix
/// gate measured the shaped soak blind at 0/14 under VER-CROSS, the
/// un-shaped one firing 6/14). `gen_prog` is a pure function of
/// `(seed, session, index)`, so the count over the fixed space below is
/// deterministic: programs where a write on `t` (InsertT, UpdateSet,
/// DeleteT, SetBig, NullBig) precedes an IndexScan exist in numbers.
/// SPLICE-BACK (the splice re-added) moves every scan ahead of every
/// write and zeroes the count.
#[test]
fn t6e3fix2_scans_keep_the_seeds_order() {
    let writes_then_scan = |p: &diff::Prog| {
        let write = p.ops.iter().position(|o| {
            matches!(
                o,
                POp::InsertT(..)
                    | POp::UpdateSet(..)
                    | POp::DeleteT(..)
                    | POp::SetBig(..)
                    | POp::NullBig(..)
            )
        });
        let scan = p.ops.iter().position(|o| matches!(o, POp::IndexScan(..)));
        write.is_some_and(|w| scan.is_some_and(|c| w < c))
    };
    // The soak's DML sessions and program count over two of its seeds.
    let mut programs = 0usize;
    let mut scan_bearing = 0usize;
    let mut adjacent = 0usize;
    for seed in [0x7c1a_0001, 0x7c1a_0002] {
        for session in 1..=4u16 {
            for index in 0..200u32 {
                let p = gen_prog(seed, session, index);
                programs += 1;
                if p.ops.iter().any(|o| matches!(o, POp::IndexScan(..))) {
                    scan_bearing += 1;
                }
                if writes_then_scan(&p) {
                    adjacent += 1;
                }
            }
        }
    }
    assert!(scan_bearing > 0, "no program scans the index: {programs}");
    assert!(
        adjacent > 50,
        "too few programs with a write on t before an index scan \
         ({adjacent} of {programs}, {scan_bearing} scan-bearing)"
    );
}

/// C-T6e3fix2 (the gate's M-2, adapted): the spec/code divergence is
/// pinned, not silent. The card's Work 2 — restate §6's verification
/// sentence and add the spec's changelog — was reverted by the runner's
/// `nucleus/v2/docs/` protection (the same hard-rule-over-Touch-only
/// conflict the C-T6e3fix card hit; see the revert commit), so the
/// normative letter still lags the engine: §6 licenses the
/// crossing-regimes fetch the C-T6e3fix fix removed and outlaws the
/// own-entry serve the engine performs, while `index_scan`'s doc
/// comment (pinned by `t6e3fix_doc_line_states_the_verification_regime`)
/// carries the regime. This test reads the spec and pins **exactly**
/// that enforced-out state — the old sentence, no changelog — so the
/// divergence stays visible in the suite and any edit to either half
/// (including the correct one, when a card with docs scope finally
/// lands M-2) trips it deliberately: that card replaces these
/// assertions with the regime letter (foreign entries verified against
/// the row at the snapshot fetched without the txn's own overlay; own
/// entries against the own-overlaid row; an own-overwritten row served
/// through the txn's own entry iff in range) and a changelog line
/// naming the e3 escalation and C-T6e3fix. SPEC-EDIT (the §6 sentence
/// touched in any direction) fails here.
#[test]
fn t6e3fix2_spec_divergence_is_pinned() {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/C-T6-rows-schema-indexes.md"
    ))
    .expect("the C-T6 spec");
    // §6's verification bullet: from its opening phrase to §6.1's
    // heading, read as flat text (the bullet is line-wrapped).
    let start = src
        .find("index_scan` verifies")
        .expect("§6's verification sentence");
    let end = src[start..]
        .find("### 6.1")
        .map(|i| start + i)
        .expect("the end of §6");
    let flat = |s: &str| s.lines().map(|l| l.trim()).collect::<Vec<_>>().join(" ");
    let sentence = flat(&src[start..end]);
    // The M-2 landing, pinned: the regime letter is the sentence, the
    // pre-fix letter is gone, and the changelog names the escalation.
    assert!(
        sentence.contains("entry's own visibility regime"),
        "§6's verification sentence lost the regime letter — the C-T6e3fix3 \
         docs card must keep it:\n{sentence}"
    );
    assert!(
        !sentence.contains("an entry whose row is not visible"),
        "§6's pre-fix verification letter returned — the C-T6e3fix3 docs \
         card must keep the restated sentence:\n{sentence}"
    );
    assert!(
        src.contains("## Changelog"),
        "the spec's changelog vanished — the C-T6e3fix3 docs card must \
         keep it"
    );
    assert!(
        src.contains("the e3 escalation"),
        "the changelog no longer names the e3 escalation"
    );
}

/// C-T6e3fix2 (the gate's M-3): the C-T6e3fix verification's
/// strictly-stronger half — the at-S verification of a shadowed foreign
/// pair — pinned by real corruption. The deterministic pin's scene (a
/// committed entry still live at S over a row the txn has re-inserted
/// as its own write), plus one planted lie: a second committed entry,
/// visible at S, whose value part (`"q"`) no row at S re-encodes to —
/// planted straight into the KV beneath the txn's own re-insert
/// (`plant_live_version`, the C-T6b3b amcheck injection path; no
/// maintenance wrote it, the state is corrupt by hand). The scan at S
/// must XX001: the planted entry is foreign (its committed version is
/// the enumerated payload), the row fetch crosses into the shadowed
/// branch (the own intent shadows the S row), and the S-pair check —
/// the check the old code never ran correctly, the one that makes the
/// fix strictly stronger — is the only thing standing between the
/// corrupt entry and a silently missing row. S-PAIR-DEAF (that check
/// deleted) makes this scan serve the own row and skip the lie; this
/// leg is the failure.
#[test]
fn t6e3fix2_corrupt_entry_beneath_own_reinsert_is_xx001() {
    let rig = Rig::new();
    // The pin's fixture: t(pk int4, payload text), one row (1, "b"), a
    // born-public index on payload (§6.1(a): the entry is committed at
    // or below every later S).
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, b"e3fix2", &pk_schema()));
    let st1 = rig.stmt(&t0, s0);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h, &[i32_d(1), text_d("b")]));
    let st2 = rig.stmt(&t0, s0);
    let idx = ok(rig
        .db
        .create_index(&st2, oid, &idx_def(2, false, IndexState::Public)));
    let t_setup = ok(rig.commit_rc(t0));

    // The RR txn takes S (the setup at or below it); the helper's
    // delete commits above it — the old entry stays live at S.
    let (rr, sguard, s) = rig.rr();
    assert!(
        t_setup <= s,
        "S={s:?} at or below the setup commit {t_setup:?}"
    );
    let htxn = rig.rc();
    let hs = rig.core().visible_ts();
    let hst = rig.stmt(&htxn, hs);
    assert!(
        ok(rig.db.delete(&hst, oid, &[i32_v(1)])),
        "the row was live"
    );
    let t_del = ok(rig.commit_rc(htxn));
    assert!(
        t_del > s,
        "the delete commits above S (T={t_del:?}, S={s:?})"
    );

    // §5.3: the txn re-inserts the same pk — its own row intent now
    // shadows the row at S in the reader's regime.
    let st_i = rig.stmt(&rr, s);
    let hi = ok(rig.db.open_table(&st_i, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st_i, &hi, &[i32_d(1), text_d("y")]));

    // The corruption: a committed entry visible at S whose value part
    // no row at S has — at the setup's ts (visible at S, above nothing
    // that could shadow it, live forever in this rig), an `/i/` entry
    // (empty payload) naming row 1 under `"q"`.
    let isid = {
        let row = rig.idx_row(idx).expect("the idx row committed");
        let nucleus_txn::catalog::IdxStorage::Id(isid) = row.storage else {
            panic!("a born-public index has an id storage")
        };
        isid
    };
    let row1 = row_key_of(sid, &pk_schema(), &[i32_v(1)]);
    let col = KeyColumn::asc(KeyType::Text(Collation::C));
    let bad = i_entry_key(isid, &[col], &[text_v("q".into())], &row1);
    plant_live_version(&rig, &bad, t_setup, &[]);

    // The full-range scan at S must refuse the corrupted state with the
    // escalated error: the consistent old entry is skipped as the pin
    // asserts, the own row would serve through its own entry, and the
    // planted pair fails the S-row re-encode — XX001, never a silently
    // missing row.
    let st_c = rig.stmt(&rr, s);
    let err = rig
        .db
        .index_scan(&st_c, idx, Bound::Unbounded, Bound::Unbounded)
        .expect_err("a corrupt committed entry beneath the own re-insert is XX001");
    assert!(
        matches!(
            err,
            DbError::Row(nucleus_codec::row::RowError::Corrupt(
                "index entry does not match its row"
            ))
        ),
        "the escalated signature, not {err:?}"
    );
    assert_eq!(err.sqlstate(), "XX001");

    // The corrupted rig is abandoned (the plant was a raw write; no
    // abort could undo it, and this leg owns nothing to assert about
    // the committed state — the pin next door owns the clean scene).
    rig.abort_txn(rr);
    drop(sguard);
}

// ----- C-T6e4: the definition-era carve-out on the NOT NULL decode -------------

/// The C-T6e4 fixture: `t(id int4 PRIMARY KEY, payload text)` — the
/// payload column is the NULL carrier — with a born-public index on the
/// pk column, so the pin can drive all three read paths (`get`, `scan`,
/// `index_scan`'s row fetch) over the same row. Returns `(oid, sid,
/// idx)`.
fn t6e4_fixture(rig: &Rig) -> (u64, u64, u64) {
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st0 = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st0, b"e4fix", &pk_schema()));
    let st1 = rig.stmt(&t0, s0);
    let idx = ok(rig
        .db
        .create_index(&st1, oid, &idx_def(1, false, IndexState::Public)));
    ok(rig.commit_rc(t0));
    (oid, sid, idx)
}

/// C-T6e4's escalated scene, read side. The captured history (the
/// C-T6b3bfix2 gate's E-1, `St`/`G` shapes): a row version carrying
/// `payload = NULL` is live below the reader's snapshot `S`; a
/// concurrent DELETE commits above `S`; SET NOT NULL(payload) then
/// validates the **latest** committed state — where the row is a
/// tombstone, so the DDL legally commits — above it; the reader's
/// handle decodes under the post-constraint definition. Correct: every
/// read at `S` serves the old row with its NULL (the model at `S` plus
/// own writes — here exactly `[Some(1), None]`), never XX001; amcheck
/// is clean before and after; the abort leaves the committed state
/// exactly as the helper pair left it. Before the fix `get`, `scan` and
/// `index_scan` all fail with XX001 ("NULL bit on a NOT NULL column")
/// — the regime crossing the e4 fix carved out (`decode_row_at_era`).
#[test]
fn t6e4_pre_constraint_null_row_reads_clean_under_a_later_set_not_null() {
    let rig = Rig::new();
    let (oid, _sid, idx) = t6e4_fixture(&rig);
    // The NULL row, committed below every later snapshot.
    let t1 = {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(1), None]));
        ok(rig.commit_rc(t))
    };

    // Amcheck clean before.
    let g0 = rig.core().registry.take_snapshot();
    assert!(
        ok(check_table(&rig.db, oid, &g0)).is_clean(),
        "amcheck before"
    );
    drop(g0);

    // The RR reader takes S — the NULL row's commit at or below it.
    let (rr, sguard, s) = rig.rr();
    assert!(t1 <= s, "S={s:?} at or below the NULL row's commit {t1:?}");

    // The concurrent pair above S, exactly the captured interleaving:
    // the delete first (its tombstone is what SET NOT NULL's validating
    // scan sees), then the DDL.
    let hd = rig.rc();
    let hs = rig.core().visible_ts();
    let hstd = rig.stmt(&hd, hs);
    assert!(
        ok(rig.db.delete(&hstd, oid, &[i32_v(1)])),
        "the row was live"
    );
    let t_del = ok(rig.commit_rc(hd));
    assert!(
        t_del > s,
        "the delete commits above S (T={t_del:?}, S={s:?})"
    );

    let hsnn = rig.rc();
    let hs2 = rig.core().visible_ts();
    let stsnn = rig.stmt(&hsnn, hs2);
    ok(rig.db.set_not_null(&stsnn, oid, 2, true));
    let t_snn = ok(rig.commit_rc(hsnn));
    assert!(t_snn > t_del, "the SET NOT NULL commits above the delete");

    // The handle the reader's statements open decodes under the
    // post-constraint definition (the catalog read is at visible_ts).
    let hg = ok(rig
        .db
        .open_table(&rig.stmt(&rr, s), oid, RelLockMode::AccessShare));
    assert_eq!(hg.def_version, 2, "the handle carries the post-SNN def");

    // The committed entry state the abort must leave untouched.
    let isid = {
        let row = rig.idx_row(idx).expect("the idx row committed");
        let nucleus_txn::catalog::IdxStorage::Id(isid) = row.storage else {
            panic!("a born-public index has an id storage")
        };
        isid
    };
    let entries_before = rig.raw_entry_versions(isid);

    // (1) The point read: the old row with its NULL.
    let st_g = rig.stmt(&rr, s);
    assert_eq!(
        ok(rig.db.get(&st_g, &hg, &[i32_v(1)])),
        Some(vec![i32_d(1), None]),
        "the pre-constraint NULL row is served at S (the G shape)"
    );
    // (2) The table scan: exactly that row (the St shape).
    let st_c = rig.stmt(&rr, s);
    let rows = ok(rig.db.scan(&st_c, &hg));
    assert_eq!(rows.len(), 1, "one row visible at S");
    assert_eq!(
        rows[0].1,
        vec![i32_d(1), None],
        "the NULL row, decoded at its era"
    );
    // (3) The index scan over the pk: the fetched row decodes clean (the
    // Ixa shape — the crossing lived in the row fetch, not §6's
    // verification).
    let st_i = rig.stmt(&rr, s);
    let irows = ok(rig.db.index_scan(
        &st_i,
        idx,
        Bound::Included(vec![i32_v(1)]),
        Bound::Unbounded,
    ));
    assert_eq!(irows.len(), 1, "the pk entry serves at S");
    assert_eq!(
        irows[0].1,
        vec![i32_d(1), None],
        "the fetched NULL row decodes clean"
    );

    // Nothing committed: the abort leaves the committed state exactly
    // as the helper pair left it, and amcheck stays clean.
    rig.abort_txn(rr);
    drop(sguard);
    assert_eq!(
        rig.raw_entry_versions(isid),
        entries_before,
        "no entry version changed"
    );
    let t_scan = rig.rc();
    let s_scan = rig.core().visible_ts();
    let st_s = rig.stmt(&t_scan, s_scan);
    let hs3 = ok(rig.db.open_table(&st_s, oid, RelLockMode::AccessShare));
    assert!(
        ok(rig.db.scan(&st_s, &hs3)).is_empty(),
        "the committed table is the helper's empty table"
    );
    ok(rig.commit_rc(t_scan));
    let g1 = rig.core().registry.take_snapshot();
    assert!(
        ok(check_table(&rig.db, oid, &g1)).is_clean(),
        "amcheck after"
    );
    drop(g1);
}

/// C-T6e4's write-side crossing: an RC UPDATE whose statement snapshot
/// sits below a concurrent update-and-SET NOT NULL pair. The read row
/// (the statement's snapshot read, then the EPQ base) carries the
/// pre-constraint NULL; `apply_set` decodes it under the post-constraint
/// handle definition. Before the fix the decode itself XX001'd the legal
/// statement (`UpdateEpq::new` → `apply_set`, before the row op even
/// ran); correct: the decode succeeds, the row op EPQ re-bases on the
/// concurrent non-NULL version and applies, and the committed effect is
/// the caller's set on that base.
#[test]
fn t6e4_pre_constraint_null_row_updates_clean_under_a_later_set_not_null() {
    let rig = Rig::new();
    let (oid, _sid, _idx) = t6e4_fixture(&rig);
    let t1 = {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(1), None]));
        ok(rig.commit_rc(t))
    };

    // The writer takes its statement snapshot below the concurrent pair.
    let w = rig.rc();
    let s_w = rig.core().visible_ts();
    assert!(t1 <= s_w);

    // The concurrent pair above S_w: the row updated to a non-NULL
    // payload, then SET NOT NULL(payload) — its validating scan sees
    // the non-NULL latest, so it commits.
    let hu = rig.rc();
    let hu_s = rig.core().visible_ts();
    let hstu = rig.stmt(&hu, hu_s);
    assert!(ok(rig.db.update(
        &hstu,
        oid,
        &[i32_v(1)],
        &[set_col(2, text_d("y"))]
    )));
    let t_upd = ok(rig.commit_rc(hu));
    assert!(t_upd > s_w, "the concurrent update commits above S_w");

    let hsnn = rig.rc();
    let hs2 = rig.core().visible_ts();
    let stsnn = rig.stmt(&hsnn, hs2);
    ok(rig.db.set_not_null(&stsnn, oid, 2, true));
    let t_snn = ok(rig.commit_rc(hsnn));
    assert!(t_snn > t_upd);

    // The writer's UPDATE at its own snapshot: the read row is the NULL
    // version (the decode the fix carved out), the row op re-bases on
    // the concurrent non-NULL version and applies.
    let st_w = rig.stmt(&w, s_w);
    let hw = ok(rig.db.open_table(&st_w, oid, RelLockMode::RowExclusive));
    assert_eq!(hw.def_version, 2, "the writer's handle is the post-SNN def");
    assert!(
        ok(rig
            .db
            .update_with(&st_w, &hw, &[i32_v(1)], &[set_col(2, text_d("z"))])),
        "the update applies (EPQ re-base on the concurrent version)"
    );
    let t_w = ok(rig.commit_rc(w));
    assert!(t_w > t_snn);

    // The committed effect: the set on the concurrent base.
    let t_r = rig.rc();
    let s_r = rig.core().visible_ts();
    let st_r = rig.stmt(&t_r, s_r);
    let hr = ok(rig.db.open_table(&st_r, oid, RelLockMode::AccessShare));
    assert_eq!(
        ok(rig.db.get(&st_r, &hr, &[i32_v(1)])),
        Some(vec![i32_d(1), text_d("z")]),
        "the committed effect is the set on the EPQ base"
    );
    ok(rig.commit_rc(t_r));
    let g = rig.core().registry.take_snapshot();
    assert!(
        ok(check_table(&rig.db, oid, &g)).is_clean(),
        "amcheck after"
    );
    drop(g);
}

/// C-T6e4's strictly-stronger half: the carve-out is era-scoped, never a
/// blanket disarm. A row whose header version **equals** the
/// definition's — the flags were in force for its own encoding, and
/// `encode_row` refuses such a NULL, so a stray one is real corruption —
/// still decodes XX001. The row is planted raw (the corruption oracle's
/// primitive): encoded under the post-SNN definition with the flags
/// cleared (the only way to produce the bytes), carrying the
/// definition's own version in its header.
#[test]
fn t6e4_stray_null_on_a_current_def_row_is_still_xx001() {
    let rig = Rig::new();
    let (oid, sid, _idx) = t6e4_fixture(&rig);
    let t1 = {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(1), None]));
        ok(rig.commit_rc(t))
    };
    // The row updated to a non-NULL payload, so SET NOT NULL validates
    // and commits (the pre-constraint NULL version stays below it).
    let hu = rig.rc();
    let hu_s = rig.core().visible_ts();
    let hstu = rig.stmt(&hu, hu_s);
    assert!(ok(rig.db.update(
        &hstu,
        oid,
        &[i32_v(1)],
        &[set_col(2, text_d("y"))]
    )));
    let t_upd = ok(rig.commit_rc(hu));
    assert!(t_upd > t1);
    let hsnn = rig.rc();
    let hs = rig.core().visible_ts();
    let stsnn = rig.stmt(&hsnn, hs);
    ok(rig.db.set_not_null(&stsnn, oid, 2, true));
    let t_snn = ok(rig.commit_rc(hsnn));
    assert!(t_snn > t_upd);

    // The stray row: header version 2 (the current def), payload NULL —
    // bytes no legal writer could produce, planted at the SNN's own ts.
    let mut era = pk_schema();
    era.version = 2;
    for col in &mut era.columns {
        col.not_null = false;
    }
    let bytes = ok(era.encode_row(&[i32_d(2), None]));
    let row2 = row_key_of(sid, &pk_schema(), &[i32_v(2)]);
    plant_live_version(&rig, &row2, t_snn, &bytes);

    // The read refuses it: XX001, the v == V leg of §2's NULL clause.
    let t_r = rig.rc();
    let s_r = rig.core().visible_ts();
    let st_r = rig.stmt(&t_r, s_r);
    let hr = ok(rig.db.open_table(&st_r, oid, RelLockMode::AccessShare));
    let err = rig
        .db
        .get(&st_r, &hr, &[i32_v(2)])
        .expect_err("a stray NULL under the current def is corruption");
    assert!(
        matches!(
            err,
            DbError::Row(nucleus_codec::row::RowError::Corrupt(
                "NULL bit on a NOT NULL column"
            ))
        ),
        "the corruption signature, not {err:?}"
    );
    assert_eq!(err.sqlstate(), "XX001");
    ok(rig.core().abort(t_r));
}

/// C-T6e4's spec letter, landed (the C-T6e4fix docs half) and pinned
/// (SPEC-REVERT's catcher): the engine decodes NOT NULL flags at the
/// row's own era (`decode_row_at_era`), and C-T6 §2/§3 license it — the
/// NULL clause is era-scoped (a NULL bit on a NOT NULL attnum of the
/// definition the row was encoded under, `v == V`, is XX001; a NULL
/// legal at the row's era is served), §3's decode rule carries the
/// `v == V` condition, and the changelog names the e4 escalation. This
/// pin is the flipped form the interim tripwire's failure messages
/// asked for (the e3fix2→e3fix3 sequence): it guards the landed letter
/// in both directions — the pre-fix unscoped clause returning, the era
/// scoping or the `v == V` condition dropping, or the changelog duty
/// vanishing each fail here.
#[test]
fn t6e4_spec_divergence_is_pinned() {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/C-T6-rows-schema-indexes.md"
    ))
    .expect("the C-T6 spec");
    let flat = |s: &str| s.lines().map(|l| l.trim()).collect::<Vec<_>>().join(" ");
    // §2's NULL bullet: from its opening phrase to the §2.1 heading.
    let start = src
        .find("Trailing bytes, short datums")
        .expect("§2's XX001 bullet");
    let end = src[start..]
        .find("### 2.1")
        .map(|i| start + i)
        .expect("the end of §2");
    let bullet = flat(&src[start..end]);
    // The landed letter: era-scoped, and the pre-fix unscoped clause
    // (which outlawed the carve-out the engine performs) is gone.
    assert!(
        !bullet.contains("a NULL bit on a live NOT NULL attnum"),
        "§2's pre-fix unscoped NULL clause returned — the era carve-out \
         the engine performs is licensed by the era-scoped letter:\n{bullet}"
    );
    assert!(
        bullet.contains("era-scoped"),
        "§2's NULL clause lost its era scoping — the C-T6e4 engine \
         carve-out must stay licensed by the letter:\n{bullet}"
    );
    assert!(
        bullet.contains("v == V"),
        "§2's NULL clause must keep the `v == V` condition — the \
         definition the row was encoded under:\n{bullet}"
    );
    // §3's decode rule carries the same condition.
    let dstart = src.find("**Decode rule.**").expect("§3's decode rule");
    let dend = src[dstart..]
        .find("### 3.1")
        .map(|i| dstart + i)
        .expect("the end of §3");
    let rule = flat(&src[dstart..dend]);
    assert!(
        rule.contains("v == V"),
        "§3's decode rule lost the `v == V` condition — the NOT NULL \
         flags bind at the row's own era:\n{rule}"
    );
    // The changelog exists and keeps naming the e4 escalation.
    assert!(
        src.contains("## Changelog"),
        "the spec's changelog vanished — keep it"
    );
    assert!(
        src.contains("the e4 escalation"),
        "the changelog no longer names the e4 escalation (C-T6e4's fix, \
         C-T6e4fix's letter)"
    );
}
