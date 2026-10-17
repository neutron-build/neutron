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

mod diff;
// diff/crash.rs (pulled in by diff/mod.rs) drives the t6b crash rig.
mod t6b_support;

use std::ops::Bound;

use diff::runner::run_workload;
use diff::{a_range, check, Config, Fate, IdxSight, OpDetail, OpOut, Run, Verdict};
use nucleus_catalog::amcheck::check_table;
use nucleus_catalog::{IndexState, Stmt};
use nucleus_txn::locks::RelLockMode;
use t6b_support::{i32_d, i32_v, idx_def, ok, pk_schema, text_d, text_v, Rig};

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
