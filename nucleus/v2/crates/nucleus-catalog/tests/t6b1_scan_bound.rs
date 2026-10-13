//! C-T6b1 item 9 / mutant N7: a row scan is bounded to
//! `[table_lo, table_lo ‖ 0xFF)` (C-T6 §5) — a raw `/t/{sid}/0xFF…`
//! version (a large-value chunk's shape) that sits in the same storage is
//! ignored by `scan`, never yielded and never decoded.

mod t6b_support;

use t6b_support::{i32_d, ok, pk_schema, text_d, Rig};

use nucleus_catalog::{table_lo, Stmt};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::write::{StmtCtx, UniqueRule};

/// Mutant N7: the scan bound widened past `0xFF` (the whole storage span,
/// or the chunk prefix included) — the chunk-shaped version would surface
/// as a row and the decode would fail (or worse, yield garbage). The
/// correct bound excludes every `0xFF…` key because no encoded key starts
/// with `0xFF` (C-Q3s §2 markers 0x00–0x02).
#[test]
fn scan_ignores_raw_0xff_versions_in_the_storage() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // One real row, committed.
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let h1 = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert(&st1, &h1, &[i32_d(1), text_d("row")]));
    ok(rig.commit_rc(t1));

    // A raw version at a chunk-shaped key — `table_lo ‖ 0xFF ‖ toast_id(20)
    // ‖ be32(0)` (§5's layout) with bytes that are NOT a row — committed
    // through the §5 write path like a real chunk write would be.
    let lo = table_lo(sid);
    let mut chunk = lo.clone();
    chunk.push(0xFF);
    chunk.extend_from_slice(&[0u8; 20]);
    chunk.extend_from_slice(&0u32.to_be_bytes());
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let seq = ok(t2.next_seq());
    ok(rig.core().insert_key(
        &t2,
        &chunk,
        None,
        b"not-a-row".to_vec(),
        StmtCtx::new(s2, seq, seq),
        UniqueRule::None,
    ));
    ok(rig.commit_rc(t2));

    // The scan returns exactly the one real row and no error: the chunk
    // key sorts at or above `table_lo ‖ 0xFF`, outside the bound.
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st3 = rig.stmt(&t3, s3);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st3, &h3));
    assert_eq!(rows.len(), 1, "the chunk-shaped version is not a row");
    assert_eq!(rows[0].1, vec![i32_d(1), text_d("row")]);
    ok(rig.commit_rc(t3));

    // And the chunk is really there in the same storage (the bound is
    // what excludes it, not the write).
    let view = rig.core().open_view();
    let found = ok(nucleus_txn::read::read_key(
        rig.core(),
        &view,
        &chunk,
        &nucleus_txn::visibility::ReadCtx {
            txn: nucleus_txn::TxnId {
                epoch: rig.core().epoch(),
                n: u64::MAX,
            },
            snapshot: rig.core().visible_ts(),
            stmt_seq: 1,
        },
        &mut nucleus_txn::read::NoSsi,
    ));
    assert_eq!(found, Some(b"not-a-row".to_vec()));
}
