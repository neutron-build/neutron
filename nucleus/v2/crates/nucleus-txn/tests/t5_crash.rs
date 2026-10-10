//! C-T5 crash matrix over `Fault<MemKv>`: a DDL that never committed is
//! gone after the crash; a committed-but-unresolved DDL is all-or-nothing
//! through the boot rebuild (row and mapping together); resolution after
//! boot changes nothing the rebuild decided. Every test names the mutant
//! it kills.

mod t5_support;

use t5_support::{ok, table_key, Rig};

use nucleus_txn::catalog::rel_key;

/// Mutant: the rebuild reading unresolved intents as rows (a lost DDL
/// would resurrect), or DDL rows written durably outside the txn (a lost
/// DDL would survive). The crash drops the unsynced intent writes; the
/// persisted counter writes survive, so the ids are consumed anyway.
#[test]
fn crash_before_the_commit_record_loses_the_ddl() {
    let rig = Rig::new();
    let t = rig.rc();
    let s = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t, s, b"lost");

    // Not committed: the crash drops every unsynced batch (the intent
    // placement writes are Durability::No).
    let rig2 = rig.crash_reopen(0);

    assert!(rig2.rel_row(oid).is_none(), "no row after the crash");
    assert!(
        !rig2.storage_maps(&table_key(sid, "probe"), oid as u32),
        "no mapping after the crash"
    );
    assert!(
        rig2.core.epoch() > rig.core.epoch(),
        "the epoch advanced at boot (§7.2)"
    );
}

/// Mutant: the rebuild resolving the catalog from resolved versions only —
/// a committed-but-unresolved DDL (its /sys/txn record durable, its intent
/// not yet turned into a version) would lose row and mapping. §4 decides
/// the read from the intent plus the loaded Committed status, so the
/// rebuild sees it; a later resolution writes the same bytes and changes
/// nothing.
#[test]
fn crash_after_commit_unresolved_is_all_or_nothing() {
    let rig = Rig::new();
    let t = rig.rc();
    let s = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t, s, b"kept");
    let owner = t.id;
    // Commit (record durable via the group's fsync), but do NOT resolve.
    let ticket = ok(rig
        .core
        .commit_submit(t, nucleus_txn::commit::SyncCommit::On));
    rig.process();
    ok(ticket.wait());

    let rig2 = rig.crash_reopen(rig.kv.unsynced_len());

    // Row and mapping are together, read out of the committed intent.
    assert_eq!(
        rig2.rel_row(oid).map(|row| row.storage_id),
        Some(sid),
        "the committed-unresolved DDL reads as its row"
    );
    assert!(
        rig2.storage_maps(&table_key(sid, "probe"), oid as u32),
        "the rebuild mapped the row's storage"
    );

    // Resolution after boot (§7.3 by hand — the queue died with the
    // process): the version appears, every decision stands.
    rig2.resolve_intent(&rel_key(oid), owner);
    assert_eq!(
        rig2.rel_row(oid).map(|row| row.storage_id),
        Some(sid),
        "resolution changes nothing the rebuild decided"
    );
    assert!(rig2.storage_maps(&table_key(sid, "probe"), oid as u32));
}

/// The TRUNCATE shape of the same matrix: an unresolved committed
/// TRUNCATE rebuilds with the **new** storage only; a lost TRUNCATE
/// rebuilds with the old one. Mutants: the rebuild mapping retired ranges
/// (old would stay mapped); the rebuild ignoring the intent (new would be
/// missing).
#[test]
fn crash_mid_truncate_is_all_or_nothing() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid0) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    // (a) Committed, unresolved: the row reads through the intent with the
    // new storage id; only the new range is mapped.
    let w = rig.rc();
    let sw = rig.core.visible_ts();
    let sid1 = rig.truncate_table(&w, sw, oid);
    let owner = w.id;
    let ticket = ok(rig
        .core
        .commit_submit(w, nucleus_txn::commit::SyncCommit::On));
    rig.process();
    ok(ticket.wait());

    let rig2 = rig.crash_reopen(rig.kv.unsynced_len());
    assert_eq!(
        rig2.rel_row(oid).map(|row| row.storage_id),
        Some(sid1),
        "the committed TRUNCATE reads through its intent"
    );
    assert!(rig2.storage_maps(&table_key(sid1, "p"), oid as u32));
    assert!(!rig2.storage_maps(&table_key(sid0, "p"), oid as u32));
    rig2.resolve_intent(&rel_key(oid), owner);
    assert_eq!(
        rig2.rel_row(oid).map(|row| row.storage_id),
        Some(sid1),
        "unchanged by resolution"
    );

    // (b) Lost (crash before the commit record): the committed state —
    // here truncate #1's `sid1` — stands, row and mapping together; the
    // never-committed second TRUNCATE's storage is unmapped and its id
    // never returns.
    let w2 = rig2.rc();
    let sw2 = rig2.core.visible_ts();
    let sid2 = rig2.truncate_table(&w2, sw2, oid);
    let rig3 = rig2.crash_reopen(0);
    assert_eq!(
        rig3.rel_row(oid).map(|row| row.storage_id),
        Some(sid1),
        "the lost TRUNCATE leaves the committed state"
    );
    assert!(rig3.storage_maps(&table_key(sid1, "p"), oid as u32));
    assert!(!rig3.storage_maps(&table_key(sid2, "p"), oid as u32));
    assert!(
        !rig3.storage_maps(&table_key(sid0, "p"), oid as u32),
        "the range retired by the committed TRUNCATE stays unmapped"
    );
}

/// Mutant: the rebuild mapping dropped relations (the tombstone read as a
/// row) or keeping dropped ranges mapped.
#[test]
fn crash_after_drop_rebuilds_without_the_relation() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    rig.drop_table(&t, s, oid);
    ok(rig.commit_rc(t));

    let rig2 = rig.crash_reopen(rig.kv.unsynced_len());
    assert!(rig2.rel_row(oid).is_none(), "the drop survives the crash");
    assert!(!rig2.storage_maps(&table_key(sid, "p"), oid as u32));
}
