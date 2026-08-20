//! Consumer groups, geo members and trims must survive a restart.
//!
//! The triage's sharpest case: groups were lost on EVERY restart, graceful
//! ones included, because the checkpoint's stream record held entries and
//! nothing else. After a restart `XREADGROUP` answered NOGROUP, and
//! re-creating the group redelivered the whole stream — at-least-once quietly
//! became at-least-once-with-amnesia.

#![cfg(feature = "server")]

use nucleus::kv::KvStore;
use nucleus::types::Value;

#[test]
fn consumer_groups_survive_a_checkpoint_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = KvStore::open(dir.path()).unwrap();
        kv.xadd("s", "1-1", vec![("f".into(), "a".into())]).unwrap();
        kv.xadd("s", "1-2", vec![("f".into(), "b".into())]).unwrap();
        kv.xgroup_create("s", "g", "0").unwrap();
        // Deliver one entry so the group carries a pending id, the state a
        // client cannot rebuild.
        let got = kv.xreadgroup("s", "g", "c1", ">", Some(1)).unwrap();
        assert_eq!(got.len(), 1);
        kv.collections().checkpoint().unwrap();
    }

    let kv = KvStore::open(dir.path()).unwrap();
    // The group exists: reading pending entries for c1 returns the delivered
    // one rather than NOGROUP.
    let pending = kv
        .xreadgroup("s", "g", "c1", "0", None)
        .expect("group did not survive the reopen");
    assert_eq!(pending.len(), 1, "pending list was lost: {pending:?}");
    assert_eq!(pending[0].id.ms, 1);
    assert_eq!(pending[0].id.seq, 1);

    // last_delivered_id survived too: the next `>` read returns entry two, not
    // entry one again.
    let next = kv.xreadgroup("s", "g", "c1", ">", None).unwrap();
    assert_eq!(next.len(), 1, "redelivered from the start: {next:?}");
    assert_eq!(next[0].id.seq, 2);
}

#[test]
fn group_state_survives_a_crash_without_a_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = KvStore::open(dir.path()).unwrap();
        kv.xadd("s", "1-1", vec![("f".into(), "a".into())]).unwrap();
        kv.xgroup_create("s", "g", "0").unwrap();
        kv.xreadgroup("s", "g", "c1", ">", None).unwrap();
        kv.collections_wal().unwrap().group_sync().unwrap();
        // No checkpoint: recovery has to come from the log records alone.
    }

    let kv = KvStore::open(dir.path()).unwrap();
    let pending = kv
        .xreadgroup("s", "g", "c1", "0", None)
        .expect("group was not replayed from the log");
    assert_eq!(pending.len(), 1, "delivery was not replayed: {pending:?}");
}

#[test]
fn acknowledged_entries_stay_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = KvStore::open(dir.path()).unwrap();
        kv.xadd("s", "1-1", vec![("f".into(), "a".into())]).unwrap();
        kv.xgroup_create("s", "g", "0").unwrap();
        let got = kv.xreadgroup("s", "g", "c1", ">", None).unwrap();
        let ids: Vec<_> = got.iter().map(|e| e.id).collect();
        assert_eq!(kv.xack("s", "g", &ids).unwrap(), 1);
        kv.collections_wal().unwrap().group_sync().unwrap();
    }

    let kv = KvStore::open(dir.path()).unwrap();
    let pending = kv.xreadgroup("s", "g", "c1", "0", None).unwrap();
    assert!(
        pending.is_empty(),
        "an acknowledged entry came back pending: {pending:?}"
    );
}

#[test]
fn geo_members_survive_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = KvStore::open(dir.path()).unwrap();
        kv.geoadd("places", 13.361389, 38.115556, "palermo")
            .unwrap();
        kv.geoadd("places", 15.087269, 37.502669, "catania")
            .unwrap();
        kv.collections_wal().unwrap().group_sync().unwrap();
    }

    let kv = KvStore::open(dir.path()).unwrap();
    assert_eq!(kv.geolen("places").unwrap(), 2, "geo members were lost");
    let pos = kv.geopos("places", "palermo").unwrap();
    assert!(pos.is_some(), "palermo did not come back");
}

#[test]
fn trimmed_entries_do_not_come_back() {
    let dir = tempfile::tempdir().unwrap();
    {
        let kv = KvStore::open(dir.path()).unwrap();
        for i in 1..=5 {
            kv.xadd(
                "s",
                &format!("1-{i}"),
                vec![("f".into(), Value::Int64(i).to_string())],
            )
            .unwrap();
        }
        assert_eq!(kv.xtrim_maxlen("s", 2).unwrap(), 3);
        kv.collections_wal().unwrap().group_sync().unwrap();
    }

    let kv = KvStore::open(dir.path()).unwrap();
    assert_eq!(
        kv.xlen("s").unwrap(),
        2,
        "trimmed entries were resurrected by replay"
    );
}
