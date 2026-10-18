//! Quick smoke test for the E0 tree (superseded by the full suite).
#![allow(missing_docs)]

use e0_btree::{BTree, MemStore};

#[test]
fn smoke_insert_get_range_check() {
    let store = MemStore::new(8192).unwrap();
    let tree = BTree::new(store, 64).unwrap();
    let mut model = std::collections::BTreeMap::new();
    // enough keys to force leaf splits, inner splits and a root split
    for i in 0u64..5000 {
        let k = i.to_be_bytes();
        let v = vec![(i % 251) as u8; (i % 300) as usize];
        let old = tree.insert(&k, &v).unwrap();
        assert_eq!(old, model.insert(k.to_vec(), v.clone()));
    }
    for i in 0u64..5000 {
        let k = i.to_be_bytes();
        assert_eq!(
            tree.get(&k).unwrap().as_deref(),
            Some(&model[k.as_slice()][..])
        );
    }
    let scan: Vec<_> = tree
        .range(&1000u64.to_be_bytes(), &2000u64.to_be_bytes())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let want: Vec<_> = model
        .range(1000u64.to_be_bytes().to_vec()..2000u64.to_be_bytes().to_vec())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_eq!(scan, want);
    let stats = tree.check().unwrap();
    // 5000 keys of ~8–300 bytes fill ~60–150 leaves of 8 KiB; sanity only.
    assert!(stats.pages >= 60, "suspiciously few pages: {stats:?}");
    assert!(stats.height >= 2 && stats.height <= 4, "{stats:?}");
    // delete half, then verify
    for i in (0u64..5000).step_by(2) {
        let k = i.to_be_bytes();
        assert_eq!(tree.delete(&k).unwrap(), model.remove(k.as_slice()));
    }
    for i in 0u64..5000 {
        let k = i.to_be_bytes();
        assert_eq!(
            tree.get(&k).unwrap(),
            model.get(k.as_slice()).cloned(),
            "key {i}"
        );
    }
    let full: Vec<_> = tree
        .range(&0u64.to_be_bytes(), &5000u64.to_be_bytes())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(full, want);
    tree.check().unwrap();
}
