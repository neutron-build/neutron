// Provenance (card A-1b): nucleus/v3/spikes/e4-recovery/tests/directed.rs at 48ac99203f97d9dd5a53b1370ed141a294c4df0c lines 3994-4149.
//! SimDisk::heal_media's unit pin (X-5c-F4-F7), copied from E4's directed tests (card A-1b).
// The spike stays E4's evidence record; nothing under spikes/ changes (card A-1b).
/// **X-5c-F4-F7's unit pin (owner ruling 2026-11-14; P's fix,
/// `E4-final-diagnostics.md` §A.3 step 4): `SimDisk::heal_media`
/// restores exactly the no-fault bytes on a re-seeded twin's cloned
/// disk, and touches nothing else.** A synthetic page file with four
/// twice-written slots takes one LOST (slot 3), one TORN (slot 5) and
/// one MISDIRECTED (slot 9's write, its stray bytes landing on slot
/// 7): healing must return the bytes the file held before any fault —
/// the in-order composition of its durable writes — over all four
/// damaged ranges (three own ranges and the misdirect's landing), leave
/// every other byte alone, and clear the taint at every damaged range.
///
/// It also pins the card's two bounds: the fault **history
/// (`disk.media`) is kept** (X-5c-F4-F7's documented choice — the heal
/// acts on bytes, not on the bookkeeping list the real chain's oracle
/// arms read), and a damaged range that is **not live** is untouched
/// (a synced rewrite after the fault already holds the issued bytes, so
/// the heal is a no-op there — with no live range anywhere it is a no-op
/// everywhere, card Limit 1). **M-TWIN-NO-HEAL** (skip the heal
/// entirely) fails the seed pins in `tests/workload.rs`, not this one.
#[test]
fn x5f4f7_heal_media_restores_the_no_fault_bytes() {
    use nucleus_env::sim::{MediaFault, MediaKind, ROOT_DIR, SimDisk};

    let mut disk = SimDisk::new();
    let f = disk.create("/pages").unwrap();
    disk.sync_dir(ROOT_DIR).unwrap();
    // Four twice-written slots: every faulted write has a non-trivial
    // pre-image (its slot's first write), and the composition over a
    // slot is its second write.
    for slot in [3u64, 5, 7, 9] {
        disk.write_at(f, slot * 4096, &vec![(slot as u8) * 2; 4096])
            .unwrap();
        disk.sync_all(f).unwrap();
        disk.write_at(f, slot * 4096, &vec![(slot as u8) * 2 + 1; 4096])
            .unwrap();
        disk.sync_all(f).unwrap();
    }
    let mut clean = disk.clone();
    let dw = disk.dwrites_of(f).to_vec();
    let second_write = |slot: u64| {
        dw.iter()
            .filter(|w| w.off == slot * 4096)
            .nth(1)
            .unwrap()
            .clone()
    };

    let lost = second_write(3);
    let torn = second_write(5);
    let mis = second_write(9);
    disk.apply_one_media_fault(&MediaFault {
        inode: f,
        off: lost.off,
        len: lost.bytes.len() as u64,
        bytes: lost.bytes.clone(),
        pre: lost.pre.clone(),
        kind: MediaKind::Lost,
    });
    disk.apply_one_media_fault(&MediaFault {
        inode: f,
        off: torn.off,
        len: torn.bytes.len() as u64,
        bytes: torn.bytes.clone(),
        pre: torn.pre.clone(),
        kind: MediaKind::Torn {
            sectors: vec![torn.off + 512, torn.off + 2048],
        },
    });
    disk.apply_one_media_fault(&MediaFault {
        inode: f,
        off: mis.off,
        len: mis.bytes.len() as u64,
        bytes: mis.bytes.clone(),
        pre: mis.pre.clone(),
        kind: MediaKind::Misdirected { to_off: 7 * 4096 },
    });
    // All four damaged ranges are live before the heal (durable differs
    // from the last issued bytes on each).
    for (off, len) in [
        (3 * 4096u64, 4096u64),
        (5 * 4096, 4096),
        (9 * 4096, 4096),
        (7 * 4096, 4096),
    ] {
        assert!(
            disk.range_tainted(f, off, len),
            "slot {}: the fault must leave the range live before the heal",
            off / 4096
        );
    }

    let still = disk.heal_media();
    assert!(
        still.is_empty(),
        "the heal must leave no damaged range tainted: {still:?}"
    );

    // Exactly the no-fault bytes, and no other byte: the healed disk
    // equals the clean clone everywhere, durable and current view.
    let extent = 10 * 4096u64;
    assert_eq!(
        disk.durable_read(f, 0, extent),
        clean.durable_read(f, 0, extent),
        "the healed durable bytes must equal the no-fault clone's, byte for byte"
    );
    assert_eq!(
        disk.read_at(f, 0, extent).unwrap(),
        clean.read_at(f, 0, extent).unwrap(),
        "the healed current view must equal the no-fault clone's, byte for byte"
    );

    // The fault history is kept (X-5c-F4-F7's documented choice).
    assert_eq!(
        disk.media.len(),
        3,
        "the heal keeps the fault history (disk.media), all three faults"
    );
    assert!(matches!(disk.media[0].kind, MediaKind::Lost));
    assert!(matches!(disk.media[1].kind, MediaKind::Torn { .. }));
    assert!(matches!(disk.media[2].kind, MediaKind::Misdirected { .. }));

    // A damaged range that is not live is untouched: after the LOST, a
    // fresh synced write re-covers the slot, durable equals the issued
    // bytes again, and the heal must not revert the newer bytes.
    let mut disk2 = SimDisk::new();
    let f2 = disk2.create("/pages").unwrap();
    disk2.sync_dir(ROOT_DIR).unwrap();
    disk2.write_at(f2, 3 * 4096, &[6u8; 4096]).unwrap();
    disk2.sync_all(f2).unwrap();
    disk2.write_at(f2, 3 * 4096, &[7u8; 4096]).unwrap();
    disk2.sync_all(f2).unwrap();
    let w2 = disk2.dwrites_of(f2).iter().last().unwrap().clone();
    disk2.apply_one_media_fault(&MediaFault {
        inode: f2,
        off: w2.off,
        len: w2.bytes.len() as u64,
        bytes: w2.bytes.clone(),
        pre: w2.pre.clone(),
        kind: MediaKind::Lost,
    });
    disk2.write_at(f2, 3 * 4096, &[42u8; 4096]).unwrap();
    disk2.sync_all(f2).unwrap();
    let still2 = disk2.heal_media();
    assert!(
        still2.is_empty(),
        "a not-live damaged range heals nothing: {still2:?}"
    );
    assert!(
        disk2
            .durable_read(f2, 3 * 4096, 4096)
            .iter()
            .all(|&b| b == 42),
        "the heal must keep the post-fault synced write's bytes (no revert to the \
         faulted write or its pre-image)"
    );
}
