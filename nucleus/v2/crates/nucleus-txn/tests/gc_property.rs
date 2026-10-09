//! The C-T4 I-GC property (C-T0 §11): seeded, deterministic random
//! histories of 1-3 keys (puts, deletes, re-inserts, commits at increasing
//! ts) over `MemKv` in LSM mode **and flat mode** (Rework 5), with random
//! registered snapshots and one open view; then a random sequence of
//! `publish`, `flush`, single-file `compact`, `drop_tombstones` and
//! `compact_all`. Some commits are left **committed-unresolved** during
//! the GC phase (the last round always; Rework 4) with newer commits
//! around them, so the phase runs over live intents the tombstone job must
//! not touch. After every GC step, every registered snapshot and the open
//! view must read the same value for every key as before the step (and
//! every read must equal the commit history's oracle). After the steps,
//! with every snapshot and view dropped, the resolver drains, then one
//! final publish + tombstone pass + full compaction must leave no
//! tombstone or moved-tombstone as any key's newest version `<= W` (the
//! I-GC-QUIESCE postcondition - the reads alone cannot see a surviving
//! newest-`<= W` tombstone, which is exactly seed 34's mutant), and every
//! committed txn must be resolved and truncated (a tombstone batch that
//! deleted an intent would leave its owner's count above 0 forever -
//! Rework 4's catch).
//!
//! Mutants (each must fail this property for at least one case): seed 3
//! (the filter drops every tombstone `<= W`), seed 33 (stream state shared
//! across streams), seed 34 (an exclusive `DeleteRange` start), Rework 4
//! (the tombstone batch also deletes `intent_key(L)`).

mod gc_support;

use std::sync::Arc;

use gc_support::{preload_gc_w, preload_ts_hwm, SharedKv};
use nucleus_kv::{Batch, Durability, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitPipeline, CommitRequest, SyncCommit};
use nucleus_txn::encoding::{
    decode_version, encode_intent, intent_key, parse_key, Entry, VersionValue, SYS_PREFIX,
};
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::registry::{SnapshotGuard, ViewGuard};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Layer, LayerData, RowLockMode, Ts, TxnError, TxnId};

/// Cases per run (>= 300 per the card).
const CASES: u64 = 300;
/// The logical keys histories write (storage id 0).
const KEYS: [&[u8]; 3] = [b"/t/0/a", b"/t/0/b", b"/t/0/c"];

fn str_err(e: TxnError) -> String {
    e.to_string()
}

/// A deterministic splitmix64 PRNG (no external dependency).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed ^ 0x2545_F491_4F6C_DD1D)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn coin(&mut self) -> bool {
        self.next() & 1 == 0
    }
}

// ---- Property-local, error-returning state building (no panics, so a
// mutant's failures can be counted per case). -------------------------------

fn place(core: &Core<SharedKv>, txn: &Txn, key: &[u8], value: Option<&[u8]>) -> Result<(), String> {
    let seq = txn.next_seq().map_err(str_err)?;
    txn.log_write(seq, key);
    core.count_placement(txn).map_err(str_err)?;
    let (data, lock) = match value {
        Some(v) => (
            LayerData::Write {
                value: v.to_vec(),
                key_changed: false,
            },
            RowLockMode::NoKeyUpdate,
        ),
        None => (LayerData::Delete { moved: false }, RowLockMode::Update),
    };
    let intent = nucleus_txn::Intent {
        txn: txn.id,
        layers: vec![Layer {
            seq,
            data_seq: seq,
            data,
            lock,
        }],
    };
    let mut batch = Batch::default();
    batch.put(intent_key(key), encode_intent(&intent).map_err(str_err)?);
    let _latch = core.latches.lock(key);
    core.write(batch, Durability::No).map_err(str_err)
}

fn commit(
    core: &Core<SharedKv>,
    pipeline: &mut CommitPipeline<SharedKv>,
    txn: Txn,
) -> Result<Ts, String> {
    let (req, ack) = CommitRequest::new(txn.id, SyncCommit::On, None, false, txn.write_set_keys());
    core.submit(req).map_err(str_err)?;
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    match ack.recv() {
        Ok(Ok(ts)) => Ok(ts),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("commit never acked".into()),
    }
}

type Snap = <SharedKv as OrderedKv>::Snap;

fn read(
    core: &Core<SharedKv>,
    view: &ViewGuard<'_, Snap>,
    key: &[u8],
    snapshot: Ts,
    reader: TxnId,
) -> Result<Option<Vec<u8>>, String> {
    let ctx = ReadCtx {
        txn: reader,
        snapshot,
        stmt_seq: 1,
    };
    read_key(core, view, key, &ctx, &mut NoSsi).map_err(str_err)
}

/// The commit-history oracle: the value of the newest commit `<= s`.
fn oracle_read(history: &[(Ts, Option<Vec<u8>>)], s: Ts) -> Option<Vec<u8>> {
    let mut out = None;
    for (ts, v) in history {
        if *ts <= s {
            out = v.clone();
        }
    }
    out
}

/// One key's commit history: `(ts, value-or-delete)` in commit order.
type History = Vec<(Ts, Option<Vec<u8>>)>;

/// Reads every key through every registered snapshot (a fresh view each,
/// §4) and through the open view at its `vts`; checks each read against
/// the oracle and returns the reads in a fixed order for before/after
/// comparison.
fn check_reads(
    core: &Core<SharedKv>,
    keys: &[&[u8]],
    histories: &[History],
    snaps: &[SnapshotGuard<'_>],
    view: Option<&ViewGuard<'_, Snap>>,
    vts: Ts,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let reader = TxnId {
        epoch: core.epoch(),
        n: u64::MAX,
    };
    let mut out = Vec::new();
    for (si, snap) in snaps.iter().enumerate() {
        for (ki, key) in keys.iter().enumerate() {
            let v = core.open_view();
            let got = read(core, &v, key, snap.ts(), reader)?;
            let want = oracle_read(&histories[ki], snap.ts());
            if got != want {
                return Err(format!(
                    "snapshot {si} at S={:?} read {got:?} of key {key:?}, want {want:?}",
                    snap.ts()
                ));
            }
            out.push(got);
        }
    }
    if let Some(view) = view {
        for (ki, key) in keys.iter().enumerate() {
            let got = read(core, view, key, vts, reader)?;
            let want = oracle_read(&histories[ki], vts);
            if got != want {
                return Err(format!(
                    "the open view at vts={vts:?} read {got:?} of key {key:?}, want {want:?}"
                ));
            }
            out.push(got);
        }
    }
    Ok(out)
}

/// The I-GC-QUIESCE oracle over raw storage: every logical key whose
/// newest version `<= w` is a tombstone or moved-tombstone.
fn quiesce_offenders(kv: &SharedKv, w: u64) -> Result<Vec<String>, String> {
    let mut offenders = Vec::new();
    let mut decided: Option<Vec<u8>> = None;
    for (stored, value) in kv.raw_entries(b"", &[0xff]) {
        if stored.starts_with(SYS_PREFIX) {
            continue;
        }
        let Some((l, Entry::Version(ts))) = parse_key(&stored) else {
            continue;
        };
        if decided.as_deref() == Some(l) || ts.0 > w {
            continue;
        }
        decided = Some(l.to_vec());
        if matches!(
            decode_version(&value).map_err(str_err)?,
            VersionValue::Tombstone { .. }
        ) {
            offenders.push(format!("{:?}@{:?}", l, ts));
        }
    }
    Ok(offenders)
}

// ---- The case ---------------------------------------------------------------

/// One property case. `lsm` picks the `MemKv` mode (Rework 5); the seed
/// fixes the whole history. Errors are strings so a mutant's failures can
/// be counted per case.
fn run_case(seed: u64, lsm: bool) -> Result<(), String> {
    let kv = if lsm {
        SharedKv::lsm()
    } else {
        SharedKv::flat()
    };
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    let core = Arc::new(Core::open(kv.clone()).map_err(str_err)?);
    let mut pipeline = CommitPipeline::new(Arc::clone(&core)).map_err(str_err)?;
    let job = GcJob::install(&core, GcConfig::default()).map_err(str_err)?;
    let mut rng = Rng::new(seed);

    // The history: 3-8 rounds; each round is one txn writing 1-2 distinct
    // keys (a put or a delete), committed at an increasing ts, with a
    // random flush splitting the versions across files.
    let nkeys = 1 + rng.below(3);
    let keys: Vec<&[u8]> = KEYS[..nkeys as usize].to_vec();
    let mut histories: Vec<History> = vec![Vec::new(); nkeys as usize];
    let rounds = 3 + rng.below(6);
    let mut snaps: Vec<SnapshotGuard<'_>> = Vec::new();
    let mut open_view: Option<ViewGuard<'_, Snap>> = None;
    let mut vts = Ts::ZERO;
    // Rework 4: commits whose intents stay unresolved during the GC phase.
    // Rounds from `resolve_from` on are left for the resolver's final
    // drain; at least the last round always is (`resolve_from <= rounds-1`
    // by construction). A round that reuses a key with a pending intent
    // resolves the queue first (one intent slot per key, 2.1).
    let resolve_from = rng.below(rounds);
    let mut pending: Vec<bool> = vec![false; nkeys as usize];
    let mut committed: Vec<TxnId> = Vec::new();

    for round in 0..rounds {
        let txn = core.begin(Isolation::ReadCommitted);
        let mut ops: Vec<(usize, Option<Vec<u8>>)> = Vec::new();
        for _ in 0..(1 + rng.below(2)) {
            let k = rng.below(nkeys) as usize;
            if ops.iter().any(|(i, _)| *i == k) || pending[k] {
                continue;
            }
            let val = if rng.coin() {
                Some(format!("v{round}-{k}").into_bytes())
            } else {
                None
            };
            let val = val.as_deref();
            place(&core, &txn, keys[k], val)?;
            ops.push((k, val.map(|v| v.to_vec())));
        }
        if ops.is_empty() {
            // Fall back to one op on the first key that is not pending
            // (`must_resolve` above keeps at least one free whenever a
            // round follows).
            let k = (0..nkeys as usize)
                .find(|k| !pending[*k])
                .expect("a non-pending key (must_resolve drained the queue)");
            let val: Option<Vec<u8>> = if rng.coin() {
                Some(format!("v{round}-{k}").into_bytes())
            } else {
                None
            };
            place(&core, &txn, keys[k], val.as_deref())?;
            ops.push((k, val));
        }
        let tid = txn.id;
        let c = commit(&core, &mut pipeline, txn)?;
        committed.push(tid);
        for (k, _) in &ops {
            pending[*k] = true;
        }
        // Resolve lazily: never for the unresolved suffix (Rework 4), and
        // eagerly only when every key is pending - the next round could
        // otherwise have no writable key (one intent slot per key, 2.1).
        let must_resolve = round + 1 < rounds && pending.iter().all(|p| *p);
        if round < resolve_from || must_resolve {
            Resolver::run_once(&core).map_err(str_err)?;
            for p in pending.iter_mut() {
                *p = false;
            }
        }
        for (k, v) in ops {
            histories[k].push((c, v));
        }
        if lsm && rng.coin() {
            kv.flush();
        }
        if open_view.is_none() && round > 0 && rng.coin() {
            vts = core.visible_ts();
            open_view = Some(core.open_view());
        }
        if snaps.len() < 3 && rng.coin() {
            snaps.push(core.registry.take_snapshot());
        }
        if !snaps.is_empty() && rng.coin() {
            let i = rng.below(snaps.len() as u64) as usize;
            snaps.remove(i);
        }
    }
    // A snapshot above every unresolved commit: the read-side catch of an
    // intent the tombstone job must not delete (Rework 4).
    snaps.push(core.registry.take_snapshot());

    // The GC phase: 4-10 random steps, checking before/after each.
    let steps = 4 + rng.below(7);
    let mut prev = check_reads(&core, &keys, &histories, &snaps, open_view.as_ref(), vts)?;
    for _ in 0..steps {
        match rng.below(5) {
            0 => {
                job.publish(0).map_err(str_err)?;
            }
            1 => {
                kv.flush();
            }
            2 => {
                let l0: Vec<u64> = kv
                    .files()
                    .into_iter()
                    .filter(|(level, _, _)| *level == 0)
                    .map(|(_, id, _)| id)
                    .collect();
                if !l0.is_empty() {
                    let id = l0[rng.below(l0.len() as u64) as usize];
                    kv.compact(0, &[id]);
                }
            }
            3 => {
                job.drop_tombstones().map_err(str_err)?;
            }
            _ => {
                kv.compact_all();
            }
        }
        let cur = check_reads(&core, &keys, &histories, &snaps, open_view.as_ref(), vts)?;
        if cur != prev {
            return Err(format!("a GC step changed a read: {prev:?} -> {cur:?}"));
        }
        prev = cur;
    }

    // The drain: with every snapshot and view dropped, resolve every
    // committed intent first (Rework 4: the tombstone pass must run over
    // the fully resolved state), then publish to visible_ts, run the
    // tombstone job and a full compaction. No tombstone may remain as any
    // key's newest version <= W, and every committed txn must have been
    // resolved and truncated - an intent deleted behind the resolver's
    // back leaves its owner's count above 0 and its entry alive forever.
    snaps.clear();
    drop(open_view.take());
    while Resolver::run_once(&core).map_err(str_err)? > 0 {}
    let w = job.publish(0).map_err(str_err)?;
    job.drop_tombstones().map_err(str_err)?;
    kv.compact_all();
    let offenders = quiesce_offenders(&kv, w.0)?;
    if !offenders.is_empty() {
        return Err(format!("not quiesced at W={w:?}: {offenders:?}"));
    }
    for id in &committed {
        if core.status.entry(*id).is_some() {
            return Err(format!(
                "committed {id:?} was not resolved and truncated (its \
                 intent count never reached 0)"
            ));
        }
    }
    Ok(())
}

#[test]
fn igc_property() {
    let mut failures = Vec::new();
    for seed in 1..=CASES {
        if let Err(e) = run_case(seed, true) {
            failures.push(format!("lsm seed {seed}: {e}"));
        }
        if let Err(e) = run_case(seed, false) {
            failures.push(format!("flat seed {seed}: {e}"));
        }
    }
    let n = failures.len();
    let shown: Vec<_> = failures.iter().take(10).cloned().collect();
    assert!(
        failures.is_empty(),
        "I-GC violations in {n}/{} cases: {}{}",
        CASES * 2,
        shown.join("; "),
        if n > shown.len() { " ..." } else { "" }
    );
}
