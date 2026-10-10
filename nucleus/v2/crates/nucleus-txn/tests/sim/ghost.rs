//! C-SIM work item 5: ghost state — written by the simulator from what it
//! asked for and what the public API returned, never read from crate
//! internals. Per txn: isolation, snapshot(s), reads with results, applied
//! writes, savepoint rollbacks, the outcome and the sync mode; plus the
//! committed history (txns acked with a ts, in ts order, with their
//! surviving writes).

use std::collections::{BTreeMap, BTreeSet};

use nucleus_txn::commit::SyncCommit;
use nucleus_txn::txn::Isolation;
use nucleus_txn::{RowLockMode, Seq, Ts, TxnId};

/// One applied write of a txn, in program order (§5.5's write-set shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GWrite {
    /// INSERT: an absolute value.
    Set(Vec<u8>),
    /// UPDATE: an increment; `wrote` is the value the txn computed and
    /// placed (the quiescence fold composes the `+1`s, which is what makes
    /// a lost update observable).
    Inc { wrote: Vec<u8> },
    /// DELETE.
    Delete,
}

#[derive(Debug, Clone)]
pub struct GhostWrite {
    pub seq: Seq,
    pub key: Vec<u8>,
    pub kind: GWrite,
}

/// One read a session performed: its snapshot `S`, the statement's `seq0`,
/// the key (and range, for scans) and the value the API returned (`None` =
/// not found).
#[derive(Debug, Clone)]
pub struct GhostRead {
    pub seq0: Seq,
    pub snapshot: Ts,
    pub key: Vec<u8>,
    /// `None` for a point read, `(lo, hi)` for a range scan.
    pub range: Option<(Vec<u8>, Vec<u8>)>,
    pub result: Option<Vec<u8>>,
}

/// How a txn ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Active,
    /// Acked with a commit ts.
    Acked(Ts),
    /// Aborted by the session (statement error or explicit abort).
    Aborted,
    /// In flight when a crash ended its era; its txn was never acked.
    LostInCrash,
    /// A commit record of it survived the crash and was loaded at boot:
    /// committed at `ts` without ever being acked.
    CommittedByRecord(Ts),
    /// Acked (or record-loaded) in an earlier era whose commit record a
    /// later crash then dropped: `synchronous_commit = off` promised
    /// nothing, so after §7.2's reboot it never committed — excluded from
    /// the history and from I-VIS; the ts may be reused by a later era.
    DroppedByCrash(Ts),
}

/// One txn's ghost record.
pub struct GhostTxn {
    pub isolation: Isolation,
    /// The txn's snapshot (RR/SER); RC records one per read.
    pub snapshot: Option<Ts>,
    pub reads: Vec<GhostRead>,
    pub writes: Vec<GhostWrite>,
    pub outcome: Outcome,
    pub sync: Option<SyncCommit>,
    /// Every `ts` observed for this txn (ack, status observation, commit
    /// record): what I-VIS may test against.
    pub known_ts: Option<Ts>,
    /// The status entry was observed `released` before it vanished
    /// (I-VIS's truncated-after-release arm).
    pub released_seen: bool,
    /// The status entry was observed gone (truncated).
    pub truncated_seen: bool,
    /// The KV record length right after this txn's last intent placement
    /// (I-WAL-ORDER: every intent write precedes the commit record).
    pub placement_bound: usize,
    /// A compact event log (session index, statement, action, key, detail)
    /// for auditing violations.
    pub events: Vec<String>,
}

impl GhostTxn {
    /// The last surviving own write on `key` with `seq < seq0`
    /// (I-HALLOWEEN: the read oracle's own-write overlay).
    pub fn own_write_at(&self, key: &[u8], seq0: Seq) -> Option<&GWrite> {
        let mut last = None;
        for w in &self.writes {
            if w.seq < seq0 && w.key == key {
                last = Some(&w.kind);
            }
        }
        last
    }
}

/// One shared row lock (FOR SHARE / FOR KEY SHARE) the simulator was granted:
/// the statement asked for `mode` on `key` at statement seq `seq` and the
/// API answered `Applied`. The ledger is built from that alone.
#[derive(Debug, Clone)]
pub struct SharedGrant {
    pub txn: TxnId,
    pub key: Vec<u8>,
    pub mode: RowLockMode,
    pub seq: Seq,
}

/// The committed history: every txn known committed with a ts, and its
/// surviving writes as of its end.
pub struct Ghost {
    pub txns: BTreeMap<TxnId, GhostTxn>,
    /// txns committed at each ts (acked, status-observed or record-loaded).
    pub committed: BTreeMap<Ts, TxnId>,
    /// Commits whose record a crash dropped: invisible after the reboot
    /// (their intents read as aborted older-epoch state), excluded from
    /// every oracle. A later era may legally reuse the ts.
    pub lost: BTreeSet<Ts>,
    /// The largest ts ever assigned to a txn we know of.
    pub max_ts: Ts,
    /// The independent ledger of granted shared row locks (I-LOCK's shared
    /// side, the I-LIVE(c) graph). Entries are dropped by savepoint
    /// rollback; a txn's entries stop counting when it ends.
    pub shared: Vec<SharedGrant>,
}

impl Ghost {
    pub fn new() -> Ghost {
        Ghost {
            txns: BTreeMap::new(),
            committed: BTreeMap::new(),
            lost: BTreeSet::new(),
            max_ts: Ts(0),
            shared: Vec::new(),
        }
    }

    pub fn create(&mut self, id: TxnId, _session: usize, isolation: Isolation) {
        self.txns.insert(
            id,
            GhostTxn {
                isolation,
                snapshot: None,
                reads: Vec::new(),
                writes: Vec::new(),
                outcome: Outcome::Active,
                sync: None,
                known_ts: None,
                released_seen: false,
                truncated_seen: false,
                placement_bound: 0,
                events: Vec::new(),
            },
        );
    }

    /// The smallest ts observed for `id`, if any (a lower bound for I-VIS).
    pub fn note_ts(&mut self, id: TxnId, ts: Ts) {
        if let Some(t) = self.txns.get_mut(&id) {
            if t.known_ts.is_none() {
                t.known_ts = Some(ts);
            }
        }
    }

    /// Marks a txn committed at `ts` (idempotent; a txn's ts never changes).
    /// A ts marked lost (its dropped record's ts, §7.2) may be **reused** by
    /// a later era: this observation is a real commit at that ts, so the ts
    /// becomes live again.
    pub fn commit_at(&mut self, id: TxnId, ts: Ts) {
        self.note_ts(id, ts);
        if self.lost.remove(&ts) || !self.committed.contains_key(&ts) {
            self.committed.insert(ts, id);
        }
        if ts > self.max_ts {
            self.max_ts = ts;
        }
    }

    /// The commit at `ts` lost its record in a crash: it is invisible
    /// after the reboot, and a later era may reuse the ts.
    pub fn mark_lost(&mut self, ts: Ts) {
        self.committed.remove(&ts);
        self.lost.insert(ts);
    }

    /// The committed txns that (last-surviving-)wrote `key`, with their ts:
    /// the key's version list, oldest first.
    pub fn versions_of(&self, key: &[u8]) -> Vec<(Ts, &GWrite)> {
        let mut out = Vec::new();
        for (&ts, &id) in &self.committed {
            if let Some(t) = self.txns.get(&id) {
                let mut last: Option<&GhostWrite> = None;
                for w in &t.writes {
                    if w.key == key {
                        last = Some(w);
                    }
                }
                if let Some(w) = last {
                    out.push((ts, &w.kind));
                }
            }
        }
        out
    }

    /// The newest committed version of `key` at or below `S`.
    pub fn version_at(&self, key: &[u8], s: Ts) -> Option<(Ts, &GWrite)> {
        let mut best: Option<(Ts, &GWrite)> = None;
        for (&ts, &id) in &self.committed {
            if ts > s {
                continue;
            }
            if let Some(t) = self.txns.get(&id) {
                let mut last: Option<&GhostWrite> = None;
                for w in &t.writes {
                    if w.key == key {
                        last = Some(w);
                    }
                }
                if let Some(w) = last {
                    if best.as_ref().is_none_or(|(bts, _)| ts > *bts) {
                        best = Some((ts, &w.kind));
                    }
                }
            }
        }
        best
    }

    /// `ROLLBACK TO s`: drop this txn's writes at `seq >= s` (§5.5).
    pub fn rollback_to(&mut self, id: TxnId, s: Seq) {
        if let Some(t) = self.txns.get_mut(&id) {
            t.writes.retain(|w| w.seq < s);
        }
        // ROLLBACK TO s releases the shared locks taken at seq >= s (§5.5)
        // and nothing else.
        self.shared.retain(|g| !(g.txn == id && g.seq >= s));
    }

    /// Records a granted shared lock.
    pub fn grant_shared(&mut self, txn: TxnId, key: &[u8], mode: RowLockMode, seq: Seq) {
        self.shared.push(SharedGrant {
            txn,
            key: key.to_vec(),
            mode,
            seq,
        });
    }

    /// Whether `id` still holds its locks at `visible_ts` (§6, §3.2): it has
    /// neither been aborted nor lost in a crash nor acked, and it has not
    /// become a visible commit (a commit record at `ts <= visible_ts` ends
    /// the holder even before its ack is observed). Pending and
    /// committed-not-visible txns hold. A crash ends every holder of the
    /// older epochs (§6: a crash aborts them all).
    pub fn holds(&self, id: TxnId, visible: Ts, epoch: u32) -> bool {
        id.epoch == epoch
            && self.txns.get(&id).is_some_and(|t| {
                t.outcome == Outcome::Active && !t.known_ts.is_some_and(|ts| ts <= visible)
            })
    }

    /// The live shared holders of `key` at `visible_ts`, from the ledger.
    pub fn shared_holders(&self, key: &[u8], visible: Ts, epoch: u32) -> Vec<(TxnId, RowLockMode)> {
        let mut out: Vec<(TxnId, RowLockMode)> = Vec::new();
        for g in &self.shared {
            if g.key == key && self.holds(g.txn, visible, epoch) && !out.contains(&(g.txn, g.mode))
            {
                out.push((g.txn, g.mode));
            }
        }
        out
    }
}

/// Decodes a row payload the simulator wrote (a big-endian u64 counter).
pub fn payload_u64(v: &[u8]) -> u64 {
    if v.len() >= 8 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&v[..8]);
        u64::from_be_bytes(b)
    } else {
        let mut x = 0u64;
        for &byte in v {
            x = (x << 8) | byte as u64;
        }
        x
    }
}

/// Encodes a row payload (a big-endian u64 counter).
pub fn u64_payload(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}
