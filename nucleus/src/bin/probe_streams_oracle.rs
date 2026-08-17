//! Streams / PubSub / CDC differential fuzzer — reference-implementation oracles.
//!
//! Sibling to `probe_streams`, which asserts *structural* properties of the same
//! three models (lengths agree with a counter, ids ascend, a deleted key reads
//! back NULL). Structure is not answers: every check in that binary passes
//! against an engine that returns the wrong entries, delivers a message to the
//! wrong channel, or attributes a change to the wrong table, as long as it does
//! so consistently. These three models were the last on `PROBES.md`'s oracle
//! matrix with no external oracle at all.
//!
//! Each section carries an independent reference implementation written from
//! the documented contract (`FRAMEWORK_CONTRACT.md` §3.9/§3.10 and
//! `MODEL_SEMANTICS.md`), not from the engine's source, and compares the FULL
//! result of every read — ids and field maps, delivered payload sequences,
//! change records — rather than a count.
//!
//! **Section 1 – Streams.** A reference log with consumer groups: XADD /
//! XLEN / XRANGE / XREAD / XGROUP_CREATE / XREADGROUP / XACK. Three streams run
//! at once so a read that leaks another stream's entries is a divergence rather
//! than a coincidence.
//!
//! **Section 2 – PubSub.** Real subscribers are created through the same hub
//! `PUBSUB_*` reads (`Executor::pubsub_sync`), and every receiver is drained and
//! compared against the exact sequence of payloads the model says it should have
//! received. A message delivered to the wrong channel, dropped, or duplicated is
//! invisible to a subscriber-count assertion and visible here.
//!
//! **Section 3 – CDC.** An independent table model computes what each DML
//! statement changed; the CDC log must contain exactly one record per
//! row-affecting statement, attributed to the right table with the right change
//! type, in sequence order. The model's agreement with the engine's own table
//! contents is asserted first, in the same iteration — otherwise a wrong
//! `rows_affected` would surface as a CDC divergence and be fixed in the wrong
//! subsystem.
//!
//! **Proving the oracle can fail** — `--negative-control <section>` runs the
//! whole probe twice at one seed, once clean and once with that section's model
//! perturbed the way a plausible engine bug would perturb it (a lost stream
//! entry, a message delivered to a subscriber that should not have it, a change
//! record never written). It passes only if the perturbed run reports MORE
//! divergences in that section and exactly as many in the other two. The
//! baseline run is the point: the first version of this control declared
//! success on a divergence that was already there before it perturbed anything,
//! and two of the three perturbations were in fact never applied — they were
//! keyed to an op index that is only sometimes an eligible event. A comparison
//! nobody has watched fail is not a comparison, and "it reported something" is
//! not the same as "it reported this".
//!
//! Build: `cargo build --release --features server --bin probe_streams_oracle`
//! Run:   `cargo run  --release --features server --bin probe_streams_oracle`
//!        `... --bin probe_streams_oracle -- --engine buffered-disk`
//!        `... --bin probe_streams_oracle -- --negative-control streams`
#![cfg(feature = "server")]

use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::metrics::harness::{EngineConfig, EngineKind, HarnessDb};
use nucleus::pubsub::Message;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;
use tokio::sync::broadcast;

static DIR_SEQ: AtomicUsize = AtomicUsize::new(0);

// ─── Deterministic PRNG (xorshift64) ─────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

// ─── Executor helpers ────────────────────────────────────────────────────────

fn exec(ex: &Executor, sql: &str) -> Result<Vec<ExecResult>, String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(format!("{e}")),
        Err(_) => Err("PANIC".to_string()),
    }
}

/// First column of the first row, for the scalar-function surface.
fn scalar(ex: &Executor, sql: &str) -> Result<Value, String> {
    let mut results = exec(ex, sql)?;
    match results.pop() {
        Some(ExecResult::Select { rows, .. }) => rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next())
            .ok_or_else(|| "empty result".to_string()),
        _ => Err("not a SELECT result".to_string()),
    }
}

fn scalar_i64(ex: &Executor, sql: &str) -> Result<i64, String> {
    match scalar(ex, sql)? {
        Value::Int64(n) => Ok(n),
        Value::Int32(n) => Ok(n as i64),
        other => Err(format!("expected integer, got {other:?}")),
    }
}

fn scalar_text(ex: &Executor, sql: &str) -> Result<String, String> {
    match scalar(ex, sql)? {
        Value::Text(s) => Ok(s),
        Value::Null => Ok(String::new()),
        other => Err(format!("expected text, got {other:?}")),
    }
}

fn rows_affected(ex: &Executor, sql: &str) -> Result<usize, String> {
    let mut results = exec(ex, sql)?;
    match results.pop() {
        Some(ExecResult::Command { rows_affected, .. }) => Ok(rows_affected),
        Some(ExecResult::Select { rows, .. }) => Ok(rows.len()),
        _ => Err("unexpected result shape".to_string()),
    }
}

fn select_pairs(ex: &Executor, sql: &str) -> Result<Vec<(i64, i64)>, String> {
    let mut results = exec(ex, sql)?;
    let rows = match results.pop() {
        Some(ExecResult::Select { rows, .. }) => rows,
        _ => return Err("not a SELECT result".to_string()),
    };
    let as_i64 = |v: &Value| -> Option<i64> {
        match v {
            Value::Int64(n) => Some(*n),
            Value::Int32(n) => Some(*n as i64),
            _ => None,
        }
    };
    rows.iter()
        .map(
            |r| match (r.first().and_then(as_i64), r.get(1).and_then(as_i64)) {
                (Some(a), Some(b)) => Ok((a, b)),
                _ => Err("non-integer row".to_string()),
            },
        )
        .collect()
}

// ─── Divergence record ───────────────────────────────────────────────────────

#[derive(Debug)]
struct Divergence {
    section: &'static str,
    check: &'static str,
    detail: String,
    repro: Vec<String>,
}

struct Report {
    found: Vec<Divergence>,
    max_report: usize,
    counts: BTreeMap<&'static str, usize>,
}

impl Report {
    fn new(max_report: usize) -> Self {
        Self {
            found: Vec::new(),
            max_report,
            counts: BTreeMap::new(),
        }
    }

    fn push(&mut self, section: &'static str, check: &'static str, detail: String, log: &[String]) {
        *self.counts.entry(section).or_insert(0) += 1;
        if self.found.len() < self.max_report {
            self.found.push(Divergence {
                section,
                check,
                detail,
                repro: log.to_vec(),
            });
        }
    }

    fn count(&self, section: &str) -> usize {
        self.counts.get(section).copied().unwrap_or(0)
    }

    fn total(&self) -> usize {
        self.counts.values().sum()
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Section 1 — Streams
// ═════════════════════════════════════════════════════════════════════════════

/// A stream entry id. `FRAMEWORK_CONTRACT.md` §3.9: `<ms>-<seq>`, ordered by
/// millisecond then sequence.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Id {
    ms: u64,
    seq: u64,
}

impl Id {
    fn parse(s: &str) -> Option<Id> {
        let (ms, seq) = s.split_once('-')?;
        Some(Id {
            ms: ms.trim().parse().ok()?,
            seq: seq.trim().parse().ok()?,
        })
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

type Fields = BTreeMap<String, String>;

/// Reference consumer group: a delivery cursor plus per-consumer pending sets.
#[derive(Default, Clone)]
struct RefGroup {
    last_delivered: Id,
    pending: BTreeMap<String, Vec<Id>>,
}

/// Reference stream — an append-only log, independent of `pubsub::Stream`.
#[derive(Default)]
struct RefStream {
    entries: Vec<(Id, Fields)>,
    groups: BTreeMap<String, RefGroup>,
}

impl RefStream {
    fn xadd(&mut self, id: Id, fields: Fields) {
        self.entries.push((id, fields));
    }

    fn xlen(&self) -> usize {
        self.entries.len()
    }

    /// Entries in `[start_ms-0, end_ms-MAX]`, first `count`.
    fn xrange(&self, start_ms: u64, end_ms: u64, count: usize) -> Vec<(Id, Fields)> {
        let lo = Id {
            ms: start_ms,
            seq: 0,
        };
        let hi = Id {
            ms: end_ms,
            seq: u64::MAX,
        };
        self.entries
            .iter()
            .filter(|(id, _)| *id >= lo && *id <= hi)
            .take(count)
            .cloned()
            .collect()
    }

    /// Entries strictly after `cursor`. A bare millisecond `m` is the cursor
    /// `m-MAX` — it can only mean "after that whole millisecond", which is why
    /// resuming from one loses whatever else landed in it; the full `<ms>-<seq>`
    /// form addresses an entry and resumes exactly after it.
    fn xread(&self, cursor: Id, count: usize) -> Vec<(Id, Fields)> {
        self.entries
            .iter()
            .filter(|(id, _)| *id > cursor)
            .take(count)
            .cloned()
            .collect()
    }

    /// Contract §3.9 and the engine's own note: creation is idempotent-overwrite.
    fn xgroup_create(&mut self, group: &str, start_ms: u64) {
        self.groups.insert(
            group.to_string(),
            RefGroup {
                last_delivered: Id {
                    ms: start_ms,
                    seq: 0,
                },
                pending: BTreeMap::new(),
            },
        );
    }

    fn xreadgroup(&mut self, group: &str, consumer: &str, count: usize) -> Vec<(Id, Fields)> {
        let Some(g) = self.groups.get(group) else {
            return Vec::new();
        };
        let cursor = g.last_delivered;
        let delivered: Vec<(Id, Fields)> = self
            .entries
            .iter()
            .filter(|(id, _)| *id > cursor)
            .take(count)
            .cloned()
            .collect();
        if let Some(g) = self.groups.get_mut(group) {
            if let Some((last, _)) = delivered.last() {
                g.last_delivered = *last;
            }
            g.pending
                .entry(consumer.to_string())
                .or_default()
                .extend(delivered.iter().map(|(id, _)| *id));
        }
        delivered
    }

    fn xack(&mut self, group: &str, id: Id) -> usize {
        let Some(g) = self.groups.get_mut(group) else {
            return 0;
        };
        let mut acked = 0;
        for pending in g.pending.values_mut() {
            let before = pending.len();
            pending.retain(|p| *p != id);
            acked += before - pending.len();
        }
        acked
    }
}

/// Parse the `[{"id":"ms-seq","fields":{...}}]` wire format of XRANGE / XREAD /
/// XREADGROUP. An empty string is the documented empty result for a stream that
/// does not exist (contract §3.9).
fn parse_entries(text: &str) -> Result<Vec<(Id, Fields)>, String> {
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let parsed: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("unparseable entry JSON: {e}"))?;
    let serde_json::Value::Array(items) = parsed else {
        return Err("entry JSON is not an array".to_string());
    };
    items
        .iter()
        .map(|item| {
            let id = item
                .get("id")
                .and_then(|v| v.as_str())
                .and_then(Id::parse)
                .ok_or_else(|| format!("entry has no parseable id: {item}"))?;
            let fields = item
                .get("fields")
                .and_then(|v| v.as_object())
                .ok_or_else(|| format!("entry has no fields object: {item}"))?
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect();
            Ok((id, fields))
        })
        .collect()
}

fn fmt_entries(entries: &[(Id, Fields)]) -> String {
    let parts: Vec<String> = entries
        .iter()
        .map(|(id, f)| {
            let fs: Vec<String> = f.iter().map(|(k, v)| format!("{k}={v}")).collect();
            format!("{id}[{}]", fs.join(","))
        })
        .collect();
    format!("[{}]", parts.join(" "))
}

fn compare_entries(
    rep: &mut Report,
    check: &'static str,
    expected: &[(Id, Fields)],
    got: &[(Id, Fields)],
    log: &[String],
) {
    if expected != got {
        rep.push(
            "streams",
            check,
            format!(
                "expected {} entries {}, got {} entries {}",
                expected.len(),
                fmt_entries(expected),
                got.len(),
                fmt_entries(got)
            ),
            log,
        );
    }
}

fn section_streams(ex: &Executor, rng: &mut Rng, ops: usize, neg: bool, rep: &mut Report) {
    let names = ["so_a", "so_b", "so_c"];
    let mut models: BTreeMap<&str, RefStream> =
        names.iter().map(|n| (*n, RefStream::default())).collect();
    let groups = ["g0", "g1"];
    let consumers = ["c0", "c1"];
    let mut log: Vec<String> = Vec::new();
    // The perturbation latches on the first ELIGIBLE event past the midpoint —
    // an op index is not eligibility, and keying it to one meant this control
    // silently never applied on runs where that op was a read.
    let mut neg_armed = neg;

    for op in 0..ops {
        let stream = names[rng.below(names.len())];
        match rng.below(12) {
            // ── XADD ─────────────────────────────────────────────────────────
            0..=4 => {
                let n_fields = 1 + rng.below(3);
                let mut fields: Fields = BTreeMap::new();
                for _ in 0..n_fields {
                    fields.insert(format!("f{}", rng.below(4)), format!("v{}", rng.below(64)));
                }
                let args: Vec<String> = fields
                    .iter()
                    .map(|(k, v)| format!("'{k}', '{v}'"))
                    .collect();
                let sql = format!("SELECT STREAM_XADD('{stream}', {})", args.join(", "));
                log.push(sql.clone());
                match scalar_text(ex, &sql) {
                    Ok(id_text) => {
                        let Some(id) = Id::parse(&id_text) else {
                            rep.push(
                                "streams",
                                "XADD returns a <ms>-<seq> id",
                                format!("unparseable id {id_text:?}"),
                                &log,
                            );
                            continue;
                        };
                        let model = models.get_mut(stream).unwrap();
                        if let Some((prev, _)) = model.entries.last()
                            && id <= *prev
                        {
                            rep.push(
                                "streams",
                                "XADD ids strictly increase",
                                format!("{id} follows {prev}"),
                                &log,
                            );
                        }
                        if neg_armed && op >= ops / 2 {
                            // The engine appended it; the model forgets it.
                            neg_armed = false;
                        } else {
                            model.xadd(id, fields);
                        }
                    }
                    Err(e) => rep.push("streams", "XADD succeeds", e, &log),
                }
            }
            // ── XLEN ─────────────────────────────────────────────────────────
            5 => {
                let sql = format!("SELECT STREAM_XLEN('{stream}')");
                log.push(sql.clone());
                let expected = models[stream].xlen() as i64;
                match scalar_i64(ex, &sql) {
                    Ok(got) if got != expected => rep.push(
                        "streams",
                        "XLEN == entries in the reference log",
                        format!("expected {expected}, got {got}"),
                        &log,
                    ),
                    Err(e) => rep.push("streams", "XLEN succeeds", e, &log),
                    _ => {}
                }
            }
            // ── XRANGE ───────────────────────────────────────────────────────
            6..=7 => {
                let model = &models[stream];
                // Bound the window on real ids so a narrow range is generated
                // as often as a full sweep — a filter that ignores its bounds
                // passes every full-sweep check.
                let (start_ms, end_ms) = match (model.entries.first(), model.entries.last()) {
                    (Some((lo, _)), Some((hi, _))) => match rng.below(4) {
                        0 => (0, u64::MAX / 2),
                        1 => (lo.ms, hi.ms),
                        2 => (hi.ms, hi.ms),
                        _ => (lo.ms + 1, hi.ms),
                    },
                    _ => (0, u64::MAX / 2),
                };
                let count = 1 + rng.below(model.entries.len().max(1) + 2);
                let sql =
                    format!("SELECT STREAM_XRANGE('{stream}', {start_ms}, {end_ms}, {count})");
                log.push(sql.clone());
                let expected = model.xrange(start_ms, end_ms, count);
                match scalar_text(ex, &sql).and_then(|t| parse_entries(&t)) {
                    Ok(got) => compare_entries(
                        rep,
                        "XRANGE returns exactly the entries in the window",
                        &expected,
                        &got,
                        &log,
                    ),
                    Err(e) => rep.push("streams", "XRANGE succeeds", e, &log),
                }
            }
            // ── XREAD ────────────────────────────────────────────────────────
            8 => {
                let model = &models[stream];
                let anchor = match model.entries.len() {
                    0 => None,
                    n => {
                        let pick = rng.below(n + 1);
                        (pick < n).then(|| model.entries[pick].0)
                    }
                };
                let count = 1 + rng.below(model.entries.len().max(1) + 2);
                // Both cursor forms, because they mean different things: the
                // bare millisecond skips to the next one, the full id resumes
                // after exactly that entry.
                let (arg, cursor) = match (anchor, rng.below(2)) {
                    (Some(id), 0) => (format!("'{id}'"), id),
                    (Some(id), _) => (
                        id.ms.to_string(),
                        Id {
                            ms: id.ms,
                            seq: u64::MAX,
                        },
                    ),
                    (None, _) => (
                        "0".to_string(),
                        Id {
                            ms: 0,
                            seq: u64::MAX,
                        },
                    ),
                };
                let sql = format!("SELECT STREAM_XREAD('{stream}', {arg}, {count})");
                log.push(sql.clone());
                let expected = model.xread(cursor, count);
                match scalar_text(ex, &sql).and_then(|t| parse_entries(&t)) {
                    Ok(got) => compare_entries(
                        rep,
                        "XREAD returns exactly the entries after the cursor",
                        &expected,
                        &got,
                        &log,
                    ),
                    Err(e) => rep.push("streams", "XREAD succeeds", e, &log),
                }
            }
            // ── XGROUP_CREATE ────────────────────────────────────────────────
            9 => {
                let group = groups[rng.below(groups.len())];
                let start_ms = if rng.below(2) == 0 { 0 } else { 1 };
                let sql = format!("SELECT STREAM_XGROUP_CREATE('{stream}', '{group}', {start_ms})");
                log.push(sql.clone());
                match exec(ex, &sql) {
                    Ok(_) => models
                        .get_mut(stream)
                        .unwrap()
                        .xgroup_create(group, start_ms),
                    Err(e) => rep.push("streams", "XGROUP_CREATE succeeds", e, &log),
                }
            }
            // ── XREADGROUP ───────────────────────────────────────────────────
            10 => {
                let group = groups[rng.below(groups.len())];
                let consumer = consumers[rng.below(consumers.len())];
                let count = 1 + rng.below(4);
                let sql = format!(
                    "SELECT STREAM_XREADGROUP('{stream}', '{group}', '{consumer}', {count})"
                );
                log.push(sql.clone());
                let expected = models
                    .get_mut(stream)
                    .unwrap()
                    .xreadgroup(group, consumer, count);
                match scalar_text(ex, &sql).and_then(|t| parse_entries(&t)) {
                    Ok(got) => compare_entries(
                        rep,
                        "XREADGROUP delivers exactly the undelivered entries",
                        &expected,
                        &got,
                        &log,
                    ),
                    Err(e) => rep.push("streams", "XREADGROUP succeeds", e, &log),
                }
            }
            // ── XACK ─────────────────────────────────────────────────────────
            _ => {
                let group = groups[rng.below(groups.len())];
                let model = &models[stream];
                // Half the time acknowledge something genuinely pending, half
                // the time something that is not: a stub returning the count it
                // was asked for passes only the first half.
                let pending: Vec<Id> = model
                    .groups
                    .get(group)
                    .map(|g| g.pending.values().flatten().copied().collect())
                    .unwrap_or_default();
                let id = match (pending.is_empty(), model.entries.is_empty()) {
                    (false, _) if rng.below(2) == 0 => pending[rng.below(pending.len())],
                    (_, false) => model.entries[rng.below(model.entries.len())].0,
                    _ => Id { ms: 1, seq: 0 },
                };
                let sql = format!(
                    "SELECT STREAM_XACK('{stream}', '{group}', {}, {})",
                    id.ms, id.seq
                );
                log.push(sql.clone());
                let expected = models.get_mut(stream).unwrap().xack(group, id) as i64;
                match scalar_i64(ex, &sql) {
                    Ok(got) if got != expected => rep.push(
                        "streams",
                        "XACK acknowledges exactly the pending entries",
                        format!("id {id}: expected {expected}, got {got}"),
                        &log,
                    ),
                    Err(e) => rep.push("streams", "XACK succeeds", e, &log),
                    _ => {}
                }
            }
        }
    }

    // ── Closing sweep ────────────────────────────────────────────────────────
    // Every stream compared in full, unconditionally: the random walk may
    // happen not to read a stream it wrote, and an oracle whose coverage
    // depends on the dice is an oracle that reports "clean" for having looked
    // nowhere.
    for (name, model) in &models {
        let sql = format!("SELECT STREAM_XLEN('{name}')");
        match scalar_i64(ex, &sql) {
            Ok(got) if got != model.xlen() as i64 => rep.push(
                "streams",
                "XLEN == entries in the reference log",
                format!("{name}: expected {}, got {got}", model.xlen()),
                std::slice::from_ref(&sql),
            ),
            Err(e) => rep.push("streams", "XLEN succeeds", e, std::slice::from_ref(&sql)),
            _ => {}
        }
        let sweep = format!(
            "SELECT STREAM_XRANGE('{name}', 0, {}, {})",
            u64::MAX / 2,
            model.xlen() + 1
        );
        match scalar_text(ex, &sweep).and_then(|t| parse_entries(&t)) {
            Ok(got) => compare_entries(
                rep,
                "XRANGE over the whole stream returns the whole log",
                &model.entries,
                &got,
                &[sweep],
            ),
            Err(e) => rep.push("streams", "XRANGE succeeds", e, &[sweep]),
        }
    }

    // ── No-gap check ─────────────────────────────────────────────────────────
    // The one guarantee a log owes a polling consumer: resuming from the last
    // entry it saw serves the next one. Ids carry a sequence, so entries can
    // share a millisecond — and until 2026-08-17 the cursor was a millisecond,
    // which made every such entry permanently unreachable. Walked over the
    // model's own entries so a failure names the entry that was lost rather
    // than a count.
    for (name, model) in &models {
        for window in model.entries.windows(2) {
            let (prev, _) = window[0];
            let (curr, _) = window[1];
            let sql = format!("SELECT STREAM_XREAD('{name}', '{prev}', 100)");
            let served = scalar_text(ex, &sql)
                .and_then(|t| parse_entries(&t))
                .unwrap_or_default();
            if !served.iter().any(|(id, _)| *id == curr) {
                rep.push(
                    "streams",
                    "XREAD from an entry's id serves the entry after it",
                    format!(
                        "a consumer positioned at {prev} polling '{name}' never sees {curr}{}",
                        if prev.ms == curr.ms {
                            ", which was appended after it in the same millisecond"
                        } else {
                            ""
                        }
                    ),
                    &[sql],
                );
                return; // one report is the finding; the rest are the same one
            }
        }
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Section 2 — PubSub
// ═════════════════════════════════════════════════════════════════════════════

struct Subscriber {
    handle: usize,
    channel: String,
    rx: broadcast::Receiver<Arc<Message>>,
    /// Payloads the model says this receiver must observe, in publish order.
    expected: Vec<String>,
}

fn section_pubsub(ex: &Executor, rng: &mut Rng, ops: usize, neg: bool, rep: &mut Report) {
    let channels = ["po_x", "po_y", "po_z"];
    let mut subs: Vec<Subscriber> = Vec::new();
    let mut ever_subscribed: BTreeSet<String> = BTreeSet::new();
    let mut next_handle = 0usize;
    let mut log: Vec<String> = Vec::new();
    let mut neg_armed = neg;

    for op in 0..ops {
        let channel = channels[rng.below(channels.len())].to_string();
        match rng.below(10) {
            // ── Subscribe (through the hub PUBSUB_* reads) ───────────────────
            0..=2 => {
                let rx = ex.pubsub_sync().write().subscribe(&channel);
                log.push(format!("-- subscribe('{channel}') -> handle {next_handle}"));
                ever_subscribed.insert(channel.clone());
                subs.push(Subscriber {
                    handle: next_handle,
                    channel,
                    rx,
                    expected: Vec::new(),
                });
                next_handle += 1;
            }
            // ── Unsubscribe (drop the receiver) ──────────────────────────────
            3 => {
                if !subs.is_empty() {
                    let idx = rng.below(subs.len());
                    // Draining first keeps the assertion honest: a receiver
                    // dropped with messages still queued proves nothing about
                    // whether they were delivered.
                    drain_and_compare(&mut subs[idx], rep, &log);
                    let gone = subs.remove(idx);
                    log.push(format!("-- drop handle {}", gone.handle));
                }
            }
            // ── Publish ──────────────────────────────────────────────────────
            4..=6 => {
                let payload = format!("m{}", rng.next() % 10_000);
                let sql = format!("SELECT PUBSUB_PUBLISH('{channel}', '{payload}')");
                log.push(sql.clone());
                let expected = subs.iter().filter(|s| s.channel == channel).count() as i64;
                match scalar_i64(ex, &sql) {
                    Ok(got) if got != expected => rep.push(
                        "pubsub",
                        "PUBLISH returns the number of live subscribers",
                        format!("channel {channel}: expected {expected}, got {got}"),
                        &log,
                    ),
                    Err(e) => rep.push("pubsub", "PUBLISH succeeds", e, &log),
                    _ => {}
                }
                for s in subs.iter_mut().filter(|s| s.channel == channel) {
                    s.expected.push(payload.clone());
                }
                if neg_armed && op >= ops / 2 {
                    // A message the engine never sent to this receiver. If the
                    // walk has left no subscriber to perturb, make one — the
                    // control must not depend on the shape of the random walk.
                    if subs.is_empty() {
                        let rx = ex.pubsub_sync().write().subscribe(&channel);
                        ever_subscribed.insert(channel.clone());
                        subs.push(Subscriber {
                            handle: next_handle,
                            channel: channel.clone(),
                            rx,
                            expected: Vec::new(),
                        });
                        next_handle += 1;
                    }
                    subs[0].expected.push(format!("{payload}-phantom"));
                    neg_armed = false;
                }
            }
            // ── PUBSUB_SUBSCRIBERS ───────────────────────────────────────────
            7 => {
                let sql = format!("SELECT PUBSUB_SUBSCRIBERS('{channel}')");
                log.push(sql.clone());
                let expected = subs.iter().filter(|s| s.channel == channel).count() as i64;
                match scalar_i64(ex, &sql) {
                    Ok(got) if got != expected => rep.push(
                        "pubsub",
                        "SUBSCRIBERS counts live subscribers on that channel",
                        format!("channel {channel}: expected {expected}, got {got}"),
                        &log,
                    ),
                    Err(e) => rep.push("pubsub", "SUBSCRIBERS succeeds", e, &log),
                    _ => {}
                }
            }
            // ── PUBSUB_CHANNELS ──────────────────────────────────────────────
            8 => {
                let sql = "SELECT PUBSUB_CHANNELS()".to_string();
                log.push(sql.clone());
                match scalar_text(ex, &sql) {
                    Ok(text) => {
                        let listed: Vec<&str> = text.split(',').filter(|s| !s.is_empty()).collect();
                        let mut sorted = listed.clone();
                        sorted.sort_unstable();
                        if listed != sorted {
                            rep.push(
                                "pubsub",
                                "CHANNELS is sorted",
                                format!("got {text:?}"),
                                &log,
                            );
                        }
                        let unique: BTreeSet<&&str> = listed.iter().collect();
                        if unique.len() != listed.len() {
                            rep.push(
                                "pubsub",
                                "CHANNELS lists each channel once",
                                format!("got {text:?}"),
                                &log,
                            );
                        }
                        // A channel with a live subscriber must be listed. The
                        // converse is deliberately not asserted: the contract
                        // says "comma-separated channel names" without saying
                        // whether a channel outlives its last subscriber, and a
                        // probe should not invent the answer.
                        for s in &subs {
                            if !listed.contains(&s.channel.as_str()) {
                                rep.push(
                                    "pubsub",
                                    "CHANNELS lists every channel with a subscriber",
                                    format!("{} missing from {text:?}", s.channel),
                                    &log,
                                );
                                break;
                            }
                        }
                    }
                    Err(e) => rep.push("pubsub", "CHANNELS succeeds", e, &log),
                }
            }
            // ── Drain and compare delivered payloads ─────────────────────────
            _ => {
                if !subs.is_empty() {
                    let idx = rng.below(subs.len());
                    drain_and_compare(&mut subs[idx], rep, &log);
                }
            }
        }
    }

    for s in subs.iter_mut() {
        drain_and_compare(s, rep, &log);
    }
}

/// Drain everything queued for one receiver and compare it, in order, against
/// what the model says was published to its channel while it was subscribed.
fn drain_and_compare(sub: &mut Subscriber, rep: &mut Report, log: &[String]) {
    let mut got: Vec<String> = Vec::new();
    loop {
        match sub.rx.try_recv() {
            Ok(msg) => {
                if msg.channel != sub.channel {
                    rep.push(
                        "pubsub",
                        "a subscriber only receives its own channel",
                        format!(
                            "handle {} on '{}' received a message for '{}'",
                            sub.handle, sub.channel, msg.channel
                        ),
                        log,
                    );
                }
                got.push(msg.payload.clone());
            }
            Err(broadcast::error::TryRecvError::Empty)
            | Err(broadcast::error::TryRecvError::Closed) => break,
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                rep.push(
                    "pubsub",
                    "no subscriber lags inside one iteration",
                    format!("handle {} lagged by {n}", sub.handle),
                    log,
                );
                break;
            }
        }
    }
    if got != sub.expected {
        rep.push(
            "pubsub",
            "a subscriber receives exactly what was published to its channel",
            format!(
                "handle {} on '{}': expected {:?}, got {:?}",
                sub.handle, sub.channel, sub.expected, got
            ),
            log,
        );
    }
    sub.expected.clear();
}

// ═════════════════════════════════════════════════════════════════════════════
// Section 3 — CDC
// ═════════════════════════════════════════════════════════════════════════════

#[derive(Clone, PartialEq, Eq, Debug)]
struct RefChange {
    table: String,
    change: &'static str,
}

/// What the CDC surface returns, parsed: `{"seq":N,"table":"t","change":"C","ts":N}`.
#[derive(PartialEq, Eq, Debug)]
struct CdcRecord {
    seq: u64,
    table: String,
    change: String,
}

fn parse_cdc(text: &str) -> Result<Vec<CdcRecord>, String> {
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let parsed: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("unparseable CDC JSON: {e}"))?;
    let serde_json::Value::Array(items) = parsed else {
        return Err("CDC JSON is not an array".to_string());
    };
    items
        .iter()
        .map(|item| {
            Ok(CdcRecord {
                seq: item
                    .get("seq")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| format!("CDC record has no seq: {item}"))?,
                table: item
                    .get("table")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| format!("CDC record has no table: {item}"))?
                    .to_string(),
                change: item
                    .get("change")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| format!("CDC record has no change: {item}"))?
                    .to_string(),
            })
        })
        .collect()
}

fn section_cdc(ex: &Executor, rng: &mut Rng, ops: usize, neg: bool, rep: &mut Report) -> usize {
    let tables = ["cdc_t0", "cdc_t1"];
    for t in tables {
        if let Err(e) = exec(
            ex,
            &format!("CREATE TABLE {t} (id INTEGER PRIMARY KEY, v INTEGER)"),
        ) {
            rep.push("cdc", "table setup succeeds", e, &[]);
            return 0;
        }
    }

    // The reference table model. Its agreement with the engine is checked after
    // every statement — a CDC divergence is only attributable to CDC once the
    // two agree about what changed.
    let mut model: BTreeMap<&str, BTreeMap<i64, i64>> =
        tables.iter().map(|t| (*t, BTreeMap::new())).collect();
    let mut expected: Vec<RefChange> = Vec::new();
    let mut log: Vec<String> = Vec::new();
    let mut next_id = 1i64;
    let mut neg_armed = neg;

    if let Ok(n) = scalar_i64(ex, "SELECT CDC_COUNT()")
        && n != 0
    {
        rep.push(
            "cdc",
            "a fresh executor has an empty change log",
            format!("CDC_COUNT() = {n}"),
            &[],
        );
    }

    for op in 0..ops {
        let table = tables[rng.below(tables.len())];
        let (sql, change, affected) = match rng.below(10) {
            // ── INSERT ───────────────────────────────────────────────────────
            0..=3 => {
                let id = next_id;
                next_id += 1;
                let v = rng.below(20) as i64;
                model.get_mut(table).unwrap().insert(id, v);
                (
                    format!("INSERT INTO {table} VALUES ({id}, {v})"),
                    "INSERT",
                    1usize,
                )
            }
            // ── UPDATE by primary key (often matches nothing) ────────────────
            4..=5 => {
                let id = 1 + rng.below(next_id.max(2) as usize + 2) as i64;
                let v = rng.below(20) as i64;
                let rows = model.get_mut(table).unwrap();
                let hit = rows.contains_key(&id);
                if hit {
                    rows.insert(id, v);
                }
                (
                    format!("UPDATE {table} SET v = {v} WHERE id = {id}"),
                    "UPDATE",
                    usize::from(hit),
                )
            }
            // ── UPDATE by range ──────────────────────────────────────────────
            6..=7 => {
                let bound = rng.below(20) as i64;
                let rows = model.get_mut(table).unwrap();
                let hits: Vec<i64> = rows
                    .iter()
                    .filter(|(_, v)| **v > bound)
                    .map(|(k, _)| *k)
                    .collect();
                for k in &hits {
                    *rows.get_mut(k).unwrap() += 1;
                }
                (
                    format!("UPDATE {table} SET v = v + 1 WHERE v > {bound}"),
                    "UPDATE",
                    hits.len(),
                )
            }
            // ── DELETE by primary key ────────────────────────────────────────
            8 => {
                let id = 1 + rng.below(next_id.max(2) as usize + 2) as i64;
                let hit = model.get_mut(table).unwrap().remove(&id).is_some();
                (
                    format!("DELETE FROM {table} WHERE id = {id}"),
                    "DELETE",
                    usize::from(hit),
                )
            }
            // ── DELETE by range ──────────────────────────────────────────────
            _ => {
                let bound = rng.below(20) as i64;
                let rows = model.get_mut(table).unwrap();
                let hits: Vec<i64> = rows
                    .iter()
                    .filter(|(_, v)| **v < bound)
                    .map(|(k, _)| *k)
                    .collect();
                for k in &hits {
                    rows.remove(k);
                }
                (
                    format!("DELETE FROM {table} WHERE v < {bound}"),
                    "DELETE",
                    hits.len(),
                )
            }
        };

        log.push(sql.clone());
        match rows_affected(ex, &sql) {
            Ok(got) if got != affected => {
                rep.push(
                    "cdc",
                    "the reference table model agrees with the engine (control)",
                    format!("{sql}: engine affected {got}, model {affected}"),
                    &log,
                );
                // The model is no longer a valid oracle for this iteration.
                return expected.len();
            }
            Err(e) => {
                rep.push("cdc", "DML succeeds", format!("{sql}: {e}"), &log);
                return expected.len();
            }
            _ => {}
        }

        // Control: the engine's own contents must match the model, otherwise a
        // CDC mismatch below would be misattributed.
        let contents = select_pairs(ex, &format!("SELECT id, v FROM {table} ORDER BY id"));
        match contents {
            Ok(got) => {
                let want: Vec<(i64, i64)> = model[table].iter().map(|(k, v)| (*k, *v)).collect();
                if got != want {
                    rep.push(
                        "cdc",
                        "the reference table model agrees with the engine (control)",
                        format!("{table}: expected {want:?}, got {got:?}"),
                        &log,
                    );
                    return expected.len();
                }
            }
            Err(e) => {
                rep.push("cdc", "table read-back succeeds", e, &log);
                return expected.len();
            }
        }

        // Contract: one change record per statement that affected at least one
        // row, and none for a statement that affected none.
        if affected > 0 {
            if neg_armed && op >= ops / 2 {
                // The engine captured the change; the model never records it.
                neg_armed = false;
            } else {
                expected.push(RefChange {
                    table: table.to_string(),
                    change,
                });
            }
        }

        // ── CDC_COUNT ────────────────────────────────────────────────────────
        match scalar_i64(ex, "SELECT CDC_COUNT()") {
            Ok(got) if got != expected.len() as i64 => {
                rep.push(
                    "cdc",
                    "one change record per row-affecting statement",
                    format!(
                        "after `{sql}` (affected {affected}): expected {} records, CDC_COUNT() = {got}",
                        expected.len()
                    ),
                    &log,
                );
                return expected.len();
            }
            Err(e) => {
                rep.push("cdc", "CDC_COUNT succeeds", e, &log);
                return expected.len();
            }
            _ => {}
        }
    }

    // ── Full log, in order ───────────────────────────────────────────────────
    let all = match scalar_text(ex, "SELECT CDC_READ(0, 100000)").and_then(|t| parse_cdc(&t)) {
        Ok(v) => v,
        Err(e) => {
            rep.push("cdc", "CDC_READ succeeds", e, &log);
            return expected.len();
        }
    };
    if all.len() != expected.len()
        || all
            .iter()
            .zip(&expected)
            .any(|(got, want)| got.table != want.table || got.change != want.change)
    {
        let got_desc: Vec<String> = all
            .iter()
            .map(|r| format!("{}:{}", r.table, r.change))
            .collect();
        let want_desc: Vec<String> = expected
            .iter()
            .map(|r| format!("{}:{}", r.table, r.change))
            .collect();
        rep.push(
            "cdc",
            "the change log is exactly the sequence of changes made",
            format!("expected {want_desc:?}, got {got_desc:?}"),
            &log,
        );
    }

    // Sequences must be strictly increasing — a consumer's cursor depends on it.
    for w in all.windows(2) {
        if w[1].seq <= w[0].seq {
            rep.push(
                "cdc",
                "change sequences strictly increase",
                format!("{} follows {}", w[1].seq, w[0].seq),
                &log,
            );
            break;
        }
    }

    // ── CDC_READ(after, limit) boundaries ────────────────────────────────────
    if !all.is_empty() {
        for _ in 0..4 {
            let idx = rng.below(all.len());
            let after = all[idx].seq;
            let limit = 1 + rng.below(all.len());
            let sql = format!("SELECT CDC_READ({after}, {limit})");
            let want: Vec<&CdcRecord> = all.iter().filter(|r| r.seq > after).take(limit).collect();
            match scalar_text(ex, &sql).and_then(|t| parse_cdc(&t)) {
                Ok(got) => {
                    if got.len() != want.len() || got.iter().zip(&want).any(|(g, w)| g.seq != w.seq)
                    {
                        rep.push(
                            "cdc",
                            "CDC_READ(after, limit) returns the records after that sequence",
                            format!(
                                "{sql}: expected seqs {:?}, got {:?}",
                                want.iter().map(|r| r.seq).collect::<Vec<_>>(),
                                got.iter().map(|r| r.seq).collect::<Vec<_>>()
                            ),
                            std::slice::from_ref(&sql),
                        );
                        break;
                    }
                }
                Err(e) => {
                    rep.push("cdc", "CDC_READ succeeds", e, &[sql]);
                    break;
                }
            }
        }
    }

    // ── CDC_TABLE_READ filters by table ──────────────────────────────────────
    for t in tables {
        let sql = format!("SELECT CDC_TABLE_READ('{t}', 0, 100000)");
        let want: Vec<&CdcRecord> = all.iter().filter(|r| r.table == t).collect();
        match scalar_text(ex, &sql).and_then(|s| parse_cdc(&s)) {
            Ok(got) => {
                if got.len() != want.len() || got.iter().zip(&want).any(|(g, w)| g.seq != w.seq) {
                    rep.push(
                        "cdc",
                        "CDC_TABLE_READ returns only that table's records",
                        format!(
                            "{t}: expected seqs {:?}, got {:?}",
                            want.iter().map(|r| r.seq).collect::<Vec<_>>(),
                            got.iter().map(|r| r.seq).collect::<Vec<_>>()
                        ),
                        std::slice::from_ref(&sql),
                    );
                }
            }
            Err(e) => rep.push("cdc", "CDC_TABLE_READ succeeds", e, &[sql]),
        }
    }

    expected.len()
}

// ═════════════════════════════════════════════════════════════════════════════
// Main
// ═════════════════════════════════════════════════════════════════════════════

struct Db {
    ex: Arc<Executor>,
    _db: Option<HarnessDb>,
    dir: Option<std::path::PathBuf>,
}

fn open_db(kind: Option<EngineKind>) -> Option<Db> {
    let Some(kind) = kind else {
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        return Some(Db {
            ex: Arc::new(Executor::new(catalog, storage)),
            _db: None,
            dir: None,
        });
    };
    let dir = std::env::temp_dir().join(format!(
        "nucleus-streams-oracle-{}-{}",
        std::process::id(),
        DIR_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let rt = tokio::runtime::Handle::current();
    let db = tokio::task::block_in_place(|| {
        rt.block_on(HarnessDb::open(kind, &dir, EngineConfig::default()))
    })
    .ok()?;
    let ex = db.executor().clone();
    Some(Db {
        ex,
        _db: Some(db),
        dir: Some(dir),
    })
}

/// One full pass. `negative` names the section whose model is perturbed, or
/// `None` for the honest run.
fn run(
    seed: u64,
    iterations: usize,
    ops_per: usize,
    engine: Option<EngineKind>,
    negative: Option<&str>,
    max_report: usize,
) -> Report {
    let mut rep = Report::new(max_report);

    for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3) | 1);
        let Some(db) = open_db(engine) else {
            eprintln!("failed to open the engine under test");
            std::process::exit(2);
        };
        let ex = db.ex.clone();

        let cdc_records = section_cdc(&ex, &mut rng, ops_per, negative == Some("cdc"), &mut rep);
        section_streams(
            &ex,
            &mut rng,
            ops_per,
            negative == Some("streams"),
            &mut rep,
        );
        section_pubsub(&ex, &mut rng, ops_per, negative == Some("pubsub"), &mut rep);

        // Cross-model: the change log captures TABLE changes. Stream appends and
        // published messages are not table changes, and a change log that grows
        // when no table changed is a log a consumer cannot trust.
        if let Ok(after) = scalar_i64(&ex, "SELECT CDC_COUNT()")
            && after != cdc_records as i64
        {
            rep.push(
                "cdc",
                "stream and pubsub activity writes no table change records",
                format!(
                    "CDC_COUNT() was {cdc_records} after the CDC section and {after} after \
                     the stream and pubsub sections"
                ),
                &[],
            );
        }

        drop(ex);
        drop(db._db);
        if let Some(dir) = db.dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    rep
}

fn main_impl() {
    let mut seed: u64 = 0x005E_ED57_EA45;
    let mut iterations = 200usize;
    let mut ops_per = 40usize;
    let mut max_report = 12usize;
    let mut engine: Option<EngineKind> = None;
    let mut negative: Option<String> = None;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args[i].parse().expect("--seed takes a number");
            }
            "--iterations" => {
                i += 1;
                iterations = args[i].parse().expect("--iterations takes a number");
            }
            "--ops" => {
                i += 1;
                ops_per = args[i].parse().expect("--ops takes a number");
            }
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().expect("--max-report takes a number");
            }
            "--engine" => {
                i += 1;
                match EngineKind::parse(&args[i]) {
                    Some(k) => engine = Some(k),
                    None => {
                        let names: Vec<&str> = EngineKind::ALL.iter().map(|k| k.name()).collect();
                        eprintln!("unknown --engine {:?}; expected one of {names:?}", args[i]);
                        std::process::exit(2);
                    }
                }
            }
            "--negative-control" => {
                i += 1;
                let section = args[i].clone();
                if !["streams", "pubsub", "cdc"].contains(&section.as_str()) {
                    eprintln!(
                        "--negative-control takes one of: streams, pubsub, cdc (got {section:?})"
                    );
                    std::process::exit(2);
                }
                negative = Some(section);
            }
            other => {
                eprintln!("unknown argument {other:?}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus Streams / PubSub / CDC differential fuzzer (reference oracles)");
    println!(
        "seed={seed} iterations={iterations} ops/section={ops_per} engine={}",
        engine.map(|k| k.name()).unwrap_or("mvcc (default)")
    );
    if let Some(section) = &negative {
        println!(
            "NEGATIVE CONTROL: the {section} model is deliberately wrong; that section MUST report"
        );
        // One iteration is enough to prove a check fires, and keeping it at one
        // makes "the other sections stay clean" a statement about this run
        // rather than an average.
        iterations = 1;
    }
    println!();

    let baseline = negative
        .as_deref()
        .map(|_| run(seed, iterations, ops_per, engine, None, max_report));
    let rep = run(
        seed,
        iterations,
        ops_per,
        engine,
        negative.as_deref(),
        max_report,
    );

    println!("════ SUMMARY ════");
    println!("iterations         : {iterations}");
    for section in ["streams", "pubsub", "cdc"] {
        match &baseline {
            Some(b) => println!(
                "{section:<19}: {} divergence(s)  (clean baseline: {})",
                rep.count(section),
                b.count(section)
            ),
            None => println!("{section:<19}: {} divergence(s)", rep.count(section)),
        }
    }

    for (idx, d) in rep.found.iter().enumerate() {
        println!(
            "\n─── DIVERGENCE #{} ─── [{} / {}]",
            idx + 1,
            d.section,
            d.check
        );
        println!("  detail : {}", d.detail);
        if !d.repro.is_empty() {
            println!(
                "  repro  (last {} of {} steps):",
                d.repro.len().min(20),
                d.repro.len()
            );
            let start = d.repro.len().saturating_sub(20);
            for step in &d.repro[start..] {
                println!("    {step}");
            }
        }
    }

    if let (Some(section), Some(base)) = (negative, baseline) {
        println!();
        let gained = rep.count(&section) as i64 - base.count(&section) as i64;
        let spilled: i64 = ["streams", "pubsub", "cdc"]
            .iter()
            .filter(|s| **s != section)
            .map(|s| rep.count(s) as i64 - base.count(s) as i64)
            .sum();
        if gained > 0 && spilled == 0 {
            println!(
                "NEGATIVE CONTROL PASSED: perturbing the {section} model added {gained} \
                 divergence(s) to {section} and none to the other sections."
            );
            std::process::exit(0);
        }
        println!(
            "NEGATIVE CONTROL FAILED: perturbing the {section} model changed {section} by \
             {gained} and the other sections by {spilled}. A check that cannot fail is not a \
             check, and a check that fires for something else is worse."
        );
        std::process::exit(1);
    }

    if rep.total() == 0 {
        println!(
            "\nStreams, PubSub and CDC agree with their reference implementations across \
             {iterations} iterations."
        );
        std::process::exit(0);
    }
    std::process::exit(1);
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
