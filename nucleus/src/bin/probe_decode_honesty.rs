//! Tuple-decode honesty probe (NU-239 class).
//!
//! The class is **a corrupt persisted value decoded into a plausible one**.
//! NU-239's instance was JSONB: malformed stored bytes came back as a valid
//! SQL `null`, so a query answered successfully with a value nobody ever
//! wrote. The fix reported corruption instead — but a fix for one type is not
//! a property, and the finding's own second half is the reason this is a probe
//! and not another `#[test]`: **the two read paths disagreed**, so which
//! answer you got depended on whether the planner projected that column.
//!
//! Neither half is JSONB-specific, so neither check is.
//!
//! **Section `canonical`.** The oracle is `encode ∘ decode = identity`. For a
//! corpus spanning every `DataType`, mutate the encoding byte by byte and
//! decode. A decoder is allowed exactly two answers: report (`None`), or
//! return a row whose *canonical re-encoding is the bytes it just read*.
//! Anything else means it accepted an input it cannot reproduce — it discarded
//! or invented information, which is the class. This needs no per-type
//! knowledge of what "corrupt" means, so it covers types nobody has thought
//! about yet, including ones added later. Two answers are deliberately NOT
//! divergences: a mutation that lands on a fixed-width integer produces a
//! different but perfectly valid integer (no redundancy exists to catch it,
//! and pretending otherwise would be a checksum, not a decoder), and trailing
//! bytes past a complete row are counted separately as leniency rather than
//! invention.
//!
//! **Section `agreement`.** `deserialize_row` and `deserialize_row_projected`
//! must reach the same verdict about the same column. The projected path
//! deliberately *sizes* columns it was not asked for without validating them —
//! that is the whole point of projection — so a blind byte sweep would flag
//! that legitimate difference as a bug. Instead every mutation here is scoped
//! to one column's payload, leaving all framing intact, so both paths are
//! obliged to read exactly the same suspect bytes and any disagreement is
//! real: `full == None` demands `projected == None`, and `full == Some(row)`
//! demands `projected == Some([row[j]])`.
//!
//! `--negative-control <canonical|agreement>` runs both sections twice at one
//! seed — clean, then with that section's decoder wrapped in an adapter that
//! reintroduces the bug's shape (a reported corruption answered as a
//! plausible value instead). It passes only if the perturbation adds
//! divergences to that section and none to the other. A check nobody has
//! watched fail is not a check.
//!
//! Build: `cargo run --release --features server --bin probe_decode_honesty`
//!        `... --bin probe_decode_honesty -- --negative-control canonical`
//!        `... --bin probe_decode_honesty -- --negative-control agreement`
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal probe harness

use std::collections::BTreeMap;

use nucleus::storage::tuple::{deserialize_row, deserialize_row_projected, serialize_row};
use nucleus::types::{DataType, Row, Value};

// ─── Divergence bookkeeping ──────────────────────────────────────────────────

#[derive(Default)]
struct Sections {
    counts: BTreeMap<&'static str, usize>,
    findings: Vec<(&'static str, String)>,
    /// Non-divergent observations, printed so a green run reports what it saw
    /// rather than only that it saw nothing.
    stats: BTreeMap<&'static str, usize>,
    /// Divergences grouped by section and corpus row, so a section that fires
    /// hundreds of times is triaged from the summary instead of from a capped
    /// finding list.
    cats: BTreeMap<String, usize>,
    shown: BTreeMap<String, usize>,
}

impl Sections {
    fn push(&mut self, section: &'static str, detail: String) {
        self.push_in(section, section, detail)
    }
    /// Capped PER CATEGORY, not per run: one row of the corpus firing hundreds
    /// of times must not bury the single line another row printed once. The
    /// first draft capped globally and hid every finding except `bool`.
    fn push_in(&mut self, section: &'static str, cat: &str, detail: String) {
        *self.counts.entry(section).or_insert(0) += 1;
        let shown = self.shown.entry(cat.to_string()).or_insert(0);
        if *shown < 3 {
            *shown += 1;
            self.findings.push((section, detail));
        }
    }
    fn bump(&mut self, key: &'static str, by: usize) {
        *self.stats.entry(key).or_insert(0) += by;
    }
    fn cat(&mut self, section: &str, name: &str) {
        *self.cats.entry(format!("{section}/{name}")).or_insert(0) += 1;
    }
    fn count(&self, section: &str) -> usize {
        self.counts.get(section).copied().unwrap_or(0)
    }
    fn total(&self) -> usize {
        self.counts.values().sum()
    }
}

const SECTIONS: [&str; 2] = ["canonical", "agreement"];

// ─── Corpus ──────────────────────────────────────────────────────────────────

/// Every schema is <= 8 columns so the null bitmap is exactly one byte and the
/// offset arithmetic below stays inspectable by hand.
fn corpus() -> Vec<(&'static str, Vec<DataType>, Row)> {
    vec![
        (
            "scalars",
            vec![
                DataType::Bool,
                DataType::Int32,
                DataType::Int64,
                DataType::Float64,
            ],
            vec![
                Value::Bool(true),
                Value::Int32(-7),
                Value::Int64(i64::MIN + 3),
                Value::Float64(1.5),
            ],
        ),
        (
            "bool_false",
            vec![DataType::Bool, DataType::Bool],
            vec![Value::Bool(false), Value::Bool(true)],
        ),
        (
            // The NU-239 shape itself: an integer key beside a JSON document.
            "jsonb",
            vec![DataType::Int32, DataType::Jsonb],
            vec![
                Value::Int32(1),
                Value::Jsonb(serde_json::json!({"a": [1, 2, null], "b": "x"})),
            ],
        ),
        (
            // A genuine JSON null must round-trip: a "fix" that treated the
            // null document as corruption would pass every corruption check
            // and break every legitimate null in the database.
            "jsonb_null_doc",
            vec![DataType::Jsonb, DataType::Jsonb],
            vec![
                Value::Jsonb(serde_json::Value::Null),
                Value::Jsonb(serde_json::json!(0)),
            ],
        ),
        (
            "text",
            vec![DataType::Text, DataType::Numeric, DataType::Text],
            vec![
                Value::Text("hello".into()),
                Value::Numeric("-12.3400".into()),
                Value::Text(String::new()),
            ],
        ),
        (
            "temporal",
            vec![
                DataType::Date,
                DataType::Timestamp,
                DataType::TimestampTz,
                DataType::Interval,
            ],
            vec![
                Value::Date(9000),
                Value::Timestamp(1_234_567_890),
                Value::TimestampTz(-42),
                Value::Interval {
                    months: 3,
                    days: -1,
                    microseconds: 86_400_000_000,
                },
            ],
        ),
        (
            "binary",
            vec![DataType::Uuid, DataType::Bytea],
            vec![
                Value::Uuid([
                    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc,
                    0xdd, 0xee, 0xff,
                ]),
                Value::Bytea(vec![0, 1, 2, 250, 255]),
            ],
        ),
        (
            "array",
            vec![DataType::Array(Box::new(DataType::Text)), DataType::Int32],
            vec![
                Value::Array(vec![
                    Value::Text("a".into()),
                    Value::Int32(2),
                    Value::Null,
                    Value::Bool(true),
                    Value::Float64(0.25),
                    Value::Int64(-5),
                ]),
                Value::Int32(77),
            ],
        ),
        (
            "vector",
            vec![DataType::Vector(3), DataType::Text],
            vec![Value::Vector(vec![1.0, -0.5, 0.0]), Value::Text("v".into())],
        ),
        (
            "nulls",
            vec![
                DataType::Int32,
                DataType::Text,
                DataType::Jsonb,
                DataType::Bool,
            ],
            vec![
                Value::Null,
                Value::Text("after a null".into()),
                Value::Null,
                Value::Bool(false),
            ],
        ),
    ]
}

/// Types whose payload opens with a 4-byte length/count prefix. Mutating that
/// prefix reframes every following column, which is a different experiment
/// from mutating the value itself.
fn has_len_prefix(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Text
            | DataType::Jsonb
            | DataType::Numeric
            | DataType::Bytea
            | DataType::Array(_)
            | DataType::Vector(_)
    )
}

/// Per-column payload extents of a clean encoding, derived by encoding each
/// column on its own. Returns `None` if the model does not sum to the whole
/// row — an offset model that has drifted would scope every mutation to the
/// wrong column and then report nothing, forever, while looking healthy.
fn column_ranges(types: &[DataType], row: &Row, clean: &[u8]) -> Option<Vec<(usize, usize)>> {
    let mut ranges = Vec::with_capacity(types.len());
    let mut cursor = types.len().div_ceil(8);
    for (i, t) in types.iter().enumerate() {
        let solo = serialize_row(&vec![row[i].clone()], std::slice::from_ref(t));
        let len = solo.len() - 1; // minus that row's own bitmap byte
        ranges.push((cursor, cursor + len));
        cursor += len;
    }
    (cursor == clean.len()).then_some(ranges)
}

// ─── The canonical-form oracle ───────────────────────────────────────────────

/// Bits in the null bitmap past the last column are padding the encoder always
/// writes as zero and the decoder always ignores. Mask them on both sides so a
/// mutation that lands there is not mistaken for a decode that invented a
/// value.
fn mask_padding(bytes: &mut [u8], ncols: usize) {
    let bitmap_bytes = ncols.div_ceil(8);
    if bitmap_bytes == 0 || bytes.len() < bitmap_bytes {
        return;
    }
    let used_in_last = ncols - (bitmap_bytes - 1) * 8;
    if used_in_last < 8 {
        bytes[bitmap_bytes - 1] &= (1u8 << used_in_last) - 1;
    }
}

enum Verdict {
    /// The decoder refused. Always acceptable.
    Reported,
    /// The decoder returned a row that re-encodes to exactly these bytes.
    Faithful,
    /// A complete row was decoded and bytes were left over.
    TrailingIgnored(usize),
    /// The decoder returned a value whose canonical encoding contradicts the
    /// bytes it read. This is the class.
    Invented(Vec<u8>),
}

fn judge(input: &[u8], types: &[DataType], decoded: Option<Row>) -> Verdict {
    let Some(row) = decoded else {
        return Verdict::Reported;
    };
    let mut re = serialize_row(&row, types);
    let mut inp = input.to_vec();
    mask_padding(&mut re, types.len());
    mask_padding(&mut inp, types.len());
    if re == inp {
        Verdict::Faithful
    } else if inp.len() > re.len() && inp.starts_with(&re) {
        Verdict::TrailingIgnored(inp.len() - re.len())
    } else {
        Verdict::Invented(re)
    }
}

fn hex(bytes: &[u8]) -> String {
    let shown: Vec<String> = bytes.iter().take(48).map(|b| format!("{b:02x}")).collect();
    let mut s = shown.join("");
    if bytes.len() > 48 {
        s.push_str("…");
    }
    s
}

// ─── Decoder adapters (the perturbation lives here) ──────────────────────────

/// The full-row read path, optionally wrapped in the NU-239 shape: where the
/// engine reports corruption, answer with a plausible value instead. All-NULL
/// is the honest model of the class — "corrupt persisted state read back as
/// empty" is the same defect JSONB expressed as `Jsonb(Null)`.
fn decode_full(data: &[u8], types: &[DataType], perturb: bool) -> Option<Row> {
    match deserialize_row(data, types) {
        Some(row) => Some(row),
        None if perturb => Some(vec![Value::Null; types.len()]),
        None => None,
    }
}

/// The projected read path, with the same optional wrapper. Perturbing only
/// this side is exactly how NU-239 was found: one path fixed, one not.
fn decode_projected(
    data: &[u8],
    types: &[DataType],
    projection: &[usize],
    perturb: bool,
) -> Option<Row> {
    match deserialize_row_projected(data, types, projection) {
        Some(row) => Some(row),
        None if perturb => Some(vec![Value::Null; projection.len()]),
        None => None,
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Section 1 — a decoder never returns bytes it cannot reproduce
// ═════════════════════════════════════════════════════════════════════════════

fn section_canonical(perturb: bool, iterations: usize, seed: u64, sec: &mut Sections) {
    let mut rng = Rng::new(seed ^ 0x9E37_79B9_7F4A_7C15);
    let mut trailing = 0usize;
    let mut examined = 0usize;
    let mut jsonb_examined = 0usize;

    for (name, types, row) in corpus() {
        // JSONB's stored form is JSON text, and JSON has many byte
        // representations of the same value — map ordering and whitespace at
        // least. `encode ∘ decode = identity` is therefore the WRONG oracle
        // for it: reordering two keys makes the re-encoding differ from the
        // input while nothing whatsoever was invented, which is exactly the
        // false positive the first run of this probe produced. JSONB gets its
        // own arm below, against serde_json as the reference.
        if types.iter().any(|t| matches!(t, DataType::Jsonb)) {
            continue;
        }
        let clean = serialize_row(&row, &types);

        // Instrument self-check: the unmutated encoding must round-trip and be
        // judged Faithful. If this fails the oracle is broken and every green
        // result below is meaningless.
        match judge(&clean, &types, deserialize_row(&clean, &types)) {
            Verdict::Faithful => {}
            _ => {
                sec.push(
                    "canonical",
                    format!(
                        "[{name}] the UNMUTATED encoding is not judged faithful — the oracle \
                         itself is wrong, so nothing else this section reports can be trusted"
                    ),
                );
                continue;
            }
        }
        if deserialize_row(&clean, &types).as_ref() != Some(&row) {
            // Coercion (Int64 literal into an INT column) can legitimately
            // change the value; the corpus avoids it, so this is a real
            // round-trip failure.
            sec.push(
                "canonical",
                format!("[{name}] clean row did not round-trip through serialize/deserialize"),
            );
        }

        let mut cases: Vec<Vec<u8>> = Vec::new();

        // Byte substitutions across the whole encoding, including framing.
        for off in 0..clean.len() {
            for sub in [0x00u8, 0xFF, clean[off] ^ 0x01, 0x7F] {
                if sub == clean[off] {
                    continue;
                }
                let mut m = clean.clone();
                m[off] = sub;
                cases.push(m);
            }
        }
        // Truncation at every boundary: a short read must be reported, never
        // completed from thin air.
        for k in 0..clean.len() {
            cases.push(clean[..k].to_vec());
        }
        // Trailing garbage.
        for extra in [1usize, 4, 16] {
            let mut m = clean.clone();
            m.extend(std::iter::repeat(0xAAu8).take(extra));
            cases.push(m);
        }
        // Randomized multi-byte damage, so the sweep is not the only shape
        // tried.
        for _ in 0..iterations {
            let mut m = clean.clone();
            let hits = 1 + (rng.next() % 3) as usize;
            for _ in 0..hits {
                if m.is_empty() {
                    break;
                }
                let off = (rng.next() % m.len() as u64) as usize;
                m[off] = (rng.next() % 256) as u8;
            }
            cases.push(m);
        }

        for m in cases {
            examined += 1;
            match judge(&m, &types, decode_full(&m, &types, perturb)) {
                Verdict::Reported | Verdict::Faithful => {}
                Verdict::TrailingIgnored(n) => {
                    trailing += 1;
                    let _ = n;
                }
                Verdict::Invented(re) => {
                    sec.cat("canonical", name);
                    sec.push_in(
                        "canonical",
                        name,
                        format!(
                            "[{name}] decode accepted bytes it cannot reproduce — read \
                             {} but re-encodes to {}; the value returned was never stored \
                             (NU-239 class)",
                            hex(&m),
                            hex(&re)
                        ),
                    );
                }
            }
        }
    }

    // ── JSONB, against serde_json as the reference oracle ────────────────
    //
    // This is NU-239's invariant stated exactly: bytes that are not valid JSON
    // must be REPORTED, and bytes that are must decode to precisely what
    // serde_json makes of them — never to a plausible substitute. Mutations
    // stay inside the JSON payload so the row's framing is untouched and the
    // decoder has no excuse but the value itself.
    for (name, types, row) in corpus() {
        if !types.iter().any(|t| matches!(t, DataType::Jsonb)) {
            continue;
        }
        let clean = serialize_row(&row, &types);
        let Some(ranges) = column_ranges(&types, &row, &clean) else {
            sec.push(
                "canonical",
                format!("[{name}] column offset model does not sum to the encoded row"),
            );
            continue;
        };
        for (j, t) in types.iter().enumerate() {
            if !matches!(t, DataType::Jsonb) {
                continue;
            }
            let (start, end) = ranges[j];
            let payload = start + 4; // past the u32 length prefix
            if payload >= end {
                continue;
            }
            let mut cases: Vec<Vec<u8>> = Vec::new();
            for off in payload..end {
                for sub in [0x00u8, 0xFF, clean[off] ^ 0x01, b'{', b'"'] {
                    if sub == clean[off] {
                        continue;
                    }
                    let mut m = clean.clone();
                    m[off] = sub;
                    cases.push(m);
                }
            }
            for _ in 0..iterations {
                let mut m = clean.clone();
                let span = end - payload;
                for _ in 0..1 + (rng.next() % 3) as usize {
                    let off = payload + (rng.next() % span as u64) as usize;
                    m[off] = (rng.next() % 256) as u8;
                }
                cases.push(m);
            }

            for m in cases {
                examined += 1;
                jsonb_examined += 1;
                let reference = serde_json::from_slice::<serde_json::Value>(&m[payload..end]);
                let decoded = decode_full(&m, &types, perturb);
                match (reference, decoded.as_ref().map(|r| &r[j])) {
                    (Err(_), None) => {} // reported. correct.
                    (Ok(ref want), Some(Value::Jsonb(got))) if want == got => {} // faithful
                    (Err(e), Some(got)) => {
                        sec.cat("canonical", &format!("{name}.jsonb"));
                        sec.push_in(
                            "canonical",
                            &format!("{name}.jsonb"),
                            format!(
                                "[{name}] column {j} holds bytes serde_json refuses ({e}) and                                  the decoder answered {got:?} — a query would return a value                                  nobody stored (NU-239). bytes {}",
                                hex(&m[payload..end])
                            ),
                        );
                    }
                    (Ok(want), got) => {
                        sec.cat("canonical", &format!("{name}.jsonb"));
                        sec.push_in(
                            "canonical",
                            &format!("{name}.jsonb"),
                            format!(
                                "[{name}] column {j} holds VALID JSON {want} but the decoder                                  answered {got:?}"
                            ),
                        );
                    }
                }
            }
        }
    }

    sec.bump("canonical.inputs", examined);
    sec.bump("canonical.jsonb_inputs", jsonb_examined);
    sec.bump("canonical.trailing_ignored", trailing);

    if perturb && sec.count("canonical") == 0 {
        sec.push(
            "canonical",
            "no invented value was detected while the decoder was deliberately answering \
             reported corruption with an all-NULL row — the oracle cannot see the defect \
             it exists to see"
                .to_string(),
        );
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Section 2 — the two read paths agree about the same column
// ═════════════════════════════════════════════════════════════════════════════

fn section_agreement(perturb: bool, iterations: usize, seed: u64, sec: &mut Sections) {
    let mut rng = Rng::new(seed ^ 0x2545_F491_4F6C_DD1D);
    let mut examined = 0usize;
    let mut disagreements_possible = 0usize;

    for (name, types, row) in corpus() {
        let clean = serialize_row(&row, &types);
        let bitmap_bytes = types.len().div_ceil(8);

        // Per-column payload extents, derived by encoding each column alone.
        // Cross-checked against the whole-row encoding below, because an
        // offset model that has drifted would silently scope every mutation to
        // the wrong column and report nothing forever.
        let Some(ranges) = column_ranges(&types, &row, &clean) else {
            sec.push(
                "agreement",
                format!(
                    "[{name}] the column offset model does not sum to the {} bytes the row \
                     encodes to — every mutation below would be scoped to the wrong column",
                    clean.len()
                ),
            );
            continue;
        };
        let _ = bitmap_bytes;

        // Clean-row agreement first: with no corruption at all, every
        // projection must reproduce the full path exactly.
        let full_clean = deserialize_row(&clean, &types);
        for j in 0..types.len() {
            let proj = deserialize_row_projected(&clean, &types, &[j]);
            let expected = full_clean.as_ref().map(|r| vec![r[j].clone()]);
            if proj != expected {
                sec.push(
                    "agreement",
                    format!(
                        "[{name}] on an UNCORRUPTED row, projecting column {j} gave {proj:?} \
                         while the full path gave {expected:?}"
                    ),
                );
            }
        }

        for (j, t) in types.iter().enumerate() {
            let (start, end) = ranges[j];
            // Skip the 4-byte length/count prefix: changing it reframes the
            // columns after this one, so the two paths would be reading
            // different bytes and a disagreement would prove nothing.
            let payload_start = if has_len_prefix(t) { start + 4 } else { start };
            if payload_start >= end {
                continue; // NULL, or a zero-length payload — nothing to damage
            }
            disagreements_possible += 1;

            let mut cases: Vec<Vec<u8>> = Vec::new();
            for off in payload_start..end {
                for sub in [0x00u8, 0xFF, clean[off] ^ 0x01, 0x7F] {
                    if sub == clean[off] {
                        continue;
                    }
                    let mut m = clean.clone();
                    m[off] = sub;
                    cases.push(m);
                }
            }
            for _ in 0..iterations {
                let mut m = clean.clone();
                let span = end - payload_start;
                let hits = 1 + (rng.next() % 3) as usize;
                for _ in 0..hits {
                    let off = payload_start + (rng.next() % span as u64) as usize;
                    m[off] = (rng.next() % 256) as u8;
                }
                cases.push(m);
            }

            for m in cases {
                examined += 1;
                let full = deserialize_row(&m, &types);
                let proj = decode_projected(&m, &types, &[j], perturb);
                let expected = full.as_ref().map(|r| vec![r[j].clone()]);
                // Compare canonical ENCODINGS, not `Value`s: `Float64(NaN) !=
                // Float64(NaN)` under `PartialEq`, and a decoded NaN is a
                // perfectly ordinary outcome of damaging eight bytes of a
                // double. Three of this section's first divergences were that
                // and nothing else.
                let one = std::slice::from_ref(t);
                let enc = |v: &Option<Row>| v.as_ref().map(|r| serialize_row(r, one));
                if enc(&proj) != enc(&expected) {
                    let (fv, pv) = (
                        match &expected {
                            Some(v) => format!("{:?}", v[0]),
                            None => "<corruption reported>".to_string(),
                        },
                        match &proj {
                            Some(v) => format!("{:?}", v[0]),
                            None => "<corruption reported>".to_string(),
                        },
                    );
                    let cat = format!("{name}.col{j}");
                    sec.cat("agreement", &cat);
                    sec.push_in(
                        "agreement",
                        &cat,
                        format!(
                            "[{name}] column {j} ({t}) damaged in its own payload: the full \
                             read path answers {fv} and the projected path answers {pv} — the \
                             answer depends on whether the planner projected the column \
                             (NU-239 class). bytes {}",
                            hex(&m)
                        ),
                    );
                }
            }
        }
    }

    sec.bump("agreement.inputs", examined);
    sec.bump("agreement.columns_damaged", disagreements_possible);

    if perturb && sec.count("agreement") == 0 {
        sec.push(
            "agreement",
            "the two read paths never disagreed while the projected path was deliberately \
             answering reported corruption with a NULL — the check cannot see the defect it \
             exists to see"
                .to_string(),
        );
    }
}

// ─── Deterministic RNG ───────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

// ─── Driver ──────────────────────────────────────────────────────────────────

fn run_sections(perturb: Option<&str>, iterations: usize, seed: u64) -> Sections {
    let mut sec = Sections::default();
    let r1 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        section_canonical(perturb == Some("canonical"), iterations, seed, &mut sec);
    }));
    if r1.is_err() {
        sec.push("canonical", "PANIC during section".to_string());
    }
    let r2 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        section_agreement(perturb == Some("agreement"), iterations, seed, &mut sec);
    }));
    if r2.is_err() {
        sec.push("agreement", "PANIC during section".to_string());
    }
    sec
}

fn main() {
    let mut negative: Option<String> = None;
    let mut iterations = 200usize;
    let mut seed = 1u64;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--negative-control" => {
                i += 1;
                let section = args.get(i).cloned().unwrap_or_default();
                if !SECTIONS.contains(&section.as_str()) {
                    eprintln!(
                        "--negative-control takes one of: {} (got {section:?})",
                        SECTIONS.join(", ")
                    );
                    std::process::exit(2);
                }
                negative = Some(section);
            }
            "--iterations" => {
                i += 1;
                iterations = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(200);
            }
            "--seed" => {
                i += 1;
                seed = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(1);
            }
            other => {
                eprintln!("unknown argument {other:?}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {}));

    if let Some(section) = &negative {
        println!(
            "NEGATIVE CONTROL: the {section} decoder is deliberately lenient; that section MUST report"
        );
        let base = run_sections(None, iterations, seed);
        let pert = run_sections(Some(section.as_str()), iterations, seed);
        println!("\n════ SUMMARY (control) ════");
        if !pert.cats.is_empty() {
            println!("  perturbed run fired in:");
            for (k, v) in &pert.cats {
                println!("    {k:<26} {v}");
            }
        }
        for s in SECTIONS {
            println!(
                "{s:<10}: {} divergence(s)  (clean baseline: {})",
                pert.count(s),
                base.count(s)
            );
        }
        let gained = pert.count(section) as i64 - base.count(section) as i64;
        let spilled: i64 = SECTIONS
            .iter()
            .filter(|s| **s != section.as_str())
            .map(|s| pert.count(s) as i64 - base.count(s) as i64)
            .sum();
        if gained > 0 && spilled == 0 {
            println!(
                "\nNEGATIVE CONTROL PASSED: making the {section} decoder lenient added {gained} \
                 divergence(s) to {section} and none to the other section."
            );
            std::process::exit(0);
        }
        println!(
            "\nNEGATIVE CONTROL FAILED: making the {section} decoder lenient changed {section} \
             by {gained} and the other section by {spilled}. A check that cannot fail is not a \
             check, and a check that fires for something else is worse."
        );
        std::process::exit(1);
    }

    println!("Nucleus tuple-decode honesty probe (NU-239 class), seed {seed}");
    let sec = run_sections(None, iterations, seed);
    for (section, detail) in &sec.findings {
        println!("─── [{section}] {detail}");
    }
    println!("\n════ SUMMARY ════");
    for (k, v) in &sec.stats {
        println!("  {k:<28} {v}");
    }
    if !sec.cats.is_empty() {
        println!("  divergences by row:");
        for (k, v) in &sec.cats {
            println!("    {k:<26} {v}");
        }
    }
    for s in SECTIONS {
        println!("{s:<10}: {} divergence(s)", sec.count(s));
    }
    if sec.total() == 0 {
        println!(
            "\nNo decode returned a value it could not reproduce, and the full and projected \
             read paths agreed on every damaged column."
        );
    } else {
        std::process::exit(1);
    }
}
