//! Cross-engine full-text benchmark — Nucleus inverted index vs Tantivy,
//! apples to apples.
//!
//! `bench_paired` deliberately measures Nucleus against an inline reference
//! and says so: its numbers are Nucleus-only and must not be published as a
//! cross-system win. `compete_vector` set the methodology this repo holds
//! cross-engine comparisons to; this binary applies it to full-text search.
//! Tantivy is a Rust library, so it runs in-process — no server, no JVM, no
//! container — which keeps the harness re-runnable on any machine.
//!
//! What "apples to apples" means here, concretely:
//!
//! - **One corpus, one query set.** Both engines index bit-identical text from
//!   the same seeded generator and answer the same queries in the same order.
//! - **One ground truth.** Rankings for both engines are measured against a
//!   single exact BM25 computed once, in this process, from the corpus
//!   statistics the generator itself accumulated. Neither engine grades its
//!   own homework.
//! - **Matched tokenization, proven not asserted.** Nucleus's `simple`
//!   analyzer and Tantivy's `SimpleTokenizer` + `LowerCaser` split on the same
//!   rule (non-alphanumeric). The corpus vocabulary is lowercase alphanumeric
//!   with space separators, so the two pipelines must produce identical token
//!   streams — and the harness CHECKS that, on every document and every
//!   query, before it times anything. (Difference that remains, stated: on
//!   real text Nucleus keeps an apostrophe inside a word while Tantivy splits
//!   `don't` into `don`/`t`; the synthetic vocabulary never exercises it.)
//! - **Matched scoring parameters.** Both engines score Okapi BM25 with
//!   k1 = 1.2, b = 0.75, idf = ln(1 + (N - df + 0.5)/(df + 0.5)),
//!   and both fix those constants at compile time, so there is no operating
//!   point to mismatch. One real scoring difference remains and is stated on
//!   every run: Tantivy quantizes fieldnorm (document length) to a 256-entry
//!   table, Nucleus uses exact lengths, so near-tie documents can rank
//!   differently. That is measured, not hidden: agreement against the exact
//!   ground truth is reported next to every latency.
//! - **Relevance is the analogue of recall.** A latency win on different
//!   answers is not a win, so overlap@k against the shared truth ships with
//!   the timing, tie-blind in the ann-benchmarks sense: a returned document
//!   counts as a hit when its ground-truth score is within 0.1% of the k-th
//!   true score, because id-overlap punishes engines for tie-break luck on
//!   documents that score identically. Where rankings diverge, example
//!   queries are printed — which answers differ is more valuable than the
//!   timing.
//! - **No transport to correct for.** In `compete_vector` the transport gap
//!   (in-process vs loopback socket) had to be measured and printed so it
//!   could be subtracted. Here BOTH engines answer in-process in this binary,
//!   so no such correction exists; that statement is printed on every run so
//!   nobody goes looking for it.
//! - **Failed operations are never timed as successes.** Every timed query's
//!   result is asserted: result count must equal min(k, true match count),
//!   every returned id must genuinely contain a query term, scores must be
//!   finite, and a query with zero true matches must return nothing. A
//!   relevance divergence (a returned doc outside the truth top-k tie class)
//!   is reported in the agreement columns and in the printed examples rather
//!   than aborting — that divergence is a finding, not a harness failure.
//!
//! Timing protocol: queries run sequentially, single-threaded (Tantivy gets
//! one writer thread so the index is one segment and DocId assignment is
//! deterministic; Nucleus's `search` is sequential). Arms are interleaved per
//! query with alternating order, so warm-up drift hits both engines equally.
//! Numbers are wall-clock on whatever machine runs this — reproduce before
//! quoting a ratio.
//!
//! Usage:
//! ```text
//! cargo run --release --features bench-tools --bin compete_fts -- \
//!     --docs 50000 --vocab 5000 --k 10 --queries 200 --seed 42
//! ```
//! Run the Nucleus column alone with `--skip-tantivy`.

use std::collections::HashMap;
use std::time::Instant;

use nucleus::fts::{Analyzer, InvertedIndex};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{FAST, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer};
use tantivy::{Index, Term, doc};

// ============================================================================
// Config
// ============================================================================

struct Config {
    docs: usize,
    vocab: usize,
    k: usize,
    queries: usize,
    seed: u64,
    /// Tantivy writer heap, MB. Single thread, so this is one arena.
    heap_mb: usize,
    skip_tantivy: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            docs: 50_000,
            vocab: 5_000,
            k: 10,
            queries: 200,
            seed: 42,
            heap_mb: 128,
            skip_tantivy: false,
        }
    }
}

const USAGE: &str = "\
compete_fts — Nucleus FTS vs Tantivy, apples to apples (methodology in the file header)

Usage: compete_fts [OPTIONS]
  --docs N        corpus size           (default 50000)
  --vocab N       vocabulary size       (default 5000)
  --k N           top-k per query       (default 10)
  --queries N     query count           (default 200)
  --seed N        RNG seed              (default 42)
  --heap MB       tantivy writer heap   (default 128)
  --skip-tantivy  run the Nucleus arm only
  --help          print this text and exit

cargo run --release --features bench-tools --bin compete_fts -- --docs 50000 --queries 200";

fn parse_args() -> Option<Config> {
    let mut cfg = Config::default();
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--docs" => cfg.docs = next(&mut i).parse().unwrap_or(cfg.docs),
            "--vocab" => cfg.vocab = next(&mut i).parse().unwrap_or(cfg.vocab),
            "--k" => cfg.k = next(&mut i).parse().unwrap_or(cfg.k),
            "--queries" => cfg.queries = next(&mut i).parse().unwrap_or(cfg.queries),
            "--seed" => cfg.seed = next(&mut i).parse().unwrap_or(cfg.seed),
            "--heap" => cfg.heap_mb = next(&mut i).parse().unwrap_or(cfg.heap_mb),
            "--skip-tantivy" => cfg.skip_tantivy = true,
            "--help" | "-h" => {
                println!("{USAGE}");
                return None;
            }
            other => {
                eprintln!("unknown argument: {other}\n\n{USAGE}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    Some(cfg)
}

// ============================================================================
// Deterministic RNG (xorshift64) — same convention as every other probe.
// ============================================================================

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
}

// ============================================================================
// Corpus: Zipf-drawn synthetic vocabulary, tokenizers cannot disagree on it.
// ============================================================================

/// Truncated Zipf (s = 1) over `n` ranks, so document frequency runs from
/// near-corpus (rank 0, stopword-like) down to unique (tail) — the spread a
/// BM25 comparison has to survive. Sampled by binary search over the CDF.
struct Zipf {
    cum: Vec<f64>,
}

impl Zipf {
    fn new(n: usize) -> Self {
        let mut total = 0.0f64;
        let cum: Vec<f64> = (0..n)
            .map(|r| {
                total += 1.0 / (1.0 + r as f64);
                total
            })
            .collect();
        Zipf { cum }
    }
    fn draw(&self, rng: &mut Rng) -> usize {
        let u = rng.below(1_000_000_000) as f64 / 1e9 * self.cum[self.cum.len() - 1];
        self.cum.partition_point(|&x| x < u).min(self.cum.len() - 1)
    }
}

const CONSONANTS: &[u8] = b"bcdfghjklmnpqrstvwxz";
const VOWELS: &[u8] = b"aeiou";

/// Lowercase alphanumeric word, consonant/vowel alternating. Deliberately not
/// English: no stemmer or stopword list on either side can treat these
/// specially, so the matched-analyzer claim is structural, not incidental.
fn gen_word(rng: &mut Rng) -> String {
    let len = 4 + rng.below(6);
    let vowel_first = rng.below(2) == 0;
    (0..len)
        .map(|i| {
            let bank = if (i % 2 == 0) == vowel_first {
                VOWELS
            } else {
                CONSONANTS
            };
            bank[rng.below(bank.len())] as char
        })
        .collect()
}

/// The whole corpus plus the statistics the ground truth needs, accumulated
/// while the documents are generated (before either engine exists).
struct Corpus {
    /// Indexed vocabulary (rank == term id).
    vocab: Vec<String>,
    /// Vocabulary reserved for queries only; never appears in a document, so a
    /// query containing one must match fewer docs (or none).
    absent: Vec<String>,
    texts: Vec<String>,
    doc_len: Vec<usize>,
    /// term id → (doc id, term frequency), sorted by doc id because documents
    /// are generated in id order. This is the ground-truth posting table.
    term_docs: Vec<Vec<(u64, u32)>>,
    total_tokens: usize,
}

fn gen_corpus(rng: &mut Rng, cfg: &Config) -> Corpus {
    // The vocabulary must be a SET: both engines key postings by term STRING
    // while the ground truth keys by vocab id, so one duplicated string would
    // desync the two (the membership assertion caught exactly this on the
    // first 50k-doc run). Regenerate on collision instead.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut unique_word = |rng: &mut Rng| -> String {
        loop {
            let w = gen_word(rng);
            if seen.insert(w.clone()) {
                return w;
            }
        }
    };
    let vocab: Vec<String> = (0..cfg.vocab).map(|_| unique_word(rng)).collect();
    let absent: Vec<String> = (0..200).map(|_| unique_word(rng)).collect();
    let zipf = Zipf::new(cfg.vocab);

    let mut texts = Vec::with_capacity(cfg.docs);
    let mut doc_len = Vec::with_capacity(cfg.docs);
    let mut term_docs: Vec<Vec<(u64, u32)>> = vec![Vec::new(); cfg.vocab];
    let mut total_tokens = 0usize;
    let mut tf: HashMap<usize, u32> = HashMap::new();

    for id in 0..cfg.docs {
        // Mostly short documents, one in ten long — BM25 length normalization
        // needs a real length spread to be exercised.
        let len = if rng.below(100) < 10 {
            300 + rng.below(300)
        } else {
            30 + rng.below(90)
        };
        tf.clear();
        let mut words: Vec<&str> = Vec::with_capacity(len);
        for _ in 0..len {
            let tid = zipf.draw(rng);
            words.push(vocab[tid].as_str());
            *tf.entry(tid).or_default() += 1;
        }
        total_tokens += len;
        for (&tid, &f) in &tf {
            term_docs[tid].push((id as u64, f));
        }
        texts.push(words.join(" "));
        doc_len.push(len);
    }

    Corpus {
        vocab,
        absent,
        texts,
        doc_len,
        term_docs,
        total_tokens,
    }
}

/// Query as term ids: `0..vocab` are real terms, `vocab..` index `absent`.
/// Terms are unique within a query (the engines' duplicate-term handling is
/// not under test). Roughly one query in ten carries an absent term.
fn gen_queries(rng: &mut Rng, cfg: &Config, corpus: &Corpus) -> Vec<Vec<usize>> {
    (0..cfg.queries)
        .map(|_| {
            let nterms = 1 + rng.below(3);
            let mut terms: Vec<usize> = Vec::with_capacity(nterms);
            while terms.len() < nterms {
                let t = if rng.below(100) < 10 {
                    cfg.vocab + rng.below(corpus.absent.len())
                } else {
                    rng.below(cfg.vocab)
                };
                if !terms.contains(&t) {
                    terms.push(t);
                }
            }
            terms
        })
        .collect()
}

fn query_string(corpus: &Corpus, terms: &[usize], vocab_len: usize) -> String {
    terms
        .iter()
        .map(|&t| {
            if t < vocab_len {
                corpus.vocab[t].as_str()
            } else {
                corpus.absent[t - vocab_len].as_str()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

// ============================================================================
// Shared ground truth — exact BM25, computed once, independent of both engines
// ============================================================================

/// BM25 parameters: the constants BOTH engines fix at compile time. There is
/// no dial to mismatch, which is why none is swept.
const K1: f64 = 1.2;
const B: f64 = 0.75;

/// A returned doc counts as a top-k hit when its ground-truth score is within
/// this relative band of the k-th true score (tie-blind, ann-benchmarks
/// convention). Sized for float summation-order noise, NOT for Tantivy's
/// fieldnorm quantization — a doc ranked below the band by Tantivy is a
/// measured disagreement, reported in the agreement columns.
const OVERLAP_TOL: f64 = 1e-3;

/// Width of the exact-tie class at the k-th score: how many documents a PERFECT
/// engine could legitimately return in the last slot. Reported per run because
/// it is the context that makes id-overlap numbers between engines readable.
const TIE_EPS: f64 = 1e-9;

struct QueryTruth {
    match_count: usize,
    /// Top min(k, matches) by (score desc, id asc) — same tie-break as
    /// Nucleus's `top_k_by_score`.
    top: Vec<(u64, f64)>,
    /// Ground-truth score of the k-th match; 0.0 when nothing matched.
    kth: f64,
    tie_width: usize,
    /// doc → score for every doc at or above `kth * (1 - OVERLOP_TOL)` — the
    /// frontier a returned doc must reach to count as a hit.
    frontier: HashMap<u64, f64>,
}

fn truth_idf(n: usize, df: usize) -> f64 {
    ((n as f64 - df as f64 + 0.5) / (df as f64 + 0.5) + 1.0).ln()
}

fn compute_truth(corpus: &Corpus, queries: &[Vec<usize>], cfg: &Config) -> Vec<QueryTruth> {
    let n = corpus.texts.len();
    let avgdl = corpus.total_tokens as f64 / n.max(1) as f64;

    queries
        .iter()
        .map(|terms| {
            // Present terms only, deduplicated, processed SHORTEST POSTING
            // FIRST — the same order Nucleus's `search` accumulates scores
            // in, so the reference arithmetic matches the engine's
            // term-by-term summation order exactly.
            let mut present: Vec<usize> = terms
                .iter()
                .copied()
                .filter(|&t| t < corpus.vocab.len())
                .collect();
            present.sort_unstable();
            present.dedup();
            present.sort_by_key(|&t| corpus.term_docs[t].len());

            let mut scores: HashMap<u64, f64> = HashMap::new();
            for &tid in &present {
                let postings = &corpus.term_docs[tid];
                let idf = truth_idf(n, postings.len());
                for &(doc, tf) in postings {
                    let tf = tf as f64;
                    let dl = corpus.doc_len[doc as usize] as f64;
                    // Same expression shape as `InvertedIndex::bm25_term_score`
                    // so the numbers are bit-comparable, not just close.
                    let numerator = tf * (K1 + 1.0);
                    let denominator = tf + K1 * (1.0 - B + B * dl / avgdl);
                    let s = idf * numerator / denominator;
                    *scores.entry(doc).or_default() += s;
                }
            }

            let match_count = scores.len();
            if match_count == 0 {
                return QueryTruth {
                    match_count: 0,
                    top: vec![],
                    kth: 0.0,
                    tie_width: 0,
                    frontier: HashMap::new(),
                };
            }

            let mut ranked: Vec<(u64, f64)> = scores.iter().map(|(&d, &s)| (d, s)).collect();
            ranked.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.0.cmp(&b.0))
            });
            let want = cfg.k.min(match_count);
            let kth = ranked[want - 1].1;
            let tie_width = ranked
                .iter()
                .filter(|(_, s)| *s >= kth * (1.0 - TIE_EPS))
                .count();
            let frontier: HashMap<u64, f64> = scores
                .into_iter()
                .filter(|(_, s)| *s >= kth * (1.0 - OVERLAP_TOL))
                .collect();
            ranked.truncate(want);
            QueryTruth {
                match_count,
                top: ranked,
                kth,
                tie_width,
                frontier,
            }
        })
        .collect()
}

/// Binary search in a (doc-sorted) ground-truth posting list.
fn posting_tf(postings: &[(u64, u32)], doc: u64) -> Option<u32> {
    let idx = postings.partition_point(|&(d, _)| d < doc);
    postings
        .get(idx)
        .filter(|&&(d, _)| d == doc)
        .map(|&(_, tf)| tf)
}

// ============================================================================
// Arms
// ============================================================================

struct TantivyArm {
    reader: tantivy::IndexReader,
    body: tantivy::schema::Field,
    analyzer: TextAnalyzer,
    /// Fast-field column per segment ordinal: tantivy DocId → corpus doc id.
    id_cols: Vec<tantivy::columnar::Column<u64>>,
}

fn build_nucleus(corpus: &Corpus) -> (InvertedIndex, f64) {
    // `simple` analyzer: lowercase, split on non-alphanumeric — the Nucleus
    // side of the matched tokenization pair (no stemming, no stopwords, which
    // is also what keeps the synthetic vocabulary honest).
    let mut index = InvertedIndex::with_analyzer(Analyzer::Simple);
    let t = Instant::now();
    for (id, text) in corpus.texts.iter().enumerate() {
        index.add_document(id as u64, text);
    }
    let build_s = t.elapsed().as_secs_f64();
    (index, build_s)
}

fn build_tantivy(corpus: &Corpus, heap_mb: usize) -> tantivy::Result<(TantivyArm, f64)> {
    let mut builder = Schema::builder();
    let text_opts = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("bench")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let body = builder.add_text_field("body", text_opts);
    let id_field = builder.add_u64_field("id", FAST);
    let schema = builder.build();

    // In-RAM: both engines hold the whole corpus in memory, so the comparison
    // measures search, not disk.
    let index = Index::create_in_ram(schema);
    index.tokenizers().register(
        "bench",
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .build(),
    );

    // ONE writer thread: the index commits as one segment, DocId assignment is
    // deterministic, and search runs on one thread — matching the sequential,
    // single-threaded Nucleus `search` arm instead of crediting Tantivy with
    // segment-parallel fans this harness never gives Nucleus.
    let mut writer = index.writer_with_num_threads(1, heap_mb * 1024 * 1024)?;
    let t = Instant::now();
    for (i, text) in corpus.texts.iter().enumerate() {
        writer.add_document(doc!(id_field => i as u64, body => text.as_str()))?;
    }
    writer.commit()?;
    writer.wait_merging_threads()?;
    // Commit + finalization + opening the reader are all part of making the
    // corpus searchable; Nucleus has no commit step, so build seconds are
    // reported side by side but are NOT like-for-like durable-index builds.
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let id_cols: Vec<tantivy::columnar::Column<u64>> = searcher
        .segment_readers()
        .iter()
        .map(|sr| sr.fast_fields().u64("id").expect("id fast field"))
        .collect();
    let build_s = t.elapsed().as_secs_f64();
    Ok((
        TantivyArm {
            reader,
            body,
            analyzer: TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .build(),
            id_cols,
        },
        build_s,
    ))
}

fn run_nucleus(index: &InvertedIndex, q: &str, k: usize) -> Vec<(u64, f64)> {
    index.search(q, k)
}

fn run_tantivy(arm: &mut TantivyArm, q: &str, k: usize) -> tantivy::Result<Vec<(u64, f64)>> {
    // Query tokenization happens INSIDE the timed region on both arms
    // (Nucleus's `search` tokenizes internally); the same analyzer that
    // indexed the corpus parses the query, and terms are deduplicated exactly
    // as the query generator guaranteed them unique.
    let mut stream = arm.analyzer.token_stream(q);
    let mut terms: Vec<String> = Vec::new();
    while stream.advance() {
        let text = stream.token().text.clone();
        if !terms.contains(&text) {
            terms.push(text);
        }
    }
    let clauses: Vec<(Occur, Box<dyn Query>)> = terms
        .iter()
        .map(|t| {
            (
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_text(arm.body, t),
                    IndexRecordOption::WithFreqsAndPositions,
                )) as Box<dyn Query>,
            )
        })
        .collect();
    // BooleanQuery::new over Should clauses requires >=1 match: OR semantics,
    // the same disjunction Nucleus's `search` implements.
    let query = BooleanQuery::new(clauses);
    let searcher = arm.reader.searcher();
    let hits = searcher.search(&query, &TopDocs::with_limit(k).order_by_score())?;
    Ok(hits
        .iter()
        .map(|(score, addr)| {
            let doc = arm.id_cols[addr.segment_ord as usize]
                .first(addr.doc_id)
                .expect("id fast field value");
            (doc, *score as f64)
        })
        .collect())
}

// ============================================================================
// Tokenizer parity — the "matched parameters" clause, checked not claimed
// ============================================================================

fn nucleus_tokens(text: &str) -> Vec<String> {
    nucleus::fts::tokenize_with(text, Analyzer::Simple)
        .into_iter()
        .map(|t| t.term)
        .collect()
}

fn tantivy_tokens(analyzer: &mut TextAnalyzer, text: &str) -> Vec<String> {
    let mut stream = analyzer.token_stream(text);
    let mut out = Vec::new();
    while stream.advance() {
        out.push(stream.token().text.clone());
    }
    out
}

/// Assert the two tokenization pipelines produce IDENTICAL token sequences on
/// every document and every query string. With a lowercase-alphanumeric
/// vocabulary this must hold; if anyone changes the corpus generator to
/// include punctuation or apostrophes, this is what fails and says so.
fn assert_tokenizer_parity(corpus: &Corpus, query_strings: &[String], arm: &mut TantivyArm) {
    let mut checked = 0usize;
    for (i, text) in corpus.texts.iter().enumerate() {
        let a = nucleus_tokens(text);
        let b = tantivy_tokens(&mut arm.analyzer, text);
        assert_eq!(
            a, b,
            "tokenizer divergence on doc {i}: nucleus={a:?} tantivy={b:?} — the corpora are no \
             longer matched; the benchmark cannot proceed"
        );
        checked += 1;
    }
    for (i, q) in query_strings.iter().enumerate() {
        let a = nucleus_tokens(q);
        let b = tantivy_tokens(&mut arm.analyzer, q);
        assert_eq!(
            a, b,
            "tokenizer divergence on query {i}: {q:?} nucleus={a:?} tantivy={b:?}"
        );
        checked += 1;
    }
    println!(
        "tokenizer parity: {checked} documents+queries identical (nucleus:simple vs \
         tantivy:SimpleTokenizer+LowerCaser)"
    );
}

// ============================================================================
// Measurement
// ============================================================================

struct Measurement {
    engine: &'static str,
    build_s: f64,
    overlap_avg: f64,
    overlap_min: f64,
    /// Queries whose overlap@k was below 1.0 — the disagreement count.
    miss_q: usize,
    /// Queries whose top-k ID set exactly equalled the truth top-k.
    exact_topk: usize,
    p50_us: f64,
    p95_us: f64,
    qps: f64,
}

fn percentile(sorted_us: &[f64], p: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_us.len() as f64 - 1.0) * p).round() as usize;
    sorted_us[idx]
}

fn summarize(
    engine: &'static str,
    build_s: f64,
    overlaps: &[f64],
    exact: usize,
    mut latencies_us: Vec<f64>,
) -> Measurement {
    latencies_us.sort_by(|a, b| a.partial_cmp(b).expect("latencies are finite"));
    let mean_us = latencies_us.iter().sum::<f64>() / latencies_us.len().max(1) as f64;
    Measurement {
        engine,
        build_s,
        overlap_avg: overlaps.iter().sum::<f64>() / overlaps.len().max(1) as f64,
        overlap_min: overlaps.iter().cloned().fold(1.0f64, f64::min),
        miss_q: overlaps.iter().filter(|&&o| o < 1.0).count(),
        exact_topk: exact,
        p50_us: percentile(&latencies_us, 0.50),
        p95_us: percentile(&latencies_us, 0.95),
        qps: if mean_us > 0.0 {
            1_000_000.0 / mean_us
        } else {
            0.0
        },
    }
}

fn main() {
    let Some(cfg) = parse_args() else {
        return;
    };

    println!(
        "corpus docs={} vocab={} (Zipf s=1) k={} queries={} seed={} tantivy {}",
        cfg.docs,
        cfg.vocab,
        cfg.k,
        cfg.queries,
        cfg.seed,
        tantivy::version_string(),
    );

    let mut rng = Rng::new(cfg.seed);
    let t = Instant::now();
    let corpus = gen_corpus(&mut rng, &cfg);
    let queries = gen_queries(&mut rng, &cfg, &corpus);
    let query_strings: Vec<String> = queries
        .iter()
        .map(|terms| query_string(&corpus, terms, cfg.vocab))
        .collect();
    println!(
        "corpus generated in {:.1}s: {} tokens, avg doc len {:.1}, {} absent-only terms",
        t.elapsed().as_secs_f64(),
        corpus.total_tokens,
        corpus.total_tokens as f64 / cfg.docs as f64,
        corpus.absent.len(),
    );

    println!("computing exact BM25 ground truth (brute force, in-process) ...");
    let t = Instant::now();
    let truths = compute_truth(&corpus, &queries, &cfg);
    let df_max = corpus
        .term_docs
        .iter()
        .map(|p| p.len())
        .max()
        .unwrap_or(0)
        .min(cfg.docs);
    println!(
        "ground truth for {} queries in {:.1}s (widest posting list: {} docs)",
        truths.len(),
        t.elapsed().as_secs_f64(),
        df_max,
    );

    // ---- Build both engines on the bit-identical corpus ----
    let (nuc_index, nuc_build_s) = build_nucleus(&corpus);
    assert_eq!(nuc_index.doc_count() as usize, cfg.docs);
    println!("nucleus index built in {nuc_build_s:.1}s");

    let mut tantivy_arm = None;
    let mut tan_build_s = f64::NAN;
    if !cfg.skip_tantivy {
        match build_tantivy(&corpus, cfg.heap_mb) {
            Ok((arm, s)) => {
                tantivy_arm = Some(arm);
                tan_build_s = s;
                println!(
                    "tantivy index built in {s:.1}s (1 writer thread, in-RAM, includes commit)"
                );
            }
            Err(e) => {
                eprintln!(
                    "tantivy build FAILED, aborting rather than timing a one-engine run: {e}"
                );
                std::process::exit(2);
            }
        }
        if let Some(arm) = tantivy_arm.as_mut() {
            assert_tokenizer_parity(&corpus, &query_strings, arm);
        }
    }

    // ---- Warm-up + assertions on untimed queries ----
    let warm = cfg.queries.min(25);
    let check = |engine: &str, qi: usize, got: &[(u64, f64)], truth: &QueryTruth| {
        let expect = truth.match_count.min(cfg.k);
        assert_eq!(
            got.len(),
            expect,
            "{engine} query {qi}: expected {expect} hits, got {}",
            got.len()
        );
        for &(doc, score) in got {
            assert!(
                score.is_finite(),
                "{engine} query {qi}: non-finite score for doc {doc}"
            );
            let member = queries[qi]
                .iter()
                .any(|&t| t < cfg.vocab && posting_tf(&corpus.term_docs[t], doc).is_some());
            assert!(
                member,
                "{engine} query {qi}: returned doc {doc} contains no query term — wrong answer, \
                 not timing it as a success"
            );
        }
    };
    for qi in 0..warm {
        let got = run_nucleus(&nuc_index, &query_strings[qi], cfg.k);
        check("nucleus", qi, &got, &truths[qi]);
        if let Some(arm) = tantivy_arm.as_mut() {
            let got = run_tantivy(arm, &query_strings[qi], cfg.k).unwrap_or_else(|e| {
                eprintln!("tantivy query {qi} failed: {e}");
                std::process::exit(2);
            });
            check("tantivy", qi, &got, &truths[qi]);
        }
    }

    // ---- Interleaved, order-rotated timed pass ----
    let mut nuc_lat: Vec<f64> = Vec::with_capacity(cfg.queries);
    let mut tan_lat: Vec<f64> = Vec::with_capacity(cfg.queries);
    let mut nuc_ids: Vec<Vec<u64>> = Vec::with_capacity(cfg.queries);
    let mut tan_ids: Vec<Vec<u64>> = Vec::with_capacity(cfg.queries);

    for (qi, q) in query_strings.iter().enumerate() {
        let truth = &truths[qi];
        // Alternate which engine answers first so cache-warm drift does not
        // systematically favor whichever arm happens to run second.
        let nucleus_first = qi % 2 == 0;

        let run_nuc = |lat: &mut Vec<f64>, ids: &mut Vec<Vec<u64>>| {
            let t0 = Instant::now();
            let got = run_nucleus(&nuc_index, q, cfg.k);
            lat.push(t0.elapsed().as_nanos() as f64 / 1000.0);
            check("nucleus", qi, &got, truth);
            ids.push(got.into_iter().map(|(d, _)| d).collect());
        };
        let run_tan =
            |lat: &mut Vec<f64>, ids: &mut Vec<Vec<u64>>, arm: &mut Option<TantivyArm>| {
                let Some(a) = arm.as_mut() else {
                    return;
                };
                let t0 = Instant::now();
                let got = run_tantivy(a, q, cfg.k).unwrap_or_else(|e| {
                    eprintln!("tantivy query {qi} failed: {e}");
                    std::process::exit(2);
                });
                lat.push(t0.elapsed().as_nanos() as f64 / 1000.0);
                check("tantivy", qi, &got, truth);
                ids.push(got.into_iter().map(|(d, _)| d).collect());
            };

        if nucleus_first {
            run_nuc(&mut nuc_lat, &mut nuc_ids);
            run_tan(&mut tan_lat, &mut tan_ids, &mut tantivy_arm);
        } else {
            run_tan(&mut tan_lat, &mut tan_ids, &mut tantivy_arm);
            run_nuc(&mut nuc_lat, &mut nuc_ids);
        }
    }

    // ---- Agreement vs the shared truth (tie-blind overlap@k) ----
    // A zero-match query (all terms absent) scores 1.0: the correct answer is
    // the empty set and both the truth and the engine returned exactly that.
    let overlap = |ids: &[Vec<u64>]| -> Vec<f64> {
        ids.iter()
            .zip(&truths)
            .map(|(got, truth)| {
                if truth.match_count == 0 {
                    return 1.0;
                }
                let denom = cfg.k.min(truth.match_count).max(1) as f64;
                let hits = got
                    .iter()
                    .filter(|&&d| truth.frontier.contains_key(&d))
                    .count();
                hits as f64 / denom
            })
            .collect()
    };
    let nuc_over = overlap(&nuc_ids);
    let tan_over = overlap(&tan_ids);
    let exact_topk = |ids: &[Vec<u64>]| -> usize {
        ids.iter()
            .zip(&truths)
            .filter(|(got, truth)| {
                got.len() == truth.top.len()
                    && got.iter().zip(truth.top.iter()).all(|(g, (t, _))| g == t)
            })
            .count()
    };

    let mut results = vec![summarize(
        "nucleus",
        nuc_build_s,
        &nuc_over,
        exact_topk(&nuc_ids),
        nuc_lat,
    )];
    if tantivy_arm.is_some() {
        results.push(summarize(
            "tantivy",
            tan_build_s,
            &tan_over,
            exact_topk(&tan_ids),
            tan_lat,
        ));
    }

    // ---- Table ----
    println!();
    println!(
        "{:<10} {:>8} {:>9} {:>9} {:>9} {:>11} {:>10} {:>7} {:>11}",
        "engine",
        "build_s",
        "p50_us",
        "p95_us",
        "qps",
        "ovlp@k",
        "ovlp_min",
        "miss_q",
        "exact_topk"
    );
    for r in &results {
        println!(
            "{:<10} {:>8.1} {:>9.0} {:>9.0} {:>9.0} {:>11.3} {:>10.3} {:>7} {:>11}",
            r.engine,
            r.build_s,
            r.p50_us,
            r.p95_us,
            r.qps,
            r.overlap_avg,
            r.overlap_min,
            r.miss_q,
            format!("{}/{}", r.exact_topk, cfg.queries),
        );
    }

    // ---- Engine-vs-engine agreement and example divergences ----
    if tantivy_arm.is_some() {
        let avg_tie: f64 =
            truths.iter().map(|t| t.tie_width as f64).sum::<f64>() / truths.len().max(1) as f64;
        let mut id_over_sum = 0.0f64;
        let mut top1_eq = 0usize;
        let mut counted = 0usize;
        for qi in 0..cfg.queries {
            let truth = &truths[qi];
            if truth.match_count == 0 {
                continue;
            }
            let denom = cfg.k.min(truth.match_count) as f64;
            let nset: std::collections::HashSet<u64> = nuc_ids[qi].iter().copied().collect();
            let hits = tan_ids[qi].iter().filter(|d| nset.contains(*d)).count();
            id_over_sum += hits as f64 / denom;
            counted += 1;
            if nuc_ids[qi].first() == tan_ids[qi].first() {
                top1_eq += 1;
            }
        }
        println!();
        println!(
            "engine agreement: id-overlap@k avg {:.3} over {counted} matching queries, top-1 \
             equal {top1_eq}/{}, avg exact-tie width at k: {:.1} docs",
            id_over_sum / counted.max(1) as f64,
            cfg.queries,
            avg_tie,
        );

        // Which answers differ — up to 3 queries where either engine placed a
        // doc outside the truth tie class, with the docs that were swapped.
        let mut printed = 0;
        for qi in 0..cfg.queries {
            if printed >= 3 {
                break;
            }
            let truth = &truths[qi];
            let nuc_miss: Vec<u64> = nuc_ids[qi]
                .iter()
                .filter(|d| !truth.frontier.contains_key(*d))
                .copied()
                .collect();
            let tan_miss: Vec<u64> = tan_ids[qi]
                .iter()
                .filter(|d| !truth.frontier.contains_key(*d))
                .copied()
                .collect();
            if nuc_miss.is_empty() && tan_miss.is_empty() {
                continue;
            }
            println!();
            println!(
                "divergence example, query {qi} [{}]: {} matches, tie width {} at kth score",
                query_strings[qi], truth.match_count, truth.tie_width,
            );
            println!(
                "  truth top-5 ids : {:?}",
                truth.top.iter().take(5).map(|(d, _)| d).collect::<Vec<_>>()
            );
            println!(
                "  nucleus ids     : {:?}  outside tie class: {nuc_miss:?}",
                nuc_ids[qi].iter().take(5).collect::<Vec<_>>()
            );
            println!(
                "  tantivy ids     : {:?}  outside tie class: {tan_miss:?}",
                tan_ids[qi].iter().take(5).collect::<Vec<_>>()
            );
            println!(
                "  (kth score {:.4}, hit band is within {:.2}% of it)",
                truth.kth,
                OVERLAP_TOL * 100.0,
            );
            printed += 1;
        }
    }

    println!();
    println!(
        "NOTE: both engines answer in-process in this binary — no socket, no transport gap to \
         subtract (unlike compete_vector, where the pgvector arm pays a loopback round trip)."
    );
    println!(
        "NOTE: BM25 parameters identical on both sides and in the ground truth (k1=1.2, b=0.75, \
         Lucene idf). Remaining scoring difference: Tantivy quantizes document length to a \
         256-entry fieldnorm table, Nucleus and the ground truth use exact lengths — that is \
         what the overlap and agreement columns measure."
    );
    println!(
        "NOTE: build_s is not like-for-like: Nucleus's build has no commit/merge step, \
         Tantivy's includes commit + reader open. Single-threaded, single-segment, sequential \
         queries on both arms."
    );
    println!(
        "NOTE: single dev-machine wall clock. Reproduce before quoting any ratio; see \
         python/benchmarks/README.md for why this machine class cannot gate on timings."
    );
}
