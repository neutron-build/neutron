//! Raw output parsers for the benchmark tools.
//!
//! Invariant owned here: parsing never panics and never invents numbers —
//! a metric that cannot be found is an error, so a silent format change in
//! a tool fails the run instead of publishing a wrong number (PLAN-v3
//! §5.2 rule 8: losses and wins are published, but never wrong numbers).
//!
//! The fixtures in `src/fixtures/` are real captured outputs (see each
//! fixture's first line for the tool version).
//!
//! One row publishes driver-side (card NB-2: `oltp-ycsb-pg17` writes the
//! NB_METRICS JSON itself — the row's scope note in rows.rs explains why),
//! so its published numbers are tested by executing its driver: the
//! [`driver`] module at the bottom of this file runs the real script over
//! the recorded fixtures and holds its output to [`parse_ycsb`], the
//! fixture-pinned reference parser (verdict NB-2, rework 1).

use crate::error::{Error, Result};
use std::path::Path;

/// Metrics extracted from one execution of a tool.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ParsedRun {
    /// Named numeric metrics (tps, p99_ms, ...).
    pub metrics: Vec<(String, f64)>,
    /// Optional digest the tool itself printed (speedtest1's
    /// `--verify` Verification Hash line).
    pub digest: Option<String>,
}

fn parse_f64(text: &str, what: &'static str) -> Result<f64> {
    text.trim().parse::<f64>().map_err(|_| Error::Parse {
        what,
        line: text.to_string(),
    })
}

/// Extract the numeric prefix of a token like "1234.56".
fn num_prefix(token: &str) -> Option<&str> {
    let end = token
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-' || c == 'e' || c == 'E'))
        .unwrap_or(token.len());
    if end == 0 { None } else { token.get(..end) }
}

/// Find the last line containing `marker` and return the first number on it.
fn last_number_after(lines: &[&str], marker: &str, metric: &'static str) -> Result<f64> {
    for line in lines.iter().rev() {
        if line.contains(marker) {
            let after = line.split(marker).nth(1).unwrap_or("");
            for token in after.split_whitespace() {
                if let Some(n) = num_prefix(token)
                    && n.parse::<f64>().is_ok()
                {
                    return parse_f64(n, metric);
                }
            }
            return Err(Error::Parse {
                what: metric,
                line: line.to_string(),
            });
        }
    }
    Err(Error::Parse {
        what: metric,
        line: format!("no line containing {marker:?}"),
    })
}

/// Parse a pgbench summary (stdout of one run).
///
/// pgbench ends with lines like:
/// ```text
/// number of transactions actually processed: 60000
/// latency average = 1.834 ms
/// latency stddev = 0.912 ms
/// initial connection time = 20.11 ms
/// tps = 432.98 (including initial connection time)
/// ```
pub fn parse_pgbench_summary(log: &str) -> Result<ParsedRun> {
    let lines: Vec<&str> = log.lines().collect();
    let tps = last_number_after(&lines, "tps =", "tps")?;
    let mut out = vec![("tps".to_string(), tps)];
    if let Ok(avg) = last_number_after(&lines, "latency average =", "latency_avg_ms") {
        out.push(("latency_avg_ms".to_string(), avg));
    }
    Ok(ParsedRun {
        metrics: out,
        digest: None,
    })
}

/// Parse a pgbench `--log` file into latency percentiles.
///
/// Each line: `client_id transaction_no latency_us file_no time_epoch
/// time_us [schedule_lag_us]` (PostgreSQL 14+ format). Percentiles are
/// type-7 over all transactions.
pub fn parse_pgbench_log_file(path: &Path) -> Result<ParsedRun> {
    let raw = std::fs::read_to_string(path)?;
    parse_pgbench_log(&raw)
}

/// In-memory variant of [`parse_pgbench_log_file`].
pub fn parse_pgbench_log(raw: &str) -> Result<ParsedRun> {
    let mut lat_us: Vec<f64> = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let _client = fields.next();
        let _txn = fields.next();
        let Some(lat) = fields.next() else { continue };
        let lat: f64 = lat.parse().map_err(|_| Error::Parse {
            what: "pgbench log",
            line: line.to_string(),
        })?;
        lat_us.push(lat);
    }
    if lat_us.is_empty() {
        return Err(Error::Parse {
            what: "pgbench log",
            line: format!("{} has no latency rows", path_placeholder()),
        });
    }
    lat_us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p = |q: f64| -> f64 {
        let n = lat_us.len();
        let rank = q * (n - 1) as f64;
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;
        lat_us[lo] + (lat_us[hi] - lat_us[lo]) * (rank - lo as f64)
    };
    Ok(ParsedRun {
        metrics: vec![
            ("p50_us".to_string(), p(0.50)),
            ("p99_us".to_string(), p(0.99)),
            ("p999_us".to_string(), p(0.999)),
        ],
        digest: None,
    })
}

fn path_placeholder() -> String {
    "log".to_string()
}

/// Parse a sysbench stats block (final block wins).
///
/// The final block looks like:
/// ```text
/// SQL statistics:
///     transactions:                        8520   (142.00 per sec.)
///     queries:                             170400 (2840.00 per sec.)
///     ignored errors:                      0      (0.00 per sec.)
///     reconnects:                          0      (0.00 per sec.)
///
/// General statistics:
///     total time:                          60.0015s
///     total number of events:              8520
///
/// Latency (ms):
///         min:                                    2.31
///        avg:                                   112.05
///        max:                                  1023.88
///        99th percentile:                       450.12
///        sum:                                 954668.32
/// ```
pub fn parse_sysbench(log: &str) -> Result<ParsedRun> {
    // take the last "SQL statistics:" section
    let section = log
        .rfind("SQL statistics:")
        .map(|i| &log[i..])
        .ok_or_else(|| Error::Parse {
            what: "sysbench",
            line: "no SQL statistics block".into(),
        })?;
    let lines: Vec<&str> = section.lines().collect();
    // sysbench prints `transactions:  <total>  (<rate> per sec.)`; the
    // rate is the number inside the parentheses.
    let per_sec = |label: &str, metric: &'static str| -> Result<f64> {
        for line in lines.iter().rev() {
            if line.contains(label) {
                if let Some(open) = line.rfind('(') {
                    let inner = line.get(open + 1..).unwrap_or("");
                    if let Some(tok) = inner.split_whitespace().next()
                        && let Some(n) = num_prefix(tok)
                        && let Ok(v) = n.parse::<f64>()
                    {
                        return Ok(v);
                    }
                }
                return Err(Error::Parse {
                    what: metric,
                    line: line.to_string(),
                });
            }
        }
        Err(Error::Parse {
            what: metric,
            line: format!("no line containing {label:?}"),
        })
    };
    let tps = per_sec("transactions:", "tps")?;
    let mut out = vec![("tps".to_string(), tps)];
    if let Ok(qps) = per_sec("queries:", "qps") {
        out.push(("qps".to_string(), qps));
    }
    if let Ok(avg) = last_number_after(&lines, "avg:", "latency_avg_ms") {
        out.push(("latency_avg_ms".to_string(), avg));
    }
    if let Ok(p99) = last_number_after(&lines, "99th percentile:", "p99_ms") {
        out.push(("p99_ms".to_string(), p99));
    }
    Ok(ParsedRun {
        metrics: out,
        digest: None,
    })
}

/// Parse a YCSB 0.17.0 final measurement report (card NB-2, row
/// `oltp-ycsb-pg17`).
///
/// **Reference parser for the driver's own extraction.** The row
/// publishes driver-side through the NB_METRICS protocol (the row's
/// scope note in rows.rs: the runner's log-parser dispatch is outside
/// card NB-2's Touch only list), so this function never runs in a
/// measured pass. `workloads/oltp-ycsb-pg17/driver.sh` extracts the
/// same fixed-format lines from the tee'd log and writes the metrics
/// JSON that becomes the row's numbers. This function stays the
/// fixture-pinned definition of the format, and the [`driver`] tests
/// below hold the driver to it: they execute the real script over the
/// recorded fixtures and require its NB_METRICS output to equal this
/// function's result (verdict NB-2, rework 1), so the card's
/// PARSER-OPS-DROPPED and PARSER-P99-AS-AVG mutants fail `cargo test`
/// on the published path, not only here.
///
/// A run ends with `[SECTION], Metric, Value` lines; the `-s` status
/// lines (every 10 s), the launcher's java-command echo and the
/// `2>&1 | tee` contract land in the same log but carry no report lines:
///
/// ```text
/// [OVERALL], RunTime(ms), 130054
/// [OVERALL], Throughput(ops/sec), 7688.8125763666825
/// [READ], Operations, 653952
/// [READ], AverageLatency(us), 1298.4990136212595
/// [READ], MinLatency(us), 560
/// [READ], MaxLatency(us), 63263
/// [READ], 95thPercentileLatency(us), 2247
/// [READ], 99thPercentileLatency(us), 5335
/// [READ], Return=OK, 653952
/// [UPDATE], Operations, 131138
/// [UPDATE], AverageLatency(us), 2360.36
/// [UPDATE], 99thPercentileLatency(us), 8438
/// [UPDATE], Return=OK, 131138
/// [CLEANUP], Operations, 1
/// ```
///
/// `[OVERALL]` is the one section every series carries, so a missing
/// `Throughput(ops/sec)` line is an error, never a zero (card NB-2
/// mutant PARSER-OPS-DROPPED). Read latencies are optional *by mix*:
/// every stock workload but E carries a `[READ]` section — workload F
/// reads (its plain reads plus the read half of each
/// read-modify-write) and inserts nothing — while E (scan-heavy)
/// carries `[SCAN]` and `[INSERT]`, so only E's series have no
/// `avg_read_us`/`p99_read_us`. Absent fields are omitted, never
/// invented; `[SCAN]`, `[READ-MODIFY-WRITE]` and `[UPDATE]` latencies
/// never feed the read metrics. The last occurrence of each line wins,
/// so a re-run appended to one log never reports the earlier
/// attempt's numbers.
#[cfg(test)]
pub fn parse_ycsb(log: &str) -> Result<ParsedRun> {
    let lines: Vec<&str> = log.lines().collect();
    let ops_sec = last_number_after(&lines, "[OVERALL], Throughput(ops/sec)", "ops_sec")?;
    let mut out = vec![("ops_sec".to_string(), ops_sec)];
    if let Ok(avg) = last_number_after(&lines, "[READ], AverageLatency(us)", "avg_read_us") {
        out.push(("avg_read_us".to_string(), avg));
    }
    if let Ok(p99) = last_number_after(&lines, "[READ], 99thPercentileLatency(us)", "p99_read_us") {
        out.push(("p99_read_us".to_string(), p99));
    }
    Ok(ParsedRun {
        metrics: out,
        digest: None,
    })
}

/// Parse a memtier_benchmark "ALL STATS" table (2.5.2 format): a header
/// row (`Type Ops/sec Hits/sec ... KB/sec`) followed by per-command rows
/// and a `Totals` row whose columns are, in order: ops/sec, hits/sec,
/// misses/sec, avg latency (ms), p50 (ms), p99 (ms), p99.9 (ms), KB/sec.
/// Older labelled output shapes (`Throughput: N ops/sec`,
/// `50.000% <= X milliseconds`) are accepted as a fallback.
pub fn parse_memtier(log: &str) -> Result<ParsedRun> {
    if let Some(section) = log.rfind("ALL STATS") {
        let lines: Vec<&str> = log[section..].lines().collect();
        let mut totals: Option<Vec<f64>> = None;
        for line in &lines {
            let t = line.trim();
            if t.starts_with("Totals") {
                let nums: Vec<f64> = t
                    .split_whitespace()
                    .skip(1)
                    .filter_map(|tok| num_prefix(tok).and_then(|n| n.parse::<f64>().ok()))
                    .collect();
                totals = Some(nums);
            }
        }
        if let Some(nums) = totals {
            // columns: ops/sec, hits/sec, misses/sec, avg, p50, p99, p99.9, KB/sec
            let ops = *nums.first().ok_or_else(|| Error::Parse {
                what: "memtier ops/sec",
                line: "Totals row has no numbers".into(),
            })?;
            let mut out = vec![("ops_sec".to_string(), ops)];
            if let Some(kb) = nums.get(7) {
                out.push(("kb_per_sec".to_string(), *kb));
            }
            if let Some(p50) = nums.get(4) {
                out.push(("p50_ms".to_string(), *p50));
            }
            if let Some(p99) = nums.get(5) {
                out.push(("p99_ms".to_string(), *p99));
            }
            if let Some(avg) = nums.get(3) {
                out.push(("latency_avg_ms".to_string(), *avg));
            }
            return Ok(ParsedRun {
                metrics: out,
                digest: None,
            });
        }
    }

    // Fallback: older labelled format.
    let totals_start = log.rfind("Totals").ok_or_else(|| Error::Parse {
        what: "memtier",
        line: "no Totals block".into(),
    })?;
    let lines: Vec<&str> = log[totals_start..].lines().collect();

    // ops/sec: "Throughput: 83333.33 ops/sec" or "Throughput (ops/sec): 83333.33"
    let mut ops = None;
    let mut kb_per_sec = None;
    for line in &lines {
        if line.contains("ops/sec") {
            for token in line.split(&[':', '(', ')', '[', ']'][..]) {
                let token = token.trim();
                if let Some(n) = num_prefix(token)
                    && let Ok(v) = n.parse::<f64>()
                {
                    ops = Some(v);
                    break;
                }
            }
        }
        if line.contains("KB/sec") || line.contains("MB/sec") {
            for token in line.split(&[':', '(', ')', '[', ']'][..]) {
                let token = token.trim();
                if let Some(n) = num_prefix(token)
                    && let Ok(v) = n.parse::<f64>()
                {
                    kb_per_sec = Some(v);
                    break;
                }
            }
        }
    }
    let ops = ops.ok_or_else(|| Error::Parse {
        what: "memtier ops/sec",
        line: "no ops/sec line in Totals".into(),
    })?;

    // percentiles: "50.000% [ 1.234 ms]" / "50.000% <= 1.234 milliseconds"
    let mut p50 = None;
    let mut p99 = None;
    for line in &lines {
        let pct = if let Some(rest) = line.trim_start().strip_prefix("p") {
            rest
        } else {
            line.trim_start()
        };
        let Some(pct_end) = pct.find('%') else {
            continue;
        };
        let pct_num: f64 = match pct
            .get(..pct_end)
            .map(str::trim)
            .and_then(|s| s.parse().ok())
        {
            Some(v) => v,
            None => continue,
        };
        // value: first number after a bracket or after "<="
        let rest = pct.get(pct_end + 1..).unwrap_or("");
        let after = rest.split(['[', '=']).next_back().unwrap_or("");
        for token in after.split_whitespace() {
            if let Some(n) = num_prefix(token)
                && let Ok(v) = n.parse::<f64>()
            {
                if (50.0..50.001).contains(&pct_num) {
                    p50 = Some(v);
                } else if (99.0..99.999).contains(&pct_num) {
                    p99 = Some(v);
                }
                break;
            }
        }
    }

    let mut out = vec![("ops_sec".to_string(), ops)];
    if let Some(k) = kb_per_sec {
        out.push(("kb_per_sec".to_string(), k));
    }
    if let Some(p) = p50 {
        out.push(("p50_ms".to_string(), p));
    }
    if let Some(p) = p99 {
        out.push(("p99_ms".to_string(), p));
    }
    Ok(ParsedRun {
        metrics: out,
        digest: None,
    })
}

/// Parse speedtest1 output as printed by SQLite 3.54.1:
///
/// ```text
///    17 - 100 SELECTs without an index.................     0.715s
///           TOTAL.......................................    12.345s
///    Verification Hash: 15286278048446160954 8f14e45f...
///    real    0m 12.90s
///    user    0m 11.01s
///    sys     0m  0.88s
/// ```
///
/// - `TOTAL` is the sum of per-test elapsed time — the number SQLite
///   itself reports for a speedtest1 run (wall seconds).
/// - `Verification Hash:` is printed by `--verify`: an accumulator plus
///   a 24-byte hash over every query result, so two runs of the same
///   dataset must print the identical line or something changed
///   (PLAN-v3 §5.2 rule 4). There is no `--sha3` option in 3.54.1.
/// - `user`/`sys` come from the busybox `time` wrapper the driver runs
///   (CPU seconds; §5.1 efficiency metrics); `real` is ignored in
///   favour of speedtest1's own TOTAL.
pub fn parse_speedtest1(log: &str) -> Result<ParsedRun> {
    let mut total_line: Option<&str> = None;
    let mut digest: Option<String> = None;
    let mut user_s: Option<f64> = None;
    let mut sys_s: Option<f64> = None;
    for line in log.lines() {
        let t = line.trim();
        if t.starts_with("TOTAL") && total_line.is_none() {
            total_line = Some(t);
        }
        if let Some(rest) = t.strip_prefix("Verification Hash:")
            && !rest.trim().is_empty()
        {
            digest = Some(rest.trim().to_string());
        }
        // busybox `time` lines: `user\t0m 11.01s`, `sys\t0m  0.88s`
        if t.starts_with("user\t") {
            user_s = parse_busybox_time_seconds(t);
        }
        if t.starts_with("sys\t") {
            sys_s = parse_busybox_time_seconds(t);
        }
    }
    let line = total_line.ok_or_else(|| Error::Parse {
        what: "speedtest1",
        line: "no TOTAL line".into(),
    })?;
    let wall = line
        .split_whitespace()
        .skip(1)
        .filter_map(|tok| num_prefix(tok).and_then(|n| n.parse::<f64>().ok()))
        .next()
        .ok_or_else(|| Error::Parse {
            what: "speedtest1 TOTAL",
            line: line.to_string(),
        })?;
    let user = user_s.ok_or_else(|| Error::Parse {
        what: "speedtest1",
        line: "no busybox `user` timing line".into(),
    })?;
    let sys = sys_s.ok_or_else(|| Error::Parse {
        what: "speedtest1",
        line: "no busybox `sys` timing line".into(),
    })?;
    Ok(ParsedRun {
        metrics: vec![
            ("wall_s".to_string(), wall),
            ("user_s".to_string(), user),
            ("sys_s".to_string(), sys),
        ],
        digest,
    })
}

/// Parse a busybox `time` line (`user\t0m 11.01s`, minutes may exceed 9)
/// into seconds.
fn parse_busybox_time_seconds(line: &str) -> Option<f64> {
    let rest = line.trim_start_matches(|c: char| !c.is_whitespace()).trim();
    let (mins, secs) = rest.split_once('m')?;
    let minutes: f64 = mins.trim().parse().ok()?;
    let seconds: f64 = secs.trim().trim_end_matches('s').trim().parse().ok()?;
    Some(minutes * 60.0 + seconds)
}

/// Parse the DuckDB TPC-H driver log (card NB-3 Work 5).
///
/// The driver (workloads/embedded-duckdb-tpch1/driver.sh) executes all
/// 22 queries in one DuckDB CLI process under `/usr/bin/time -v` and
/// prints, at the end of the log, one machine-readable summary line per
/// metric:
/// ```text
/// TPCH_TOTAL_WALL_S 12.345
/// TPCH_TOTAL_USER_S 11.010
/// TPCH_TOTAL_SYS_S 0.880
/// TPCH_MAX_RSS_KB 123456
/// TPCH_QUERIES 22
/// ```
/// Totals come from `/usr/bin/time -v` (elapsed / user / system /
/// maximum RSS), so they include the CLI process's own startup — the
/// honest cost of the embedded run. `TPCH_QUERIES` is the count of
/// query files that actually ran; the driver refuses to start unless
/// exactly 22 are present, so a value other than 22 is a driver defect
/// and fails the row here.
///
/// Per-query wall times are printed as `TPCH_QUERY qNN <s>` evidence
/// lines (from DuckDB's own `.timer on` output) and are not metrics.
pub fn parse_duckdb_tpch(log: &str) -> Result<ParsedRun> {
    let tagged = |tag: &str| -> Result<f64> {
        for line in log.lines().rev() {
            let t = line.trim();
            if let Some(rest) = t.strip_prefix(tag)
                && let Some(num) = rest.split_whitespace().next()
            {
                return parse_f64(num, "duckdb tpch");
            }
        }
        Err(Error::Parse {
            what: "duckdb tpch",
            line: format!("no {tag} summary line"),
        })
    };
    let wall = tagged("TPCH_TOTAL_WALL_S")?;
    let user = tagged("TPCH_TOTAL_USER_S")?;
    let sys = tagged("TPCH_TOTAL_SYS_S")?;
    let rss = tagged("TPCH_MAX_RSS_KB")?;
    let queries = tagged("TPCH_QUERIES")?;
    Ok(ParsedRun {
        metrics: vec![
            ("wall_s".to_string(), wall),
            ("user_s".to_string(), user),
            ("sys_s".to_string(), sys),
            ("rss_kb".to_string(), rss),
            ("queries".to_string(), queries),
        ],
        digest: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pgbench_summary_real_format() {
        // Real captured output (PG 17.11, card B-0 smoke run).
        let log = include_str!("fixtures/pgbench-run.txt");
        let parsed = parse_pgbench_summary(log).expect("parse");
        let tps = parsed
            .metrics
            .iter()
            .find(|(k, _)| k == "tps")
            .map(|(_, v)| *v)
            .expect("tps");
        assert!((tps - 1908.815189).abs() < 1e-6, "unexpected tps {tps}");
        let avg = parsed
            .metrics
            .iter()
            .find(|(k, _)| k == "latency_avg_ms")
            .map(|(_, v)| *v)
            .expect("avg");
        assert!((avg - 0.522).abs() < 1e-9, "unexpected avg {avg}");
    }

    #[test]
    fn pgbench_tps_picks_the_including_line() {
        // Some versions print two tps lines; the including-initial-connection
        // one is the honest client-visible number, and it is printed last.
        let log = "tps = 1400.00 (excluding initial connection time)\n\
                   tps = 1399.20 (including initial connection time)\n";
        let parsed = parse_pgbench_summary(log).expect("parse");
        assert_eq!(
            parsed
                .metrics
                .iter()
                .find(|(k, _)| k == "tps")
                .map(|(_, v)| *v),
            Some(1399.2)
        );
    }

    #[test]
    fn pgbench_log_percentiles() {
        let log = "0 1 1000 0 100 0\n0 2 3000 0 100 0\n0 3 2000 0 101 0\n0 4 4000 0 102 0\n";
        let parsed = parse_pgbench_log(log).expect("parse");
        let get = |k: &str| {
            parsed
                .metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get("p50_us"), 2500.0);
        // rank 0.99 * 3 = 2.97 -> 3000 + 0.97 * (4000-3000)
        assert_eq!(get("p99_us"), 3970.0);
    }

    #[test]
    fn pgbench_missing_tps_is_an_error() {
        assert!(parse_pgbench_summary("nothing here\n").is_err());
        assert!(parse_pgbench_log("").is_err());
    }

    #[test]
    fn sysbench_final_block_wins() {
        let log = "SQL statistics:\n    transactions: 100 (10.00 per sec.)\n\
                   \nGeneral statistics:\n    total time: 10.0001s\n\
                   \nSQL statistics:\n    transactions: 8520 (142.00 per sec.)\n\
                      queries: 170400 (2840.00 per sec.)\n\
                   Latency (ms):\n         min: 2.31\n        avg: 112.05\n\
                          max: 1023.88\n        99th percentile: 450.12\n";
        let parsed = parse_sysbench(log).expect("parse");
        let get = |k: &str| {
            parsed
                .metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get("tps"), 142.0);
        assert_eq!(get("qps"), 2840.0);
        assert_eq!(get("latency_avg_ms"), 112.05);
        assert_eq!(get("p99_ms"), 450.12);
    }

    #[test]
    fn ycsb_real_run_fixture() {
        // Real captured output (YCSB 0.17.0 over pgwire/JDBC against
        // PG 17, card NB-2 smoke-size run; the file's first lines are
        // the launcher's java-command echo and "YCSB Client 0.17.0").
        // The values are the fixture's own, pinned exactly — the
        // real output carries all three row metrics, so none is
        // dropped (card NB-2 Work item 4: the fixture is recorded,
        // not invented).
        let log = include_str!("fixtures/ycsb-run.txt");
        let parsed = parse_ycsb(log).expect("parse");
        let get = |k: &str| {
            parsed
                .metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert!(
            (get("ops_sec") - 2801.1204481792715).abs() < 1e-9,
            "{}",
            get("ops_sec")
        );
        assert!((get("avg_read_us") - 127.0767754318618).abs() < 1e-9);
        assert_eq!(get("p99_read_us"), 440.0);
        // p99 and avg are distinct real fields, not one value reused
        assert_ne!(get("p99_read_us"), get("avg_read_us"));
        // exactly the row's three metrics: [UPDATE]/[CLEANUP]/GC
        // sections must not leak into the row's metric set
        let names: Vec<&str> = parsed.metrics.iter().map(|(m, _)| m.as_str()).collect();
        assert_eq!(names, vec!["ops_sec", "avg_read_us", "p99_read_us"]);
        // the -s status line and the launcher echo carry no brackets
        // and must not confuse the parser (the fixture has both)
    }

    #[test]
    fn ycsb_report_sections_and_order() {
        // Card NB-2: [OVERALL] Throughput is the one mandatory metric;
        // [READ] latencies come from the [READ] section only, and the
        // LAST occurrence of each line wins (a re-run appended to one
        // log must not report the earlier attempt's numbers).
        let log = "[OVERALL], RunTime(ms), 1000\n\
                   [OVERALL], Throughput(ops/sec), 100.5\n\
                   [READ], AverageLatency(us), 111.0\n\
                   [READ], 99thPercentileLatency(us), 222.0\n\
                   [OVERALL], Throughput(ops/sec), 7688.81\n\
                   [READ], AverageLatency(us), 1298.5\n\
                   [READ], 99thPercentileLatency(us), 5335.0\n";
        let parsed = parse_ycsb(log).expect("parse");
        let get = |p: &ParsedRun, k: &str| {
            p.metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get(&parsed, "ops_sec"), 7688.81);
        assert_eq!(get(&parsed, "avg_read_us"), 1298.5);
        assert_eq!(get(&parsed, "p99_read_us"), 5335.0);
    }

    #[test]
    fn ycsb_only_the_scan_heavy_mix_lacks_read_metrics() {
        // The stock mixes, on real 0.17.0 reports (recorded with the
        // real client; the `driver` module below says how):
        // - E is scan-heavy: `[SCAN]` + `[INSERT]` and no `[READ]`, so
        //   the row's read-latency metrics are absent — omitted, never
        //   invented — and the [SCAN] latencies must not feed them;
        // - F reads, read-modifies-writes and inserts nothing: its
        //   report carries `[READ]` (plus `[READ-MODIFY-WRITE]` and
        //   `[UPDATE]`), so its series do carry the read metrics, from
        //   the [READ] section only (verdict NB-2 NIT: F has a [READ]
        //   section and no [INSERT]).
        let get = |p: &ParsedRun, k: &str| {
            p.metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        let scan_mix = parse_ycsb(include_str!("fixtures/ycsb-run-e.txt")).expect("parse E");
        assert_eq!(
            scan_mix.metrics.len(),
            1,
            "ops_sec only: {:?}",
            scan_mix.metrics
        );
        assert_eq!(
            scan_mix.metrics[0],
            ("ops_sec".to_string(), 55555.555555555555)
        );

        let rmw_mix = parse_ycsb(include_str!("fixtures/ycsb-run-f.txt")).expect("parse F");
        let names: Vec<&str> = rmw_mix.metrics.iter().map(|(m, _)| m.as_str()).collect();
        assert_eq!(names, vec!["ops_sec", "avg_read_us", "p99_read_us"]);
        // the [READ] lines' own values — not the [READ-MODIFY-WRITE]
        // ones (24.573…, 67) or the [UPDATE] ones
        assert_eq!(get(&rmw_mix, "ops_sec"), 29411.764705882353);
        assert_eq!(get(&rmw_mix, "avg_read_us"), 0.36);
        assert_eq!(get(&rmw_mix, "p99_read_us"), 0.0);
    }

    #[test]
    fn ycsb_missing_throughput_is_an_error() {
        // A silent format change in the tool must fail the run, never
        // publish a zero (card NB-2 mutant PARSER-OPS-DROPPED: the
        // parser that never reads [OVERALL] fails here).
        let log = "[OVERALL], RunTime(ms), 1000\n\
                   [READ], AverageLatency(us), 10.0\n";
        let err = parse_ycsb(log).expect_err("must error without Throughput");
        let text = format!("{err}");
        assert!(text.contains("ops_sec"), "names the missing metric: {text}");
        assert!(parse_ycsb("10 sec: 100 operations\n").is_err());
    }

    #[test]
    fn ycsb_percentiles_are_not_averages() {
        // Distinct pinned values: a parser that reports
        // AverageLatency(us) as p99_read_us (card NB-2 mutant
        // PARSER-P99-AS-AVG) fails both assertions below.
        let log = "[OVERALL], Throughput(ops/sec), 500.0\n\
                   [READ], AverageLatency(us), 100.0\n\
                   [READ], 99thPercentileLatency(us), 9900.0\n";
        let parsed = parse_ycsb(log).expect("parse");
        let get = |k: &str| {
            parsed
                .metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get("avg_read_us"), 100.0);
        assert_eq!(get("p99_read_us"), 9900.0);
        assert_ne!(get("avg_read_us"), get("p99_read_us"));
    }

    #[test]
    fn memtier_totals_table_real_format() {
        // Real captured output (memtier 2.5.2, card B-0 run), trimmed of
        // the per-second progress line.
        let log = include_str!("fixtures/memtier-run.txt");
        let p = parse_memtier(log).expect("parse");
        let get = |p: &ParsedRun, k: &str| {
            p.metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get(&p, "ops_sec"), 88022.26);
        assert_eq!(get(&p, "p50_ms"), 0.639);
        assert_eq!(get(&p, "p99_ms"), 1.735);
        assert_eq!(get(&p, "kb_per_sec"), 3734.42);
        assert_eq!(get(&p, "latency_avg_ms"), 0.72638);
    }

    #[test]
    fn memtier_older_labelled_format_still_parses() {
        let v1 = "Totals\n-----\nThroughput: 83333.33 ops/sec\n\
                  Throughput: 2666.67 MB/sec\n\
                  50.000% <= 1.234 milliseconds\n99.900% <= 5.678 milliseconds\n";
        let p1 = parse_memtier(v1).expect("parse v1");
        let get = |p: &ParsedRun, k: &str| {
            p.metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get(&p1, "ops_sec"), 83333.33);
        assert_eq!(get(&p1, "p50_ms"), 1.234);
        assert_eq!(get(&p1, "p99_ms"), 5.678);

        let v2 = "Totals\n-----\nThroughput (ops/sec): 100000.00\n\
                  p50.000% [  1.500 ms]\np99.900% [  6.250 ms]\n";
        let p2 = parse_memtier(v2).expect("parse v2");
        assert_eq!(get(&p2, "ops_sec"), 100000.0);
        assert_eq!(get(&p2, "p50_ms"), 1.5);
        assert_eq!(get(&p2, "p99_ms"), 6.25);
    }

    #[test]
    fn memtier_without_totals_errors() {
        assert!(parse_memtier("Client-side output\n").is_err());
        // a Totals row with no numbers is an error, not a zero
        assert!(parse_memtier("ALL STATS\nTotals --- --- ---\n").is_err());
    }

    #[test]
    fn speedtest1_total_and_digest() {
        // Real captured output (SQLite 3.54.1 speedtest1 --verify, card
        // B-0 run; the file's first line names the exact build).
        let log = include_str!("fixtures/speedtest1-run.txt");
        let parsed = parse_speedtest1(log).expect("parse");
        let get = |k: &str| {
            parsed
                .metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        // spot values are asserted against the fixture file itself
        assert!(get("wall_s") > 0.0);
        assert!(get("user_s") > 0.0);
        assert!(get("sys_s") >= 0.0);
        // the Verification Hash line must be captured whole (accumulator
        // plus hex digest) — it is the row's per-run content fingerprint
        let d = parsed.digest.as_deref().expect("digest");
        let parts: Vec<&str> = d.split_whitespace().collect();
        assert_eq!(parts.len(), 2, "hash line: {d}");
        assert!(
            parts[1].len() == 48 && parts[1].chars().all(|c| c.is_ascii_hexdigit()),
            "hash hex: {d}"
        );
    }

    #[test]
    fn speedtest1_synthetic_shapes() {
        let log = "   1 - 100000 INSERTs........................     1.234s\n\
                   \x20\x20\x20\x20\x20\x20\x20TOTAL..................................    12.345s\n\
                   Verification Hash: 15286278048446160954 8f14e45fceea167a5a36dedd4bea2543f8f14e45fceea167a5a36dedd\n\
                   real\t0m 12.90s\n\
                   user\t0m 11.01s\n\
                   sys\t0m  0.88s\n";
        let parsed = parse_speedtest1(log).expect("parse");
        let get = |k: &str| {
            parsed
                .metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get("wall_s"), 12.345);
        assert_eq!(get("user_s"), 11.01);
        assert_eq!(get("sys_s"), 0.88);
        // minutes > 9 keep parsing
        let long = "TOTAL....  677.378s\nuser\t12m 05.67s\nsys\t1m 02.00s\n";
        let p2 = parse_speedtest1(long).expect("parse long");
        let get2 = |k: &str| {
            p2.metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get2("user_s"), 725.67);
        assert_eq!(get2("sys_s"), 62.0);
    }

    #[test]
    fn speedtest1_missing_pieces_error() {
        // no TOTAL at all
        assert!(parse_speedtest1("run started\nTime: 1.0 s\n").is_err());
        // TOTAL but no busybox CPU lines (driver forgot the time wrapper)
        assert!(parse_speedtest1("TOTAL....  1.000s\n").is_err());
        // TOTAL with no number on the line
        assert!(parse_speedtest1("TOTAL....\nuser\t0m 1.00s\nsys\t0m 0.10s\n").is_err());
    }

    #[test]
    fn num_prefix_stops_at_units() {
        assert_eq!(num_prefix("1234.56"), Some("1234.56"));
        assert_eq!(num_prefix("83333.33 ops/sec"), Some("83333.33"));
        assert_eq!(num_prefix("ms"), None);
        assert_eq!(num_prefix("-1.5e3x"), Some("-1.5e3"));
    }

    #[test]
    fn duckdb_tpch_real_fixture() {
        // Real captured driver log (card NB-3's smoke run; the file's
        // first line names the exact DuckDB build). The parser must find
        // all five summary lines and report the row's contract: 22
        // queries actually ran.
        let log = include_str!("fixtures/duckdb-tpch-run.txt");
        let parsed = parse_duckdb_tpch(log).expect("parse");
        let get = |k: &str| {
            parsed
                .metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get("queries"), 22.0);
        assert!(get("wall_s") > 0.0);
        assert!(get("user_s") > 0.0);
        assert!(get("sys_s") >= 0.0);
        assert!(get("rss_kb") > 0.0);
    }

    #[test]
    fn duckdb_tpch_synthetic_shapes() {
        let log = "TPCH_QUERY q01 0.123\n\
                   TPCH_QUERY q02 0.456\n\
                   TPCH_TOTAL_WALL_S 12.345\n\
                   TPCH_TOTAL_USER_S 11.010\n\
                   TPCH_TOTAL_SYS_S 0.880\n\
                   TPCH_MAX_RSS_KB 123456\n\
                   TPCH_QUERIES 22\n";
        let parsed = parse_duckdb_tpch(log).expect("parse");
        let get = |k: &str| {
            parsed
                .metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get("wall_s"), 12.345);
        assert_eq!(get("user_s"), 11.010);
        assert_eq!(get("sys_s"), 0.880);
        assert_eq!(get("rss_kb"), 123456.0);
        assert_eq!(get("queries"), 22.0);
        // the last occurrence of a tag wins (a re-printed summary must
        // not resurrect an earlier, stale value)
        let dup = "TPCH_TOTAL_WALL_S 1.000\nTPCH_TOTAL_WALL_S 2.000\n\
                   TPCH_TOTAL_USER_S 1.0\nTPCH_TOTAL_SYS_S 0.1\n\
                   TPCH_MAX_RSS_KB 1\nTPCH_QUERIES 22\n";
        let parsed = parse_duckdb_tpch(dup).expect("parse");
        assert_eq!(
            parsed
                .metrics
                .iter()
                .find(|(m, _)| m == "wall_s")
                .map(|(_, v)| *v),
            Some(2.0)
        );
    }

    #[test]
    fn duckdb_tpch_missing_lines_error() {
        // A metric that cannot be found is an error, never a zero: a
        // driver that forgot its time wrapper or RSS line fails the run
        // instead of publishing a wrong number (§5.2 rule 8).
        assert!(parse_duckdb_tpch("run started\n").is_err());
        // one missing summary line (no RSS) is an error
        let no_rss = "TPCH_TOTAL_WALL_S 1.0\nTPCH_TOTAL_USER_S 1.0\n\
                      TPCH_TOTAL_SYS_S 0.1\nTPCH_QUERIES 22\n";
        assert!(parse_duckdb_tpch(no_rss).is_err());
        // a summary line with no number is an error
        let bad = "TPCH_TOTAL_WALL_S \nTPCH_TOTAL_USER_S 1.0\n\
                   TPCH_TOTAL_SYS_S 0.1\nTPCH_MAX_RSS_KB 1\nTPCH_QUERIES 22\n";
        assert!(parse_duckdb_tpch(bad).is_err());
    }
}

#[cfg(test)]
mod probe {
    use super::*;
    #[test]
    fn probe_real_failing_log() {
        let log = include_str!("fixtures/memtier-run2.txt");
        let parsed = parse_memtier(log);
        assert!(parsed.is_ok(), "probe failed: {:?}", parsed.err());
    }
}

/// Tests that execute the row's own driver (card NB-2 rework 1: verdict
/// NB-2 R1 and R2).
///
/// `oltp-ycsb-pg17` publishes driver-side: `workloads/oltp-ycsb-pg17/
/// driver.sh` writes the NB_METRICS JSON the runner reads, and the same
/// script is what refuses a pass with failed operations and what
/// restores the dataset an earlier pass grew; `digest.sh` hashes the
/// server's own row count. The only honest test of those code paths
/// runs the real scripts. Each test executes one with a
/// cleared environment whose PATH starts with a stub `nb-podman` — no
/// call can reach the real wrapper, `systemd-run` or a container —
/// which records its arguments and replays a recorded YCSB log in place
/// of the client. The driver is bash, so this is unix-only.
///
/// The fixtures are all real output of YCSB Client 0.17.0; the `-db`
/// class on each file's `Command line:` line says how it was produced:
/// - `ycsb-run.txt` and `ycsb-load.txt`: the pinned image against
///   PostgreSQL 17 through JDBC (`site.ycsb.db.JdbcDBClient`) — the
///   run and the load phase of the 2026-11-27 smoke-size passes;
/// - `ycsb-run-d.txt`, `ycsb-run-d-failed.txt`, `ycsb-run-e.txt`,
///   `ycsb-run-f.txt`: the same client from the pinned tarball, run
///   offline with `fixtures/ycsb-standin/StandinTable.java` (`-db
///   StandinTable`), a file-backed table whose insert returns ERROR on
///   a duplicate key, as the JDBC binding does on PostgreSQL's unique
///   violation. The report shapes, the `Return=` and the `-FAILED`
///   lines are the client's own; the latencies are the stand-in's and
///   mean nothing. `ycsb-run-d-failed.txt` is a second workload-D pass
///   on the table the first pass had grown — the failure the verdict
///   found on the real row.
#[cfg(all(test, unix))]
mod driver {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    /// Recorded logs the stub replays, by YCSB phase.
    const RUN_A: &str = include_str!("fixtures/ycsb-run.txt");
    const LOAD: &str = include_str!("fixtures/ycsb-load.txt");
    const RUN_D: &str = include_str!("fixtures/ycsb-run-d.txt");
    const RUN_D_FAILED: &str = include_str!("fixtures/ycsb-run-d-failed.txt");
    const RUN_E: &str = include_str!("fixtures/ycsb-run-e.txt");
    const RUN_F: &str = include_str!("fixtures/ycsb-run-f.txt");

    /// The stand-in for `bin/nb-podman`. Every call is appended to
    /// `$NB_STUB_CALLS` (one line, arguments joined by spaces) and each
    /// kind of call the driver makes is answered from the test's
    /// variables instead of starting a container.
    const STUB: &str = r#"#!/usr/bin/env bash
printf '%s\n' "$*" >> "${NB_STUB_CALLS:?}"
case ${1:-} in
  exec)
    case $* in
      *"postgres --version"*) printf 'postgres (PostgreSQL) %s\n' "${NB_STUB_PG_VERSION:?}" ;;
      *"SELECT version()"*) printf 'PostgreSQL %s on x86_64-pc-linux-gnu\n' "${NB_STUB_PG_VERSION}" ;;
      *"SELECT count(*) FROM usertable"*) printf '%s\n' "${NB_STUB_ROWS:?}" ;;
    esac
    ;;
  run)
    case $* in
      *"java -version"*)
        printf 'openjdk version "%s"\nYCSB release: ycsb-0.17.0\n' "${NB_STUB_JAVA_VERSION:?}" ;;
      *" load jdbc "*) cat "${NB_STUB_LOAD:?}" ;;
      *" run jdbc "*) cat "${NB_STUB_RUN:?}" ;;
    esac
    ;;
esac
"#;

    /// What one driver execution is given.
    struct Scenario {
        /// `NB_MODE`: init, reinit, cold, warmup or run.
        mode: &'static str,
        /// `NB_SERIES` (`k=v,k=v`; empty for init).
        series: &'static str,
        /// The server's answer to `SELECT count(*) FROM usertable`.
        rows: &'static str,
        /// The log replayed for a YCSB `load` call.
        load: String,
        /// The log replayed for a YCSB `run` call.
        run: String,
    }

    impl Scenario {
        /// A measured pass of `series` over a table of `rows` rows.
        fn measured(
            mode: &'static str,
            series: &'static str,
            rows: &'static str,
            run: &str,
        ) -> Self {
            Scenario {
                mode,
                series,
                rows,
                load: LOAD.to_string(),
                run: run.to_string(),
            }
        }

        /// init or reinit: the load phase, no measured pass.
        fn load_phase(mode: &'static str, series: &'static str, load: &str) -> Self {
            Scenario {
                mode,
                series,
                rows: "",
                load: load.to_string(),
                run: String::new(),
            }
        }
    }

    /// What one driver execution produced.
    struct Outcome {
        success: bool,
        stderr: String,
        /// The stub's calls, in order.
        calls: Vec<String>,
        /// `NB_LOG`, the tee'd pass log.
        log: String,
        /// `NB_METRICS`, when the driver wrote it.
        metrics: Option<String>,
        /// `${NB_LOG}.reload`, when the driver reloaded the dataset.
        reload_log: Option<String>,
        /// `versions.txt`, when the driver wrote it.
        versions: Option<String>,
    }

    impl Outcome {
        /// Index of the first recorded call whose arguments contain
        /// `needle`.
        fn call_index(&self, needle: &str) -> Option<usize> {
            self.calls.iter().position(|c| c.contains(needle))
        }

        fn has_call(&self, needle: &str) -> bool {
            self.call_index(needle).is_some()
        }
    }

    fn read_if_exists(path: &Path) -> Option<String> {
        std::fs::read_to_string(path).ok()
    }

    // The helpers below return their failures instead of unwrapping:
    // the crate's lint policy (`expect_used = "deny"`, relaxed only
    // inside `#[test]` bodies) applies to them too.

    /// A private, initially empty directory for one execution.
    fn tmpdir(name: &str) -> std::result::Result<PathBuf, String> {
        let dir = std::env::temp_dir()
            .join("nbench-tests")
            .join(format!("{}-ycsb-driver-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
        Ok(dir)
    }

    /// Write the [`STUB`] into `dir` and return its directory, to go
    /// first on PATH.
    fn stub_bin(dir: &Path) -> std::result::Result<PathBuf, String> {
        let io = |e: std::io::Error| e.to_string();
        let bin = dir.join("bin");
        std::fs::create_dir_all(&bin).map_err(io)?;
        let stub = bin.join("nb-podman");
        std::fs::write(&stub, STUB).map_err(io)?;
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).map_err(io)?;
        Ok(bin)
    }

    /// Execute the real `driver.sh` once under `sc`, the way the
    /// runner does (its `exec_driver` environment), with `nb-podman`
    /// replaced by [`STUB`].
    fn run_driver(
        name: &str,
        sc: &Scenario,
        recordcount: &str,
        operations: &str,
    ) -> std::result::Result<Outcome, String> {
        let io = |e: std::io::Error| e.to_string();
        let dir = tmpdir(name)?;
        let bin = stub_bin(&dir)?;
        let stub_load = dir.join("stub-load.log");
        let stub_run = dir.join("stub-run.log");
        std::fs::write(&stub_load, &sc.load).map_err(io)?;
        std::fs::write(&stub_run, &sc.run).map_err(io)?;
        let log = dir.join("pass.log");
        let metrics = dir.join("pass.metrics.json");
        let calls = dir.join("calls.txt");
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let driver = root
            .join("workloads")
            .join("oltp-ycsb-pg17")
            .join("driver.sh");

        let out = Command::new("bash")
            .arg(&driver)
            .env_clear()
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    bin.display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .env("NB_HARNESS", root)
            .env("NB_CONTAINER", "nbench-postgres17-stub")
            .env("NB_PORT", "5432")
            .env("NB_DB", "nbench")
            .env("NB_MODE", sc.mode)
            .env("NB_SERIES", sc.series)
            .env("NB_RUN_INDEX", "0")
            .env("NB_LOG", &log)
            .env("NB_METRICS", &metrics)
            .env("NB_OUT", &dir)
            .env("NB_P_RECORDCOUNT", recordcount)
            .env("NB_P_YCSB_OPERATIONS", operations)
            .env("NB_STUB_CALLS", &calls)
            .env("NB_STUB_ROWS", sc.rows)
            .env("NB_STUB_LOAD", &stub_load)
            .env("NB_STUB_RUN", &stub_run)
            .env("NB_STUB_PG_VERSION", "17.11-stub")
            .env("NB_STUB_JAVA_VERSION", "17.0.21-stub")
            .output()
            .map_err(io)?;

        Ok(Outcome {
            success: out.status.success(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            calls: read_if_exists(&calls)
                .unwrap_or_default()
                .lines()
                .map(str::to_string)
                .collect(),
            log: read_if_exists(&log).unwrap_or_default(),
            metrics: read_if_exists(&metrics),
            reload_log: read_if_exists(&PathBuf::from(format!("{}.reload", log.display()))),
            versions: read_if_exists(&dir.join("versions.txt")),
        })
    }

    /// The numeric pairs of an NB_METRICS file, in file order — the
    /// same reading the runner's `parse_result` gives them.
    fn metrics_of(text: &str) -> std::result::Result<Vec<(String, f64)>, String> {
        let v = crate::json::parse(text).map_err(|e| e.to_string())?;
        Ok(v.pairs()
            .unwrap_or(&[])
            .iter()
            .filter_map(|(k, val)| val.as_f64().map(|n| (k.clone(), n)))
            .collect())
    }

    /// The recorded YCSB run call's arguments.
    fn run_call(out: &Outcome) -> Option<String> {
        out.calls
            .iter()
            .find(|c| c.contains(" run jdbc -s "))
            .cloned()
    }

    /// Execute the real `digest.sh` once, with the stub answering the
    /// row count; returns the digest it printed.
    fn run_digest(rows: &str) -> std::result::Result<String, String> {
        let io = |e: std::io::Error| e.to_string();
        let dir = tmpdir("digest")?;
        let bin = stub_bin(&dir)?;
        let calls = dir.join("calls.txt");
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let out = Command::new("bash")
            .arg(
                root.join("workloads")
                    .join("oltp-ycsb-pg17")
                    .join("digest.sh"),
            )
            .env_clear()
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    bin.display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .env("NB_HARNESS", root)
            .env("NB_CONTAINER", "nbench-postgres17-stub")
            .env("NB_DB", "nbench")
            .env("NB_PHASE", "init")
            .env("NB_OUT", &dir)
            .env("NB_LOG", dir.join("digest.log"))
            .env("NB_STUB_CALLS", &calls)
            .env("NB_STUB_ROWS", rows)
            .output()
            .map_err(io)?;
        if !out.status.success() {
            return Err(format!(
                "digest.sh exited {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    #[test]
    fn digest_hashes_the_servers_own_row_count() {
        // digest.sh is one pipeline: the server's `SELECT count(*)`
        // line goes through sha256sum unchanged, so the digest of a
        // table is the hash of its count line (`printf '1000\n' |
        // sha256sum`). The record-then-match protocol cannot see a
        // digest of a fixed string within one run (card NB-2 mutant
        // DIGEST-CONSTANT); executing the script can. 83c02ac2… is
        // also the smoke's recorded init expectation.
        for (rows, expected) in [
            (
                "1000",
                "83c02ac2d48c863dab2ccf6870455aadfc2cec073b8db269b517c879d76aa6d9",
            ),
            (
                "1500",
                "d6e02e548a55f01b58ebb4addbe8e78c0a67198d09c6a41af9c6e6f640d7f4dd",
            ),
        ] {
            let got = run_digest(rows).expect("run digest.sh");
            assert_eq!(got.trim(), expected, "digest of a {rows}-row table");
        }
    }

    #[test]
    fn driver_publishes_the_reference_parsers_numbers() {
        // Verdict NB-2 R1: the row's published numbers come from the
        // driver's own sed extraction, so that extraction — not only
        // the reference parser — is under test. Applied to the driver,
        // the card's PARSER-OPS-DROPPED (no ops line: the driver dies
        // on an unbound variable) and PARSER-P99-AS-AVG (p99 read from
        // AverageLatency) both fail here, in every measured mode.
        let expected = parse_ycsb(RUN_A).expect("reference parse").metrics;
        for mode in ["cold", "warmup", "run"] {
            let sc = Scenario::measured(mode, "workload=a,threads=1", "1000", RUN_A);
            let out = run_driver("publish", &sc, "1000", "1000").expect("run the driver");
            assert!(out.success, "{mode}: driver failed: {}", out.stderr);
            let text = out.metrics.as_deref().expect("NB_METRICS written");
            assert_eq!(
                metrics_of(text).expect("metrics json"),
                expected,
                "{mode}: the published metrics must equal the reference parser's"
            );
            // the log's own tokens, verbatim
            for token in [
                "\"ops_sec\":2801.1204481792715",
                "\"avg_read_us\":127.0767754318618",
                "\"p99_read_us\":440",
            ] {
                assert!(text.contains(token), "{mode}: {text} lacks {token}");
            }
            // the 2>&1 | tee contract: NB_LOG is the pass's own output
            assert_eq!(out.log, RUN_A, "{mode}: NB_LOG must hold the tee'd run");
            // a pristine table (rows == recordcount): no reload, then
            // the pass with the series point and the sizes
            assert!(
                !out.has_call(" load jdbc "),
                "{mode}: no reload on a pristine table"
            );
            assert!(
                !out.has_call("TRUNCATE"),
                "{mode}: no truncate on a pristine table"
            );
            assert!(
                out.reload_log.is_none(),
                "{mode}: no reload log on a pristine table"
            );
            let run = run_call(&out).expect("the pass ran");
            for arg in [
                "-P /opt/ycsb/workloads/workloada",
                "-p recordcount=1000",
                "-p operationcount=1000",
                "-p threadcount=1",
                "db.url=jdbc:postgresql://127.0.0.1:5432/nbench",
                "db.driver=org.postgresql.Driver",
            ] {
                assert!(
                    run.contains(arg),
                    "{mode}: YCSB run call lacks {arg}: {run}"
                );
            }
        }
    }

    #[test]
    fn driver_publishes_only_the_metrics_a_mix_carries() {
        // E (scan-heavy) carries no [READ]: the driver publishes ops_sec
        // alone, exactly as the reference parser reads the same report.
        // The series point reaches YCSB: workload file, thread count
        // and both sizes.
        let sc = Scenario::measured("run", "workload=e,threads=64", "1500", RUN_E);
        let out = run_driver("mix-e", &sc, "1500", "5000").expect("run the driver");
        assert!(out.success, "driver failed: {}", out.stderr);
        let text = out.metrics.as_deref().expect("NB_METRICS written");
        assert_eq!(
            metrics_of(text).expect("metrics json"),
            parse_ycsb(RUN_E).expect("parse E").metrics
        );
        assert_eq!(
            text, "{\"ops_sec\":55555.555555555555}\n",
            "no read metric invented"
        );
        let run = run_call(&out).expect("the pass ran");
        for arg in [
            "-P /opt/ycsb/workloads/workloade",
            "-p recordcount=1500",
            "-p operationcount=5000",
            "-p threadcount=64",
        ] {
            assert!(run.contains(arg), "YCSB run call lacks {arg}: {run}");
        }
        assert!(
            !out.has_call(" load jdbc "),
            "1500 rows is a pristine table at recordcount 1500"
        );

        // F reads and inserts nothing: all three metrics, the [READ]
        // section's own (not the [READ-MODIFY-WRITE] latencies).
        let sc = Scenario::measured("run", "workload=f,threads=1", "1000", RUN_F);
        let out = run_driver("mix-f", &sc, "1000", "1000").expect("run the driver");
        assert!(out.success, "driver failed: {}", out.stderr);
        let text = out.metrics.as_deref().expect("NB_METRICS written");
        let got = metrics_of(text).expect("metrics json");
        assert_eq!(got, parse_ycsb(RUN_F).expect("parse F").metrics);
        let names: Vec<&str> = got.iter().map(|(m, _)| m.as_str()).collect();
        assert_eq!(names, vec!["ops_sec", "avg_read_us", "p99_read_us"]);
        assert_eq!(
            text,
            "{\"ops_sec\":29411.764705882353,\"avg_read_us\":0.36,\"p99_read_us\":0}\n"
        );
    }

    #[test]
    fn driver_fails_a_pass_whose_operations_failed() {
        // Verdict NB-2 R2: YCSB exits 0 with failed operations (this
        // fixture: a second workload-D pass re-inserting the keys the
        // first pass created), and its throughput line still looks
        // healthy. Such a pass must never be published: the driver
        // fails it, names the report's own lines as the reason and
        // writes no NB_METRICS file. Mutant RETURN-ERROR-IGNORED (the
        // guard not called, or its condition inverted) fails here.
        let sc = Scenario::measured("run", "workload=d,threads=1", "1000", RUN_D_FAILED);
        let out = run_driver("failed", &sc, "1000", "1000").expect("run the driver");
        assert!(
            !out.success,
            "a pass with failed operations must fail the driver"
        );
        assert!(
            out.metrics.is_none(),
            "no metrics may be published: {:?}",
            out.metrics
        );
        assert!(
            out.stderr.contains("[INSERT], Return=ERROR, 44"),
            "names the failed return code: {}",
            out.stderr
        );
        assert!(
            out.stderr.contains("[INSERT-FAILED], Operations, 44"),
            "names the FAILED section: {}",
            out.stderr
        );
        // the pass log is kept, so the reason stays reachable
        assert_eq!(out.log, RUN_D_FAILED);

        // A report with no successful operation at all is not a
        // measurement either (the recorded report minus its Return=OK
        // lines; it still carries a throughput line to publish).
        let no_ok = RUN_A
            .lines()
            .filter(|l| !l.contains(", Return=OK, "))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(no_ok.contains("Throughput(ops/sec)"));
        let sc = Scenario::measured("run", "workload=a,threads=1", "1000", &no_ok);
        let out = run_driver("no-ok", &sc, "1000", "1000").expect("run the driver");
        assert!(
            !out.success,
            "a report without one successful operation must fail"
        );
        assert!(
            out.metrics.is_none(),
            "nothing published: {:?}",
            out.metrics
        );
        assert!(
            out.stderr.contains("no successful operation"),
            "names the reason: {}",
            out.stderr
        );

        // A report whose only Return=OK lines count zero operations is
        // the same case (the recorded report with its OK counts
        // zeroed).
        let zero_ok = RUN_A
            .replace("Return=OK, 521", "Return=OK, 0")
            .replace("Return=OK, 479", "Return=OK, 0");
        let sc = Scenario::measured("run", "workload=a,threads=1", "1000", &zero_ok);
        let out = run_driver("zero-ok", &sc, "1000", "1000").expect("run the driver");
        assert!(
            !out.success,
            "a report of zero successful operations must fail"
        );
        assert!(
            out.metrics.is_none(),
            "nothing published: {:?}",
            out.metrics
        );
        assert!(
            out.stderr.contains("no successful operation"),
            "names the reason: {}",
            out.stderr
        );

        // A missing or empty report is not a successful one either.
        let sc = Scenario::measured("run", "workload=a,threads=1", "1000", "");
        let out = run_driver("empty", &sc, "1000", "1000").expect("run the driver");
        assert!(!out.success, "an empty report must fail the pass");
        assert!(
            out.metrics.is_none(),
            "nothing published: {:?}",
            out.metrics
        );
        assert!(
            out.stderr.contains("missing or empty"),
            "names the reason: {}",
            out.stderr
        );
    }

    #[test]
    fn driver_reloads_before_a_pass_an_earlier_pass_grew() {
        // Verdict NB-2 R2, the cause: every `ycsb run` restarts its
        // insert key sequence at recordcount, so a pass that follows
        // one that inserted (stock D and E, 5 %) must start from the
        // digested dataset again. The row count is the one thing a pass
        // changes and exactly what the init digest pins: more rows than
        // recordcount -> truncate + reload, then the pass. (1044 is the
        // fixture's own first pass: 1 000 loaded + 44 inserted.)
        let sc = Scenario::measured("warmup", "workload=d,threads=1", "1044", RUN_D);
        let out = run_driver("reload", &sc, "1000", "1000").expect("run the driver");
        assert!(out.success, "driver failed: {}", out.stderr);
        let count = out
            .call_index("SELECT count(*) FROM usertable")
            .expect("row count asked");
        let truncate = out
            .call_index("TRUNCATE usertable")
            .expect("table truncated");
        let load = out.call_index(" load jdbc ").expect("dataset reloaded");
        let run = out.call_index(" run jdbc -s ").expect("the pass ran");
        assert!(
            count < truncate && truncate < load && load < run,
            "restore order is count, truncate, reload, run: {:?}",
            out.calls
        );
        let load_call = &out.calls[load];
        for arg in [
            "-P /opt/ycsb/workloads/workloadd",
            "-p recordcount=1000",
            "-p threadcount=1",
        ] {
            assert!(
                load_call.contains(arg),
                "reload call lacks {arg}: {load_call}"
            );
        }
        // the reload's log is its own; the pass log is the pass's
        assert_eq!(out.reload_log.as_deref(), Some(LOAD));
        assert_eq!(out.log, RUN_D);
        let text = out.metrics.as_deref().expect("NB_METRICS written");
        assert_eq!(
            metrics_of(text).expect("metrics json"),
            parse_ycsb(RUN_D).expect("parse D").metrics
        );
        assert!(
            out.stderr.contains("1044 rows, more than recordcount=1000"),
            "the reload is on record: {}",
            out.stderr
        );

        // A reload whose own load reports failures is refused before
        // the pass runs.
        let mut sc = Scenario::measured("run", "workload=d,threads=1", "1044", RUN_D);
        sc.load = RUN_D_FAILED.to_string();
        let out = run_driver("reload-failed", &sc, "1000", "1000").expect("run the driver");
        assert!(!out.success, "a failed reload must fail the pass");
        assert!(
            !out.has_call(" run jdbc -s "),
            "the pass must not run on a failed reload"
        );
        assert!(
            out.metrics.is_none(),
            "nothing published: {:?}",
            out.metrics
        );
        assert!(
            out.stderr.contains("failed operations") && out.stderr.contains("Return=ERROR, 44"),
            "names the reload's failed operations: {}",
            out.stderr
        );

        // Fewer rows than recordcount is a lost dataset — never
        // reloaded away silently.
        let sc = Scenario::measured("run", "workload=d,threads=1", "999", RUN_D);
        let out = run_driver("shrink", &sc, "1000", "1000").expect("run the driver");
        assert!(!out.success, "a shrunken dataset must fail the pass");
        assert!(
            !out.has_call("TRUNCATE") && !out.has_call(" load jdbc "),
            "no reload: {:?}",
            out.calls
        );
        assert!(!out.has_call(" run jdbc -s "), "the pass must not run");
        assert!(
            out.metrics.is_none(),
            "nothing published: {:?}",
            out.metrics
        );
        assert!(
            out.stderr.contains("999 rows, fewer than recordcount=1000"),
            "names the reason: {}",
            out.stderr
        );

        // Anything but a row count from the server is refused before
        // the pass runs, never read as a table state.
        let sc = Scenario::measured("run", "workload=d,threads=1", "1 044", RUN_D);
        let out = run_driver("count-garbage", &sc, "1000", "1000").expect("run the driver");
        assert!(!out.success, "a non-numeric row count must fail the pass");
        assert!(
            !out.has_call(" load jdbc ") && !out.has_call(" run jdbc -s "),
            "{:?}",
            out.calls
        );
        assert!(
            out.stderr.contains("not a row count"),
            "names the reason: {}",
            out.stderr
        );
    }

    #[test]
    fn driver_init_records_versions_then_loads() {
        // Provenance (§5.2 rule 6): a row whose init mode writes no
        // versions.txt fails loudly in the runner (card NB-2 mutant
        // VERSIONS-SKIPPED, which the Host smoke catches at row level;
        // this is the same catcher on the driver alone). The file must
        // carry the server binary's, the server's SQL, the client JVM's
        // and the YCSB release's version.
        let sc = Scenario::load_phase("init", "", LOAD);
        let out = run_driver("init", &sc, "1000", "1000").expect("run the driver");
        assert!(out.success, "driver failed: {}", out.stderr);
        let versions = out.versions.as_deref().expect("versions.txt written");
        for line in [
            "postgres (PostgreSQL) 17.11-stub",
            "PostgreSQL 17.11-stub on x86_64-pc-linux-gnu",
            "openjdk version \"17.0.21-stub\"",
            "YCSB release: ycsb-0.17.0",
        ] {
            assert!(
                versions.contains(line),
                "versions.txt lacks {line}: {versions}"
            );
        }
        // the DDL precedes the load; init has no series, so the load
        // falls back to workloada and writes no metrics
        let ddl = out
            .call_index("CREATE TABLE IF NOT EXISTS usertable")
            .expect("ddl");
        let load = out.call_index(" load jdbc ").expect("load");
        assert!(ddl < load, "ddl before load: {:?}", out.calls);
        let load_call = &out.calls[load];
        for arg in [
            "-P /opt/ycsb/workloads/workloada",
            "-p recordcount=1000",
            "-p threadcount=1",
        ] {
            assert!(
                load_call.contains(arg),
                "load call lacks {arg}: {load_call}"
            );
        }
        assert!(out.metrics.is_none(), "init writes no metrics");

        // reinit is the same, with the series' own workload file
        let sc = Scenario::load_phase("reinit", "workload=d,threads=16", LOAD);
        let out = run_driver("reinit", &sc, "1000", "1000").expect("run the driver");
        assert!(out.success, "driver failed: {}", out.stderr);
        assert!(out.versions.is_some(), "reinit rewrites versions.txt");
        let load = out.call_index(" load jdbc ").expect("load");
        assert!(
            out.calls[load].contains("-P /opt/ycsb/workloads/workloadd"),
            "the series' workload file: {}",
            out.calls[load]
        );

        // a load that reports failed operations is refused too — the
        // strict digest would also void the row; this names the reason
        let sc = Scenario::load_phase("init", "", RUN_D_FAILED);
        let out = run_driver("init-failed", &sc, "1000", "1000").expect("run the driver");
        assert!(!out.success, "a load with failed operations must fail");
        assert!(out.stderr.contains("Return=ERROR, 44"), "{}", out.stderr);
    }
}
