//! Raw output parsers for the benchmark tools.
//!
//! Invariant owned here: parsing never panics and never invents numbers —
//! a metric that cannot be found is an error, so a silent format change in
//! a tool fails the run instead of publishing a wrong number (PLAN-v3
//! §5.2 rule 8: losses and wins are published, but never wrong numbers).
//!
//! The fixtures in `src/fixtures/` are real captured outputs (see each
//! fixture's first line for the tool version).

use crate::error::{Error, Result};
use std::path::Path;

/// Metrics extracted from one execution of a tool.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ParsedRun {
    /// Named numeric metrics (tps, p99_ms, ...).
    pub metrics: Vec<(String, f64)>,
    /// Optional digest the tool itself printed (speedtest1 `--sha3`).
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

/// Parse a memtier_benchmark "Totals" block.
///
/// ```text
/// Totals
/// ------------------------------------------------------------
/// Number of Hits:              5000000
/// Number of Misses:            0
/// Number of Updates:           500000
/// ...
/// Throughput:                  83333.33 ops/sec
/// Throughput:                  2666.67 MB/sec
/// ...
/// Latency Distribution
/// ------------------------------------------------------------
/// 50.000% <= 1.234 milliseconds
/// 99.900% <= 5.678 milliseconds
/// ```
///
/// Newer memtier (2.x) prints `Throughput (ops/sec)` and percentile lines
/// like `p50.000% [  1.234 ms]`; both shapes are accepted.
pub fn parse_memtier(log: &str) -> Result<ParsedRun> {
    let totals_start = log.rfind("Totals").ok_or_else(|| Error::Parse {
        what: "memtier",
        line: "no Totals block".into(),
    })?;
    let section = &log[totals_start..];
    let lines: Vec<&str> = section.lines().collect();

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

/// Parse speedtest1 output: the final timing table row `TOTAL` plus the
/// `SHA3 Digest:` line when `--sha3` was passed.
///
/// ```text
///   ...
///   Totals:
///              12.342 seconds
///   TOTAL          10.111   0.222   10.333   10.334   1287090816
///   SHA3 Digest: 8f14e45fceea167a5a36dedd4bea2543f...
/// ```
///
/// The exact column layout varies by SQLite version; the first four
/// numbers on the TOTAL line are user / system / total / wall seconds.
pub fn parse_speedtest1(log: &str) -> Result<ParsedRun> {
    let mut total_line = None;
    let mut digest = None;
    for line in log.lines() {
        let t = line.trim();
        if t.starts_with("TOTAL") && total_line.is_none() {
            total_line = Some(t.to_string());
        }
        if t.starts_with("SHA3 Digest:") {
            digest = t.get("SHA3 Digest:".len()..).map(|s| s.trim().to_string());
        }
    }
    let line = total_line.ok_or_else(|| Error::Parse {
        what: "speedtest1",
        line: "no TOTAL line".into(),
    })?;
    let nums: Vec<f64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|tok| num_prefix(tok).and_then(|n| n.parse::<f64>().ok()))
        .collect();
    if nums.len() < 4 {
        return Err(Error::Parse {
            what: "speedtest1 TOTAL",
            line,
        });
    }
    Ok(ParsedRun {
        metrics: vec![
            ("user_s".to_string(), nums[0]),
            ("sys_s".to_string(), nums[1]),
            ("wall_s".to_string(), nums[3]),
        ],
        digest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pgbench_summary_real_format() {
        let log = include_str!("fixtures/pgbench-run.txt");
        let parsed = parse_pgbench_summary(log).expect("parse");
        let tps = parsed
            .metrics
            .iter()
            .find(|(k, _)| k == "tps")
            .map(|(_, v)| *v)
            .expect("tps");
        assert!(tps > 1000.0 && tps < 2000.0, "unexpected tps {tps}");
        let avg = parsed
            .metrics
            .iter()
            .find(|(k, _)| k == "latency_avg_ms")
            .map(|(_, v)| *v)
            .expect("avg");
        assert!(avg > 10.0 && avg < 15.0, "unexpected avg {avg}");
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
    fn memtier_totals_both_shapes() {
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
    }

    #[test]
    fn speedtest1_total_and_digest() {
        let log = "...lots of output...\n\
                   TOTAL         10.111   0.222   10.333   10.334   1287090816\n\
                   SHA3 Digest: 8f14e45fceea167a5a36dedd4bea2543f\n";
        let parsed = parse_speedtest1(log).expect("parse");
        let get = |k: &str| {
            parsed
                .metrics
                .iter()
                .find(|(m, _)| m == k)
                .map(|(_, v)| *v)
                .expect("metric")
        };
        assert_eq!(get("user_s"), 10.111);
        assert_eq!(get("sys_s"), 0.222);
        assert_eq!(get("wall_s"), 10.334);
        assert_eq!(
            parsed.digest.as_deref(),
            Some("8f14e45fceea167a5a36dedd4bea2543f")
        );
    }

    #[test]
    fn speedtest1_without_total_errors() {
        assert!(parse_speedtest1("run started\nTime: 1.0 s\n").is_err());
    }

    #[test]
    fn num_prefix_stops_at_units() {
        assert_eq!(num_prefix("1234.56"), Some("1234.56"));
        assert_eq!(num_prefix("83333.33 ops/sec"), Some("83333.33"));
        assert_eq!(num_prefix("ms"), None);
        assert_eq!(num_prefix("-1.5e3x"), Some("-1.5e3"));
    }
}
