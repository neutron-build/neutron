//! Markdown report generation (card B-0 Work item 5).
//!
//! Invariants owned here:
//! - The report prints a ratio and a §5.3 label (**beats** / **competitive**
//!   / **behind**) only when a Nucleus number exists for the same
//!   (row, config, metric); otherwise the Nucleus column says `n/a`
//!   (PLAN-v3 §5.3; Nucleus columns arrive with cards C-9/D-*).
//! - Latency-class metrics are ratios of competitor/nucleus so that
//!   "higher ratio is better" holds uniformly; the direction comes from
//!   the row registry, not from a per-report judgment.
//! - A voided run (digest mismatch) is rendered as `VOID`, never silently
//!   dropped (§5.2 rule 4).

use crate::error::Result;
use crate::json::Value;
use crate::rows::{Direction, ROWS, RowDef, label_for_ratio};
use std::fmt::Write as _;

/// Format a metric value for the table.
fn fmt_num(v: f64) -> String {
    if v.abs() >= 1000.0 {
        format!("{v:.0}")
    } else if v.abs() >= 10.0 {
        format!("{v:.1}")
    } else {
        format!("{v:.3}")
    }
}

/// Ratio and label for one metric comparison.
///
/// Throughput: nucleus/competitor. Latency: competitor/nucleus. In both
/// cases >= 1.0 means Nucleus is at least as good.
pub fn ratio_and_label(
    nucleus: Option<f64>,
    competitor: f64,
    dir: Direction,
) -> (Option<f64>, &'static str) {
    let Some(n) = nucleus else {
        return (None, "n/a");
    };
    if competitor == 0.0 {
        return (None, "n/a");
    }
    let ratio = match dir {
        Direction::HigherIsBetter => n / competitor,
        Direction::LowerIsBetter => competitor / n,
    };
    (Some(ratio), label_for_ratio(ratio))
}

fn config_str(config: &Value) -> String {
    config
        .as_arr()
        .map(|pairs| {
            pairs
                .iter()
                .filter_map(|p| {
                    let k = p.as_arr().and_then(|a| a.first()).and_then(Value::as_str);
                    let v = p.as_arr().and_then(|a| a.get(1)).and_then(Value::as_str);
                    k.zip(v).map(|(k, v)| format!("{k}={v}"))
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default()
}

fn find_row(id: &str) -> Option<&'static RowDef> {
    ROWS.iter().find(|r| r.id == id)
}

/// Render one results file (one row) into table lines appended to `out`.
fn render_row(out: &mut String, v: &Value) -> Result<()> {
    let row_id = v.get("row").and_then(Value::as_str).unwrap_or("?");
    let system = v.get("system").and_then(Value::as_str).unwrap_or("?");
    let row = find_row(row_id);
    let desc = row.map(|r| r.description).unwrap_or("");
    let void = v
        .get("digest")
        .and_then(|d| d.get("voided"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let _ = writeln!(
        out,
        "### {row_id} — {system}\n\n{desc}\n\nStatus: {}\n",
        if void {
            "**VOID (digest mismatch)**"
        } else {
            "ok"
        }
    );
    // Operator-recorded anomalies (co-tenant load, host incidents) are
    // part of the evidence and must survive into the published report
    // (gate nv3-B-0-gate-1 finding 4), never silently dropped.
    let empty = Vec::new();
    let anomalies = v.get("anomalies").and_then(Value::as_arr).unwrap_or(&empty);
    if !anomalies.is_empty() {
        let _ = writeln!(out, "Anomalies:\n");
        for a in anomalies {
            if let Some(s) = a.as_str() {
                let _ = writeln!(out, "- {s}");
            }
        }
        let _ = writeln!(out);
    }

    // metric column order from the registry
    let metrics: Vec<&str> = row
        .map(|r| r.metrics.iter().map(|(m, _)| *m).collect())
        .unwrap_or_default();

    let mut header = String::from("| config | run |");
    let mut sep = String::from("|---|---|");
    for m in &metrics {
        let _ = write!(header, " {m} |");
        sep.push_str("---|");
    }
    let _ = write!(header, " Nucleus ratio | label |");
    sep.push_str("---|---|");
    let _ = writeln!(out, "{header}\n{sep}");

    let empty = Vec::new();
    let series = v.get("series").and_then(Value::as_arr).unwrap_or(&empty);
    for s in series {
        let cfg = config_str(s.get("config").unwrap_or(&Value::Null));
        // cold line
        if let Some(cold) = s.get("cold").and_then(|c| c.get("metrics")) {
            let _ = write!(out, "| {cfg} | cold |");
            for m in &metrics {
                let val = cold.get(m).and_then(Value::as_f64);
                let _ = write!(out, " {} |", val.map(fmt_num).unwrap_or_else(|| "-".into()));
            }
            let _ = writeln!(out, " n/a | n/a |");
        }
        // warm line with CI
        if let Some(warm) = s.get("warm") {
            let runs = warm.get("runs").and_then(Value::as_f64).unwrap_or(0.0) as usize;
            let _ = write!(out, "| {cfg} | warm (n={runs}) |");
            // ratio example from the first metric that exists
            let mut first_ratio: Option<(Option<f64>, &'static str)> = None;
            for m in &metrics {
                let cell = warm
                    .get("metrics")
                    .and_then(|ms| ms.get(m))
                    .and_then(|e| {
                        let median = e.get("median").and_then(Value::as_f64)?;
                        let lo = e
                            .get("ci95")
                            .and_then(Value::as_arr)
                            .and_then(|a| a.first())
                            .and_then(Value::as_f64)?;
                        let hi = e
                            .get("ci95")
                            .and_then(Value::as_arr)
                            .and_then(|a| a.get(1))
                            .and_then(Value::as_f64)?;
                        Some(format!(
                            "{} [{}, {}]",
                            fmt_num(median),
                            fmt_num(lo),
                            fmt_num(hi)
                        ))
                    })
                    .unwrap_or_else(|| "-".into());
                let _ = write!(out, " {cell} |");
                if first_ratio.is_none() {
                    let dir = row
                        .and_then(|r| {
                            r.metrics
                                .iter()
                                .find(|(name, _)| name == m)
                                .map(|(_, d)| *d)
                        })
                        .unwrap_or(Direction::HigherIsBetter);
                    let median = warm
                        .get("metrics")
                        .and_then(|ms| ms.get(m))
                        .and_then(|e| e.get("median"))
                        .and_then(Value::as_f64);
                    if let Some(median) = median {
                        // nucleus numbers do not exist yet (C-9/D-* fill them)
                        first_ratio = Some(ratio_and_label(None, median, dir));
                    }
                }
            }
            match first_ratio {
                Some((Some(r), l)) => {
                    let _ = writeln!(out, " {:.2}x | {l} |", r);
                }
                _ => {
                    let _ = writeln!(out, " n/a | n/a |");
                }
            }
        }
    }
    let _ = writeln!(out);
    Ok(())
}

/// Generate the full report from parsed results documents.
pub fn generate(date: &str, files: &[(String, Value)]) -> Result<String> {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# NBench E3 baselines — {date}\n\n\
         Generated by `nbench report` (card B-0). Ratios and labels follow \
         PLAN-v3 §5.3: **beats** >= 1.0x, **competitive** >= 0.7x, \
         **behind** < 0.7x. Nucleus columns are `n/a` until cards C-9/D-* \
         produce Nucleus numbers; this report fixes the competitor side of \
         each ratio (§5.2 fairness rules).\n"
    );
    if let Some((_, first)) = files.first()
        && let Some(host) = first.get("provenance").and_then(|p| p.get("host"))
    {
        let _ = writeln!(out, "## Host\n");
        let _ = writeln!(out, "```");
        for (k, v) in host.pairs().unwrap_or(&[]) {
            if let Some(s) = v.as_str() {
                let _ = writeln!(out, "{k}: {s}");
            }
        }
        let _ = writeln!(out, "```\n");
    }
    let _ = writeln!(out, "## Rows\n");
    for (name, v) in files {
        let _ = writeln!(out, "<!-- source: {name} -->");
        render_row(&mut out, v)?;
    }
    let _ = writeln!(
        out,
        "## Nucleus comparison\n\n\
         | row | config | metric | Nucleus | competitor | ratio | label |\n|---|---|---|---|---|---|---|\n\
         | (all rows) | n/a | n/a | n/a | recorded above | n/a | n/a |\n\n\
         Nucleus results do not exist yet; every cell is `n/a` by design.\n"
    );
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json;

    fn metric_entry(samples: &[f64], median: f64, lo: f64, hi: f64) -> Value {
        Value::obj(vec![
            (
                "samples",
                Value::Arr(samples.iter().map(|s| Value::n(*s)).collect()),
            ),
            ("median", Value::n(median)),
            ("ci95", Value::Arr(vec![Value::n(lo), Value::n(hi)])),
        ])
    }

    fn fixture_results() -> Value {
        Value::obj(vec![
            ("row", Value::s("kv-memtier-valkey8")),
            ("system", Value::s("valkey8")),
            (
                "series",
                Value::Arr(vec![Value::obj(vec![
                    (
                        "config",
                        Value::Arr(vec![Value::Arr(vec![
                            Value::s("appendfsync"),
                            Value::s("always"),
                        ])]),
                    ),
                    (
                        "cold",
                        Value::obj(vec![(
                            "metrics",
                            Value::obj(vec![("ops_sec", Value::n(90000.0))]),
                        )]),
                    ),
                    (
                        "warm",
                        Value::obj(vec![
                            ("runs", Value::n(5.0)),
                            (
                                "metrics",
                                Value::obj(vec![
                                    (
                                        "ops_sec",
                                        metric_entry(&[100000.0; 5], 100000.0, 99000.0, 101000.0),
                                    ),
                                    ("p99_ms", metric_entry(&[2.0; 5], 2.0, 1.9, 2.1)),
                                ]),
                            ),
                        ]),
                    ),
                ])]),
            ),
            (
                "digest",
                Value::obj(vec![
                    (
                        "init",
                        Value::obj(vec![
                            ("value", Value::s("abc123")),
                            ("expected", Value::s("abc123")),
                            ("match", Value::Bool(true)),
                        ]),
                    ),
                    ("voided", Value::Bool(false)),
                ]),
            ),
            (
                "provenance",
                Value::obj(vec![("host", Value::obj(vec![("cpus", Value::s("16"))]))]),
            ),
        ])
    }

    #[test]
    fn report_shows_medians_ci_and_na_nucleus() {
        let files = vec![("kv-memtier-valkey8.json".to_string(), fixture_results())];
        let md = generate("2026-10-17", &files).expect("report");
        assert!(md.contains("kv-memtier-valkey8"));
        assert!(md.contains("100000 [99000, 101000]"));
        assert!(md.contains("appendfsync=always"));
        assert!(md.contains("| cold |"));
        assert!(md.contains("| warm (n=5) |"));
        // nucleus placeholders in table cells, and no fabricated labels
        // (the word "beats" may appear only in the header explanation)
        assert!(md.matches("n/a").count() >= 3);
        assert!(!md.contains("| beats |"));
        assert!(!md.contains("| behind |"));
        assert!(!md.contains("| competitive |"));
    }

    #[test]
    fn voided_run_is_visible() {
        let mut voided = fixture_results();
        if let Value::Obj(top) = &mut voided {
            let digest = Value::obj(vec![
                (
                    "init",
                    Value::obj(vec![
                        ("value", Value::s("x")),
                        ("expected", Value::s("y")),
                        ("match", Value::Bool(false)),
                    ]),
                ),
                ("voided", Value::Bool(true)),
            ]);
            top.retain(|(k, _)| k != "digest");
            top.push(("digest".into(), digest));
        }
        let files = vec![("x.json".to_string(), voided)];
        let md = generate("2026-10-17", &files).expect("report");
        assert!(md.contains("**VOID (digest mismatch)**"));
    }

    #[test]
    fn anomalies_are_published_not_dropped() {
        let mut with_anomalies = fixture_results();
        if let Value::Obj(top) = &mut with_anomalies {
            top.retain(|(k, _)| k != "anomalies");
            top.push((
                "anomalies".into(),
                Value::Arr(vec![
                    Value::s("co-tenant cargo build at 18:27, load1 ~22"),
                    Value::s("gate smoke ran beside this row"),
                ]),
            ));
        }
        let files = vec![("a.json".to_string(), with_anomalies)];
        let md = generate("2026-10-17", &files).expect("report");
        assert!(md.contains("Anomalies:"));
        assert!(md.contains("- co-tenant cargo build at 18:27, load1 ~22"));
        assert!(md.contains("- gate smoke ran beside this row"));
        // and a row without anomalies gets no empty section
        let md2 =
            generate("2026-10-17", &[("b.json".to_string(), fixture_results())]).expect("report2");
        assert!(!md2.contains("Anomalies:"));
    }

    #[test]
    fn ratios_follow_direction() {
        // throughput: nucleus 50 vs competitor 100 -> 0.5x behind
        let (r, l) = ratio_and_label(Some(50.0), 100.0, Direction::HigherIsBetter);
        assert_eq!((r.unwrap(), l), (0.5, "behind"));
        // latency: nucleus 2ms vs competitor 1ms -> 0.5x behind
        let (r, l) = ratio_and_label(Some(2.0), 1.0, Direction::LowerIsBetter);
        assert_eq!((r.unwrap(), l), (0.5, "behind"));
        // latency: nucleus 1ms vs competitor 2ms -> 2.0x beats
        let (r, l) = ratio_and_label(Some(1.0), 2.0, Direction::LowerIsBetter);
        assert_eq!((r.unwrap(), l), (2.0, "beats"));
        // no nucleus
        let (r, l) = ratio_and_label(None, 1.0, Direction::HigherIsBetter);
        assert!((r.is_none()) && l == "n/a");
        // zero competitor
        let (r, l) = ratio_and_label(Some(1.0), 0.0, Direction::HigherIsBetter);
        assert!((r.is_none()) && l == "n/a");
    }

    #[test]
    fn number_formatting() {
        assert_eq!(fmt_num(1234.56), "1235");
        assert_eq!(fmt_num(42.26), "42.3");
        assert_eq!(fmt_num(0.123456), "0.123");
    }

    #[test]
    fn json_fixture_parses() {
        let text = json::to_string(&fixture_results()).expect("ser");
        let back = json::parse(&text).expect("de");
        assert_eq!(
            back.get("row").and_then(Value::as_str),
            Some("kv-memtier-valkey8")
        );
    }
}
