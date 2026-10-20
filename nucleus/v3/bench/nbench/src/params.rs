//! Run parameters and `--set` overrides.
//!
//! Invariant owned here: every knob that changes what a row measures is a
//! named parameter with a documented default, and overrides are applied by
//! *value*, never by editing scripts — so a results file plus its params
//! fully describes the run (PLAN-v3 §5.2 rule 6, provenance).

use crate::error::{Error, Result};

/// Tunable parameters for a row run. Defaults implement card B-0 Work
/// item 3 (pgbench scale 100, clients 1/16/64; pipeline 1/16; both fsync
/// settings; HNSW m=16 ef_construction=200 with an ef_search sweep).
#[derive(Clone, Debug)]
pub struct Params {
    /// Warm (measured) runs per config point. §5.2 rule 5: at least 5.
    pub runs: usize,
    /// Cold runs per config point (executed right after a restart).
    pub cold_runs: usize,
    /// Warmup runs (not measured) before the warm runs.
    pub warmup_runs: usize,
    /// Duration of one measured execution, in seconds.
    pub duration_s: u64,
    /// pgbench scale factor (TPC-B; 100 = 10M accounts).
    pub scale: u64,
    /// pgbench client counts.
    pub clients: Vec<u64>,
    /// `synchronous_commit` modes to sweep for pgbench.
    pub modes: Vec<String>,
    /// sysbench workload scripts.
    pub workloads: Vec<String>,
    /// sysbench thread counts.
    pub threads: Vec<u64>,
    /// sysbench tables and rows per table.
    pub tables: u64,
    /// sysbench rows per table.
    pub table_size: u64,
    /// AOF fsync policies to sweep for KV rows.
    pub fsync_modes: Vec<String>,
    /// memtier pipeline depths.
    pub pipelines: Vec<u64>,
    /// memtier threads and clients-per-thread.
    pub mt_threads: u64,
    /// memtier clients per thread.
    pub mt_clients: u64,
    /// memtier key range upper bound.
    pub key_max: u64,
    /// memtier value size in bytes.
    pub data_size: u64,
    /// Vector search: HNSW m (fixed per card).
    pub hnsw_m: u64,
    /// Vector search: ef_construction (fixed per card).
    pub ef_construction: u64,
    /// Vector search: ef_search sweep.
    pub ef: Vec<u64>,
    /// Vector search: number of queries per run (the SIFT1M query set size).
    pub queries: u64,
    /// speedtest1 --size multiplier. The default (10000 = 100x the
    /// speedtest1 default of 100) is bounded by test 140: its
    /// `group_concat(c)` accumulates one `z1` number-name per matching
    /// row, and `z1` holds `5 * size` rows (speedtest1.c testset_main)
    /// of at most ~75 bytes each — at 10000 even a match-everything
    /// concatenation stays ~3x under SQLite's documented default
    /// `SQLITE_MAX_LENGTH` of 1e9. At 100000 (50M rows) the run of
    /// 2026-10-18 died in test 140 with `string or blob too big`
    /// (SQLITE_LIMIT_LENGTH, error 18). The bound is pinned by a unit
    /// test in this file.
    pub st_size: u64,
}

impl Default for Params {
    fn default() -> Self {
        Params {
            runs: 5,
            cold_runs: 1,
            warmup_runs: 1,
            duration_s: 60,
            scale: 100,
            clients: vec![1, 16, 64],
            modes: vec!["on".into(), "off".into()],
            workloads: vec!["oltp_read_write".into(), "oltp_point_select".into()],
            threads: vec![1, 16, 64],
            tables: 10,
            table_size: 100_000,
            fsync_modes: vec!["always".into(), "everysec".into()],
            pipelines: vec![1, 16],
            mt_threads: 16,
            mt_clients: 4,
            key_max: 10_000_000,
            data_size: 32,
            hnsw_m: 16,
            ef_construction: 200,
            ef: vec![16, 32, 64, 128, 256],
            queries: 10_000,
            st_size: 10_000,
        }
    }
}

impl Params {
    /// Apply `key=value` overrides (comma-separated lists become vectors).
    pub fn with_overrides(sets: &[(&str, &str)]) -> Result<Params> {
        let mut p = Params::default();
        for (k, v) in sets {
            p.set(k, v)?;
        }
        Ok(p)
    }

    /// Apply one override.
    pub fn set(&mut self, k: &str, v: &str) -> Result<()> {
        let bad = || Error::Usage(format!("bad value for {k}: {v}"));
        let parse_u64 = |v: &str| v.parse::<u64>().map_err(|_| bad());
        let parse_usize = |v: &str| v.parse::<usize>().map_err(|_| bad());
        let parse_list_u64 = |v: &str| -> Result<Vec<u64>> {
            v.split(',')
                .map(|s| s.trim().parse::<u64>())
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| bad())
        };
        match k {
            "runs" => self.runs = parse_usize(v)?,
            "cold_runs" => self.cold_runs = parse_usize(v)?,
            "warmup_runs" => self.warmup_runs = parse_usize(v)?,
            "duration_s" => self.duration_s = parse_u64(v)?,
            "scale" => self.scale = parse_u64(v)?,
            "clients" => self.clients = parse_list_u64(v)?,
            "modes" => self.modes = v.split(',').map(|s| s.trim().to_string()).collect(),
            "workloads" => self.workloads = v.split(',').map(|s| s.trim().to_string()).collect(),
            "threads" => self.threads = parse_list_u64(v)?,
            "tables" => self.tables = parse_u64(v)?,
            "table_size" => self.table_size = parse_u64(v)?,
            "fsync_modes" => {
                self.fsync_modes = v.split(',').map(|s| s.trim().to_string()).collect()
            }
            "pipelines" => self.pipelines = parse_list_u64(v)?,
            "mt_threads" => self.mt_threads = parse_u64(v)?,
            "mt_clients" => self.mt_clients = parse_u64(v)?,
            "key_max" => self.key_max = parse_u64(v)?,
            "data_size" => self.data_size = parse_u64(v)?,
            "ef" => self.ef = parse_list_u64(v)?,
            "queries" => self.queries = parse_u64(v)?,
            "st_size" => self.st_size = parse_u64(v)?,
            _ => return Err(Error::Usage(format!("unknown parameter {k}"))),
        }
        Ok(())
    }

    /// Serialize all parameters as ordered pairs for the results JSON.
    pub fn to_pairs(&self) -> Vec<(String, String)> {
        let join = |v: &[String]| v.join(",");
        let join_u = |v: &[u64]| {
            v.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(",")
        };
        vec![
            ("runs".into(), self.runs.to_string()),
            ("cold_runs".into(), self.cold_runs.to_string()),
            ("warmup_runs".into(), self.warmup_runs.to_string()),
            ("duration_s".into(), self.duration_s.to_string()),
            ("scale".into(), self.scale.to_string()),
            ("clients".into(), join_u(&self.clients)),
            ("modes".into(), join(&self.modes)),
            ("workloads".into(), join(&self.workloads)),
            ("threads".into(), join_u(&self.threads)),
            ("tables".into(), self.tables.to_string()),
            ("table_size".into(), self.table_size.to_string()),
            ("fsync_modes".into(), join(&self.fsync_modes)),
            ("pipelines".into(), join_u(&self.pipelines)),
            ("mt_threads".into(), self.mt_threads.to_string()),
            ("mt_clients".into(), self.mt_clients.to_string()),
            ("key_max".into(), self.key_max.to_string()),
            ("data_size".into(), self.data_size.to_string()),
            ("hnsw_m".into(), self.hnsw_m.to_string()),
            ("ef_construction".into(), self.ef_construction.to_string()),
            ("ef".into(), join_u(&self.ef)),
            ("queries".into(), self.queries.to_string()),
            ("st_size".into(), self.st_size.to_string()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_meet_card_b0() {
        let p = Params::default();
        assert_eq!(p.scale, 100);
        assert_eq!(p.clients, vec![1, 16, 64]);
        assert_eq!(p.pipelines, vec![1, 16]);
        assert_eq!(p.fsync_modes.len(), 2);
        assert_eq!(p.hnsw_m, 16);
        assert_eq!(p.ef_construction, 200);
        assert!(p.runs >= 5);
        assert!(!p.ef.is_empty());
        // speedtest1 test 140 bound: z1 holds 5*st_size number-names of
        // <= ~75 bytes plus a separator each; the concatenation must
        // stay under SQLite's documented default SQLITE_MAX_LENGTH (1e9)
        // even if every row matches the LIKE, or the suite aborts with
        // error 18 (observed at st_size=100000 on 2026-10-18).
        assert!(
            p.st_size * 5 * 76 < 1_000_000_000,
            "st_size {} can overflow SQLITE_MAX_LENGTH in test 140",
            p.st_size
        );
    }

    #[test]
    fn overrides_apply_and_serialize() {
        let p = Params::with_overrides(&[
            ("runs", "2"),
            ("clients", "1"),
            ("modes", "on"),
            ("ef", "64,128"),
        ])
        .expect("overrides");
        assert_eq!(p.runs, 2);
        assert_eq!(p.clients, vec![1]);
        assert_eq!(p.modes, vec!["on"]);
        assert_eq!(p.ef, vec![64, 128]);
        let pairs = p.to_pairs();
        assert_eq!(
            pairs
                .iter()
                .find(|(k, _)| k == "scale")
                .map(|(_, v)| v.as_str()),
            Some("100")
        );
    }

    #[test]
    fn unknown_or_bad_keys_error() {
        let mut p = Params::default();
        assert!(p.set("nope", "1").is_err());
        assert!(p.set("runs", "five").is_err());
        assert!(p.set("clients", "1,,2").is_err());
    }
}
