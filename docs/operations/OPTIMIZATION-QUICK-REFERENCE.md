# Nucleus Phase 4 Optimization Quick Reference

## Start Here

**Building Nucleus**:
```bash
cd nucleus
cargo build --release --features bench-tools
```

**Running with optimizations**:
```bash
./target/release/nucleus \
  --binary-port 5433 \
  --zone-map-threshold-rows 1000000 \
  --enable-lazy-materialization \
  --metrics-port 9090
```

**Running without optimizations (baseline)**:
```bash
./target/release/nucleus \
  --disable-binary-protocol \
  --disable-zone-maps \
  --disable-group-by-specialization \
  --disable-lazy-materialization \
  --disable-simd
```

## Health Check

```bash
# Is Nucleus healthy?
curl http://localhost:9090/health | jq .status

# Detailed health with metrics
curl http://localhost:9090/health | jq .

# Prometheus metrics
curl http://localhost:9090/metrics | head -20
```

## Benchmarking

```bash
# Quick benchmark (10 iterations, small dataset)
cargo run --release --features bench-tools --bin compete -- \
  --iterations 10 --rows 1000

# Full benchmark (100 iterations, 50k rows)
cargo run --release --features bench-tools --bin compete -- \
  --iterations 100 --rows 50000

# Compare specific backends
cargo run --release --features bench-tools --bin compete -- \
  --backends nucleus,pg,sqlite --iterations 50 --rows 10000

# Skip certain backends
cargo run --release --features bench-tools --bin compete -- \
  --skip mongodb,tidb --iterations 100 --rows 50000
```

## Emergency Procedures

### Disable Feature Immediately (No Restart)

```bash
# Send SIGHUP to reload config
kill -HUP $(pgrep nucleus)
```

Then restart with disable flag:
```bash
systemctl stop nucleus
./target/release/nucleus --disable-<optimization>
systemctl start nucleus
```

### Disable Specific Optimization

```bash
# Binary protocol
./target/release/nucleus --disable-binary-protocol

# Zone maps
./target/release/nucleus --disable-zone-maps

# GROUP BY specialization
./target/release/nucleus --disable-group-by-specialization

# Lazy materialization
./target/release/nucleus --disable-lazy-materialization

# SIMD
./target/release/nucleus --disable-simd
```

## Monitoring Commands

### Check Optimization Status

```bash
# Binary protocol connections
curl http://localhost:9090/health | jq '.binary_protocol'

# Zone map effectiveness
curl http://localhost:9090/health | jq '.zone_maps'

# Query metrics
curl http://localhost:9090/health | jq '.metrics'
```

### Prometheus Queries

```bash
# Binary latency percentiles
histogram_quantile(0.50, nucleus_binary_latency_microseconds_bucket)
histogram_quantile(0.99, nucleus_binary_latency_microseconds_bucket)

# Zone map skip ratio
nucleus_zone_map_skip_ratio_percent

# GROUP BY specialization ratio
nucleus_group_by_specialized_total / (nucleus_group_by_specialized_total + nucleus_group_by_generic_fallback_total)

# Lazy materialization memory saved
increase(nucleus_lazy_materialization_memory_saved_bytes_total[1h])

# SIMD AVX512 dispatch ratio
nucleus_simd_cpu_dispatch_avx512_ratio_percent

# Query latency p99
histogram_quantile(0.99, nucleus_query_duration_seconds_bucket)
```

## Configuration Flags

### Binary Protocol (Port 5433)
```
--disable-binary-protocol          (disable feature)
--binary-port 5433                 (listen port)
--binary-max-message-size 67108864 (64 MB)
--binary-compression-enabled true
--binary-read-timeout-secs 30
```

### Zone Maps
```
--disable-zone-maps                      (disable feature)
--zone-map-threshold-rows 10000000       (minimum table size, 10M rows)
--zone-map-granule-size-bytes 1048576    (1 MB per granule)
--zone-map-recompute-threshold 1000000   (recompute after N inserts)
--zone-map-max-skip-ratio-percent 90     (don't recompute if >90% skipping)
```

### GROUP BY
```
--disable-group-by-specialization            (disable feature)
--group-by-column-pruning true               (drop unused columns)
--group-by-specialization-threshold-ms 100   (auto-specialize threshold)
--group-by-hash-table-max-size 10000000      (10M distinct groups)
```

### Lazy Materialization
```
--disable-lazy-materialization                        (disable feature)
--lazy-materialization-threshold-rows 1000            (min result set size)
--lazy-materialization-memory-limit-bytes 536870912   (512 MB limit)
--lazy-materialization-cleanup-enabled true           (auto cleanup)
--lazy-materialization-cleanup-batch-size 10000       (cleanup batch)
```

### SIMD
```
--disable-simd                             (disable feature)
--simd-target auto|avx512|avx2|sse42|scalar (CPU target)
--simd-correctness-check false             (compare SIMD vs scalar)
--simd-correctness-check-sample-ratio 1000 (1 in 1000 queries)
--simd-min-rows 1000                       (minimum row threshold)
```

### Metrics
```
--metrics-port 9090              (Prometheus metrics endpoint)
--metrics-enabled true           (enable /metrics)
--health-check-enabled true      (enable /health)
--health-check-detailed true     (include detailed metrics)
```

## Testing

### Run Tests

```bash
# Binary protocol tests
cargo test --lib wire --lib transport -q

# Zone map tests
cargo test --lib storage::buffer --lib storage::mvcc -q

# GROUP BY tests
cargo test --lib executor::aggregate -q

# Lazy materialization tests
cargo test --lib executor::project -q

# SIMD tests
cargo test --lib simd -q

# All executor tests (3184 tests)
cargo test --lib executor -q

# Full test suite
cargo test --lib -q
```

### Run Benchmarks

```bash
# Phase 4 benchmark
cargo bench --bench phase4_bench

# Query benchmark
cargo bench --bench query_bench

# Storage benchmark
cargo bench --bench storage_bench

# All benchmarks
cargo bench
```

## CI/CD Status

Check GitHub Actions:
```bash
# Recent workflow runs
gh run list --repo neutron-build/nucleus --limit 10

# Watch a specific run
gh run watch <run-id>

# View logs
gh run view <run-id> --log

# View artifacts (compete results)
gh run view <run-id> --artifacts
```

## Troubleshooting

### Binary latency high (>30μs)

```bash
# 1. Check if specific query or all queries
SELECT query, avg(duration_us) FROM nucleus_query_log
  WHERE protocol = 'binary' AND timestamp > now() - interval '5 minutes'
  GROUP BY query ORDER BY avg(duration_us) DESC LIMIT 5;

# 2. Check CPU/IO
top -b -n 1 | head -20
iostat -x 1 5

# 3. Identify slow query and optimize
REINDEX TABLE table_name;

# 4. If no improvement in 30s, disable
./target/release/nucleus --disable-binary-protocol
```

### Zone maps wrong results

```bash
# 1. Disable immediately
./target/release/nucleus --disable-zone-maps

# 2. Compare results with/without (should match now)
SELECT * FROM suspect_table WHERE condition;

# 3. Recompute zone maps
REINDEX zone_maps ON suspect_table;

# 4. Re-enable gradually
kill -HUP $(pgrep nucleus)  # reload config
```

### Memory leak (lazy materialization)

```bash
# 1. Check memory usage
free -h
ps aux | grep nucleus | grep -v grep

# 2. Disable lazy materialization
./target/release/nucleus --disable-lazy-materialization

# 3. Monitor memory drop (should be within 1 minute)
watch -n 10 'free -h'

# 4. If recovers, profile which queries leak
SELECT query, count(*) FROM nucleus_query_log
  WHERE timestamp > now() - interval '1 hour'
  GROUP BY query
  ORDER BY count(*) DESC;
```

### SIMD correctness mismatch

```bash
# 1. Check if mismatch logged
grep "SIMD correctness" /var/log/nucleus.log

# 2. Identify which aggregate type/data
# (from error log message)

# 3. Disable SIMD
./target/release/nucleus --disable-simd

# 4. Re-run query, should get same result

# 5. Investigate SIMD implementation for that type
```

## Performance Targets

| Metric | Target | Alert |
|--------|--------|-------|
| Binary latency p50 | <10μs | — |
| Binary latency p99 | <30μs | >30μs for 2 min |
| Binary error rate | <0.1% | >1% for 1 min |
| Zone map skip ratio | >20% | <5% for 10 min |
| GROUP BY specialization | >95% | — |
| Lazy mat memory saved | 10-20% | <5% |
| SIMD AVX512 ratio | >50% | — |
| Query latency p99 | <5s | >5s for 5 min |

## Key Files

| Purpose | File |
|---------|------|
| Deployment guide | `DEPLOYMENT-GUIDE.md` |
| Incident response | `RUNBOOK.md` |
| Monitoring setup | `MONITORING-SETUP.md` |
| CI/CD architecture | `CI-CD-INFRASTRUCTURE.md` |
| Metrics code | `nucleus/src/metrics/optimizations.rs` |
| Config code | `nucleus/src/config/optimizations.rs` |
| Binary protocol CI | `.github/workflows/binary_protocol.yml` |
| Analytics CI | `.github/workflows/analytics_optimization.yml` |
| Regression CI | `.github/workflows/full_regression.yml` |

## Useful Commands

```bash
# Systemd operations
systemctl start nucleus
systemctl stop nucleus
systemctl restart nucleus
systemctl status nucleus
journalctl -u nucleus -f          # follow logs
journalctl -u nucleus -n 100      # last 100 lines

# Build & deploy
cd nucleus
cargo build --release --features bench-tools
sudo systemctl restart nucleus

# Health & metrics
curl http://localhost:9090/health | jq .
curl http://localhost:9090/metrics | grep nucleus_binary
curl http://localhost:9090/metrics | grep nucleus_zone_map

# Database inspection (if running)
psql -h localhost -p 5432
# Then: SELECT * FROM nucleus_schema ...

# Debugging
RUST_LOG=debug ./target/release/nucleus
RUST_BACKTRACE=1 ./target/release/nucleus

# Process inspection
pgrep nucleus                    # find PID
ps aux | grep nucleus
kill -TERM <pid>                # graceful shutdown
kill -HUP <pid>                 # reload config
```

## See Also

- `nucleus/CLAUDE.md` — Architecture overview
- `nucleus/README.md` — Feature list
- Phase 4 spec document — Optimization details
- Production runbook (team Wiki)

## Support

- **Critical issues**: Page on-call (see RUNBOOK.md)
- **Questions**: Nucleus Slack channel
- **Code reviews**: GitHub pull requests
- **Design**: Confluence design docs

---

**Version**: 0.1 | **Updated**: 2026-03-14 | **Maintained by**: DevOps Team
