# Nucleus Production Incident Response Runbook

## Purpose

Emergency procedures for Nucleus Phase 4 optimization issues in production.

**On-call contact**: [Your team] | **Escalation**: [VP Engineering]

---

## 1. Binary Protocol Latency Spike

**Severity**: Medium | **Detection**: Alerts on `nucleus_binary_latency_p99 > 30μs`

### Symptoms
- Clients report slow queries via binary protocol
- PostgreSQL clients (SQL protocol) unaffected
- Latency spike appears suddenly in metrics

### Investigation (Immediate)

```bash
# 1. Check active connections and query load
curl http://localhost:9090/health | jq '.binary_protocol'

# 2. Check if specific queries slow or all queries
SELECT query, avg(duration_us) FROM nucleus_query_log
  WHERE protocol = 'binary'
  AND timestamp > now() - interval '5 minutes'
  GROUP BY query
  ORDER BY avg(duration_us) DESC LIMIT 5;

# 3. Check CPU and IO
top -b -n 1 | head -20
iostat -x 1 5

# 4. Check network
netstat -an | grep 5433 | wc -l  # count binary connections
```

### Root Cause Analysis

| Cause | Indicator | Fix |
|-------|-----------|-----|
| **Hot-spot query** | Single query in top 5 | Rebuild index, re-plan |
| **Network congestion** | High latency + packet loss | Scale network, QoS prioritization |
| **CPU contention** | CPU >85% utilization | Scale horizontally, adjust thread pool |
| **Memory pressure** | Lots of GC / swap usage | Reduce buffer pool, disable lazy mat |
| **Codec bug** | Latency = 0μs in metric but slow in client | Disable binary protocol, investigate |

### Immediate Actions

```bash
# Option 1: Identify slow query and optimize
# Re-index: REINDEX TABLE table_name
# Re-plan: ANALYZE table_name
# Example: REINDEX TABLE orders BY (order_id, customer_id)

# Option 2: Scale connections
# Reduce binary port from 5433 to 5432 (merge with SQL protocol)
# Or: Increase thread pool: --thread-pool-size 64

# Option 3: Disable binary protocol (if >30s no improvement)
kill -HUP $(pgrep nucleus)
# Or: systemctl restart nucleus (with --disable-binary-protocol)
```

### Rollback

```bash
# Immediate: Disable binary protocol
systemctl stop nucleus
./target/release/nucleus --disable-binary-protocol
systemctl start nucleus

# Expected recovery: <30s
# Verification: curl http://localhost:9090/health | grep '"enabled": false'
```

### Post-Incident

- [ ] Identify root cause
- [ ] Fix and test in staging
- [ ] Re-enable binary protocol gradually (1% canary)
- [ ] Add metric alert for specific root cause
- [ ] Update runbook with new trigger

---

## 2. Binary Protocol Error Rate Spike

**Severity**: Critical | **Detection**: Alerts on `nucleus_binary_error_rate > 1%`

### Symptoms
- Binary clients get connection errors
- Error messages: "connection reset", "protocol violation"
- SQL protocol clients unaffected

### Investigation (Immediate)

```bash
# 1. Check error logs
journalctl -u nucleus -n 100 | grep -i error | head -20

# 2. Check if parsing or message handling issue
dmesg | grep -i segfault | tail -5  # check for crashes

# 3. Check message size
SELECT avg(message_size_bytes), max(message_size_bytes)
  FROM nucleus_binary_messages WHERE timestamp > now() - interval '5 minutes';

# 4. Check client versions (multiple clients = compatibility issue)
SELECT client_version, count(*)
  FROM nucleus_connections WHERE protocol = 'binary'
  GROUP BY client_version;
```

### Root Cause Analysis

| Cause | Indicator | Fix |
|-------|-----------|-----|
| **Codec regression** | High error rate after deploy | Rollback version |
| **Client incompatibility** | Error only from specific client ver | Update client library |
| **Message size overflow** | Errors on large result sets | Implement chunking |
| **Memory corruption** | Segfault in logs | Restart + disable feature |
| **TLS handshake fail** | SSL_ERROR in logs | Check certificates |

### Immediate Actions

```bash
# Step 1: Check if regression from recent deploy
git log --oneline -5 nucleus/src/wire/

# Step 2: Rollback if recent change to wire protocol
git revert <commit>
cargo build --release
systemctl restart nucleus

# Step 3: If rollback doesn't help, disable feature
systemctl stop nucleus
./target/release/nucleus --disable-binary-protocol
systemctl start nucleus
```

### Rollback

```bash
# Immediate: Disable binary protocol
systemctl stop nucleus
./target/release/nucleus --disable-binary-protocol
systemctl start nucleus

# Recovery time: <15 seconds (no in-flight data)
# Verify: curl http://localhost:9090/health | grep '"error_rate": 0'
```

### Post-Incident

- [ ] Root cause analysis
- [ ] Add integration test for regression
- [ ] Add error rate alerting
- [ ] Review codec change review process

---

## 3. Zone Map Wrong Results

**Severity**: Critical | **Detection**: Query result inconsistencies, data audit failures

### Symptoms
- Query returns different results with/without zone maps
- Specific tables affected (>10M rows)
- Results missing rows or include wrong rows

### Investigation (Immediate)

```bash
# 1. Disable zone maps and re-run query
systemctl stop nucleus
./target/release/nucleus --disable-zone-maps
systemctl start nucleus

# 2. Compare results
SELECT * FROM suspect_table WHERE condition;
# Compare with previous output

# 3. Check if results now match
# If YES: zone maps have a bug
# If NO: problem elsewhere

# 4. Inspect zone map metadata
SELECT table_name, granule_id, min_value, max_value, predicate_selectivity
  FROM nucleus_zone_map_stats
  WHERE table_name = 'suspect_table'
  LIMIT 10;
```

### Root Cause Analysis

| Cause | Indicator | Fix |
|-------|-----------|-----|
| **Stale statistics** | Min/max bounds don't match actual data | REINDEX zone_maps |
| **Index ordering change** | Zone map metadata doesn't match current sort | Rebuild index |
| **Null handling bug** | Zone map excludes NULLs incorrectly | Disable zone maps |
| **Codec overflow** | Min/max values truncated | Investigate storage layer |
| **Concurrent update bug** | Zone map reads during VACUUM | Implement read lock |

### Immediate Actions

```bash
# Option 1: Recompute zone maps
REINDEX zone_maps ON suspect_table;

# Option 2: Disable zone maps (safer)
systemctl stop nucleus
./target/release/nucleus --disable-zone-maps
systemctl start nucleus

# Option 3: Verify data integrity
SELECT checksum(*) FROM suspect_table;
# Compare checksums before/after disable
```

### Rollback

```bash
# Immediate: Disable zone maps (no recovery needed)
systemctl stop nucleus
./target/release/nucleus --disable-zone-maps
systemctl start nucleus

# Recovery time: <30 seconds
# Data integrity: 100% (all data remains, just queries slower)

# Verification:
# Run query that was wrong, verify results now correct
```

### Post-Incident

- [ ] Audit all data with checksums
- [ ] Identify which granules had stale metadata
- [ ] Add automated zone map validation (daily)
- [ ] Add SQL result checksum table
- [ ] Implement automatic zone map refresh on ANALYZE

---

## 4. Memory Leak (Lazy Materialization)

**Severity**: High | **Detection**: Alerts on `nucleus_memory_usage > baseline + 20%`

### Symptoms
- Memory grows over time (hours/days)
- OOM after 24h of running
- Lazy materialization just enabled
- Other optimizations not correlated

### Investigation (Immediate)

```bash
# 1. Check memory usage
free -h
ps aux | grep nucleus | grep -v grep

# 2. Check if problem specific to lazy materialization
systemctl stop nucleus
./target/release/nucleus --disable-lazy-materialization
systemctl start nucleus

# 3. Monitor memory over 30 minutes
watch -n 10 'free -h'

# If memory stabilizes: lazy materialization is leaking
# If memory continues growing: different root cause
```

### Root Cause Analysis

| Cause | Indicator | Fix |
|-------|-----------|-----|
| **Row cleanup not called** | Buffer grows with each query | Add drop_buffer() call |
| **Reference cycle** | Materialized rows held in Arc | Implement explicit drop |
| **Wrong allocator** | Memory not returned to OS | Switch to jemalloc |
| **Blocking operation** | Cleanup waits on lock | Implement async cleanup |
| **Query keeps buffer alive** | Long-running query + lazy mat | Timeout or streaming |

### Immediate Actions

```bash
# Step 1: Disable lazy materialization
systemctl stop nucleus
./target/release/nucleus --disable-lazy-materialization
systemctl start nucleus

# Step 2: Monitor memory (should drop within 1 minute)
watch -n 5 'free -h'

# Step 3: If memory doesn't drop, restart with hard restart
systemctl restart nucleus --force

# Step 4: Identify which query caused leak
# Check query log for long-running queries at time of spike
```

### Rollback

```bash
# Immediate: Disable lazy materialization
systemctl stop nucleus
./target/release/nucleus --disable-lazy-materialization
systemctl start nucleus

# Recovery time: 1-5 minutes (memory drop)
# Data impact: 0 (no state change, just slower queries)

# Verify: Monitor memory for 30 minutes
free -h
# Should return to baseline within 30 minutes
```

### Post-Incident

- [ ] Identify specific query/sequence that triggered leak
- [ ] Add test case to catch memory leak
- [ ] Implement buffer cleanup profiling
- [ ] Add memory leak detector (valgrind nightly)
- [ ] Add lazy materialization to benchmarks

---

## 5. SIMD Aggregate Correctness Issue

**Severity**: Critical | **Detection**: Result checksums don't match SIMD vs scalar

### Symptoms
- Aggregates (SUM, AVG, COUNT) return wrong values
- SIMD-enabled queries differ from scalar
- Large aggregates (>1M rows) more likely to fail
- Specific data types affected

### Investigation (Immediate)

```bash
# 1. Verify if SIMD issue
# Run same aggregate with scalar only:
SELECT SUM(value) FROM table WHERE condition
# (Nucleus automatically disables SIMD if AVX512 not available)

# 2. Check CPU dispatch
curl http://localhost:9090/metrics | grep simd_cpu_dispatch

# 3. Check aggregate type
SELECT aggregate_type, cpu_dispatch, count(*)
  FROM nucleus_aggregate_log
  WHERE timestamp > now() - interval '1 hour'
  GROUP BY aggregate_type, cpu_dispatch;
```

### Root Cause Analysis

| Cause | Indicator | Fix |
|-------|-----------|-----|
| **Overflow handling** | Results differ for large numbers | Implement saturating arithmetic |
| **Precision loss** | Float rounding differs SIMD vs scalar | Check float precision |
| **NULL handling** | SIMD/scalar treat NULLs differently | Standardize NULL handling |
| **Data alignment** | Misaligned data causes wrong results | Align to SIMD boundary |
| **Register reuse** | Result contamination across iterations | Clear registers between iterations |

### Immediate Actions

```bash
# Option 1: Disable SIMD aggregates (safest)
systemctl stop nucleus
./target/release/nucleus --disable-simd
systemctl start nucleus

# Option 2: Disable specific aggregate type
# Edit executor/aggregate.rs:
#   SIMD_ENABLED = false for affected_type

# Option 3: Reduce SIMD to AVX2 (from AVX512)
# Edit executor/aggregate.rs:
#   SIMD_TARGET = "avx2" (fallback)
```

### Rollback

```bash
# Immediate: Disable SIMD
systemctl stop nucleus
./target/release/nucleus --disable-simd
systemctl start nucleus

# Recovery time: <30 seconds
# Performance impact: -10% on aggregates (minor)
# Data impact: 0 (correctness restored immediately)

# Verification:
# Re-run queries, check results match baseline
```

### Post-Incident

- [ ] Identify specific aggregate + data pattern that failed
- [ ] Add test case with that data
- [ ] Review SIMD implementation for the failing aggregate
- [ ] Add correctness tests for SIMD vs scalar paths
- [ ] Implement runtime SIMD validation (sample checking)

---

## 6. General Optimization Regression

**Severity**: Medium | **Detection**: Query latency increase >5%, throughput decrease >5%

### Symptoms
- Overall query latency increases after optimization deployment
- Specific query types slower (e.g., complex JOINs)
- Throughput decreases (fewer queries/sec)

### Investigation (Immediate)

```bash
# 1. Identify affected query type
SELECT query_template, avg(duration_us), count(*)
  FROM nucleus_query_log
  WHERE timestamp > now() - interval '30 minutes'
  GROUP BY query_template
  ORDER BY avg(duration_us) DESC LIMIT 5;

# 2. Compare before/after optimization
# (requires baseline metrics from competing commits)

# 3. Check plan changes
EXPLAIN (ANALYZE) SELECT ...;
# Look for unexpected full table scans, nested loops

# 4. Check if specific optimization causing regression
# Disable each optimization one by one:
systemctl stop nucleus
./target/release/nucleus --disable-zone-maps
cargo test --lib executor
# If tests still slow: not zone maps

systemctl stop nucleus
./target/release/nucleus --disable-lazy-materialization
cargo test --lib executor
# If tests still slow: not lazy mat

systemctl stop nucleus
./target/release/nucleus --disable-simd
cargo test --lib executor
# If tests still slow: not SIMD
```

### Root Cause Analysis

| Cause | Indicator | Fix |
|-------|-----------|-----|
| **Sub-optimal plan** | Different JOIN order or index choice | Analyze, rebuild index |
| **Optimization overhead** | Fast path slow for certain queries | Optimize decision logic |
| **Data distribution change** | Statistics outdated | ANALYZE tables |
| **Resource contention** | CPU/IO overloaded by other processes | Scale infrastructure |

### Immediate Actions

```bash
# Option 1: Disable the problematic optimization
systemctl stop nucleus
./target/release/nucleus --disable-<optimization>
systemctl start nucleus

# Option 2: Tune optimizer
# Increase plan cache: --plan-cache-size 10000
# Adjust cost model: --cpu-cost-per-row 10

# Option 3: Analyze and rebuild
ANALYZE;  # update statistics
REINDEX;  # rebuild indexes
```

### Rollback

```bash
# Graceful: Disable optimization
systemctl stop nucleus
./target/release/nucleus --disable-zone-maps
systemctl start nucleus

# Hard: Revert to previous version
git revert <commit>
cargo build --release
systemctl restart nucleus
```

### Post-Incident

- [ ] Add this query to continuous benchmarking
- [ ] Review optimization decision logic
- [ ] Add query latency regression tests
- [ ] Document optimization trade-offs

---

## Escalation Matrix

| Severity | Response | Escalation | Communication |
|----------|----------|------------|-----------------|
| Critical (data loss) | <5 min | VP Eng immediately | Status page + Slack |
| Critical (data wrong) | <5 min | VP Eng immediately | Status page + Slack |
| Critical (service down) | <5 min | VP Eng immediately | Status page + Slack |
| High (error rate >1%) | <15 min | Eng manager | Slack #incidents |
| Medium (latency >5%) | <30 min | On-call | Slack #incidents |
| Low (other) | <1 hour | Team lead | Slack #dev-ops |

---

## Diagnostics Toolkit

```bash
# Collect all diagnostic data
mkdir -p /tmp/nucleus-diagnostics-$(date +%s)
cd /tmp/nucleus-diagnostics-*/

# Logs
journalctl -u nucleus -n 1000 > nucleus.log

# Current state
curl http://localhost:9090/health | jq . > health.json
curl http://localhost:9090/metrics > metrics.prom

# System state
free -h > memory.txt
top -b -n 1 > top.txt
iostat -x 1 5 > iostat.txt
netstat -an | grep 5433 > connections.txt

# Database state (if responsive)
sqlite3 nucleus.db "SELECT table_name, row_count FROM tables;" > tables.sql

# Compact archive
tar czf nucleus-diagnostics-$(date +%s).tar.gz *
```

Upload to debugging system:
```bash
curl -X POST https://diagnostics.internal/upload \
  -F "file=@nucleus-diagnostics-*.tar.gz"
```

---

## See Also

- `DEPLOYMENT-GUIDE.md` — Normal deployment procedures
- `.github/workflows/full_regression.yml` — CI/CD automation
- `nucleus/src/metrics/mod.rs` — Metrics implementation
