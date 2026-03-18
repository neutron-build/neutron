# Nucleus Deployment Guide

## Overview

This guide covers deploying Nucleus with Phase 4 optimizations (binary protocol, analytics, zone maps, SIMD) to production with zero downtime.

## Deployment Strategy: Blue-Green with Feature Flags

### Phase 1: Binary Protocol (Week 0-3)

#### 1.1 Pre-Deployment Checklist

- [ ] All CI tests passing on main
- [ ] Compete benchmark baseline established (see Metrics section)
- [ ] Staging environment matches production schema/data
- [ ] Rollback procedure tested
- [ ] On-call team briefed on metrics to monitor

#### 1.2 Deployment Steps

1. **Build release binary**:
   ```bash
   cd nucleus
   cargo build --release --features bench-tools
   ```

2. **Deploy with binary protocol DISABLED** (first deployment):
   ```bash
   # Start new instance with feature flag disabled
   ./target/release/nucleus \
     --binary-port 5433 \
     --disable-binary-protocol=true \
     --metrics-port 9090
   ```
   - Standard PostgreSQL protocol (5432) remains active
   - Binary protocol port (5433) available but feature disabled
   - All tests must pass without regression

3. **Verify health**:
   ```bash
   # Check health endpoint
   curl http://localhost:9090/health

   # Expected response:
   # {
   #   "status": "healthy",
   #   "binary_protocol": { "enabled": false },
   #   "uptime_seconds": 120.5
   # }
   ```

4. **Run smoke tests** against staging:
   ```bash
   # Run compete benchmark (10 iterations, small dataset)
   cd nucleus
   cargo run --release --features bench-tools --bin compete \
     --skip cockroach,tidb,mongodb \
     --iterations 10 --rows 1000
   ```

5. **Enable for 1% of traffic** (canary):
   ```bash
   # Update load balancer: route 1% of connections to binary protocol
   # Client: set connection_type=binary for 1% of sessions
   ```
   - Monitor for 10 minutes
   - Expected metrics:
     - `binary_latency_p50 < 15μs`
     - `binary_error_rate = 0%`

6. **If canary succeeds, expand to 10%**:
   ```bash
   # Update load balancer: route 10% of connections
   # Monitor for 30 minutes
   ```

7. **If 10% succeeds, expand to 50%**:
   ```bash
   # Update load balancer: route 50% of connections
   # Monitor for 1 hour
   # Alert thresholds: latency <20μs, error rate <0.1%
   ```

8. **If 50% succeeds, expand to 100%**:
   ```bash
   # Update load balancer: route 100% of connections
   # Keep old instances running for 1 hour (rollback window)
   ```

#### 1.3 Rollback Procedures

**Immediate rollback** (if binary protocol crashes):
```bash
# Option 1: Disable flag on running instance (instant, no restart)
# Send SIGHUP to trigger config reload
kill -HUP $(pgrep nucleus)
# Expected: all binary connections switch to error-on-connect

# Option 2: Restart with feature disabled
pkill nucleus
./target/release/nucleus --disable-binary-protocol=true
```
- Data loss: **0** (no writes during protocol negotiation)
- Connection loss: All binary connections drop gracefully (15s max)
- Recovery time: <30 seconds
- User impact: Clients see brief network error, auto-reconnect to SQL protocol

**Gradual rollback** (if latency degrades):
```bash
# Reduce binary traffic 100% → 50% → 25% → 0%
# Each step: monitor for 5 minutes
# Once at 0%, disable flag and remove old instances
```

### Phase 2: Analytics Optimizations (Week 4-7)

#### 2.1 Zone Maps (Week 4)

1. **Enable automatically for tables >10M rows**:
   ```bash
   # No CLI flag needed — automatic based on table size
   ./target/release/nucleus --zone-map-threshold-rows 10000000
   ```

2. **Monitor zone map effectiveness**:
   ```
   Metric: nucleus_zone_map_granules_skipped_total
   Alert: skip_ratio < 10% (zone maps not effective)
   Target: skip_ratio > 20% (good selectivity)
   ```

3. **If effective for large tables, lower threshold to 1M rows**:
   ```bash
   ./target/release/nucleus --zone-map-threshold-rows 1000000
   ```

#### 2.2 GROUP BY Specialization (Week 5)

- **Always enabled** (backward compatible, safe optimization)
- No feature flag or config needed
- Monitor: `nucleus_group_by_specialized_queries_total` vs `nucleus_group_by_generic_fallback_total`
- Target: >95% queries use specialized path

#### 2.3 Lazy Materialization (Week 6)

1. **Enable with feature flag**:
   ```bash
   ./target/release/nucleus --enable-lazy-materialization
   ```

2. **Monitor memory savings**:
   ```
   Metric: nucleus_lazy_materialization_memory_saved_bytes_total
   Alert: if 0 or negative (memory not being saved)
   Target: >10% memory reduction vs eager materialization
   ```

3. **Compare against eager path**:
   - Run same workload with `--disable-lazy-materialization`
   - Measure: query latency, peak memory, tail latency (p99)
   - Decision: keep if latency not worse than +5%, memory saved >5%

#### 2.4 SIMD Aggregates (Week 7)

- **Always enabled** (backward compatible, safe optimization)
- No feature flag or config needed
- Monitor CPU dispatch percentages:
  ```
  nucleus_simd_cpu_dispatch_avx512 (should be high on modern hardware)
  nucleus_simd_cpu_dispatch_avx2
  nucleus_simd_cpu_dispatch_scalar (should be low)
  ```

### Phase 3: Full Optimization Stack (Week 8)

1. **Enable all optimizations**:
   ```bash
   ./target/release/nucleus \
     --binary-port 5433 \
     --zone-map-threshold-rows 1000000 \
     --enable-lazy-materialization \
     --metrics-port 9090
   ```

2. **Run full compete benchmark**:
   ```bash
   cd nucleus
   cargo run --release --features bench-tools --bin compete \
     --iterations 500 --rows 50000
   ```

3. **Validate all metrics**:
   - Binary latency p50 <10μs, p99 <30μs
   - Zone map skip ratio >20%
   - Memory usage 10-20% lower than baseline
   - Query throughput +15% vs baseline

## Monitoring & Alerts

### Health Check Endpoint

```bash
GET http://localhost:9090/health
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2026-03-14T15:30:00Z",
  "binary_protocol": {
    "enabled": true,
    "latency_p50_us": 12.3,
    "latency_p95_us": 18.5,
    "latency_p99_us": 25.1,
    "error_rate": 0.0001,
    "active_connections": 45
  },
  "optimizations": {
    "zone_maps": true,
    "group_by_specialization": true,
    "lazy_materialization": true,
    "simd_aggregates": true
  },
  "metrics": {
    "queries_total": 1234567,
    "query_duration_p50_seconds": 0.0034,
    "query_duration_p99_seconds": 0.0891,
    "cache_hit_ratio": 0.872,
    "uptime_seconds": 86400
  }
}
```

### Prometheus Metrics Endpoint

```bash
GET http://localhost:9090/metrics
```

Exposes all metrics in Prometheus text format.

### Critical Alerts

| Alert | Threshold | Action |
|-------|-----------|--------|
| Binary latency p99 | >30μs | Page on-call, investigate hot-spot queries |
| Binary error rate | >1% | Disable binary protocol, investigate crash logs |
| Zone map skip ratio | <5% | Disable zone maps, review selectivity analysis |
| Memory OOM | Any | Disable lazy materialization, restart |
| Cache hit ratio | <50% | Check cache eviction, increase buffer pool |
| Query latency p99 | >5s | Check plan changes, rebuild indexes |

### Weekly Performance Trending

```bash
#!/bin/bash
# Run weekly (Wednesday 2 AM UTC)
cd nucleus
cargo build --release
./target/release/compete --iterations 100 --rows 50000 \
  --output compete_results_$(date +%Y%m%d).json

# Upload to monitoring system
curl -X POST https://monitoring.internal/api/benchmarks \
  -F "file=@compete_results_$(date +%Y%m%d).json"
```

## Emergency Procedures

### Scenario: Binary Protocol Crashes in Production

**Time to action**: <5 minutes

```bash
# Step 1: Disable feature immediately (no restart)
kill -HUP $(pgrep nucleus)
# Clients will see connection reset, reconnect via SQL protocol

# Step 2: Alert team
# "Binary protocol disabled due to crash. Investigating."

# Step 3: Collect diagnostic data
journalctl -u nucleus -n 1000 > /tmp/nucleus_crash.log

# Step 4: When ready, restart with feature disabled
systemctl stop nucleus
./target/release/nucleus --disable-binary-protocol
systemctl start nucleus

# Step 5: Verify recovery
curl http://localhost:9090/health | jq '.binary_protocol.enabled'
# Expected: false

# Step 6: Run smoke tests
cd nucleus
cargo test --lib wire -q
```

### Scenario: Zone Maps Cause Wrong Results

**Time to action**: <10 minutes

```bash
# Step 1: Disable zone maps
kill -HUP $(pgrep nucleus)  # or restart with --disable-zone-maps

# Step 2: Verify query results match expected output
# Run validation queries against both old and new instances

# Step 3: Recompute zone maps if safe
# nucleus CLI: REINDEX zone_maps

# Step 4: Re-enable gradually (same canary process as initial deployment)
```

### Scenario: Memory Usage Spike (Lazy Materialization)

**Time to action**: <3 minutes

```bash
# Step 1: Check memory before disabling
free -h

# Step 2: Disable lazy materialization
./target/release/nucleus --disable-lazy-materialization

# Step 3: Monitor memory drop (should be within 1 minute)
watch -n 5 'free -h'

# Step 4: If memory recovers, investigate row cleanup
# Profile: which queries leak materialization buffers

# Step 5: Fix + re-enable
```

## Capacity Planning

Track monthly:

| Metric | Baseline | Phase 4 | Change |
|--------|----------|---------|--------|
| Binary throughput (qps) | N/A | 50k+ | +∞ |
| SQL latency p99 (ms) | 10 | 8 | -20% |
| Zone map skip ratio (%) | 0 | 25 | +25 |
| Memory per GB data | 1.5 GB | 1.2 GB | -20% |
| CPU util (4 cores) | 40% | 35% | -12.5% |

## Runbook Summary

1. **Deploy binary protocol** → Canary 1% → 10% → 50% → 100%
2. **Enable zone maps** → Monitor skip ratio → Expand if >20%
3. **Enable lazy materialization** → Compare memory vs latency trade-off
4. **Enable SIMD** → Monitor CPU dispatch percentages
5. **Validate full stack** → Run compete benchmark → measure vs baseline

## See Also

- `RUNBOOK.md` — Incident response procedures
- `nucleus/src/metrics/mod.rs` — Metrics implementation
- `.github/workflows/binary_protocol.yml` — CI/CD automation
- `.github/workflows/analytics_optimization.yml` — Analytics CI/CD
- `.github/workflows/full_regression.yml` — Full regression testing
