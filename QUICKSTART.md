# Phase 4 Infrastructure Quick Start (5 Minutes)

Get Nucleus Phase 4 optimizations running with monitoring in 5 minutes.

## Prerequisites

- Rust toolchain installed
- 5 minutes of time
- Terminal access

## Step 1: Integration (1 minute)

```bash
cd /Users/tyler/Documents/proj\ rn/tystack

# Add config module export (one line)
echo "" >> nucleus/src/config/mod.rs
echo "pub mod optimizations;" >> nucleus/src/config/mod.rs

# Verify metrics module export (should already be done)
grep "pub mod optimizations" nucleus/src/metrics/mod.rs
# Should output: pub mod optimizations;
```

**If grep returned nothing, add it**:
```bash
# Find where pub mod declarations are
head -50 nucleus/src/metrics/mod.rs | grep "pub mod"

# Add after the other pub mod lines
echo "pub mod optimizations;" >> nucleus/src/metrics/mod.rs
```

## Step 2: Build (2 minutes)

```bash
cd nucleus
cargo build --release --features bench-tools 2>&1 | tail -20
# Should complete with "Finished" message
```

## Step 3: Run (1 minute)

```bash
# Start Nucleus with metrics
./target/release/nucleus \
  --binary-port 5433 \
  --metrics-port 9090 \
  --zone-map-threshold-rows 1000000 &

NUCLEUS_PID=$!
sleep 2
```

## Step 4: Verify (1 minute)

```bash
# Health check
echo "=== Health Check ==="
curl -s http://localhost:9090/health | jq '.status'
# Expected: "healthy"

# Metrics
echo "=== Sample Metrics ==="
curl -s http://localhost:9090/metrics | head -10
# Expected: Prometheus format metrics

# Quick benchmark
echo "=== Quick Benchmark ==="
./target/release/compete --iterations 2 --rows 100 2>&1 | tail -5
```

## Results

If all three commands succeeded, you have:

✓ **Nucleus running** with all Phase 4 optimizations
✓ **Health endpoint** at `http://localhost:9090/health`
✓ **Metrics endpoint** at `http://localhost:9090/metrics`
✓ **Benchmark working** (compete.rs)

## Next Steps

### Option A: Local Development (5 more minutes)

```bash
# Install Prometheus (Docker)
docker run -d \
  -p 9091:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus

# Create prometheus.yml
cat > prometheus.yml <<EOF
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: 'nucleus'
    static_configs:
      - targets: ['localhost:9090']
EOF

# Check Prometheus
open http://localhost:9091
# Click "Status" → "Targets", should show "nucleus" as UP

# Query a metric
# In Prometheus, type: nucleus_queries_total
# Should return a time series
```

### Option B: Production Deployment (See DEPLOYMENT-GUIDE.md)

```bash
# 1. Review deployment strategy
cat DEPLOYMENT-GUIDE.md

# 2. Run against staging database
./target/release/nucleus \
  --zone-map-threshold-rows 10000000 \
  --lazy-materialization-memory-limit-bytes 536870912

# 3. Follow Week 0-3 binary protocol rollout
# (1% → 10% → 50% → 100% traffic)

# 4. Follow Week 4-7 analytics rollout
# (zone maps → GROUP BY → lazy mat → SIMD)
```

### Option C: Learn About Optimizations (See docs)

```bash
# Configuration options
cat nucleus/src/config/optimizations.rs | head -100

# Metrics available
cat nucleus/src/metrics/optimizations.rs | head -100

# Emergency procedures
cat RUNBOOK.md | head -50

# Quick commands
cat OPTIMIZATION-QUICK-REFERENCE.md
```

## Troubleshooting

### Build fails

```bash
# Clear and rebuild
cd nucleus
cargo clean
cargo build --release --features bench-tools

# Check dependencies
cargo check
```

### Health endpoint 404

```bash
# Verify metrics server is running
ps aux | grep nucleus

# Check port
lsof -i :9090
# Should show nucleus listening

# Try again
curl -v http://localhost:9090/health
```

### Compete benchmark fails

```bash
# Check if PostgreSQL is running (required for compete)
psql -h localhost -l

# If not, skip Redis backend
./target/release/compete --skip redis --iterations 2 --rows 100
```

## Useful Commands

```bash
# Health check
curl http://localhost:9090/health | jq .

# All metrics
curl http://localhost:9090/metrics

# Binary protocol latency (Prometheus)
curl 'http://localhost:9091/api/v1/query?query=nucleus_binary_latency_microseconds_bucket'

# Stop Nucleus
kill $NUCLEUS_PID

# Full test suite
cargo test --lib -q

# Benchmarks
cargo bench --bench phase4_bench
```

## Performance Expectations

With optimizations enabled:

| Metric | Value |
|--------|-------|
| Binary latency p50 | 10-15 μs |
| Binary latency p99 | 20-30 μs |
| Zone map skip ratio | 20-30% |
| Query throughput | +15-20% vs baseline |
| Memory usage | -10-20% with lazy mat |
| SIMD AVX512 ratio | >50% on modern hardware |

## 5-Minute Summary

```
✓ Integrated config/metrics modules (30 sec)
✓ Built nucleus release (2 min)
✓ Started with optimizations (30 sec)
✓ Verified endpoints working (1 min)
✓ Total: ~4 minutes
```

## Next 30 Minutes

1. Set up Prometheus + Grafana (15 min)
2. Run compete benchmark (5 min)
3. Review DEPLOYMENT-GUIDE.md (5 min)
4. Plan canary deployment (5 min)

## Files to Review

| File | Purpose | Time |
|------|---------|------|
| INTEGRATION-CHECKLIST.md | Detailed integration steps | 5 min |
| DEPLOYMENT-GUIDE.md | How to safely deploy | 20 min |
| RUNBOOK.md | What to do if something breaks | 20 min |
| MONITORING-SETUP.md | Prometheus + Grafana setup | 15 min |
| OPTIMIZATION-QUICK-REFERENCE.md | Quick commands | 5 min |

## Getting Help

| Question | Answer |
|----------|--------|
| "How do I deploy to prod?" | See DEPLOYMENT-GUIDE.md |
| "What broke and how do I fix it?" | See RUNBOOK.md |
| "How do I monitor?" | See MONITORING-SETUP.md |
| "What commands do I need?" | See OPTIMIZATION-QUICK-REFERENCE.md |
| "How do I integrate?" | See INTEGRATION-CHECKLIST.md |

---

**Status**: Ready to run
**Time**: 5 minutes
**Effort**: Easy
**Risk**: Very Low

**Next**: Review DEPLOYMENT-GUIDE.md for safe production deployment strategy.
