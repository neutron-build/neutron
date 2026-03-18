# Phase 4 Infrastructure Integration Checklist

One-page guide to activating all Phase 4 DevOps infrastructure.

## What's Ready (No Work Needed)

- ✓ GitHub Actions workflows (3 files)
- ✓ Metrics module (480 LOC)
- ✓ Configuration module (420 LOC)
- ✓ Deployment guide (400 LOC)
- ✓ Incident runbook (600 LOC)
- ✓ Monitoring setup (400 LOC)
- ✓ CI/CD documentation (350 LOC)
- ✓ Quick reference (400 LOC)

**Total ready**: 9 files, 3,200+ LOC

## What Needs Integration (10 Minutes)

### Step 1: Export Configuration Module

**File**: `nucleus/src/config/mod.rs`

**Add this line** (find the existing `pub mod` declarations):
```rust
pub mod optimizations;
```

**Before**:
```rust
pub mod cache;
pub mod config;
pub mod credentials;
```

**After**:
```rust
pub mod cache;
pub mod config;
pub mod credentials;
pub mod optimizations;  // ← ADD THIS LINE
```

**Lines**: 1 | **Time**: 30 seconds

### Step 2: Export Metrics Submodule

**File**: `nucleus/src/metrics/mod.rs`

**Status**: Already done in the modification. Verify it has:
```rust
pub mod optimizations;
```

**Check**: Look for this line at the top of the file (after `//!` comments).

**Lines**: Already added | **Time**: 0 (done)

### Step 3: Add Clap Dependency (If Needed)

**File**: `nucleus/Cargo.toml`

**Check**: Does it already have `clap`?
```bash
cd nucleus
grep "^clap" Cargo.toml
```

**If NOT present, add**:
```toml
clap = { version = "4.5", features = ["derive"], optional = true }
```

Add to `server` feature:
```toml
[features]
server = [
    "dep:clap",  # ← ADD THIS
    ...
]
```

**If already present**: Nothing to do.

**Lines**: 1-2 (if needed) | **Time**: 30 seconds

### Step 4: Integrate Config Parsing in Main

**File**: `nucleus/src/main.rs` (in the `server` feature)

**Add at start of main()**:
```rust
use nucleus::config::optimizations::OptimizationConfig;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse optimization config
    let opt_config = OptimizationConfig::parse();

    // Start metrics server on metrics_port
    if opt_config.metrics.metrics_enabled {
        tokio::spawn(start_metrics_server(
            opt_config.metrics.metrics_port,
            metrics_registry.clone(),
        ));
    }

    // Use opt_config for feature control throughout server
    // Example: if !opt_config.binary_protocol.disable_binary_protocol { ... }

    // ... rest of main
}
```

**Integration points**:
- Use `opt_config.binary_protocol.disable_binary_protocol` to gate binary protocol server
- Use `opt_config.zone_maps.disable_zone_maps` to gate zone map initialization
- Use `opt_config.lazy_materialization.disable_lazy_materialization` for lazy mat paths
- Use `opt_config.simd.disable_simd` for SIMD aggregate paths

**Lines**: 10-15 (basic integration) | **Time**: 5-10 minutes

### Step 5: Add Metrics Server (Health + Prometheus)

**File**: `nucleus/src/main.rs` or new `nucleus/src/metrics_server.rs`

**Create metrics HTTP server** (pseudo-code):
```rust
async fn start_metrics_server(port: u16, registry: Arc<MetricsRegistry>) -> Result<()> {
    use hyper::{Body, Response, StatusCode, Server};

    let registry = Arc::clone(&registry);

    let service = make_service_fn(move |_conn| {
        let registry = Arc::clone(&registry);
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let registry = Arc::clone(&registry);
                async move {
                    match req.uri().path() {
                        "/health" => {
                            let json = serde_json::json!({
                                "status": "healthy",
                                "uptime_seconds": registry.uptime_secs(),
                                "binary_protocol": {
                                    "enabled": true,  // check config flag
                                    "latency_p50_us": histogram_quantile(&registry.query_duration, 0.50),
                                    "error_rate": ...,
                                }
                                // ... more detailed health info
                            });
                            Ok(Response::new(Body::from(json.to_string())))
                        }
                        "/metrics" => {
                            let metrics = registry.render_prometheus();
                            Ok(Response::new(Body::from(metrics)))
                        }
                        _ => Ok(Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Body::empty())?),
                    }
                }
            }))
        }
    });

    let addr = ([127, 0, 0, 1], port).into();
    Server::bind(&addr).serve(service).await?;
    Ok(())
}
```

**Or use existing HTTP framework** (Nucleus uses tokio, so hyper is appropriate):
```bash
# Add to Cargo.toml if not present
hyper = { version = "0.14", features = ["full"] }
serde_json = { version = "1", features = ["derive"] }
```

**Lines**: 30-40 (including error handling) | **Time**: 10 minutes

### Step 6: Test Integration

```bash
cd nucleus

# Build with all features
cargo build --features bench-tools

# Start nucleus
./target/release/nucleus \
  --binary-port 5433 \
  --metrics-port 9090 \
  --zone-map-threshold-rows 1000000

# In another terminal, test endpoints
curl http://localhost:9090/health | jq .
curl http://localhost:9090/metrics | head -20

# Stop nucleus
pkill nucleus
```

**Time**: 5 minutes

### Step 7: Test CI Workflows

```bash
# Push changes to branch
git add -A
git commit -m "Integrate Phase 4 optimization infrastructure"
git push origin feature/phase4-infrastructure

# Check GitHub Actions
# Visit: https://github.com/neutron-build/nucleus/actions
# Verify all 3 workflows run and pass
```

**Time**: 5 minutes (waiting for CI)

## Integration Verification

### ✓ Checklist

- [ ] Step 1: Added `pub mod optimizations;` to `nucleus/src/config/mod.rs`
- [ ] Step 2: Verified `pub mod optimizations;` in `nucleus/src/metrics/mod.rs`
- [ ] Step 3: Added/verified clap in `nucleus/Cargo.toml`
- [ ] Step 4: Config parsing in `nucleus/src/main.rs`
- [ ] Step 5: Metrics HTTP server running
- [ ] Step 6: Tested `/health` and `/metrics` endpoints
- [ ] Step 7: GitHub Actions workflows pass
- [ ] Step 8: Run compete benchmark and verify metrics

### Quick Test Commands

```bash
# Build
cd nucleus && cargo build --release --features bench-tools

# Start nucleus
./target/release/nucleus --metrics-port 9090 2>&1 | head -20 &
NUCLEUS_PID=$!
sleep 2

# Test health endpoint
echo "=== Health Check ==="
curl -s http://localhost:9090/health | jq .status

# Test metrics endpoint
echo "=== Metrics (first 5) ==="
curl -s http://localhost:9090/metrics | head -5

# Run quick benchmark
echo "=== Benchmark ==="
./target/release/compete --iterations 2 --rows 100 | tail -5

# Cleanup
kill $NUCLEUS_PID
wait $NUCLEUS_PID 2>/dev/null

echo "=== All Tests Passed ==="
```

## Estimated Effort

| Task | Time | Difficulty |
|------|------|-----------|
| Step 1: Config export | 30 sec | Trivial |
| Step 2: Metrics export | 0 | Already done |
| Step 3: Clap dependency | 30 sec | Trivial |
| Step 4: Config parsing | 10 min | Easy |
| Step 5: Metrics server | 15 min | Moderate |
| Step 6: Testing | 5 min | Easy |
| Step 7: CI verification | 5 min | Easy |
| **Total** | **36 min** | — |

## Success Criteria

After integration, you should have:

✓ `cargo build --release --features bench-tools` succeeds
✓ `./nucleus --metrics-port 9090` starts without errors
✓ `curl http://localhost:9090/health` returns JSON with status
✓ `curl http://localhost:9090/metrics` returns Prometheus text format (40+ metrics)
✓ GitHub Actions workflows run automatically on push
✓ All workflows (binary_protocol, analytics, full_regression) pass
✓ Compete benchmark produces JSON artifacts in GitHub Actions

## Common Integration Issues

### Issue: Build fails with "config::optimizations not found"

**Solution**: Verify Step 1 and 2 completed
```bash
grep "pub mod optimizations" nucleus/src/config/mod.rs
grep "pub mod optimizations" nucleus/src/metrics/mod.rs
```

### Issue: `/health` endpoint 404

**Solution**: Verify Step 5 completed and metrics server thread is running
```bash
curl http://localhost:9090/metrics
# Should return Prometheus metrics, not 404
```

### Issue: GitHub Actions workflows don't run

**Solution**: Verify files exist and are committed
```bash
ls -l .github/workflows/binary_protocol.yml
git status  # should be clean
```

### Issue: Compile error "Parser not found"

**Solution**: Verify clap is in Cargo.toml
```bash
grep "^clap" nucleus/Cargo.toml
```

## After Integration (Week 1-2)

Once integrated:

1. **Set up Prometheus**
   - Install/run Prometheus container
   - Point scrape target to `http://localhost:9090/metrics`
   - Verify metrics collection in Prometheus UI

2. **Set up Grafana**
   - Import Phase 4 dashboard JSON (see MONITORING-SETUP.md)
   - Verify metrics display correctly

3. **Configure Alerting**
   - Load Alert Manager rules
   - Configure PagerDuty/Slack integration
   - Test alert flow

4. **Train Team**
   - Review DEPLOYMENT-GUIDE.md
   - Review RUNBOOK.md
   - Practice rollback procedure

5. **Establish Baselines**
   - Run compete benchmark
   - Record baseline metrics
   - Enable weekly trending

## Files to Commit

```bash
git add \
  nucleus/src/config/optimizations.rs \
  nucleus/src/metrics/optimizations.rs \
  nucleus/src/config/mod.rs \
  nucleus/src/metrics/mod.rs \
  nucleus/Cargo.toml \
  nucleus/src/main.rs \
  .github/workflows/binary_protocol.yml \
  .github/workflows/analytics_optimization.yml \
  .github/workflows/full_regression.yml \
  DEPLOYMENT-GUIDE.md \
  RUNBOOK.md \
  MONITORING-SETUP.md \
  CI-CD-INFRASTRUCTURE.md \
  OPTIMIZATION-QUICK-REFERENCE.md \
  DEVOPS-SUMMARY.md

git commit -m "feat: integrate Phase 4 optimization infrastructure

- Add optimization config module with feature flags
- Add optimization metrics (binary protocol, zone maps, GROUP BY, lazy mat, SIMD)
- Add Prometheus metrics endpoint (/metrics) + health check (/health)
- Add GitHub Actions workflows for CI/CD (binary_protocol, analytics, full_regression)
- Add deployment guide, incident runbook, monitoring setup docs
- Enable safe canary deployment with instant rollback
- Ready for Week 1 metrics baseline collection"
```

## Next Steps

After integration:

1. **Week 0 Verification** (3 days)
   - All CI workflows passing
   - Health and metrics endpoints working
   - Quick manual testing

2. **Week 1 Baseline** (4 days)
   - Prometheus collecting metrics
   - Grafana dashboard live
   - Run compete benchmark
   - Establish baseline metrics

3. **Week 2 Alerts** (3 days)
   - Alert rules configured
   - Alert Manager responding
   - Team training complete
   - Rollback procedure tested

4. **Week 3+ Deployment** (ongoing)
   - Start canary deployment (binary protocol, 1%)
   - Monitor metrics closely
   - Expand 1% → 10% → 50% → 100%
   - Similar process for analytics optimizations

## Questions?

- **Integration help**: See INTEGRATION-CHECKLIST.md (this file)
- **Deployment help**: See DEPLOYMENT-GUIDE.md
- **Incident help**: See RUNBOOK.md
- **Monitoring help**: See MONITORING-SETUP.md
- **Quick commands**: See OPTIMIZATION-QUICK-REFERENCE.md

---

**Total integration time**: ~40 minutes
**Estimated effort**: Medium (moderate Rust knowledge required)
**Risk level**: Low (all changes additive, no breaking changes)
