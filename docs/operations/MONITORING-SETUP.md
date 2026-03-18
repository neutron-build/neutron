# Nucleus Monitoring & Observability Setup

## Overview

Complete monitoring infrastructure for Phase 4 optimizations:
- Prometheus metrics endpoint (`/metrics`)
- Health check endpoint (`/health`)
- Grafana dashboards
- Alert rules
- Weekly trending reports

## 1. Prometheus Setup

### 1.1 Metrics Endpoint

Nucleus exposes Prometheus metrics at:
```
http://localhost:9090/metrics
```

Key optimization metrics:

**Binary Protocol**:
```
nucleus_binary_connections_active
nucleus_binary_latency_microseconds_bucket{le="..."}
nucleus_binary_error_rate
nucleus_binary_parse_errors_total
```

**Zone Maps**:
```
nucleus_zone_map_granules_scanned_total
nucleus_zone_map_granules_skipped_total
nucleus_zone_map_skip_ratio_percent
nucleus_zone_map_recompute_operations_total
```

**GROUP BY**:
```
nucleus_group_by_specialized_total
nucleus_group_by_generic_fallback_total
nucleus_group_by_int_specialization_total
nucleus_group_by_string_specialization_total
```

**Lazy Materialization**:
```
nucleus_lazy_materialization_materialized_rows_total
nucleus_lazy_materialization_deferred_rows_total
nucleus_lazy_materialization_memory_saved_bytes_total
nucleus_lazy_materialization_ratio_percent
```

**SIMD**:
```
nucleus_simd_aggregates_executed_total
nucleus_simd_cpu_dispatch_avx512_total
nucleus_simd_cpu_dispatch_scalar_total
nucleus_simd_cpu_dispatch_avx512_ratio_percent
nucleus_simd_correctness_mismatches_total
```

### 1.2 Prometheus Configuration

Add to `prometheus.yml`:
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'nucleus'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 5s  # Tight interval for optimization metrics
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance

  # Optional: separate scrape for baseline comparison
  - job_name: 'nucleus-baseline'
    static_configs:
      - targets: ['baseline-host:9090']
    scrape_interval: 5s
```

Start Prometheus:
```bash
docker run -d \
  -p 9091:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus
```

### 1.3 Verify Metrics Collection

```bash
# Check if Nucleus is scraping
curl http://localhost:9091/api/v1/targets

# Query a metric
curl 'http://localhost:9091/api/v1/query?query=nucleus_queries_total'
```

## 2. Health Check Endpoint

### 2.1 Health Check Response

```bash
curl http://localhost:9090/health | jq .
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2026-03-14T15:30:00Z",
  "version": "0.1.0",
  "uptime_seconds": 86400,

  "binary_protocol": {
    "enabled": true,
    "port": 5433,
    "connections_active": 45,
    "latency_p50_us": 12.3,
    "latency_p95_us": 18.5,
    "latency_p99_us": 25.1,
    "error_rate": 0.0001,
    "errors_total": 10
  },

  "zone_maps": {
    "enabled": true,
    "granules_scanned": 1000000,
    "granules_skipped": 250000,
    "skip_ratio_percent": 25.0
  },

  "optimizations": {
    "group_by_specialization": true,
    "lazy_materialization": true,
    "simd_aggregates": true
  },

  "database": {
    "active_connections": 120,
    "open_transactions": 5,
    "cache_hit_ratio": 0.872,
    "wal_size_bytes": 1073741824
  },

  "metrics": {
    "queries_total": 1234567,
    "queries_per_second": 250.5,
    "query_duration_p50_seconds": 0.0034,
    "query_duration_p95_seconds": 0.0125,
    "query_duration_p99_seconds": 0.0891,
    "rows_scanned_total": 5000000000
  }
}
```

### 2.2 Health Check for Load Balancers

Kubernetes:
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 9090
  initialDelaySeconds: 10
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /health
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 1
```

HAProxy:
```
listen nucleus
  bind 0.0.0.0:5432
  mode tcp
  option tcplog
  server nucleus1 127.0.0.1:5432 check inter 1000 rise 2 fall 3 port 9090
```

## 3. Grafana Dashboards

### 3.1 Dashboard: Phase 4 Optimizations Overview

Key panels:

**Binary Protocol**:
- Connections (gauge)
- Latency percentiles (p50, p95, p99) — line chart
- Throughput (qps) — bar chart
- Error rate — gauge with threshold

**Zone Maps**:
- Granules scanned vs skipped (stacked bar)
- Skip ratio percentage (gauge)
- Recomputation duration (histogram)

**Analytics**:
- GROUP BY specialization ratio (pie chart)
- Lazy materialization ratio (gauge)
- Memory saved (counter)

**SIMD**:
- CPU dispatch breakdown (pie: AVX512 vs AVX2 vs scalar)
- Aggregate throughput (line)
- Correctness mismatches (counter)

### 3.2 Dashboard JSON

Save as `grafana-phase4.json`:
```json
{
  "dashboard": {
    "title": "Nucleus Phase 4 Optimizations",
    "tags": ["nucleus", "phase4", "optimizations"],
    "timezone": "UTC",
    "panels": [
      {
        "title": "Binary Latency (p50, p95, p99)",
        "targets": [
          {
            "expr": "histogram_quantile(0.50, nucleus_binary_latency_microseconds_bucket)"
          },
          {
            "expr": "histogram_quantile(0.95, nucleus_binary_latency_microseconds_bucket)"
          },
          {
            "expr": "histogram_quantile(0.99, nucleus_binary_latency_microseconds_bucket)"
          }
        ],
        "yaxes": [{"label": "Microseconds"}]
      },
      {
        "title": "Zone Map Skip Ratio",
        "targets": [
          {
            "expr": "nucleus_zone_map_skip_ratio_percent"
          }
        ],
        "yaxes": [{"label": "Percentage", "max": 100}]
      },
      {
        "title": "SIMD CPU Dispatch",
        "targets": [
          {
            "expr": "nucleus_simd_cpu_dispatch_avx512_ratio_percent"
          },
          {
            "expr": "nucleus_simd_cpu_dispatch_scalar_ratio_percent"
          }
        ]
      }
    ]
  }
}
```

Import into Grafana:
```bash
curl -X POST http://localhost:3000/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d @grafana-phase4.json
```

## 4. Alert Rules

### 4.1 Prometheus Alert Rules

Create `prometheus-rules.yml`:
```yaml
groups:
  - name: nucleus_phase4
    interval: 30s
    rules:
      # Binary Protocol Alerts
      - alert: BinaryProtocolLatencyHigh
        expr: histogram_quantile(0.99, nucleus_binary_latency_microseconds_bucket) > 30
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Binary protocol latency p99 > 30μs"
          description: "{{ $value }}μs"

      - alert: BinaryProtocolErrorRate
        expr: increase(nucleus_binary_parse_errors_total[5m]) > 100
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Binary protocol errors spiking"

      # Zone Map Alerts
      - alert: ZoneMapSkipRatioLow
        expr: nucleus_zone_map_skip_ratio_percent < 5
        for: 10m
        labels:
          severity: info
        annotations:
          summary: "Zone maps not effective (skip ratio < 5%)"

      # Memory Alerts
      - alert: LazyMaterializationMemoryHigh
        expr: process_resident_memory_bytes > 536870912  # 512 MB
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Memory usage elevated"

      # SIMD Correctness
      - alert: SimdCorrectnessIssue
        expr: increase(nucleus_simd_correctness_mismatches_total[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "SIMD result mismatch detected"

      # Query Latency
      - alert: QueryLatencyHigh
        expr: histogram_quantile(0.99, nucleus_query_duration_seconds_bucket) > 5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Query latency p99 > 5s"
```

Load into Prometheus:
```yaml
# prometheus.yml
rule_files:
  - "prometheus-rules.yml"

alerting:
  alertmanagers:
    - static_configs:
        - targets: ['localhost:9093']
```

### 4.2 Alert Manager Configuration

Create `alertmanager.yml`:
```yaml
global:
  resolve_timeout: 5m

route:
  group_by: ['alertname', 'cluster', 'service']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'pagerduty'
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty'
      group_wait: 0s
      repeat_interval: 5m
    - match:
        severity: warning
      receiver: 'slack'
      group_wait: 1m

receivers:
  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: '...'
  - name: 'slack'
    slack_configs:
      - api_url: 'https://hooks.slack.com/...'
        channel: '#nucleus-alerts'
```

Start Alert Manager:
```bash
docker run -d \
  -p 9093:9093 \
  -v $(pwd)/alertmanager.yml:/etc/alertmanager/alertmanager.yml \
  prom/alertmanager
```

## 5. Weekly Performance Trending

### 5.1 Automated Benchmarking

Cron job (run weekly Wednesday 2 AM UTC):
```bash
#!/bin/bash
# /usr/local/bin/nucleus-weekly-benchmark.sh

set -e

DATE=$(date +%Y%m%d)
RESULTS_DIR="/var/nucleus/benchmarks"

mkdir -p "$RESULTS_DIR"

cd /opt/nucleus

# Build release
cargo build --release --features bench-tools

# Run benchmark
echo "Running compete benchmark..."
./target/release/compete \
  --iterations 100 \
  --rows 50000 \
  --output "$RESULTS_DIR/compete_results_$DATE.json"

# Upload to monitoring system
echo "Uploading results..."
curl -X POST https://monitoring.internal/api/benchmarks \
  -H "Authorization: Bearer $MONITORING_API_KEY" \
  -F "file=@$RESULTS_DIR/compete_results_$DATE.json"

# Generate trend report
echo "Generating trend report..."
python3 /opt/nucleus/scripts/trend_analysis.py \
  --input-dir "$RESULTS_DIR" \
  --output "$RESULTS_DIR/trend_report_$DATE.html"

echo "Benchmark complete: $RESULTS_DIR/compete_results_$DATE.json"
```

Add to crontab:
```
0 2 * * 3 /usr/local/bin/nucleus-weekly-benchmark.sh >> /var/log/nucleus-benchmark.log 2>&1
```

### 5.2 Trend Analysis Script

Create `scripts/trend_analysis.py`:
```python
#!/usr/bin/env python3
"""Analyze nucleus benchmark trends over time."""

import json
import sys
from pathlib import Path
from typing import Dict, List
import statistics

def load_results(directory: str) -> List[Dict]:
    """Load all compete results from directory."""
    results = []
    for file in sorted(Path(directory).glob('compete_results_*.json')):
        with open(file) as f:
            data = json.load(f)
            data['date'] = file.stem.split('_')[-1]
            results.append(data)
    return results

def analyze_trends(results: List[Dict]) -> Dict:
    """Analyze performance trends."""
    if not results:
        return {}

    trends = {
        'dates': [r['date'] for r in results],
        'metrics': {}
    }

    # Extract key metrics
    for result in results:
        for metric, value in result.get('metrics', {}).items():
            if metric not in trends['metrics']:
                trends['metrics'][metric] = []
            trends['metrics'][metric].append(value)

    # Calculate trends
    for metric, values in trends['metrics'].items():
        if len(values) < 2:
            continue

        first = values[0]
        last = values[-1]
        change_pct = ((last - first) / first * 100) if first != 0 else 0

        trends['metrics'][metric] = {
            'values': values,
            'first': first,
            'last': last,
            'change_pct': change_pct,
            'avg': statistics.mean(values),
            'stdev': statistics.stdev(values) if len(values) > 1 else 0
        }

    return trends

def main():
    """Generate trend report."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--input-dir', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()

    results = load_results(args.input_dir)
    trends = analyze_trends(results)

    # Generate HTML report
    html = """
    <html>
    <head>
        <title>Nucleus Weekly Benchmark Trends</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body { font-family: Arial; margin: 20px; }
            .metric { margin: 20px 0; padding: 10px; border: 1px solid #ccc; }
            .warning { color: red; }
            .ok { color: green; }
        </style>
    </head>
    <body>
        <h1>Nucleus Phase 4 Benchmark Trends</h1>
    """

    for metric, data in trends.get('metrics', {}).items():
        status = 'warning' if data['change_pct'] > 5 else 'ok'
        html += f"""
        <div class="metric">
            <h2>{metric}</h2>
            <div class="{status}">
                First: {data['first']:.2f} | Last: {data['last']:.2f}
                | Change: {data['change_pct']:.1f}%
            </div>
            <div id="chart_{metric}"></div>
            <script>
                Plotly.newPlot('chart_{metric}', [{{
                    x: {trends['dates']},
                    y: {data['values']},
                    type: 'scatter'
                }}]);
            </script>
        </div>
        """

    html += "</body></html>"

    with open(args.output, 'w') as f:
        f.write(html)

    print(f"Report: {args.output}")

if __name__ == '__main__':
    main()
```

## 6. Continuous Monitoring Best Practices

### 6.1 Metric Collection Interval

- **Nucleus metrics**: 5-second scrape interval (optimization focus)
- **Host metrics**: 30-second scrape interval
- **Custom application metrics**: 15-second scrape interval

### 6.2 Storage & Retention

```yaml
# prometheus.yml
global:
  external_labels:
    cluster: production
    service: nucleus

# Keep 30 days of data
command: --storage.tsdb.retention.time=30d
```

### 6.3 Backup Metrics

```bash
# Daily snapshot of metrics database
0 3 * * * /usr/local/bin/prometheus-backup.sh
```

## 7. Capacity Planning

Track monthly in spreadsheet:

| Date | Binary QPS | Zone Map Skip% | Memory (GB) | CPU% | Latency p99 (ms) |
|------|-----------|----------------|------------|------|------------------|
| 2026-03-01 | 10k | 20% | 4.5 | 35% | 8.5 |
| 2026-04-01 | 12k | 22% | 4.8 | 38% | 9.2 |
| 2026-05-01 | 15k | 24% | 5.2 | 42% | 9.8 |

Decision rules:
- If CPU >85% for 1 week: scale horizontally
- If memory >90% of limit: increase buffer pool
- If latency p99 >20ms: rebuild indexes or tune optimizer

## See Also

- `DEPLOYMENT-GUIDE.md` — Deployment procedures
- `RUNBOOK.md` — Incident response
- `.github/workflows/full_regression.yml` — CI/CD
