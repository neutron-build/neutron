// Command orm-project compares genuine Go APIs against an independent native
// PostgreSQL oracle. It creates and drops only a randomly named owned database.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Sample struct {
	Index       int    `json:"index"`
	Nanoseconds int64  `json:"nanoseconds"`
	Error       string `json:"error,omitempty"`
}
type Measurement struct {
	Trial           int      `json:"trial"`
	Position        int      `json:"position"`
	Provider        string   `json:"provider"`
	Workload        string   `json:"workload"`
	Concurrency     int      `json:"concurrency"`
	Calls           int      `json:"calls"`
	Errors          int      `json:"errors"`
	WallNanoseconds int64    `json:"wall_nanoseconds"`
	Throughput      float64  `json:"throughput_calls_per_second"`
	P50             int64    `json:"p50_nanoseconds"`
	P95             int64    `json:"p95_nanoseconds"`
	P99             int64    `json:"p99_nanoseconds"`
	Samples         []Sample `json:"samples"`
}
type Report struct {
	Status       string              `json:"status"`
	Stage        string              `json:"stage"`
	Started      string              `json:"started_utc"`
	Finished     string              `json:"finished_utc"`
	Database     string              `json:"owned_database,omitempty"`
	Cleaned      bool                `json:"owned_database_removed"`
	Source       map[string]string   `json:"source_sha256"`
	Provenance   map[string]any      `json:"provenance"`
	Checks       []Check             `json:"correctness"`
	Audit        map[string][]string `json:"untimed_sql_audit"`
	Associations []Check             `json:"association_probes"`
	Measurements []Measurement       `json:"measurements"`
	Failure      string              `json:"failure,omitempty"`
}

func guard(path string) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return err
	}
	if uint64(stat.Bavail)*uint64(stat.Bsize) < 6*1024*1024*1024 {
		return errors.New("disk free space below 6 GiB guard")
	}
	return nil
}
func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0600)
}
func sourceHashes() (map[string]string, error) {
	files := []string{"main.go", "providers.go", "fixture.go", "checks.go", "schema.sql", "go.mod", "go.sum", "README.md"}
	// Include the actual candidate implementation, not only this harness.
	more, err := filepath.Glob("../../nucleus/*.go")
	if err != nil {
		return nil, err
	}
	for _, p := range more {
		if !strings.HasSuffix(p, "_test.go") {
			files = append(files, p)
		}
	}
	result := map[string]string{}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		h := sha256.Sum256(data)
		result[path] = hex.EncodeToString(h[:])
	}
	return result, nil
}
func provenance() map[string]any {
	info := map[string]any{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0)}
	if executable, err := os.Executable(); err == nil {
		if data, err := os.ReadFile(executable); err == nil {
			h := sha256.Sum256(data)
			info["executable_sha256"] = hex.EncodeToString(h[:])
		}
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		info["module_build_info"] = bi
	}
	if rev, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
		info["git_revision"] = strings.TrimSpace(string(rev))
	}
	info["pool_contract"] = map[string]any{"max_connections": 4, "idle_timeout_seconds": 300, "lifetime_seconds": 3600, "pgx_min_connections": 0, "gorm_max_idle_connections": 4, "pgx_lifetime_jitter": 0, "protocol": "simple for all three; prepared caching deliberately not benchmarked"}
	info["contract"] = map[string]any{"relation": "two sequential queries; missing parent one query", "writes": "correctness only; explicit transaction all providers; GORM default write transactions enabled", "numeric": "Neutron exact string kind; GORM configured Decimal Scanner/Valuer; native oracle amount::text", "latency": "API call through complete row materialization, validation and report serialization excluded", "throughput": "includes workload dispatch, post-call validation and concurrency scheduling", "scope": "warm local reads; not cold start, production capacity, temporal values, memory, or cross-language rankings", "fixture": "2 tenants; 101 projects and 2000 documents per tenant; last project empty"}
	return info
}
func run(out string, samples, warmups, trials int, correctnessOnly bool) (returnErr error) {
	parent := filepath.Dir(out)
	if err := guard(parent); err != nil {
		return err
	}
	if !filepath.IsAbs(out) {
		return errors.New("output must be absolute")
	}
	if err := os.Mkdir(out, 0700); err != nil {
		return errors.New("output directory must be fresh")
	}
	report := Report{Status: "failed", Stage: "provenance", Started: time.Now().UTC().Format(time.RFC3339Nano), Provenance: provenance(), Audit: map[string][]string{}}
	defer func() {
		report.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		if returnErr != nil {
			report.Status = "failed"
			report.Failure = returnErr.Error()
		}
		if err := writeJSON(filepath.Join(out, "report.json"), report); err != nil && returnErr == nil {
			returnErr = err
		}
	}()
	var err error
	report.Source, err = sourceHashes()
	if err != nil {
		return errors.New("could not hash comparison sources")
	}
	adminURL := os.Getenv("ORM_ADMIN_URL")
	if adminURL == "" {
		return errors.New("ORM_ADMIN_URL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	report.Stage = "fixture"
	f, err := createFixture(ctx, adminURL)
	if err != nil {
		if f != nil {
			report.Database, report.Cleaned = f.Database, f.Cleaned
		}
		return errors.New("fixture creation failed: " + errorCategory(err))
	}
	report.Database = f.Database
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := f.Close(cleanupCtx); err != nil {
			report.Cleaned = false
			returnErr = errors.New("owned fixture cleanup failed")
		} else {
			report.Cleaned = true
		}
	}()
	report.Provenance["ddl_sha256"], report.Provenance["initial_native_digest"] = f.DDLHash, f.Digest
	report.Provenance["native_seed_validation"] = map[string]any{"documents": len(f.Docs), "projects": len(f.Projects), "explicit_exact_value_anchors": 8, "validated_before_provider_reads": true, "digest_tables": []string{"projects", "documents"}}
	var serverVersion string
	if err := f.Oracle.QueryRow(ctx, "SELECT version()").Scan(&serverVersion); err != nil {
		return errors.New("server identity failed")
	}
	report.Provenance["postgres_version"] = serverVersion
	settings := map[string]string{}
	for _, setting := range []string{"shared_buffers", "max_connections", "synchronous_commit", "jit", "work_mem", "timezone", "default_transaction_isolation"} {
		var value string
		if err := f.Oracle.QueryRow(ctx, "SELECT current_setting($1)", setting).Scan(&value); err != nil {
			return errors.New("server settings failed")
		}
		settings[setting] = value
	}
	report.Provenance["postgres_settings"] = settings
	audit := &queryAudit{}
	auditProviders, err := openProviders(ctx, f.URL, audit)
	if err != nil {
		return errors.New("audit provider open failed: " + errorCategory(err))
	}
	report.Stage = "untimed_audit"
	for _, p := range auditProviders {
		for _, workload := range []string{"point", "page", "relation", "missing_parent"} {
			audit.reset()
			if err := invoke(ctx, p, workload, 0); err != nil {
				for _, p := range auditProviders {
					p.Close()
				}
				return errors.New("SQL audit failed")
			}
			queries := audit.snapshot()
			report.Audit[p.Name()+":"+workload] = queries
			want := 1
			if workload == "relation" {
				want = 2
			}
			if len(queries) != want {
				for _, p := range auditProviders {
					p.Close()
				}
				return errors.New("SQL audit statement count differed for " + p.Name() + ":" + workload)
			}
		}
		if p, ok := p.(*gormProvider); ok {
			report.Associations, report.Audit["gorm:native_preload"] = associationProbe(ctx, f, p, audit)
		}
	}
	for _, p := range auditProviders {
		p.Close()
	}
	providers, err := openProviders(ctx, f.URL, nil)
	if err != nil {
		return errors.New("provider open failed: " + errorCategory(err))
	}
	defer func() {
		for _, p := range providers {
			p.Close()
		}
	}()
	report.Stage = "correctness"
	report.Checks, err = correctness(ctx, f, providers)
	if err != nil {
		return err
	}
	if correctnessOnly {
		report.Status, report.Stage = "passed", "correctness_complete"
		report.Provenance["measurement_skipped"] = true
		return nil
	}
	// Composite native preload remains a capability probe, separately reported.
	// A failed native association is not hidden by the matched manual strategy.
	report.Stage = "measurement"
	report.Provenance["samples_per_trial"], report.Provenance["warmups_per_phase"], report.Provenance["trials"] = samples, warmups, trials
	orders := [][]int{{0, 1, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}, {1, 0, 2}, {0, 2, 1}}
	report.Provenance["provider_order"] = orders
	for trial := 0; trial < trials; trial++ {
		for _, workload := range []string{"point", "page", "relation"} {
			for _, concurrency := range []int{1, 4} {
				for position, index := range orders[trial%len(orders)] {
					p := providers[index]
					for i := 0; i < warmups; i++ {
						if err := invokeChecked(ctx, p, workload, i, f); err != nil {
							return errors.New("warmup failed for " + p.Name())
						}
					}
					m := measure(ctx, p, workload, trial, position, concurrency, samples, f)
					report.Measurements = append(report.Measurements, m)
					if m.Errors > 0 {
						return errors.New("measured correctness failed for " + p.Name())
					}
				}
			}
		}
	}
	digest, err := nativeDigest(ctx, f.Oracle)
	if err != nil || digest != f.Digest {
		return errors.New("measurement mutated native fixture")
	}
	report.Provenance["final_native_digest"] = digest
	report.Status, report.Stage = "passed", "complete"
	return nil
}

func invoke(ctx context.Context, p Provider, workload string, i int) error {
	tenant := "a"
	if i%2 == 1 {
		tenant = "b"
	}
	switch workload {
	case "point":
		_, err := p.Point(ctx, tenant, int32(i%2000+1))
		return err
	case "page":
		_, err := p.Page(ctx, tenant, int32((i*31)%1980))
		return err
	case "relation":
		_, err := p.Relation(ctx, tenant, int32(i%100+1))
		return err
	case "missing_parent":
		_, err := p.Relation(ctx, tenant, 102)
		return err
	default:
		return errors.New("unknown workload")
	}
}
func invokeValue(ctx context.Context, p Provider, workload string, i int) (any, error) {
	tenant := "a"
	if i%2 == 1 {
		tenant = "b"
	}
	switch workload {
	case "point":
		return p.Point(ctx, tenant, int32(i%2000+1))
	case "page":
		return p.Page(ctx, tenant, int32((i*31)%1980))
	case "relation":
		return p.Relation(ctx, tenant, int32(i%100+1))
	default:
		return nil, errors.New("unknown workload")
	}
}
func expected(f *Fixture, workload string, i int) any {
	tenant := "a"
	if i%2 == 1 {
		tenant = "b"
	}
	switch workload {
	case "point":
		return []Document{f.Docs[key(tenant, int32(i%2000+1))]}
	case "page":
		return f.expectedPage(tenant, int32((i*31)%1980))
	case "relation":
		return f.expectedRelation(tenant, int32(i%100+1))
	default:
		return nil
	}
}
func invokeChecked(ctx context.Context, p Provider, workload string, i int, f *Fixture) error {
	got, err := invokeValue(ctx, p, workload, i)
	if err != nil {
		return err
	}
	return same(got, expected(f, workload, i))
}
func measure(ctx context.Context, p Provider, workload string, trial, position, concurrency, n int, f *Fixture) Measurement {
	m := Measurement{Trial: trial + 1, Position: position + 1, Provider: p.Name(), Workload: workload, Concurrency: concurrency, Calls: n, Samples: make([]Sample, n)}
	var wg sync.WaitGroup
	start := time.Now()
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < n; i += concurrency {
				t := time.Now()
				got, err := invokeValue(ctx, p, workload, i)
				elapsed := time.Since(t).Nanoseconds()
				if err == nil {
					err = same(got, expected(f, workload, i))
				}
				s := Sample{Index: i, Nanoseconds: elapsed}
				if err != nil {
					s.Error = errorCategory(err)
				}
				m.Samples[i] = s
			}
		}(worker)
	}
	wg.Wait()
	m.WallNanoseconds = time.Since(start).Nanoseconds()
	m.Throughput = float64(n) / float64(m.WallNanoseconds) * 1e9
	latencies := make([]int64, 0, n)
	for _, s := range m.Samples {
		latencies = append(latencies, s.Nanoseconds)
		if s.Error != "" {
			m.Errors++
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(q float64) int64 { return latencies[int(math.Ceil(q*float64(len(latencies))))-1] }
	m.P50, m.P95, m.P99 = percentile(.5), percentile(.95), percentile(.99)
	return m
}
func main() {
	out := flag.String("out", "", "fresh absolute private output directory")
	samples := flag.Int("samples", 100, "calls per provider/workload/concurrency/trial (minimum 100)")
	warmups := flag.Int("warmups", 100, "unmeasured verified warmup calls per phase (minimum 20)")
	trials := flag.Int("trials", 6, "balanced provider trials (multiple of 6)")
	correctnessOnly := flag.Bool("correctness-only", false, "run actual correctness, SQL audit and association probes without timing")
	flag.Parse()
	if *out == "" || *samples < 100 || *warmups < 20 || *trials < 6 || *trials%6 != 0 {
		fmt.Fprintln(os.Stderr, "require --out, samples >=100, warmups >=20, trials a positive multiple of 6")
		os.Exit(2)
	}
	if err := run(*out, *samples, *warmups, *trials, *correctnessOnly); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Go comparison passed; owned fixture removed; report.json contains evidence")
}
