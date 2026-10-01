package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/nucleus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ExtendedCall struct {
	Trial    int    `json:"trial"`
	Position int    `json:"position"`
	Provider string `json:"provider"`
	Kind     string `json:"kind"`
	Worker   int    `json:"worker"`
	Index    int    `json:"index"`
	Start    int64  `json:"start_unix_nanoseconds"`
	API      int64  `json:"api_nanoseconds"`
	Consumer int64  `json:"consumer_nanoseconds"`
	Error    string `json:"error,omitempty"`
}
type RSSSample struct {
	At  int64 `json:"unix_nanoseconds"`
	KiB int64 `json:"rss_kib"`
}
type ExtendedWindow struct {
	Start             int64
	End               int64
	Calls             int
	Errors            int
	Throughput        float64
	P50               int64
	P95               int64
	P99               int64
	SampledRSSPeakKiB int64
}
type ExtendedPhase struct {
	Trial       int
	Position    int
	Provider    string
	Kind        string
	Concurrency int
	Start       int64
	End         int64
	Calls       []ExtendedCall
	Windows     []ExtendedWindow
	RSS         []RSSSample
}
type ChildResult struct {
	Provider                 string
	BaselineRSSKiB           int64
	ConnectFirstQueryNS      int64
	ParentSpawnFirstResultNS int64
	RSS                      []RSSSample
	VerifiedCalls            int
}
type ExtendedReport struct {
	Status     string
	Failure    string
	Database   string
	Cleaned    bool
	Source     map[string]string
	Provenance map[string]any
	Checks     []Check
	Phases     []ExtendedPhase
	Cold       []ChildResult
	Adoption   []Check
}

func openOne(ctx context.Context, url, name string) (Provider, error) {
	cfg, e := poolConfig(url, nil)
	if e != nil {
		return nil, e
	}
	switch name {
	case "neutron-native-sql":
		c, e := nucleus.Connect(ctx, url, nucleus.WithPoolConfig(cfg))
		if e != nil {
			return nil, e
		}
		return &nativeProvider{c}, nil
	case "raw-pgx":
		c, e := pgxpool.NewWithConfig(ctx, cfg)
		if e != nil {
			return nil, e
		}
		return &rawProvider{c}, nil
	case "gorm":
		d, e := gorm.Open(postgres.New(postgres.Config{DSN: url, PreferSimpleProtocol: true}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if e != nil {
			return nil, e
		}
		db, e := d.DB()
		if e != nil {
			return nil, e
		}
		db.SetMaxOpenConns(4)
		db.SetMaxIdleConns(4)
		db.SetConnMaxLifetime(time.Hour)
		db.SetConnMaxIdleTime(5 * time.Minute)
		return &gormProvider{d}, nil
	}
	return nil, errors.New("unknown provider")
}
func rss(pid int) int64 {
	b, e := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if e != nil {
		return -1
	}
	n, e := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if e != nil {
		return -1
	}
	return n
}
func sampleRSS(pid int, stop <-chan struct{}, result chan<- []RSSSample) {
	var samples []RSSSample
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			result <- samples
			return
		case <-ticker.C:
			samples = append(samples, RSSSample{time.Now().UnixNano(), rss(pid)})
		}
	}
}

// Each write commits independently; native oracle queries are outside API timers.
// ID slots are disjoint by worker and reused only after verified deletion.
func issueLifecycle(ctx context.Context, f *Fixture, p Provider, worker, index int, emit func(string, int64, int64, error)) error {
	note := "opened"
	d := Document{Tenant: "a", ID: int32(10000 + worker), ProjectID: 1, Version: largeVersion, Amount: Decimal(exactAmount), Note: &note, Payload: []byte{0, 255, 23}}
	step := func(op string, fn func() error, check func() error) error {
		start := time.Now()
		e := fn()
		api := time.Since(start).Nanoseconds()
		if e == nil {
			e = check()
		}
		emit(op, api, time.Since(start).Nanoseconds(), e)
		return e
	}
	oracle := func(want []Document) error {
		got, e := extendedOracle(ctx, f, d.Tenant, d.ID)
		if e != nil {
			return e
		}
		return same(got, want)
	}
	write := func(op string) func() error {
		return func() error {
			n, e := p.Write(ctx, op, d, false)
			if e == nil && n != 1 {
				e = errors.New("write affected unexpected rows")
			}
			return e
		}
	}
	if e := step("create_commit", write("insert"), func() error { return oracle([]Document{d}) }); e != nil {
		return e
	}
	before := d
	changed := "resolved"
	d.Note = &changed
	if e := step("cas_update_commit", write("cas"), func() error { want := d; want.Version++; return oracle([]Document{want}) }); e != nil {
		return e
	}
	d.Version++
	var read []Document
	if e := step("read_updated", func() error { var e error; read, e = p.Point(ctx, d.Tenant, d.ID); return e }, func() error {
		if e := same(read, []Document{d}); e != nil {
			return e
		}
		return oracle([]Document{d})
	}); e != nil {
		return e
	}
	if e := step("stale_cas", func() error {
		n, e := p.Write(ctx, "cas", before, false)
		if e == nil && n != 0 {
			e = errors.New("stale CAS changed row")
		}
		return e
	}, func() error { return oracle([]Document{d}) }); e != nil {
		return e
	}
	return step("delete_commit", write("delete"), func() error { return oracle([]Document{}) })
}
func extendedPhase(ctx context.Context, f *Fixture, p Provider, trial, pos, n, c, seconds int, sustained bool) (ExtendedPhase, error) {
	for worker := 0; worker < c; worker++ {
		for warm := 0; warm < 5; warm++ {
			if e := issueLifecycle(ctx, f, p, worker, warm, func(string, int64, int64, error) {}); e != nil {
				return ExtendedPhase{}, e
			}
		}
	}
	kind := "write_lifecycle"
	if sustained {
		kind = "sustained_mixed"
	}
	phase := ExtendedPhase{Trial: trial, Position: pos, Provider: p.Name(), Kind: kind, Concurrency: c, Start: time.Now().UnixNano()}
	stop := make(chan struct{})
	rs := make(chan []RSSSample, 1)
	go sampleRSS(os.Getpid(), stop, rs)
	var mu sync.Mutex
	var first error
	var wg sync.WaitGroup
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for w := 0; w < c; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; (!sustained && i < n) || (sustained && time.Now().Before(deadline)); i++ {
				emit := func(op string, api, consumer int64, e error) {
					call := ExtendedCall{trial, pos, p.Name(), op, worker, i, time.Now().UnixNano() - consumer, api, consumer, ""}
					if e != nil {
						call.Error = errorCategory(e)
					}
					mu.Lock()
					phase.Calls = append(phase.Calls, call)
					if first == nil && e != nil {
						first = e
					}
					mu.Unlock()
				}
				if sustained {
					start := time.Now()
					got, e := p.Page(ctx, "b", 20)
					api := time.Since(start).Nanoseconds()
					if e == nil {
						e = same(got, f.expectedPage("b", 20))
					}
					emit("keyset_read", api, time.Since(start).Nanoseconds(), e)
					if e != nil {
						return
					}
				}
				if e := issueLifecycle(ctx, f, p, worker, i, emit); e != nil {
					return
				}
			}
		}(w)
	}
	wg.Wait()
	phase.End = time.Now().UnixNano()
	close(stop)
	phase.RSS = <-rs
	phase.Windows = extendedWindows(phase)
	return phase, first
}
func extendedChild() error {
	var input struct {
		URL      string
		Provider string
		Point    []Document
		Page     []Document
	}
	if e := json.NewDecoder(os.Stdin).Decode(&input); e != nil {
		return errors.New("child input invalid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r := ChildResult{Provider: input.Provider, BaselineRSSKiB: rss(os.Getpid())}
	if r.BaselineRSSKiB <= 0 {
		return errors.New("RSS baseline unavailable")
	}
	t := time.Now()
	p, e := openOne(ctx, input.URL, input.Provider)
	if e != nil {
		return errors.New("child connect failed")
	}
	defer p.Close()
	got, e := p.Point(ctx, "a", 17)
	r.ConnectFirstQueryNS = time.Since(t).Nanoseconds()
	if e != nil || same(got, input.Point) != nil {
		return errors.New("child first read failed")
	}
	if e = json.NewEncoder(os.Stdout).Encode(r); e != nil {
		return e
	}
	for i := 0; i < 1000; i++ {
		got, e = p.Page(ctx, "a", 20)
		if e != nil || same(got, input.Page) != nil {
			return errors.New("child bounded read failed")
		}
		r.VerifiedCalls++
	}
	r.RSS = append(r.RSS, RSSSample{time.Now().UnixNano(), rss(os.Getpid())})
	return json.NewEncoder(os.Stdout).Encode(r)
}
func coldChild(ctx context.Context, f *Fixture, name string) (ChildResult, error) {
	exe, e := os.Executable()
	if e != nil {
		return ChildResult{}, e
	}
	cmd := exec.CommandContext(ctx, exe, "--extended-child")
	in, e := cmd.StdinPipe()
	if e != nil {
		return ChildResult{}, e
	}
	out, e := cmd.StdoutPipe()
	if e != nil {
		return ChildResult{}, e
	}
	cmd.Stderr = io.Discard
	start := time.Now()
	if e = cmd.Start(); e != nil {
		return ChildResult{}, e
	}
	stop := make(chan struct{})
	samples := make(chan []RSSSample, 1)
	go sampleRSS(cmd.Process.Pid, stop, samples)
	json.NewEncoder(in).Encode(map[string]any{"URL": f.URL, "Provider": name, "Point": []Document{f.Docs[key("a", 17)]}, "Page": f.expectedPage("a", 20)})
	in.Close()
	dec := json.NewDecoder(out)
	var first, last ChildResult
	e = dec.Decode(&first)
	elapsed := time.Since(start).Nanoseconds()
	if e == nil {
		e = dec.Decode(&last)
	}
	wait := cmd.Wait()
	close(stop)
	last.RSS = append(last.RSS, (<-samples)...)
	last.ParentSpawnFirstResultNS = elapsed
	if e != nil || wait != nil {
		return last, errors.New("child evaluation failed")
	}
	return last, nil
}
func runExtended(out string, n, trials, seconds int, correctnessOnly bool) (err error) {
	if seconds < 10 {
		return errors.New("duration must be >=10 seconds")
	}
	if !filepath.IsAbs(out) {
		return errors.New("absolute output required")
	}
	if e := guard(filepath.Dir(out)); e != nil {
		return e
	}
	if e := os.Mkdir(out, 0700); e != nil {
		return errors.New("fresh output required")
	}
	r := ExtendedReport{Status: "failed", Provenance: provenance()}
	defer func() {
		if err != nil {
			r.Failure = errorCategory(err)
		}
		if e := writeJSON(filepath.Join(out, "extended-report.json"), r); e != nil && err == nil {
			err = e
		}
	}()
	r.Source, err = sourceHashes()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	f, e := createFixture(ctx, os.Getenv("ORM_ADMIN_URL"))
	if e != nil {
		if f != nil {
			r.Database = f.Database
			c, cc := context.WithTimeout(context.Background(), 30*time.Second)
			ce := f.Close(c)
			cc()
			r.Cleaned = ce == nil
		}
		return errors.New("fixture failed")
	}
	r.Database = f.Database
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if e := f.Close(c); e != nil {
			err = errors.New("cleanup failed")
		} else {
			r.Cleaned = true
		}
	}()
	var version string
	if e := f.Oracle.QueryRow(ctx, "SELECT version()").Scan(&version); e != nil {
		return errors.New("server version failed")
	}
	r.Provenance["postgres_version"] = version
	r.Provenance["extended_contract"] = map[string]any{"duration_seconds": seconds, "fixed_write_lifecycles_per_worker": n, "untimed_warmup_lifecycles_per_worker": 5, "sample_interval_ms": 100, "parent_RSS": "orchestrator including retained raw samples; not isolated provider footprint or suitable comparative library-memory ranking", "RSS": "sampled process RSS, not OS high water; shared binary includes all provider packages; no server memory", "cold": "fresh process, warm OS executable cache and PostgreSQL; parent spawn-to-first-result includes baseline ps probe", "writes": "independently committed CRUD and CAS transactions; oracle excluded from API timer, included in consumer time; five steps per lifecycle", "sustained": "closed-loop c4 one page plus five write lifecycle steps; no fixed arrival rate or saturation proof", "adoption": "executable project issue resolve and stale optimistic update workflow, not userstudy; fixture schema authority native SQL, no ORM AutoMigrate"}
	r.Provenance["initial_native_digest"] = f.Digest
	names := []string{"neutron-native-sql", "raw-pgx", "gorm"}
	for _, name := range names {
		p, e := openOne(ctx, f.URL, name)
		if e != nil {
			return errors.New("provider failed")
		}
		cs, e := correctness(ctx, f, []Provider{p})
		r.Checks = append(r.Checks, cs...)
		if e == nil {
			e = adoptionScenario(ctx, f, p)
		}
		r.Adoption = append(r.Adoption, Check{Provider: name, Name: "tenant_scoped_issue_service_create_edit_list_relation_conflict_rollback_delete", Passed: e == nil})
		p.Close()
		if e != nil {
			return errors.New("extended correctness failed")
		}
	}
	if !correctnessOnly {
		orders := [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
		for trial := 0; trial < trials; trial++ {
			for pos, id := range orders[trial%6] {
				name := names[id]
				if e := guard(filepath.Dir(out)); e != nil {
					return e
				}
				cr, e := coldChild(ctx, f, name)
				if e != nil {
					return e
				}
				r.Cold = append(r.Cold, cr)
				p, e := openOne(ctx, f.URL, name)
				if e != nil {
					return errors.New("provider failed")
				}
				for _, c := range []int{1, 4} {
					if e := guard(filepath.Dir(out)); e != nil {
						p.Close()
						return e
					}
					phase, e := extendedPhase(ctx, f, p, trial, pos, n, c, seconds, false)
					r.Phases = append(r.Phases, phase)
					if pe := writeJSON(filepath.Join(out, "extended-report.json"), r); pe != nil {
						p.Close()
						return pe
					}
					if e != nil {
						p.Close()
						return errors.New("write phase failed")
					}
				}
				if e := guard(filepath.Dir(out)); e != nil {
					p.Close()
					return e
				}
				phase, e := extendedPhase(ctx, f, p, trial, pos, n, 4, seconds, true)
				r.Phases = append(r.Phases, phase)
				if pe := writeJSON(filepath.Join(out, "extended-report.json"), r); pe != nil {
					p.Close()
					return pe
				}
				p.Close()
				if e != nil {
					return errors.New("sustained phase failed")
				}
				if e = guard(filepath.Dir(out)); e != nil {
					return e
				}
			}
		}
	}
	digest, e := nativeDigest(ctx, f.Oracle)
	if e != nil || digest != f.Digest {
		return errors.New("final native digest changed")
	}
	r.Provenance["final_native_digest"] = digest
	r.Status = "passed"
	fmt.Println("Go extended contract passed; report retained")
	return nil
}

func extendedWindows(p ExtendedPhase) []ExtendedWindow {
	var out []ExtendedWindow
	for start := p.Start; start < p.End; start += int64(5 * time.Second) {
		end := start + int64(5*time.Second)
		if end > p.End {
			end = p.End
		}
		w := ExtendedWindow{Start: start, End: end}
		var lat []int64
		for _, c := range p.Calls {
			if c.Start >= start && c.Start < end {
				w.Calls++
				lat = append(lat, c.API)
				if c.Error != "" {
					w.Errors++
				}
			}
		}
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		if len(lat) > 0 {
			q := func(f float64) int64 { return lat[int(math.Ceil(f*float64(len(lat))))-1] }
			w.P50 = q(.5)
			w.P95 = q(.95)
			w.P99 = q(.99)
		}
		w.Throughput = float64(w.Calls) * 1e9 / float64(end-start)
		for _, r := range p.RSS {
			if r.At >= start && r.At < end && r.KiB > w.SampledRSSPeakKiB {
				w.SampledRSSPeakKiB = r.KiB
			}
		}
		out = append(out, w)
	}
	return out
}

// Oracle casts exact scalar types to text and bytes to hex, independently of
// provider row scanners and their SELECT projections.
func extendedOracle(ctx context.Context, f *Fixture, tenant string, id int32) ([]Document, error) {
	var d Document
	var sid, project, version, amount string
	var payload *string
	err := f.Oracle.QueryRow(ctx, "SELECT tenant,id::text,project_id::text,version::text,amount::text,note,encode(payload,'hex') FROM documents WHERE tenant=$1 AND id=$2", tenant, id).Scan(&d.Tenant, &sid, &project, &version, &amount, &d.Note, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return []Document{}, nil
	}
	if err != nil {
		return nil, err
	}
	d.ID, err = parseInt32(sid)
	if err != nil {
		return nil, err
	}
	d.ProjectID, err = parseInt32(project)
	if err != nil {
		return nil, err
	}
	d.Version, err = strconv.ParseInt(version, 10, 64)
	if err != nil {
		return nil, err
	}
	d.Amount = Decimal(amount)
	if payload != nil {
		d.Payload, err = hex.DecodeString(*payload)
		if err != nil {
			return nil, err
		}
	}
	return []Document{d}, nil
}
