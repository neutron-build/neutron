// Command go-admission-native records fresh-built finite Go admission facts
// with a PostgreSQL oracle.
//
// AUTHORED, NOT EXECUTED. Nothing here is qualification until the coordinator
// builds it, runs it against owned endpoints and reviews the report.
//
// Build: copy this file as main.go into a fresh consumer module that requires
// github.com/neutron-build/neutron/go (a read-only checkout of the revision
// under test via replace, or a published version), then run a plain `go build`
// (NOT -trimpath: the binary proves its orm sources through their build-time
// paths). Run with environment variable NAMES only; URLs and credentials never
// appear in reports. Provisioning and native state checks use raw pgx, never
// ORM DDL. The orm admission, CRUD, owned transaction/savepoint and refusal
// checks run against a PostgreSQL control first and the Nucleus candidate.
//
// The binary hash only verifies the file passed with -binary-file. The report
// is NOT attestation: the coordinator must bind the actual endpoint process to
// that binary (process image, listening port, data directory) and record the
// engine source and configuration out of band. This bounded gate has no
// timing, soak, package enablement or PostgreSQL parity claim and leaves
// package_enabled false.
//
//	go-admission-native -postgres-url-env PG_OWNED_URL -nucleus-url-env NUCLEUS_OWNED_URL \
//	  -module-root /fresh/checkout/go -binary-file /path/to/exact/nucleus \
//	  -binary-sha256 RECORDED_SHA256 -report /path/to/evidence/go-nucleus-finite.json
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/orm"
)

const modulePath = "github.com/neutron-build/neutron/go"

type model struct {
	ID     int64     `db:"id"`
	Active bool      `db:"active"`
	Title  string    `db:"title"`
	Data   *orm.JSON `db:"data,nullable"`
	Stamp  time.Time `db:"stamp"`
	N      *int32    `db:"n,nullable"`
}

type numericModel struct {
	ID int64       `db:"id"`
	V  orm.Decimal `db:"v"`
}

type columns struct {
	id    orm.Column[model, int64]
	title orm.Column[model, string]
	data  orm.Column[model, *orm.JSON]
}

type view struct {
	ID     int64
	Active bool
	Title  string
	Data   *string
	Stamp  string
	N      *int32
}

type nativeRow struct {
	ID            string
	Active        bool
	Title         string
	Data          *string
	DataIsSQLNull bool
	N             *int32
}

type fact struct {
	Phase  string
	ORM    view
	Native []nativeRow
}

var errRollback = errors.New("qualifier rollback marker")

func digest(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func viewOf(m model) view {
	v := view{ID: m.ID, Active: m.Active, Title: m.Title, Stamp: m.Stamp.UTC().Format(time.RFC3339Nano), N: m.N}
	if m.Data != nil {
		text := m.Data.String()
		v.Data = &text
	}
	return v
}

func snapshot(ctx context.Context, raw *pgx.Conn, schema string) ([]nativeRow, error) {
	rows, err := raw.Query(ctx, `SELECT id::text, active, title, data::text, (data IS NULL), n FROM "`+schema+`"."docs" ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []nativeRow{}
	for rows.Next() {
		var row nativeRow
		if err := rows.Scan(&row.ID, &row.Active, &row.Title, &row.Data, &row.DataIsSQLNull, &row.N); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func refused(label string, err error) (string, error) {
	if err == nil {
		return "", fmt.Errorf("%s: operation outside the finite profile was admitted", label)
	}
	if !errors.Is(err, orm.ErrProfileRefused) {
		return "", fmt.Errorf("%s: refused for the wrong reason (%T)", label, err)
	}
	return label, nil
}

type savepointFunc func(func(orm.Executor) error) error
type transactionFunc func(context.Context, func(orm.Executor, savepointFunc) error) error

func runEngine(ctx context.Context, engine, url string, raw *pgx.Conn, schema string) ([]fact, []string, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, nil, err
	}
	defer pool.Close()
	table, err := orm.NewTable[model](schema, "docs")
	if err != nil {
		return nil, nil, err
	}
	var c columns
	if c.id, err = orm.NewColumn[model, int64](table, "ID"); err != nil {
		return nil, nil, err
	}
	active, err := orm.NewColumn[model, bool](table, "Active")
	if err != nil {
		return nil, nil, err
	}
	if c.title, err = orm.NewColumn[model, string](table, "Title"); err != nil {
		return nil, nil, err
	}
	if c.data, err = orm.NewColumn[model, *orm.JSON](table, "Data"); err != nil {
		return nil, nil, err
	}
	stamp, err := orm.NewColumn[model, time.Time](table, "Stamp")
	if err != nil {
		return nil, nil, err
	}
	n, err := orm.NewColumn[model, *int32](table, "N")
	if err != nil {
		return nil, nil, err
	}

	var exec orm.Executor = pool
	var finite *orm.FiniteExecutor
	var transact transactionFunc
	if engine == "nucleus" {
		finiteTable, err := orm.NewFiniteTable(table)
		if err != nil {
			return nil, nil, err
		}
		if finite, err = orm.AdmitNucleusFinite(ctx, pool, finiteTable); err != nil {
			return nil, nil, err
		}
		if finite.Identity().PackageEnabled() {
			return nil, nil, errors.New("finite identity enabled a package")
		}
		exec = finite
		transact = func(ctx context.Context, body func(orm.Executor, savepointFunc) error) error {
			return finite.WithTransaction(ctx, pool, orm.TransactionOptions{}, func(scope *orm.FiniteScope) error {
				return body(scope, func(inner func(orm.Executor) error) error {
					return scope.Savepoint(ctx, func(child *orm.FiniteScope) error { return inner(child) })
				})
			})
		}
	} else {
		if _, err := orm.AdmitPostgresDirect(ctx, pool); err != nil {
			return nil, nil, err
		}
		transact = func(ctx context.Context, body func(orm.Executor, savepointFunc) error) error {
			return orm.WithTransaction(ctx, pool, orm.TransactionOptions{}, func(scope *orm.Scope) error {
				return body(scope, func(inner func(orm.Executor) error) error {
					return scope.Savepoint(ctx, func(child *orm.Scope) error { return inner(child) })
				})
			})
		}
	}

	one := func(ex orm.Executor) (view, error) {
		rows, err := orm.Select(ctx, ex, table, orm.Query[model]{}.Where(c.id.Eq(1)))
		if err != nil {
			return view{}, err
		}
		if len(rows) != 1 {
			return view{}, fmt.Errorf("expected one row, found %d", len(rows))
		}
		return viewOf(rows[0]), nil
	}
	record := func(facts []fact, phase string, shown view) ([]fact, error) {
		native, err := snapshot(ctx, raw, schema)
		return append(facts, fact{phase, shown, native}), err
	}

	facts := []fact{}
	zero := int32(0)
	moment := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	inserted, err := orm.InsertOne(ctx, exec, table, orm.Set(c.id, orm.Some(int64(1))), orm.Set(active, orm.Some(false)),
		orm.Set(c.title, orm.Some("")), orm.Set(c.data, orm.Some((*orm.JSON)(nil))), orm.Set(stamp, orm.Some(moment)), orm.Set(n, orm.Some(&zero)))
	if err != nil {
		return nil, nil, err
	}
	if facts, err = record(facts, "insert", viewOf(inserted)); err != nil {
		return nil, nil, err
	}

	jsonNull, err := orm.ParseJSON("null")
	if err != nil {
		return nil, nil, err
	}
	var inTransaction view
	err = transact(ctx, func(ex orm.Executor, savepoint savepointFunc) error {
		if _, err := orm.Update(ctx, ex, table, c.id.Eq(1), orm.Set(c.title, orm.Some("outer")), orm.Set(c.data, orm.Some(&jsonNull))); err != nil {
			return err
		}
		err := savepoint(func(inner orm.Executor) error {
			if _, err := orm.Update(ctx, inner, table, c.id.Eq(1), orm.Set(c.title, orm.Some("inner"))); err != nil {
				return err
			}
			return errRollback
		})
		if !errors.Is(err, errRollback) {
			return fmt.Errorf("savepoint failure was not surfaced: %v", err)
		}
		inTransaction, err = one(ex)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if facts, err = record(facts, "savepoint-recovery-commit", inTransaction); err != nil {
		return nil, nil, err
	}

	err = transact(ctx, func(ex orm.Executor, _ savepointFunc) error {
		if _, err := orm.Update(ctx, ex, table, c.id.Eq(1), orm.Set(c.title, orm.Some("must-rollback"))); err != nil {
			return err
		}
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		return nil, nil, fmt.Errorf("outer rollback was not surfaced: %v", err)
	}
	after, err := one(exec)
	if err != nil {
		return nil, nil, err
	}
	if facts, err = record(facts, "outer-rollback", after); err != nil {
		return nil, nil, err
	}

	checks := []string{}
	if engine == "nucleus" {
		note := func(label string, err error) error {
			done, err := refused(label, err)
			if err == nil {
				checks = append(checks, done)
			}
			return err
		}
		_, err = finite.Exec(ctx, `CREATE TABLE "`+schema+`".surprise (id int)`)
		if err = note("raw-ddl", err); err != nil {
			return nil, nil, err
		}
		rows, err := finite.Query(ctx, "SELECT pg_cancel_backend(1)")
		if rows != nil {
			rows.Close()
		}
		if err = note("raw-function-query", err); err != nil {
			return nil, nil, err
		}
		_, err = orm.Select(ctx, exec, table, orm.Query[model]{})
		if err = note("unbound-scan", err); err != nil {
			return nil, nil, err
		}
		_, err = orm.Select(ctx, exec, table, orm.Query[model]{}.Where(c.id.Eq(1)).Limit(1))
		if err = note("limit-query-algebra", err); err != nil {
			return nil, nil, err
		}
		_, err = orm.SelectOne(ctx, exec, table, orm.Query[model]{}.Where(c.id.Eq(1)))
		if err = note("select-one", err); err != nil {
			return nil, nil, err
		}
		err = orm.Stream(ctx, exec, table, orm.Query[model]{}.Where(c.id.Eq(1)), 1, func(model) (bool, error) { return true, nil })
		if err = note("stream", err); err != nil {
			return nil, nil, err
		}
		err = finite.WithTransaction(ctx, pool, orm.TransactionOptions{Isolation: pgx.Serializable}, func(*orm.FiniteScope) error { return nil })
		if err = note("serializable-transaction", err); err != nil {
			return nil, nil, err
		}
		numeric, err := orm.NewTable[numericModel](schema, "numeric_surface")
		if err != nil {
			return nil, nil, err
		}
		_, err = orm.NewFiniteTable(numeric)
		if err = note("numeric-table", err); err != nil {
			return nil, nil, err
		}
		unchanged, err := one(exec)
		if err != nil {
			return nil, nil, err
		}
		if facts, err = record(facts, "after-refusals", unchanged); err != nil {
			return nil, nil, err
		}
	}
	return facts, checks, nil
}

type report struct {
	Status            string            `json:"status"`
	PackageEnabled    bool              `json:"package_enabled"`
	Profile           string            `json:"profile"`
	Scope             string            `json:"scope"`
	ExecutableSHA256  string            `json:"executableSha256"`
	GoVersion         string            `json:"goVersion"`
	ModuleVersion     string            `json:"moduleVersion"`
	ModuleSum         string            `json:"moduleSum"`
	PgxVersion        string            `json:"pgxVersion"`
	ModuleFiles       map[string]string `json:"moduleFiles"`
	BinarySHA256      string            `json:"binarySha256"`
	BinaryAttestation string            `json:"binaryAttestation"`
	Facts             map[string]any    `json:"facts"`
	Failure           string            `json:"failure,omitempty"`
	CleanupFailure    string            `json:"cleanupFailure,omitempty"`
}

func fail(r *report, path string, err error) {
	r.Status = "fail"
	if r.Failure == "" {
		r.Failure = err.Error()
	}
	write(r, path)
	os.Exit(1)
}

func write(r *report, path string) {
	data, _ := json.MarshalIndent(r, "", "  ")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err == nil {
		_ = os.WriteFile(path, append(data, '\n'), 0o644)
	}
}

func main() {
	postgresEnv := flag.String("postgres-url-env", "", "environment variable NAME holding the owned PostgreSQL URL")
	nucleusEnv := flag.String("nucleus-url-env", "", "environment variable NAME holding the owned Nucleus URL")
	moduleRoot := flag.String("module-root", "", "read-only root of the go module under test")
	binaryFile := flag.String("binary-file", "", "exact Nucleus binary file")
	binarySHA := flag.String("binary-sha256", "", "recorded SHA-256 of the Nucleus binary")
	reportPath := flag.String("report", "", "report output path")
	flag.Parse()
	if *postgresEnv == "" || *nucleusEnv == "" || *moduleRoot == "" || *binaryFile == "" || *binarySHA == "" || *reportPath == "" {
		fmt.Fprintln(os.Stderr, "all of -postgres-url-env -nucleus-url-env -module-root -binary-file -binary-sha256 -report are required")
		os.Exit(2)
	}
	r := &report{Status: "fail", Profile: orm.NucleusCandidateProfile, Scope: "finite Go admission/CRUD/rollback facts only",
		GoVersion: runtime.Version(), BinarySHA256: *binarySHA, Facts: map[string]any{},
		BinaryAttestation: "coordinator-provided executed binary; endpoint report alone is not attestation"}

	root, err := filepath.EvalSymlinks(*moduleRoot)
	if err != nil {
		fail(r, *reportPath, err)
	}
	if root, err = filepath.Abs(root); err != nil {
		fail(r, *reportPath, err)
	}
	pc := reflect.ValueOf(orm.AdmitNucleusFinite).Pointer()
	file, _ := runtime.FuncForPC(pc).FileLine(pc)
	built, err := filepath.EvalSymlinks(filepath.Dir(file))
	if err != nil || built != filepath.Join(root, "orm") {
		fail(r, *reportPath, errors.New("orm package was not built from -module-root (build without -trimpath from the fresh checkout)"))
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch dep.Path {
			case modulePath:
				r.ModuleVersion, r.ModuleSum = dep.Version, dep.Sum
			case "github.com/jackc/pgx/v5":
				r.PgxVersion = dep.Version
			}
		}
	}
	r.ModuleFiles = map[string]string{}
	sources, _ := filepath.Glob(filepath.Join(root, "orm", "*.go"))
	sources = append(sources, filepath.Join(root, "go.mod"), filepath.Join(root, "go.sum"))
	sort.Strings(sources)
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		sum, err := digest(source)
		if err != nil {
			fail(r, *reportPath, err)
		}
		relative, _ := filepath.Rel(root, source)
		r.ModuleFiles[filepath.ToSlash(relative)] = sum
	}
	if self, err := os.Executable(); err == nil {
		r.ExecutableSHA256, _ = digest(self)
	}
	if sum, err := digest(*binaryFile); err != nil || sum != *binarySHA {
		fail(r, *reportPath, errors.New("Nucleus binary does not match required coordinator SHA256"))
	}
	urls := map[string]string{}
	for engine, name := range map[string]string{"postgres": *postgresEnv, "nucleus": *nucleusEnv} {
		value := os.Getenv(name)
		if value == "" {
			fail(r, *reportPath, fmt.Errorf("environment variable %s is not set", name))
		}
		urls[engine] = value
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		fail(r, *reportPath, err)
	}
	schema := "np01_" + hex.EncodeToString(suffix)
	var raws []*pgx.Conn
	var failure error
	results := map[string][]fact{}
	checks := []string{}
	func() {
		for _, engine := range []string{"postgres", "nucleus"} {
			raw, err := pgx.Connect(ctx, urls[engine])
			if err != nil {
				failure = errors.New("connect " + engine + " failed (" + fmt.Sprintf("%T", err) + ")")
				return
			}
			raws = append(raws, raw)
			for _, statement := range []string{
				`CREATE SCHEMA "` + schema + `"`,
				`CREATE TABLE "` + schema + `".docs (id bigint PRIMARY KEY, active boolean NOT NULL, title text NOT NULL, data jsonb, stamp timestamptz NOT NULL, n integer)`,
			} {
				if _, err := raw.Exec(ctx, statement); err != nil {
					failure = fmt.Errorf("provision %s: %v", engine, err)
					return
				}
			}
			facts, refusals, err := runEngine(ctx, engine, urls[engine], raw, schema)
			if err != nil {
				failure = fmt.Errorf("%s: %v", engine, err)
				return
			}
			results[engine] = facts
			if engine == "nucleus" {
				checks = refusals
				pool, err := pgxpool.New(ctx, urls[engine])
				if err != nil {
					failure = err
					return
				}
				_, err = orm.AdmitPostgresDirect(ctx, pool)
				pool.Close()
				if _, err = refused("default-profile", err); err != nil {
					failure = err
					return
				}
			}
		}
		control, candidate := results["postgres"], results["nucleus"]
		if len(candidate) < len(control) || !reflect.DeepEqual(control, candidate[:len(control)]) {
			failure = errors.New("PostgreSQL finite checkpoints disagree")
			return
		}
		if !reflect.DeepEqual(candidate[len(candidate)-1].Native, control[len(control)-1].Native) {
			failure = errors.New("refused operations changed committed rows")
			return
		}
		first, second, third := control[0], control[1], control[2]
		switch {
		case first.ORM.Title != "" || first.ORM.Active || first.ORM.N == nil || *first.ORM.N != 0:
			failure = errors.New("zero/false/empty values were not preserved")
		case !first.Native[0].DataIsSQLNull:
			failure = errors.New("SQL NULL was not preserved")
		case second.Native[0].DataIsSQLNull || second.Native[0].Data == nil || *second.Native[0].Data != "null" || second.ORM.Data == nil:
			failure = errors.New("JSON null is not distinct from SQL NULL")
		case second.ORM.Title != "outer" || !reflect.DeepEqual(third.ORM, second.ORM) || !reflect.DeepEqual(third.Native, second.Native):
			failure = errors.New("rollback did not restore committed state")
		}
	}()
	if failure == nil {
		r.Facts = map[string]any{"checkpointAgreement": true, "defaultProfileRefusesNucleus": true,
			"refusals": checks, "unsupportedOperationsPreserveRows": true}
		r.Status = "pass"
	} else {
		r.Failure = failure.Error()
	}
	for i := len(raws) - 1; i >= 0; i-- {
		if _, err := raws[i].Exec(ctx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`); err != nil {
			r.Status = "fail"
			r.CleanupFailure = fmt.Sprintf("%T", err)
			failure = err
		}
		_ = raws[i].Close(ctx)
	}
	write(r, *reportPath)
	if failure != nil {
		os.Exit(1)
	}
}
