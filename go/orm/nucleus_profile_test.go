package orm

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NP01 unit tests use fake executors only. Every database-facing behavior
// against the exact Nucleus binary and a PostgreSQL control belongs to the
// native qualifier (conformance/polyglot/nucleus/go-admission-native).

const (
	np01Startup  = "16.0 (Nucleus)"
	np01Reported = "PostgreSQL 16.0 (Nucleus 1.2.2 — The Definitive Database)"
)

type np01Model struct {
	ID     int64     `db:"id"`
	Active bool      `db:"active"`
	Title  string    `db:"title"`
	Data   *JSON     `db:"data,nullable"`
	Stamp  time.Time `db:"stamp"`
	N      *int32    `db:"n,nullable"`
}

type np01Rows struct {
	pgx.Rows
	values []string
	next   int
}

func (r *np01Rows) Close()     {}
func (r *np01Rows) Err() error { return nil }
func (r *np01Rows) Next() bool {
	if r.next >= len(r.values) {
		return false
	}
	r.next++
	return true
}
func (r *np01Rows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.values[r.next-1]
	return nil
}

type np01Executor struct {
	startup, version string
	statements       []string
	args             [][]any
}

func (e *np01Executor) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	e.statements = append(e.statements, sql)
	e.args = append(e.args, args)
	switch sql {
	case "SELECT current_setting('server_version')":
		return &np01Rows{values: []string{e.startup}}, nil
	case "SELECT version()":
		return &np01Rows{values: []string{e.version}}, nil
	}
	return &np01Rows{}, nil
}

func (e *np01Executor) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	e.statements = append(e.statements, sql)
	e.args = append(e.args, args)
	return pgconn.CommandTag{}, nil
}

func (e *np01Executor) caller() []string {
	var out []string
	for _, sql := range e.statements {
		if sql != "SELECT current_setting('server_version')" && sql != "SELECT version()" {
			out = append(out, sql)
		}
	}
	return out
}

type np01Columns struct {
	id     Column[np01Model, int64]
	active Column[np01Model, bool]
	title  Column[np01Model, string]
	data   Column[np01Model, *JSON]
	stamp  Column[np01Model, time.Time]
	n      Column[np01Model, *int32]
}

func np01Fixture(t *testing.T) (Table[np01Model], np01Columns, FiniteTable) {
	t.Helper()
	table, err := NewTable[np01Model]("np01", "docs")
	if err != nil {
		t.Fatal(err)
	}
	var c np01Columns
	if c.id, err = NewColumn[np01Model, int64](table, "ID"); err != nil {
		t.Fatal(err)
	}
	if c.active, err = NewColumn[np01Model, bool](table, "Active"); err != nil {
		t.Fatal(err)
	}
	if c.title, err = NewColumn[np01Model, string](table, "Title"); err != nil {
		t.Fatal(err)
	}
	if c.data, err = NewColumn[np01Model, *JSON](table, "Data"); err != nil {
		t.Fatal(err)
	}
	if c.stamp, err = NewColumn[np01Model, time.Time](table, "Stamp"); err != nil {
		t.Fatal(err)
	}
	if c.n, err = NewColumn[np01Model, *int32](table, "N"); err != nil {
		t.Fatal(err)
	}
	finite, err := NewFiniteTable(table)
	if err != nil {
		t.Fatal(err)
	}
	return table, c, finite
}

func np01Admitted(t *testing.T) (*np01Executor, *FiniteExecutor, Table[np01Model], np01Columns) {
	t.Helper()
	table, c, finite := np01Fixture(t)
	db := &np01Executor{startup: np01Startup, version: np01Reported}
	fe, err := AdmitNucleusFinite(context.Background(), db, finite)
	if err != nil {
		t.Fatal(err)
	}
	return db, fe, table, c
}

func np01Refused(t *testing.T, db *np01Executor, label string, err error) {
	t.Helper()
	var profile *ProfileError
	if !errors.Is(err, ErrProfileRefused) || !errors.As(err, &profile) {
		t.Fatalf("%s: not refused by the profile: %v", label, err)
	}
	if len(db.caller()) != 0 {
		t.Fatalf("%s: refused operation reached the executor", label)
	}
}

func TestNucleusCandidateIdentityIsExactUncertifiedAndImmutable(t *testing.T) {
	identity, err := admitEndpoint(np01Startup, np01Reported, NucleusCandidateProfile)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Engine() != "nucleus" || identity.Version() != "1.2.2" || identity.Profile() != NucleusCandidateProfile ||
		identity.PackageEnabled() || identity.Qualification() != "uncertified-finite-candidate" {
		t.Fatal(identity)
	}
	capabilities := identity.Capabilities()
	if strings.Join(capabilities, ",") != "point-crud,read-committed-transaction,savepoint" {
		t.Fatal(capabilities)
	}
	capabilities[0] = "bounded-stream"
	if identity.Capabilities()[0] != "point-crud" {
		t.Fatal("capabilities are mutable through the accessor")
	}
	for _, pair := range [][2]string{
		{"16.0", np01Reported},
		{np01Startup, "PostgreSQL 16.0"},
		{np01Startup, strings.Replace(np01Reported, "1.2.2", "1.2.1", 1)},
		{"17.6 (Debian 17.6-1)", "PostgreSQL 17.6 on x86_64"},
		{"", ""},
	} {
		if _, err := admitEndpoint(pair[0], pair[1], NucleusCandidateProfile); !errors.Is(err, ErrProfileRefused) {
			t.Fatal("contradictory identity admitted", pair, err)
		}
	}
	if _, err := admitEndpoint(np01Startup, np01Reported, "nucleus"); !errors.Is(err, ErrProfileRefused) {
		t.Fatal("unknown profile admitted", err)
	}
}

func TestPostgresDirectAdmitsPostgreSQLOnly(t *testing.T) {
	identity, err := admitEndpoint("17.6 (Debian 17.6-1)", "PostgreSQL 17.6 on x86_64-pc-linux-gnu", PostgresDirectProfile)
	if err != nil || identity.Engine() != "postgresql" || identity.Version() != "17.6" {
		t.Fatal(identity, err)
	}
	for _, pair := range [][2]string{
		{np01Startup, np01Reported},
		{"17.6", "CockroachDB v24.1"},
		{"17.6", "PostgreSQL 16.6"},
		{"unknown", "PostgreSQL 17.6"},
		{"17.6", "PostgreSQL 17.6 YugabyteDB"},
	} {
		if _, err := admitEndpoint(pair[0], pair[1], PostgresDirectProfile); !errors.Is(err, ErrProfileRefused) {
			t.Fatal("non-PostgreSQL identity admitted", pair, err)
		}
	}
	db := &np01Executor{startup: np01Startup, version: np01Reported}
	if _, err := AdmitPostgresDirect(context.Background(), db); !errors.Is(err, ErrProfileRefused) {
		t.Fatal("postgres-direct admitted Nucleus", err)
	}
	if len(db.caller()) != 0 {
		t.Fatal("identity probe ran a caller statement")
	}
}

func TestAdmitNucleusFiniteProbesIdentityBeforeAnyStatement(t *testing.T) {
	_, _, finite := np01Fixture(t)
	db := &np01Executor{startup: np01Startup, version: np01Reported}
	fe, err := AdmitNucleusFinite(context.Background(), db, finite)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(db.statements, "|") != "SELECT current_setting('server_version')|SELECT version()" {
		t.Fatal(db.statements)
	}
	if fe.Identity().PackageEnabled() || fe.Identity().Engine() != "nucleus" {
		t.Fatal(fe.Identity())
	}
	wrong := &np01Executor{startup: "16.0", version: np01Reported}
	if got, err := AdmitNucleusFinite(context.Background(), wrong, finite); got != nil || !errors.Is(err, ErrProfileRefused) {
		t.Fatal("contradictory identity admitted", err)
	}
	empty := &np01Executor{startup: np01Startup, version: np01Reported}
	if got, err := AdmitNucleusFinite(context.Background(), empty, FiniteTable{}); got != nil || !errors.Is(err, ErrProfileRefused) {
		t.Fatal("zero-value finite table admitted", err)
	}
	if len(empty.statements) != 0 {
		t.Fatal("table refusal must precede identity statements", empty.statements)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := &np01Executor{startup: np01Startup, version: np01Reported}
	if _, err := AdmitNucleusFinite(ctx, canceled, finite); !errors.Is(err, context.Canceled) || len(canceled.statements) != 0 {
		t.Fatal("canceled admission dispatched", err)
	}
}

type np01Numeric struct {
	ID int64   `db:"id"`
	V  Decimal `db:"v"`
}
type np01Plain struct {
	ID int `db:"id"`
}
type np01Float struct {
	ID int64   `db:"id"`
	V  float64 `db:"v"`
}
type np01Binary struct {
	ID int64 `db:"id"`
	V  UUID  `db:"v"`
}
type np01Temporal struct {
	ID int64 `db:"id"`
	V  Date  `db:"v"`
}

func np01Finite[M any](t *testing.T) error {
	t.Helper()
	table, err := NewTable[M]("np01", "surface")
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewFiniteTable(table)
	return err
}

func TestFiniteTableRefusesScalarsOutsideTheFiniteFamilies(t *testing.T) {
	for label, err := range map[string]error{
		"numeric":  np01Finite[np01Numeric](t),
		"int":      np01Finite[np01Plain](t),
		"float":    np01Finite[np01Float](t),
		"uuid":     np01Finite[np01Binary](t),
		"temporal": np01Finite[np01Temporal](t),
	} {
		if !errors.Is(err, ErrProfileRefused) {
			t.Fatal(label, "admitted", err)
		}
	}
	if err := np01Finite[np01Model](t); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFiniteTable(Table[np01Model]{}); !errors.Is(err, ErrProfileRefused) {
		t.Fatal("uninitialized table admitted", err)
	}
}

func TestFiniteGuardAdmitsGeneratedPointCRUD(t *testing.T) {
	db, fe, table, c := np01Admitted(t)
	ctx := context.Background()
	stamp := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	zero := int32(0)
	if _, err := InsertOne(ctx, fe, table, Set(c.id, Some(int64(1))), Set(c.active, Some(false)), Set(c.title, Some("")),
		Set(c.data, Some((*JSON)(nil))), Set(c.stamp, Some(stamp)), Set(c.n, Some(&zero))); !errors.Is(err, ErrCardinality) {
		t.Fatal("fake executor returns no row; expected cardinality error after dispatch", err)
	}
	if _, err := InsertOne(ctx, fe, table, Set(c.id, Some(int64(2))), Set(c.title, Default[string]())); !errors.Is(err, ErrCardinality) {
		t.Fatal(err)
	}
	if _, err := InsertOne(ctx, fe, table); !errors.Is(err, ErrCardinality) {
		t.Fatal(err)
	}
	if _, err := Update(ctx, fe, table, c.id.Eq(1), Set(c.title, Some("")), Set(c.active, Some(false)), Set(c.n, Some((*int32)(nil))), Set(c.stamp, Some(stamp))); err != nil {
		t.Fatal(err)
	}
	if _, err := Delete(ctx, fe, table, c.id.Eq(1)); err != nil {
		t.Fatal(err)
	}
	if rows, err := Select(ctx, fe, table, Query[np01Model]{}.Where(c.title.Eq(""))); err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if got := len(db.caller()); got != 6 {
		t.Fatal("admitted statements did not reach the executor", db.caller())
	}
	if !strings.HasPrefix(db.caller()[0], `INSERT INTO "np01"."docs"`) || !strings.HasPrefix(db.caller()[5], `SELECT `) {
		t.Fatal(db.caller())
	}
}

func TestFiniteGuardRefusesEverythingOutsideThePointGrammarBeforeDispatch(t *testing.T) {
	db, fe, table, c := np01Admitted(t)
	ctx := context.Background()
	other, err := NewTable[np01Model]("np01", "unregistered")
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := NewColumn[np01Model, int64](other, "ID")
	if err != nil {
		t.Fatal(err)
	}
	_, selectErr := Select(ctx, fe, table, Query[np01Model]{})
	np01Refused(t, db, "unbound scan", selectErr)
	_, err = Select(ctx, fe, table, Query[np01Model]{}.Where(c.id.Eq(1)).Limit(1))
	np01Refused(t, db, "limit", err)
	_, err = Select(ctx, fe, table, Query[np01Model]{}.Where(c.id.Eq(1)).OrderBy(c.id.Asc()))
	np01Refused(t, db, "order by", err)
	_, err = Select(ctx, fe, table, Query[np01Model]{}.Where(c.id.Gt(1)))
	np01Refused(t, db, "range predicate", err)
	_, err = Select(ctx, fe, table, Query[np01Model]{}.Where(And(c.id.Eq(1), c.active.Eq(true))))
	np01Refused(t, db, "conjunction", err)
	_, err = Select(ctx, fe, table, Query[np01Model]{}.Where(c.id.In(1, 2)))
	np01Refused(t, db, "membership", err)
	_, err = Select(ctx, fe, table, Query[np01Model]{}.Where(c.data.Eq(nil)))
	np01Refused(t, db, "null test", err)
	_, err = SelectOne(ctx, fe, table, Query[np01Model]{}.Where(c.id.Eq(1)))
	np01Refused(t, db, "select one cardinality limit", err)
	err = Stream(ctx, fe, table, Query[np01Model]{}.Where(c.id.Eq(1)), 1, func(np01Model) (bool, error) { return true, nil })
	np01Refused(t, db, "stream", err)
	_, err = Select(ctx, fe, table, Query[np01Model]{}.Distinct().Where(c.id.Eq(1)))
	np01Refused(t, db, "distinct", err)
	_, err = Delete(ctx, fe, other, otherID.Eq(1))
	np01Refused(t, db, "unregistered table", err)
	_, err = Delete(ctx, fe, table, Or(c.id.Eq(1), c.id.Eq(2)))
	np01Refused(t, db, "disjunction", err)
	_, err = Update(ctx, fe, table, c.id.Eq(1), Set(c.stamp, Some(time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("offset", 3600)))))
	np01Refused(t, db, "non-UTC timestamp", err)

	_, err = fe.Exec(ctx, `CREATE TABLE "np01"."surprise" (id int)`)
	np01Refused(t, db, "raw ddl", err)
	_, err = fe.Query(ctx, "SELECT pg_cancel_backend(1)")
	np01Refused(t, db, "raw function", err)
	_, err = fe.Exec(ctx, `DELETE FROM "np01"."docs"`)
	np01Refused(t, db, "delete without predicate", err)
	_, err = fe.Exec(ctx, `DELETE FROM "np01"."docs" WHERE "id" = $1; DROP TABLE "np01"."docs"`, int64(1))
	np01Refused(t, db, "multiple statements", err)
	_, err = fe.Exec(ctx, `DELETE FROM "np01"."docs" WHERE "id" = $1 -- tail`, int64(1))
	np01Refused(t, db, "comment", err)
	_, err = fe.Exec(ctx, `DELETE FROM "np01"."docs" WHERE "id" = pg_cancel_backend(1)`)
	np01Refused(t, db, "function predicate", err)
	_, err = fe.Exec(ctx, `DELETE FROM "np01"."docs" WHERE "id" = $2`, int64(1))
	np01Refused(t, db, "placeholder gap", err)
	_, err = fe.Exec(ctx, `DELETE FROM "np01"."docs" WHERE "id" = $1`)
	np01Refused(t, db, "missing argument", err)
	_, err = fe.Exec(ctx, `INSERT INTO "np01"."docs" ("id") VALUES ($1), ($2)`, int64(1), int64(2))
	np01Refused(t, db, "multi-row insert", err)
	_, err = fe.Exec(ctx, `INSERT INTO "np01"."docs" ("id") VALUES ($1) ON CONFLICT DO NOTHING`, int64(1))
	np01Refused(t, db, "upsert", err)
	_, err = fe.Exec(ctx, "BEGIN")
	np01Refused(t, db, "transaction control", err)
	for _, arg := range []any{1, 1.5, float32(1), []byte("1"), int16(1), uint8(1), struct{}{}, []any{}, pgx.NamedArgs{}, time.Time{}.Add(time.Nanosecond)} {
		_, err = fe.Exec(ctx, `DELETE FROM "np01"."docs" WHERE "id" = $1`, arg)
		np01Refused(t, db, "argument type", err)
	}
	if _, err = fe.Exec(ctx, `DELETE FROM "np01"."docs" WHERE "id" = $1`, int64(1)); err != nil {
		t.Fatal("control statement refused", err)
	}
	if _, err = fe.Exec(ctx, `DELETE FROM "np01"."docs" WHERE "id" = $1`, int32(1)); err != nil {
		t.Fatal("int32 argument refused", err)
	}
}

func TestFiniteGuardPreservesNullZeroAndExplicitJSON(t *testing.T) {
	db, fe, table, c := np01Admitted(t)
	ctx := context.Background()
	null, err := ParseJSON("null")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Update(ctx, fe, table, c.id.Eq(1), Set(c.data, Some(&null)), Set(c.title, Some("")), Set(c.active, Some(false))); err != nil {
		t.Fatal(err)
	}
	if _, err = Update(ctx, fe, table, c.id.Eq(1), Set(c.data, Some((*JSON)(nil)))); err != nil {
		t.Fatal(err)
	}
	if len(db.args) < 2 {
		t.Fatal(db.args)
	}
	first, last := db.args[len(db.args)-2], db.args[len(db.args)-1]
	if _, ok := first[0].(JSON); !ok || !first[0].(JSON).IsNull() || last[0] != nil {
		t.Fatal("JSON null and SQL NULL arguments collapsed", first, last)
	}
}

func TestFiniteTransactionRefusesStrongerOptionsBeforePinning(t *testing.T) {
	pool := new(pgxpool.Pool)
	fe := &FiniteExecutor{inner: pool, identity: EndpointIdentity{engine: "nucleus"}}
	callback := func(*FiniteScope) error { t.Fatal("callback ran"); return nil }
	for _, options := range []TransactionOptions{
		{Isolation: pgx.Serializable},
		{Isolation: pgx.RepeatableRead},
		{Access: pgx.ReadOnly},
		{Isolation: pgx.Serializable, Access: pgx.ReadOnly, Deferrable: pgx.Deferrable},
	} {
		if err := fe.WithTransaction(context.Background(), pool, options, callback); !errors.Is(err, ErrProfileRefused) {
			t.Fatal("stronger transaction admitted", options, err)
		}
	}
	if err := fe.WithTransaction(context.Background(), pool, TransactionOptions{}, nil); !errors.Is(err, ErrProfileRefused) {
		t.Fatal("nil callback admitted", err)
	}
	if err := fe.WithTransaction(context.Background(), new(pgxpool.Pool), TransactionOptions{}, callback); !errors.Is(err, ErrProfileRefused) {
		t.Fatal("foreign pool admitted", err)
	}
	fake := &FiniteExecutor{inner: &np01Executor{}}
	if err := fake.WithTransaction(context.Background(), pool, TransactionOptions{}, callback); !errors.Is(err, ErrProfileRefused) {
		t.Fatal("non-pool executor admitted", err)
	}
}
