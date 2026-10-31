package orm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

type testModel struct {
	ID     int64   `db:"id"`
	Tenant string  `db:"tenant"`
	Active bool    `db:"active"`
	Score  int64   `db:"score"`
	Name   string  `db:"name"`
	Note   *string `db:"note,nullable"`
}

func setupMetadata(t *testing.T) (Table[testModel], Column[testModel, int64], Column[testModel, string], Column[testModel, bool], Column[testModel, int64], Column[testModel, string], Column[testModel, *string]) {
	t.Helper()
	table, err := NewTable[testModel]("tenant schema", "records")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewColumn[testModel, int64](table, "ID")
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := NewColumn[testModel, string](table, "Tenant")
	if err != nil {
		t.Fatal(err)
	}
	active, err := NewColumn[testModel, bool](table, "Active")
	if err != nil {
		t.Fatal(err)
	}
	score, err := NewColumn[testModel, int64](table, "Score")
	if err != nil {
		t.Fatal(err)
	}
	name, err := NewColumn[testModel, string](table, "Name")
	if err != nil {
		t.Fatal(err)
	}
	note, err := NewColumn[testModel, *string](table, "Note")
	if err != nil {
		t.Fatal(err)
	}
	return table, id, tenant, active, score, name, note
}

func TestQualifiedBoundComposition(t *testing.T) {
	table, id, tenant, _, _, name, note := setupMetadata(t)
	attack := "'; DROP TABLE records; --"
	q := Query[testModel]{}.Where(And(tenant.Eq(attack), Or(id.Eq(7), note.Eq(nil)))).OrderBy(name.Desc(), id.Asc()).Limit(4).Offset(2)
	sql, args, err := selectSQL(table, table.info.columns(), q)
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT "id", "tenant", "active", "score", "name", "note" FROM "tenant schema"."records" WHERE ("tenant" = $1 AND ("id" = $2 OR "note" IS NULL)) ORDER BY "name" DESC, "id" ASC LIMIT $3 OFFSET $4`
	if sql != want || !reflect.DeepEqual(args, []any{attack, int64(7), 4, 2}) {
		t.Fatalf("%s %#v", sql, args)
	}
	if strings.Contains(sql, attack) {
		t.Fatal("value interpolated into SQL")
	}
	_, _, err = selectSQL(table, "id", Query[testModel]{}.Where(Predicate[testModel]{}))
	if err == nil {
		t.Fatal("explicit empty where accepted")
	}
}

func TestWritePresenceAndScope(t *testing.T) {
	table, id, tenant, active, score, name, note := setupMetadata(t)
	sql, args, err := insertSQL(table, []Assignment[testModel]{Set(tenant, Some("owner")), Set(active, Some(false)), Set(score, Some(int64(0))), Set(name, Some("")), Set(note, Some((*string)(nil))), Set(id, Omit[int64]())})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, `("tenant", "active", "score", "name", "note") VALUES ($1, $2, $3, $4, $5)`) || !reflect.DeepEqual(args, []any{"owner", false, int64(0), "", nil}) {
		t.Fatal(sql, args)
	}
	sql, args, err = updateSQL(table, And(tenant.Eq("owner"), id.Eq(9)), []Assignment[testModel]{Set(name, Default[string]()), Set(score, Some(int64(0)))})
	if err != nil || sql != `UPDATE "tenant schema"."records" SET "name" = DEFAULT, "score" = $1 WHERE ("tenant" = $2 AND "id" = $3)` || !reflect.DeepEqual(args, []any{int64(0), "owner", int64(9)}) {
		t.Fatal(sql, args, err)
	}
	if _, _, err := updateSQL(table, Predicate[testModel]{}, []Assignment[testModel]{Set(name, Some("bad"))}); err == nil {
		t.Fatal("unscoped update accepted")
	}
	if _, _, err := deleteSQL(table, And[testModel]()); err == nil {
		t.Fatal("empty group delete accepted")
	}
	if _, _, err := updateSQL(table, id.Eq(1), []Assignment[testModel]{Set(name, Omit[string]())}); err == nil {
		t.Fatal("empty update accepted")
	}
	if _, _, err := insertSQL(table, []Assignment[testModel]{Set(name, Some("first")), Set(name, Omit[string]())}); err == nil {
		t.Fatal("duplicate assignment accepted")
	}
	other, err := NewTable[testModel]("other", "records")
	if err != nil {
		t.Fatal(err)
	}
	otherID, _ := NewColumn[testModel, int64](other, "ID")
	if _, _, err := deleteSQL(table, otherID.Eq(1)); err == nil {
		t.Fatal("foreign table predicate accepted")
	}
	if _, _, err := insertSQL(table, []Assignment[testModel]{Set(otherID, Some(int64(1)))}); err == nil {
		t.Fatal("foreign table assignment accepted")
	}
}

func TestNullableInputsAreSnapshots(t *testing.T) {
	table, _, _, _, _, _, note := setupMetadata(t)
	value := "original"
	p := note.Eq(&value)
	a := Set(note, Some(&value))
	value = "mutated"
	args := []any{}
	_, err := renderPredicate(table.info, p.expr, &args)
	if err != nil || !reflect.DeepEqual(args, []any{"original"}) {
		t.Fatal(args, err)
	}
	_, _, args, err = writeParts(table, []Assignment[testModel]{a})
	if err != nil || !reflect.DeepEqual(args, []any{"original"}) {
		t.Fatal(args, err)
	}
	if _, err := renderPredicate(table.info, note.Gt(nil).expr, &args); err == nil {
		t.Fatal("ordered NULL comparison accepted")
	}
}

func TestNilAndCanceledExecutionRefused(t *testing.T) {
	table, _, _, _, _, _, _ := setupMetadata(t)
	if _, err := Select(context.Background(), nil, table, Query[testModel]{}); err == nil {
		t.Fatal("nil executor accepted")
	}
	if _, err := Select(nil, nil, table, Query[testModel]{}); err == nil {
		t.Fatal("nil context accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Select(ctx, nil, table, Query[testModel]{}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation must refuse before executor access", err)
	}
}

func TestMetadataValidation(t *testing.T) {
	type duplicate struct {
		A string `db:"same"`
		B string `db:"same"`
	}
	if _, err := NewTable[duplicate]("public", "t"); err == nil {
		t.Fatal("duplicate accepted")
	}
	type mismatch struct {
		A *string `db:"a"`
	}
	if _, err := NewTable[mismatch]("public", "t"); err == nil {
		t.Fatal("nullable mismatch accepted")
	}
	type unsupported struct {
		A []string `db:"a"`
	}
	if _, err := NewTable[unsupported]("public", "t"); err == nil {
		t.Fatal("unsupported codec accepted")
	}
	table, _, _, _, _, _, _ := setupMetadata(t)
	if _, err := NewColumn[testModel, string](table, "ID"); err == nil {
		t.Fatal("wrong typed column accepted")
	}
	if quote(`odd"name`) != `"odd""name"` {
		t.Fatal("identifier quote escaping")
	}
	if _, err := NewTable[testModel](strings.Repeat("x", 64), "t"); err == nil {
		t.Fatal("truncated identifier accepted")
	}
}

func TestErrorPreservesStateAndRedactsServerMessage(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "23505", Message: "secret-parameter-value"}
	err := wrap("insert", pgErr)
	var wrapped *Error
	var original *pgconn.PgError
	if !errors.As(err, &wrapped) || !errors.As(err, &original) || original != pgErr || wrapped.SQLState() != "23505" {
		t.Fatal(err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatal("server parameter leaked")
	}
	if !errors.Is(wrap("select", context.Canceled), context.Canceled) {
		t.Fatal("context cause lost")
	}
}
