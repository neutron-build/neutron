package orm

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

type compositeLiveModel struct {
	ID       int64      `db:"id"`
	Record   Composite  `db:"record"`
	Optional *Composite `db:"optional,nullable"`
}

func TestPostgresBoundedCompositePrecisionNullsAndPreservingRefusal(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	name := schemaSQL + ".composite_values"
	if _, err := admin.Exec(ctx, "CREATE TYPE "+schemaSQL+".exact_record AS (integer_value bigint,decimal_value numeric,text_value text,empty_value text,null_value text); CREATE TABLE "+name+" (id bigint PRIMARY KEY,record "+schemaSQL+".exact_record NOT NULL,optional "+schemaSQL+".exact_record); CREATE TYPE "+schemaSQL+".unqualified_record AS (x timestamptz); CREATE TABLE "+schemaSQL+".unqualified_composite (id bigint NOT NULL,record "+schemaSQL+".unqualified_record NOT NULL,optional "+schemaSQL+".unqualified_record); INSERT INTO "+schemaSQL+".unqualified_composite VALUES (1,ROW('2026-01-01 UTC'::timestamptz),NULL)"); err != nil {
		t.Fatal(err)
	}
	table, err := NewPostgresTable[compositeLiveModel](ctx, admin, schema, "composite_values")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewColumn[compositeLiveModel, int64](table, "ID")
	record, _ := NewColumn[compositeLiveModel, Composite](table, "Record")
	optional, _ := NewColumn[compositeLiveModel, *Composite](table, "Optional")
	text := "comma, quote\" slash\\ ()"
	fields := []Nullable[string]{{Valid: true, Value: "9223372036854775807"}, {Valid: true, Value: "9007199254740993.12345678901234567890"}, {Valid: true, Value: text}, {Valid: true, Value: ""}, {}}
	value, err := NewComposite(fields)
	if err != nil {
		t.Fatal(err)
	}
	written, err := InsertOne(ctx, admin, table, Set(id, Some(int64(1))), Set(record, Some(value)), Set(optional, Some((*Composite)(nil))))
	if err != nil || !reflect.DeepEqual(written.Record.Fields(), fields) || written.Optional != nil {
		t.Fatal("native composite round trip", err)
	}
	var integer int64
	var decimal, nativeText, empty string
	var nativeNull *string
	if err := admin.QueryRow(ctx, "SELECT (record).integer_value,(record).decimal_value::text,(record).text_value,(record).empty_value,(record).null_value FROM "+name+" WHERE id=1").Scan(&integer, &decimal, &nativeText, &empty, &nativeNull); err != nil {
		t.Fatal(err)
	}
	if integer != 9223372036854775807 || decimal != fields[1].Value || nativeText != text || empty != "" || nativeNull != nil {
		t.Fatal("independent native composite field oracle")
	}
	allNull, _ := NewComposite(make([]Nullable[string], 5))
	written, err = InsertOne(ctx, admin, table, Set(id, Some(int64(2))), Set(record, Some(allNull)), Set(optional, Some(&allNull)))
	if err != nil || written.Optional == nil || !reflect.DeepEqual(written.Optional.Fields(), allNull.Fields()) {
		t.Fatal("all-null record conflated with whole SQL NULL", err)
	}
	loaded, err := SelectOne(ctx, admin, table, Query[compositeLiveModel]{}.Where(id.Eq(1)))
	if err != nil || !reflect.DeepEqual(loaded.Record.Fields(), fields) {
		t.Fatal("native select composite", err)
	}
	var before, after string
	if err := admin.QueryRow(ctx, "SELECT record::text FROM "+schemaSQL+".unqualified_composite WHERE id=1").Scan(&before); err != nil {
		t.Fatal(err)
	}
	_, err = NewPostgresTable[compositeLiveModel](ctx, admin, schema, "unqualified_composite")
	var refusal *CodecError
	if !errors.Is(err, ErrCodecUnsupported) || !errors.As(err, &refusal) || refusal.TypeName != "unqualified_record" || refusal.OID == 0 {
		t.Fatal("unsupported nested field profile admitted", err)
	}
	if err := admin.QueryRow(ctx, "SELECT record::text FROM "+schemaSQL+".unqualified_composite WHERE id=1").Scan(&after); err != nil || before != after {
		t.Fatal("preserving refusal", err)
	}
}
