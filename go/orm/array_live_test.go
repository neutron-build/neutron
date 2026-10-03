package orm

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

type arrayModel struct {
	ID           int64          `db:"id"`
	Values       Array[*int64]  `db:"values"`
	Optional     *Array[*int64] `db:"optional,nullable"`
	Documents    Array[*JSON]   `db:"documents"`
	Blob         Bytea          `db:"blob"`
	NullableBlob *Bytea         `db:"nullable_blob,nullable"`
}

func TestPostgresDimensionPreservingArraysAndBytea(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	name := schemaSQL + ".array_values"
	if _, err := admin.Exec(ctx, "CREATE TABLE "+name+` (id bigint PRIMARY KEY,"values" bigint[] NOT NULL,optional bigint[],documents jsonb[] NOT NULL,blob bytea NOT NULL,nullable_blob bytea); INSERT INTO `+name+` VALUES (1,'[-2:-1][5:6]={{9223372036854775807,NULL},{-9223372036854775808,0}}',NULL,ARRAY['null'::jsonb,NULL::jsonb,'{"n":9007199254740993}'::jsonb],decode('00ff5c01','hex'),NULL),(2,'{}','{}','{}',decode('','hex'),decode('','hex'))`); err != nil {
		t.Fatal(err)
	}
	table, err := NewTable[arrayModel](schema, "array_values")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewColumn[arrayModel, int64](table, "ID")
	if err != nil {
		t.Fatal(err)
	}
	values, err := NewColumn[arrayModel, Array[*int64]](table, "Values")
	if err != nil {
		t.Fatal(err)
	}
	optional, err := NewColumn[arrayModel, *Array[*int64]](table, "Optional")
	if err != nil {
		t.Fatal(err)
	}
	documents, err := NewColumn[arrayModel, Array[*JSON]](table, "Documents")
	if err != nil {
		t.Fatal(err)
	}
	blob, err := NewColumn[arrayModel, Bytea](table, "Blob")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := Select(ctx, admin, table, Query[arrayModel]{}.OrderBy(id.Asc()))
	if err != nil || len(loaded) != 2 {
		t.Fatal("native array read", err)
	}
	first := loaded[0]
	elements := first.Values.Elements()
	docs := first.Documents.Elements()
	if !reflect.DeepEqual(first.Values.Dimensions(), []pgtype.ArrayDimension{{Length: 2, LowerBound: -2}, {Length: 2, LowerBound: 5}}) || len(elements) != 4 || *elements[0] != 9223372036854775807 || elements[1] != nil || *elements[2] != -9223372036854775808 || *elements[3] != 0 {
		t.Fatal("dimension/extreme/NULL array loss")
	}
	if len(docs) != 3 || docs[0] == nil || !docs[0].IsNull() || docs[1] != nil || !strings.Contains(docs[2].String(), "9007199254740993") {
		t.Fatal("JSON null vs SQL NULL array element loss")
	}
	second := loaded[1]
	if first.Optional != nil || second.Optional == nil || len(second.Optional.Elements()) != 0 || len(second.Values.Dimensions()) != 0 || len(second.Values.Elements()) != 0 || second.NullableBlob == nil || len(second.NullableBlob.Bytes()) != 0 {
		t.Fatal("empty vs SQL NULL loss")
	}
	var nativeDims, nativeHex string
	var nativeLength int32
	var nullElement bool
	if err := admin.QueryRow(ctx, "SELECT array_dims(\"values\"),cardinality(\"values\"),\"values\"[-2][6] IS NULL,encode(blob,'hex') FROM "+name+" WHERE id=1").Scan(&nativeDims, &nativeLength, &nullElement, &nativeHex); err != nil {
		t.Fatal(err)
	}
	if nativeDims != "[-2:-1][5:6]" || nativeLength != int32(len(elements)) || !nullElement || nativeHex != "00ff5c01" || !reflect.DeepEqual(first.Blob.Bytes(), []byte{0, 255, 92, 1}) {
		t.Fatal("native independent array/bytea oracle mismatch")
	}
	x := int64(9007199254740993)
	written, err := NewArray([]pgtype.ArrayDimension{{Length: 2, LowerBound: 0}}, []*int64{&x, nil})
	if err != nil {
		t.Fatal(err)
	}
	jsonNull, err := ParseJSON("null")
	if err != nil {
		t.Fatal(err)
	}
	jsonArray, err := NewArray([]pgtype.ArrayDimension{{Length: 2, LowerBound: -4}}, []*JSON{&jsonNull, nil})
	if err != nil {
		t.Fatal(err)
	}
	result, err := InsertOne(ctx, admin, table, Set(id, Some(int64(3))), Set(values, Some(written)), Set(optional, Some(&written)), Set(documents, Some(jsonArray)), Set(blob, Some(NewBytea([]byte{0, 255}))))
	if err != nil {
		t.Fatal("native array write", err)
	}
	if !reflect.DeepEqual(result.Values.Dimensions(), written.Dimensions()) || result.Optional == nil || !reflect.DeepEqual(result.Optional.Elements(), written.Elements()) {
		t.Fatal("array RETURNING shape loss")
	}
	var writtenText, jsonText string
	if err := admin.QueryRow(ctx, "SELECT \"values\"::text,documents::text FROM "+name+" WHERE id=3").Scan(&writtenText, &jsonText); err != nil {
		t.Fatal(err)
	}
	if writtenText != "[0:1]={9007199254740993,NULL}" || jsonText != "[-4:-3]={\"null\",NULL}" {
		t.Fatal("independent native write oracle", writtenText, jsonText)
	}
	// The ORM only owns the random schema; unrelated custom catalog objects are
	// not altered or flattened when a model refuses an unsupported codec.
	if _, err := NewTable[struct {
		Unsupported []string `db:"unsupported"`
	}](schema, "array_values"); err == nil {
		t.Fatal("flat array silently loses dimension contract")
	}
}
