package orm

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

type vectorLiveModel struct {
	ID        int64   `db:"id"`
	Embedding Vector  `db:"embedding"`
	Optional  *Vector `db:"optional,nullable"`
}

func TestPostgresExtensionQualifiedVectorAndNativeDimensionChecks(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	// A newly installed extension belongs to this fixture schema and its cleanup.
	// An existing extension retains its owner's schema and lifecycle.
	if _, err := admin.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA "+schemaSQL); err != nil {
		t.Fatal("native pgvector fixture requires the pgvector image", err)
	}
	var extensionSchema string
	if err := admin.QueryRow(ctx, `SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='vector'`).Scan(&extensionSchema); err != nil {
		t.Fatal(err)
	}
	impostorSchema := schema + "_impostor"
	impostorSQL := quote(impostorSchema)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+impostorSQL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+impostorSQL+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	typeSQL := quote(extensionSchema) + ".vector"
	name := schemaSQL + ".vector_values"
	if _, err := admin.Exec(ctx, "CREATE TABLE "+name+" (id bigint PRIMARY KEY,embedding "+typeSQL+"(3) NOT NULL,optional "+typeSQL+"); CREATE TYPE "+impostorSQL+".vector AS (x bigint); CREATE TABLE "+impostorSQL+".vector_impostor (id bigint NOT NULL,embedding "+impostorSQL+".vector NOT NULL,optional "+impostorSQL+".vector)"); err != nil {
		t.Fatal(err)
	}
	table, err := NewPostgresTable[vectorLiveModel](ctx, admin, schema, "vector_values")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewColumn[vectorLiveModel, int64](table, "ID")
	embedding, _ := NewColumn[vectorLiveModel, Vector](table, "Embedding")
	optional, _ := NewColumn[vectorLiveModel, *Vector](table, "Optional")
	vector, err := NewVector([]float32{math.SmallestNonzeroFloat32, math.MaxFloat32, -0.5})
	if err != nil {
		t.Fatal(err)
	}
	written, err := InsertOne(ctx, admin, table, Set(id, Some(int64(1))), Set(embedding, Some(vector)), Set(optional, Some((*Vector)(nil))))
	if err != nil || !reflect.DeepEqual(written.Embedding.Elements(), vector.Elements()) || written.Optional != nil {
		t.Fatal("native vector float32/NULL round trip", err)
	}
	var nativeText string
	if err := admin.QueryRow(ctx, "SELECT embedding::text FROM "+name+" WHERE id=1").Scan(&nativeText); err != nil {
		t.Fatal(err)
	}
	// Independent native scalar unpacking, without the ORM's vector parser.
	var first, second, third float32
	if err := admin.QueryRow(ctx, "SELECT (embedding::real[])[1],(embedding::real[])[2],(embedding::real[])[3] FROM "+name+" WHERE id=1").Scan(&first, &second, &third); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]float32{first, second, third}, vector.Elements()) || nativeText == "" {
		t.Fatal("independent native vector oracle")
	}
	reloaded, err := SelectOne(ctx, admin, table, Query[vectorLiveModel]{}.Where(id.Eq(1)))
	if err != nil || !reflect.DeepEqual(reloaded.Embedding.Elements(), vector.Elements()) {
		t.Fatal("native vector select", err)
	}
	short, _ := NewVector([]float32{1, 2})
	if _, err := InsertOne(ctx, admin, table, Set(id, Some(int64(2))), Set(embedding, Some(short)), Set(optional, Some(&vector))); err == nil {
		t.Fatal("native vector typmod dimension bypass")
	}
	_, err = NewPostgresTable[vectorLiveModel](ctx, admin, impostorSchema, "vector_impostor")
	var refusal *CodecError
	if !errors.Is(err, ErrCodecUnsupported) || !errors.As(err, &refusal) || refusal.TypeName != "vector" || refusal.OID == 0 {
		t.Fatal("same-name vector impostor admitted", err)
	}
	var count int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+name).Scan(&count); err != nil || count != 1 {
		t.Fatal("failed dimension changed rows", err)
	}
}
