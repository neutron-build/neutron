package orm

import (
	"strings"
	"testing"
)

func TestPostgresCatalogAdmissionIgnoresHostileSearchPathOperators(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	// A writable schema preceding pg_catalog can shadow operators even though all
	// catalog relations are qualified. Build demonstrably hostile exact overloads.
	sql := "CREATE FUNCTION " + schemaSQL + ".name_equal(pg_catalog.name,pg_catalog.name) RETURNS pg_catalog.bool LANGUAGE sql IMMUTABLE AS 'SELECT false'; CREATE OPERATOR " + schemaSQL + ".= (PROCEDURE=" + schemaSQL + ".name_equal,LEFTARG=pg_catalog.name,RIGHTARG=pg_catalog.name); CREATE FUNCTION " + schemaSQL + ".oid_equal(pg_catalog.oid,pg_catalog.oid) RETURNS pg_catalog.bool LANGUAGE sql IMMUTABLE AS 'SELECT false'; CREATE OPERATOR " + schemaSQL + ".= (PROCEDURE=" + schemaSQL + ".oid_equal,LEFTARG=pg_catalog.oid,RIGHTARG=pg_catalog.oid); CREATE DOMAIN " + schemaSQL + ".integer_domain AS pg_catalog.int8; CREATE TYPE " + schemaSQL + ".exact_record AS (x pg_catalog.int8); CREATE TABLE " + schemaSQL + ".catalog_operator_domain (id " + schemaSQL + ".integer_domain NOT NULL); CREATE TABLE " + schemaSQL + ".catalog_operator_composite (id bigint NOT NULL,record " + schemaSQL + ".exact_record NOT NULL,optional " + schemaSQL + ".exact_record); CREATE TABLE " + schemaSQL + ".fk_parent (id bigint PRIMARY KEY,peer bigint NOT NULL); CREATE TABLE " + schemaSQL + ".fk_child (id bigint PRIMARY KEY,peer bigint NOT NULL,CONSTRAINT child_peer_fk FOREIGN KEY(peer) REFERENCES " + schemaSQL + ".fk_parent(id) DEFERRABLE INITIALLY IMMEDIATE); SET search_path TO " + schemaSQL + ",pg_catalog"
	if _, err := admin.Exec(ctx, sql); err != nil {
		t.Fatal(err)
	}
	var poisonedName, poisonedOID, nativeName, nativeOID bool
	if err := admin.QueryRow(ctx, `SELECT 'x'::pg_catalog.name = 'x'::pg_catalog.name,1::pg_catalog.oid = 1::pg_catalog.oid,'x'::pg_catalog.name OPERATOR(pg_catalog.=) 'x'::pg_catalog.name,1::pg_catalog.oid OPERATOR(pg_catalog.=) 1::pg_catalog.oid`).Scan(&poisonedName, &poisonedOID, &nativeName, &nativeOID); err != nil {
		t.Fatal(err)
	}
	if poisonedName || poisonedOID || !nativeName || !nativeOID {
		t.Fatal("hostile operator fixture ineffective")
	}
	table, err := NewPostgresTable[cteGroupModel](ctx, admin, schema, "records")
	if err != nil {
		t.Fatal("qualified catalog admission", err)
	}
	id, _ := NewColumn[cteGroupModel, int64](table, "ID")
	if _, err := NewUniqueConstraint(ctx, admin, table, "records_pkey", BindColumn(id)); err != nil {
		t.Fatal("qualified unique constraint admission", err)
	}
	child, _ := NewTable[cycleA](schema, "fk_child")
	if _, err := NewDeferredForeignKey(ctx, admin, child, "child_peer_fk"); err != nil {
		t.Fatal("qualified deferred FK admission", err)
	}
	type domainModel struct {
		ID int64 `db:"id"`
	}
	if _, err := NewPostgresTable[domainModel](ctx, admin, schema, "catalog_operator_domain"); err != nil {
		t.Fatal("qualified domain base", err)
	}
	if _, err := NewPostgresTable[compositeLiveModel](ctx, admin, schema, "catalog_operator_composite"); err != nil {
		t.Fatal("qualified composite attributes", err)
	}
	if _, err := NewPostgresTable[cteGroupModel](ctx, admin, schema+"_missing", "records"); err == nil {
		t.Fatal("wrong schema admitted")
	}
}
