package orm

import (
	"errors"
	"strings"
	"testing"
)

type catalogModel struct {
	ID     int64   `db:"id"`
	State  Enum    `db:"state"`
	Number Decimal `db:"number"`
}

func TestPostgresCatalogEnumDomainAndUnknownOIDRefusal(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	name := schemaSQL + ".catalog_values"
	if _, err := admin.Exec(ctx, "CREATE TYPE "+schemaSQL+`.state_kind AS ENUM ('','ready','quoted''label'); CREATE DOMAIN `+schemaSQL+`.positive_decimal AS numeric CHECK (VALUE>0); CREATE TYPE `+schemaSQL+`.opaque_composite AS (x bigint,y text); CREATE TABLE `+name+` (id bigint PRIMARY KEY,state `+schemaSQL+`.state_kind NOT NULL,number `+schemaSQL+`.positive_decimal NOT NULL); CREATE TABLE `+schemaSQL+`.unsupported_values (id bigint PRIMARY KEY,value `+schemaSQL+`.opaque_composite NOT NULL); INSERT INTO `+name+` VALUES (1,'ready',9007199254740993.12345678901234567890); INSERT INTO `+schemaSQL+`.unsupported_values VALUES (1,(9223372036854775807,'preserved')); SET search_path TO pg_catalog`); err != nil {
		t.Fatal(err)
	}
	table, err := NewPostgresTable[catalogModel](ctx, admin, schema, "catalog_values")
	if err != nil {
		t.Fatal("qualified enum/domain", err)
	}
	id, err := NewColumn[catalogModel, int64](table, "ID")
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewColumn[catalogModel, Enum](table, "State")
	if err != nil {
		t.Fatal(err)
	}
	number, err := NewColumn[catalogModel, Decimal](table, "Number")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := SelectOne(ctx, admin, table, Query[catalogModel]{}.Where(id.Eq(1)))
	if err != nil {
		t.Fatal("native enum/domain read", err)
	}
	var nativeState, nativeNumber string
	if err := admin.QueryRow(ctx, "SELECT state::text,number::text FROM "+name+" WHERE id=1").Scan(&nativeState, &nativeNumber); err != nil {
		t.Fatal(err)
	}
	if loaded.State.Label() != nativeState || loaded.Number.String() != nativeNumber {
		t.Fatal("enum/domain native oracle")
	}
	decimal, err := ParseDecimal("1.25000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InsertOne(ctx, admin, table, Set(id, Some(int64(2))), Set(state, Some(NewEnum("quoted'label"))), Set(number, Some(decimal))); err != nil {
		t.Fatal("enum/domain native write", err)
	}
	bad, err := ParseDecimal("-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InsertOne(ctx, admin, table, Set(id, Some(int64(3))), Set(state, Some(NewEnum("ready"))), Set(number, Some(bad))); err == nil {
		t.Fatal("domain constraint bypassed")
	}
	if _, err := InsertOne(ctx, admin, table, Set(id, Some(int64(4))), Set(state, Some(NewEnum("unknown label"))), Set(number, Some(decimal))); err == nil {
		t.Fatal("enum membership bypassed")
	}
	type wrongEnum struct {
		ID     int64   `db:"id"`
		State  string  `db:"state"`
		Number Decimal `db:"number"`
	}
	if _, err := NewPostgresTable[wrongEnum](ctx, admin, schema, "catalog_values"); !errors.Is(err, ErrCodecUnsupported) {
		t.Fatal("enum silently mapped as ordinary text", err)
	}
	type wrongNumeric struct {
		ID     int64   `db:"id"`
		State  Enum    `db:"state"`
		Number float64 `db:"number"`
	}
	if _, err := NewPostgresTable[wrongNumeric](ctx, admin, schema, "catalog_values"); !errors.Is(err, ErrCodecUnsupported) {
		t.Fatal("domain numeric silently narrowed to float", err)
	}
	type unknown struct {
		ID    int64  `db:"id"`
		Value string `db:"value"`
	}
	_, err = NewPostgresTable[unknown](ctx, admin, schema, "unsupported_values")
	var refused *CodecError
	if !errors.Is(err, ErrCodecUnsupported) || !errors.As(err, &refused) || refused.TypeName != "opaque_composite" || refused.OID == 0 || refused.Column != "value" {
		t.Fatal("unknown composite refusal identity", err)
	}
	var nativeValue string
	var typeOID uint32
	if err := admin.QueryRow(ctx, "SELECT value::text,pg_typeof(value)::oid FROM "+schemaSQL+".unsupported_values WHERE id=1").Scan(&nativeValue, &typeOID); err != nil {
		t.Fatal(err)
	}
	if nativeValue != "(9223372036854775807,preserved)" || typeOID != refused.OID {
		t.Fatal("preserving refusal altered database object/value")
	}
}
