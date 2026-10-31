package orm

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

// This sample adapter intentionally treats a composite as lossless native text.
// Structured semantic decomposition belongs to a separately certified adapter.
type nativeCompositeText struct{ text string }

func (v nativeCompositeText) Value() (driver.Value, error) { return v.text, nil }
func (v *nativeCompositeText) Scan(source any) error {
	switch value := source.(type) {
	case string:
		v.text = value
	case []byte:
		v.text = string(value)
	default:
		return ErrScalarValue
	}
	if strings.Contains(v.text, "refuse") {
		return ErrScalarValue
	}
	return nil
}

type customCodecModel struct {
	ID    int64                         `db:"id"`
	Value SQLValue[nativeCompositeText] `db:"value"`
}

func TestPostgresExplicitCustomCodecContractAndRollback(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	name := schemaSQL + ".custom_codec"
	if _, err := admin.Exec(ctx, "CREATE TYPE "+schemaSQL+`.custom_tuple AS (n bigint,label text); CREATE TABLE `+name+` (id bigint PRIMARY KEY,value `+schemaSQL+`.custom_tuple NOT NULL); INSERT INTO `+name+` VALUES (1,ROW(9223372036854775807,'comma,quote"'))`); err != nil {
		t.Fatal(err)
	}
	var oid uint32
	if err := admin.QueryRow(ctx, "SELECT t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname=$1 AND t.typname='custom_tuple'", schema).Scan(&oid); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPostgresTable[customCodecModel](ctx, admin, schema, "custom_codec"); !errors.Is(err, ErrCodecUnsupported) {
		t.Fatal("implicit unknown OID accepted", err)
	}
	contract, err := CodecFor[customCodecModel, nativeCompositeText]("Value", schema, "custom_tuple", oid)
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := CodecFor[customCodecModel, nativeCompositeText]("Value", schema, "custom_tuple", oid+1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewPostgresTable[customCodecModel](ctx, admin, schema, "custom_codec", wrong); !errors.Is(err, ErrCodecUnsupported) {
		t.Fatal("wrong custom OID contract accepted", err)
	}
	table, err := NewPostgresTable[customCodecModel](ctx, admin, schema, "custom_codec", contract)
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewColumn[customCodecModel, int64](table, "ID")
	if err != nil {
		t.Fatal(err)
	}
	value, err := NewColumn[customCodecModel, SQLValue[nativeCompositeText]](table, "Value")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := SelectOne(ctx, admin, table, Query[customCodecModel]{}.Where(id.Eq(1)))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := loaded.Value.Decode()
	if err != nil {
		t.Fatal(err)
	}
	var oracle string
	if err := admin.QueryRow(ctx, "SELECT value::text FROM "+name+" WHERE id=1").Scan(&oracle); err != nil {
		t.Fatal(err)
	}
	if decoded.text != oracle {
		t.Fatal("independent custom native oracle")
	}
	written, err := NewSQLValue(nativeCompositeText{`(9007199254740993,"two, words")`})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InsertOne(ctx, admin, table, Set(id, Some(int64(2))), Set(value, Some(written))); err != nil {
		t.Fatal("explicit custom native write", err)
	}
	if err := admin.QueryRow(ctx, "SELECT value::text FROM "+name+" WHERE id=2").Scan(&oracle); err != nil {
		t.Fatal(err)
	}
	if oracle != `(9007199254740993,"two, words")` {
		t.Fatal("custom write native precision/text loss", oracle)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO "+name+" VALUES (3,ROW(0,'refuse'))"); err != nil {
		t.Fatal(err)
	}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		if _, err := scope.Exec(ctx, "UPDATE "+name+" SET value=ROW(0,'must rollback') WHERE id=2"); err != nil {
			return err
		}
		_, decodeErr := SelectOne(ctx, scope, table, Query[customCodecModel]{}.Where(id.Eq(3)))
		if !errors.Is(decodeErr, ErrScalarValue) {
			return errors.New("custom scanner refusal missing")
		}
		return nil // Scope must retain the decode failure even when swallowed.
	})
	if !errors.Is(err, ErrScopeDecode) || !errors.Is(err, ErrScalarValue) {
		t.Fatal("custom scanner failure committed", err)
	}
	if err := admin.QueryRow(ctx, "SELECT value::text FROM "+name+" WHERE id=2").Scan(&oracle); err != nil || oracle != `(9007199254740993,"two, words")` {
		t.Fatal("custom scanner rollback", err)
	}
	requirePoolReuse(t, ctx, pool)
}
