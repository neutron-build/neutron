package orm

import (
	"errors"
	"strings"
	"testing"
	"time"
)

type scalarModel struct {
	ID             int64     `db:"id"`
	Big            int64     `db:"big"`
	Number         Decimal   `db:"number"`
	Identifier     UUID      `db:"identifier"`
	Moment         time.Time `db:"moment"`
	SQLNull        *string   `db:"sql_null,nullable"`
	Document       *JSON     `db:"document,nullable"`
	RawJSON        JSON      `db:"raw_json"`
	NullableNumber *Decimal  `db:"nullable_number,nullable"`
	NullableUUID   *UUID     `db:"nullable_uuid,nullable"`
}

func TestPostgresExactScalarCodecs(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	name := schemaSQL + ".scalar_values"
	if _, err := admin.Exec(ctx, "CREATE TABLE "+name+` (id bigint PRIMARY KEY,big bigint NOT NULL DEFAULT 0,number numeric NOT NULL DEFAULT '0.00',identifier uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',moment timestamptz NOT NULL DEFAULT '2000-01-01 00:00:00+00',sql_null text,document jsonb,raw_json json NOT NULL DEFAULT 'null',nullable_number numeric,nullable_uuid uuid); INSERT INTO `+name+` (id,big,number,identifier,moment,document,raw_json,nullable_number,nullable_uuid) VALUES (1,9223372036854775807,'9007199254740993.123456789012345678901234567890','abcdef12-3456-7890-abcd-ef1234567890','2026-10-02 01:02:03.123456+05:30','null','{"duplicate":1,"duplicate":2,"n":9007199254740993}',1.2500,'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'),(2,-9223372036854775808,'-0.000000000000000000000001','00000000-0000-0000-0000-000000000000','2026-10-02 01:02:03.654321+00',NULL,'null',NULL,NULL)`); err != nil {
		t.Fatal(err)
	}
	table, err := NewTable[scalarModel](schema, "scalar_values")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewColumn[scalarModel, int64](table, "ID")
	if err != nil {
		t.Fatal(err)
	}
	number, err := NewColumn[scalarModel, Decimal](table, "Number")
	if err != nil {
		t.Fatal(err)
	}
	identifier, err := NewColumn[scalarModel, UUID](table, "Identifier")
	if err != nil {
		t.Fatal(err)
	}
	document, err := NewColumn[scalarModel, *JSON](table, "Document")
	if err != nil {
		t.Fatal(err)
	}
	rawJSON, err := NewColumn[scalarModel, JSON](table, "RawJSON")
	if err != nil {
		t.Fatal(err)
	}
	moment, err := NewColumn[scalarModel, time.Time](table, "Moment")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := Select(ctx, admin, table, Query[scalarModel]{}.OrderBy(id.Asc()))
	if err != nil || len(loaded) != 2 {
		t.Fatal("native scalar read", err)
	}
	// Independent native text casts expose exact PostgreSQL values; no ORM decoder
	// or shared serializer supplies the expected precision or NULL classification.
	for _, row := range loaded {
		var nativeNumber, nativeUUID, nativeJSON string
		var nativeDoc *string
		var micros int64
		var isNull bool
		if err := admin.QueryRow(ctx, "SELECT number::text,identifier::text,raw_json::text,document::text,(extract(epoch FROM moment)*1000000)::bigint,sql_null IS NULL FROM "+name+" WHERE id=$1", row.ID).Scan(&nativeNumber, &nativeUUID, &nativeJSON, &nativeDoc, &micros, &isNull); err != nil {
			t.Fatal(err)
		}
		if row.Number.String() != nativeNumber || row.Identifier.String() != nativeUUID || row.RawJSON.String() != nativeJSON || row.Moment.UnixMicro() != micros || (row.SQLNull == nil) != isNull {
			t.Fatal("scalar disagrees with native oracle", row.ID)
		}
		if nativeDoc == nil {
			if row.Document != nil {
				t.Fatal("SQL NULL decoded as JSON null")
			}
		} else if row.Document == nil || row.Document.String() != *nativeDoc {
			t.Fatal("JSON document disagrees with native oracle")
		}
	}
	if loaded[0].Big != int64(9223372036854775807) || loaded[1].Big != int64(-9223372036854775808) || loaded[0].Document == nil || !loaded[0].Document.IsNull() || loaded[0].NullableNumber == nil || loaded[0].NullableNumber.String() != "1.2500" || loaded[0].NullableUUID == nil || loaded[1].NullableNumber != nil || loaded[1].NullableUUID != nil {
		t.Fatal("nullable/extreme native scalars")
	}
	projected, err := SelectColumn(ctx, admin, document, Query[scalarModel]{}.OrderBy(id.Asc()))
	if err != nil || len(projected) != 2 || projected[0] == nil || !projected[0].IsNull() || projected[1] != nil {
		t.Fatal("nullable JSON projection conflates nulls", err)
	}
	pairs, err := SelectPair(ctx, admin, number, document, Query[scalarModel]{}.OrderBy(id.Asc()))
	if err != nil || pairs[0].Second == nil || !pairs[0].Second.IsNull() || pairs[1].Second != nil {
		t.Fatal("pair nullable JSON projection", err)
	}
	precise, err := ParseDecimal("1.234567890123456789012345678901234567890e5")
	if err != nil {
		t.Fatal(err)
	}
	uuid, err := ParseUUID("12345678-1234-1234-1234-123456789abc")
	if err != nil {
		t.Fatal(err)
	}
	jsonNull, err := ParseJSON("null")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := ParseJSON(`{"n":9007199254740993,"k":1,"k":2}`)
	if err != nil {
		t.Fatal(err)
	}
	instant := time.Date(2026, 10, 2, 12, 13, 14, 654321000, time.FixedZone("source", -7*3600))
	inserted, err := InsertOne(ctx, admin, table, Set(id, Some(int64(3))), Set(number, Some(precise)), Set(identifier, Some(uuid)), Set(document, Some(&jsonNull)), Set(rawJSON, Some(raw)), Set(moment, Some(instant)))
	if err != nil {
		t.Fatal("native bound scalar write", err)
	}
	if inserted.Number.String() != precise.String() || inserted.Identifier != uuid || inserted.Document == nil || !inserted.Document.IsNull() || inserted.RawJSON.String() != raw.String() || !inserted.Moment.Equal(instant) {
		t.Fatal("scalar RETURNING mismatch")
	}
	var nativeNumber, nativeUUID, nativeRaw string
	var jsonIsNull, sqlIsNull bool
	var micros int64
	if err := admin.QueryRow(ctx, "SELECT number::text,identifier::text,raw_json::text,document='null'::jsonb,document IS NULL,(extract(epoch FROM moment)*1000000)::bigint FROM "+name+" WHERE id=3").Scan(&nativeNumber, &nativeUUID, &nativeRaw, &jsonIsNull, &sqlIsNull, &micros); err != nil {
		t.Fatal(err)
	}
	if nativeNumber != precise.String() || nativeUUID != uuid.String() || nativeRaw != raw.String() || !jsonIsNull || sqlIsNull || micros != instant.UnixMicro() {
		t.Fatal("independent native scalar write oracle")
	}
	found, err := Select(ctx, admin, table, Query[scalarModel]{}.Where(identifier.Eq(uuid)))
	if err != nil || len(found) != 1 || found[0].ID != 3 {
		t.Fatal("native UUID bound predicate", err)
	}
	if count, err := Update(ctx, admin, table, id.Eq(3), Set(document, Some((*JSON)(nil))), Set(number, Default[Decimal]())); err != nil || count != 1 {
		t.Fatal("scalar null/default write", err)
	}
	var defaultNumber string
	if err := admin.QueryRow(ctx, "SELECT number::text,document IS NULL FROM "+name+" WHERE id=3").Scan(&defaultNumber, &sqlIsNull); err != nil || defaultNumber != "0.00" || !sqlIsNull {
		t.Fatal("SQL NULL/default oracle", err)
	}
	zero, err := SelectColumn(ctx, admin, number, Query[scalarModel]{}.Where(id.Eq(3)))
	if err != nil || len(zero) != 1 || zero[0].String() != "0" {
		t.Fatal("native numeric zero value", err)
	}
	// Pinned pgx normalizes binary numeric zero's scale; the exact numeric value
	// is preserved, but representation 0.00 is not a round-trip scale guarantee.
	var isZero bool
	if err := admin.QueryRow(ctx, "SELECT number=0::numeric FROM "+name+" WHERE id=3").Scan(&isZero); err != nil || !isZero {
		t.Fatal("native zero numeric oracle", err)
	}
	// Non-finite PostgreSQL values remain explicitly unsupported, including reads.
	for _, special := range []string{"NaN", "Infinity", "-Infinity"} {
		if _, err := admin.Exec(ctx, "UPDATE "+name+" SET number=$1::numeric WHERE id=3", special); err != nil {
			t.Fatal(err)
		}
		if _, err := SelectColumn(ctx, admin, number, Query[scalarModel]{}.Where(id.Eq(3))); !errors.Is(err, ErrScalarValue) {
			t.Fatal("non-finite native numeric not refused", special, err)
		}
	}
}
