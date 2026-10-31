package orm

import (
	"errors"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestDecimalExactFinitePolicy(t *testing.T) {
	for input, want := range map[string]string{"9007199254740993.123456789012345678901234567890": "9007199254740993.123456789012345678901234567890", "9.007199254740993123456789e15": "9007199254740993.123456789", "-.0001": "-0.0001", "1e3": "1000", "0.000": "0.000"} {
		value, err := ParseDecimal(input)
		if err != nil || value.String() != want {
			t.Fatal("exact decimal parse", input, value.String(), err)
		}
		native, err := value.NumericValue()
		if err != nil {
			t.Fatal(err)
		}
		native.Int.SetInt64(999)
		if value.String() != want {
			t.Fatal("native coefficient aliased immutable value")
		}
	}
	for _, input := range []string{"NaN", "Infinity", "-Infinity", "1e9999999999", "1e-16384", "1e131072", "", ".", "1.2.3", " 1", "0x10"} {
		if _, err := ParseDecimal(input); !errors.Is(err, ErrScalarValue) {
			t.Fatal("unsupported decimal accepted", input, err)
		}
	}
	var value Decimal
	if _, err := value.NumericValue(); !errors.Is(err, ErrScalarValue) {
		t.Fatal("invalid decimal encoded")
	}
	for _, native := range []pgtype.Numeric{{}, {NaN: true, Valid: true}, {InfinityModifier: pgtype.Infinity, Valid: true}, {InfinityModifier: pgtype.NegativeInfinity, Valid: true}} {
		if err := value.ScanNumeric(native); !errors.Is(err, ErrScalarValue) {
			t.Fatal("non-finite/null scan accepted", err)
		}
	}
	coefficient := big.NewInt(123)
	if err := value.ScanNumeric(pgtype.Numeric{Int: coefficient, Exp: -2, Valid: true}); err != nil {
		t.Fatal(err)
	}
	coefficient.SetInt64(456)
	if value.String() != "1.23" {
		t.Fatal("scanned coefficient alias")
	}
	// PostgreSQL17 documented unconstrained numeric boundaries.
	if _, err := ParseDecimal("1e131071"); err != nil {
		t.Fatal("maximum integral digits refused", err)
	}
	if _, err := ParseDecimal("1e-16383"); err != nil {
		t.Fatal("maximum fractional digits refused", err)
	}
}

func TestUUIDNativePolicy(t *testing.T) {
	value, err := ParseUUID("ABCDEF12-3456-7890-ABCD-EF1234567890")
	if err != nil || value.String() != "abcdef12-3456-7890-abcd-ef1234567890" {
		t.Fatal("uuid canonicalization", err)
	}
	for _, text := range []string{"abcdef1234567890abcdef1234567890", "abcdef12x3456-7890-abcd-ef1234567890", "not-uuid"} {
		if _, err := ParseUUID(text); !errors.Is(err, ErrScalarValue) {
			t.Fatal("invalid uuid accepted", text)
		}
	}
	native, err := value.UUIDValue()
	if err != nil {
		t.Fatal(err)
	}
	var scanned UUID
	if err := scanned.ScanUUID(native); err != nil || scanned != value {
		t.Fatal("native uuid", err)
	}
	if err := scanned.ScanUUID(pgtype.UUID{}); !errors.Is(err, ErrScalarValue) {
		t.Fatal("SQL NULL decoded as nil uuid")
	}
	if (UUID{}).String() != "00000000-0000-0000-0000-000000000000" {
		t.Fatal("zero uuid is not nil uuid")
	}
}

func TestJSONNullAndImmutableDestination(t *testing.T) {
	document, err := ParseJSON(`{"n":9007199254740993,"duplicate":1,"duplicate":2}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(document.String(), "9007199254740993") {
		t.Fatal("JSON number lost precision")
	}
	jsonNull, err := ParseJSON(" null ")
	if err != nil || !jsonNull.IsNull() {
		t.Fatal("JSON null not represented", err)
	}
	if _, err := ParseJSON("{"); !errors.Is(err, ErrScalarValue) {
		t.Fatal("invalid JSON accepted")
	}
	if _, err := (JSON{}).Value(); !errors.Is(err, ErrScalarValue) {
		t.Fatal("zero JSON encoded")
	}
	var nullable *JSON
	destination := scanDestination(reflect.ValueOf(&nullable).Elem()).(pgtype.BytesScanner)
	if err := destination.ScanBytes([]byte("null")); err != nil || nullable == nil || !nullable.IsNull() {
		t.Fatal("JSON null collapsed into SQL NULL", err)
	}
	if err := destination.ScanBytes(nil); err != nil || nullable != nil {
		t.Fatal("SQL NULL became JSON null", err)
	}
	bytes := []byte(`{"a":1}`)
	if err := destination.ScanBytes(bytes); err != nil {
		t.Fatal(err)
	}
	bytes[2] = 'x'
	if nullable.String() != `{"a":1}` {
		t.Fatal("wire bytes aliased")
	}
	if err := document.ScanBytes(nil); !errors.Is(err, ErrScalarValue) {
		t.Fatal("nonnullable JSON accepted SQL NULL")
	}
}

func TestScalarMetadataAndInvalidBoundWrites(t *testing.T) {
	type model struct {
		Number     Decimal `db:"number"`
		Identifier UUID    `db:"identifier"`
		Document   *JSON   `db:"document,nullable"`
	}
	table, err := NewTable[model]("owned", "scalars")
	if err != nil {
		t.Fatal(err)
	}
	number, err := NewColumn[model, Decimal](table, "Number")
	if err != nil {
		t.Fatal(err)
	}
	document, err := NewColumn[model, *JSON](table, "Document")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := insertSQL(table, []Assignment[model]{Set(number, Some(Decimal{}))}); !errors.Is(err, ErrScalarValue) {
		t.Fatal("invalid Decimal write admitted", err)
	}
	if _, _, err := selectSQL(table, table.info.columns(), Query[model]{}.Where(number.Eq(Decimal{}))); !errors.Is(err, ErrScalarValue) {
		t.Fatal("invalid Decimal predicate admitted", err)
	}
	invalid := JSON{}
	if _, _, err := insertSQL(table, []Assignment[model]{Set(document, Some(&invalid))}); !errors.Is(err, ErrScalarValue) {
		t.Fatal("invalid nullable JSON write admitted", err)
	}
	if _, _, err := insertSQL(table, []Assignment[model]{Set(document, Some((*JSON)(nil)))}); err != nil {
		t.Fatal("explicit SQL NULL refused", err)
	}
	type alias Decimal
	type unsupported struct {
		Number alias `db:"number"`
	}
	if _, err := NewTable[unsupported]("owned", "alias"); err == nil {
		t.Fatal("uncertified named alias accepted")
	}
}
