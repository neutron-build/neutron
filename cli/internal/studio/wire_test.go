package studio

import (
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestFormatNumeric(t *testing.T) {
	numeric := func(digits string, exp int32) pgtype.Numeric {
		n := new(big.Int)
		n.SetString(digits, 10)
		return pgtype.Numeric{Int: n, Exp: exp, Valid: true, InfinityModifier: pgtype.Finite}
	}
	cases := []struct {
		name string
		n    pgtype.Numeric
		want string
	}{
		{"scale preserved", numeric("1234560", -4), "123.4560"},
		{"trailing zeros to scale", numeric("99000", -4), "9.9000"},
		{"no scale", numeric("123", 0), "123"},
		{"fraction shorter than scale", numeric("5", -4), "0.0005"},
		{"zero", numeric("0", -2), "0.00"},
		{"negative", numeric("-1234560", -4), "-123.4560"},
		{"negative fraction only", numeric("-5", -1), "-0.5"},
		{"positive exponent (negative scale)", numeric("123456", 2), "12345600"},
		{"huge precision", numeric("12345678901234567890123456789012", -8), "123456789012345678901234.56789012"},
		{"NaN", pgtype.Numeric{NaN: true, Valid: true}, "NaN"},
		{"Infinity", pgtype.Numeric{Valid: true, InfinityModifier: pgtype.Infinity}, "Infinity"},
		{"-Infinity", pgtype.Numeric{Valid: true, InfinityModifier: pgtype.NegativeInfinity}, "-Infinity"},
		{"invalid", pgtype.Numeric{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatNumeric(tc.n); got != tc.want {
				t.Errorf("formatNumeric(%v) = %q, want %q", tc.n, got, tc.want)
			}
		})
	}
}

func TestWireTagForOID(t *testing.T) {
	cases := map[uint32]string{
		20:   "int8",
		1700: "numeric",
		17:   "bytea",
		1082: "date",
		1114: "timestamp",
		1184: "timestamptz",
		23:   "", // int4
		25:   "", // text
		16:   "", // bool
		701:  "", // float8
		3802: "", // jsonb
	}
	for oid, want := range cases {
		if got := wireTag(oid); got != want {
			t.Errorf("wireTag(%d) = %q, want %q", oid, got, want)
		}
	}
}

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestEncodeTaggedCell(t *testing.T) {
	cases := []struct {
		name string
		oid  uint32
		v    any
		want taggedCell
	}{
		{"int8", 20, int64(9007199254740993), taggedCell{"int8", "9007199254740993"}},
		{"int8 min", 20, int64(-9223372036854775808), taggedCell{"int8", "-9223372036854775808"}},
		{"bytea empty", 17, []byte{}, taggedCell{"bytea", ""}},
		{"bytea", 17, []byte{0x00, 0xff, 0x10}, taggedCell{"bytea", "00ff10"}},
		{"date", 1082, mustTime("2026-01-02T00:00:00Z"), taggedCell{"date", "2026-01-02"}},
		{"date BC", 1082, time.Date(-43, 3, 15, 0, 0, 0, 0, time.UTC), taggedCell{"date", "0044-03-15 BC"}},
		{"timestamp", 1114, mustTime("2026-01-01T19:04:05.678123Z"), taggedCell{"timestamp", "2026-01-01T19:04:05.678123"}},
		{"timestamp trims trailing us zeros", 1114, mustTime("2026-01-01T19:04:05.100000Z"), taggedCell{"timestamp", "2026-01-01T19:04:05.1"}},
		{"timestamptz normalized to UTC", 1184, mustTime("2026-03-08T07:30:00.123456+05:00"), taggedCell{"timestamptz", "2026-03-08T02:30:00.123456Z"}},
		{"numeric", 1700, pgtype.Numeric{Int: big.NewInt(1234560), Exp: -4, Valid: true, InfinityModifier: pgtype.Finite}, taggedCell{"numeric", "123.4560"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := encodeTaggedCell(tc.oid, tc.v)
			cell, ok := got.(taggedCell)
			if !ok {
				t.Fatalf("encodeTaggedCell returned %T (%v), want taggedCell", got, got)
			}
			if cell != tc.want {
				t.Errorf("encodeTaggedCell = %+v, want %+v", cell, tc.want)
			}
			// The cell marshals to exactly the pinned {"t":..,"v":..} shape.
			bs, err := json.Marshal(cell)
			if err != nil {
				t.Fatal(err)
			}
			if string(bs[0:6]) != `{"t":"` {
				t.Errorf("tagged cell JSON shape drifted: %s", bs)
			}
		})
	}

	// Passthrough: plain types and untagged OIDs never get wrapped.
	for _, v := range []any{nil, 1.5, "text", true} {
		if got := encodeTaggedCell(25, v); got != v {
			t.Errorf("plain value %v got wrapped: %v", v, got)
		}
	}
	if got := encodeTaggedCell(20, "already-a-string"); got != "already-a-string" {
		t.Errorf("unexpected Go type should pass through, got %v", got)
	}
}

func TestDecodeWireValue(t *testing.T) {
	// Tagged cells decode to exact driver values.
	v, err := decodeWireValue(map[string]any{"t": "int8", "v": "9007199254740993"})
	if err != nil || v != int64(9007199254740993) {
		t.Errorf("int8 decode = %v %v", v, err)
	}
	v, err = decodeWireValue(map[string]any{"t": "bytea", "v": "00ff10"})
	b, ok := v.([]byte)
	if err != nil || !ok || len(b) != 3 || b[0] != 0 || b[1] != 0xff || b[2] != 0x10 {
		t.Errorf("bytea decode = %v %v", v, err)
	}
	v, err = decodeWireValue(map[string]any{"t": "numeric", "v": "1.50"})
	if err != nil || v != "1.50" {
		t.Errorf("numeric decode = %v %v", v, err)
	}
	v, err = decodeWireValue(map[string]any{"t": "date", "v": "2026-01-02"})
	if err != nil || v != "2026-01-02" {
		t.Errorf("date decode = %v %v", v, err)
	}

	// Plain JSON: null, strings, bools; numbers stay exact via json.Number.
	if v, err = decodeWireValue(nil); err != nil || v != nil {
		t.Errorf("null decode = %v %v", v, err)
	}
	if v, err = decodeWireValue("x"); err != nil || v != "x" {
		t.Errorf("string decode = %v %v", v, err)
	}
	if v, err = decodeWireValue(true); err != nil || v != true {
		t.Errorf("bool decode = %v %v", v, err)
	}
	if v, err = decodeWireValue(json.Number("9007199254740993")); err != nil || v != int64(9007199254740993) {
		t.Errorf("integral number decode = %v (%T) %v — must be exact int64", v, v, err)
	}
	if v, err = decodeWireValue(json.Number("1.5")); err != nil || v != float64(1.5) {
		t.Errorf("fractional number decode = %v %v", v, err)
	}
	// JSON objects pass through for json/jsonb columns.
	obj := map[string]any{"a": json.Number("1")}
	if v, err = decodeWireValue(obj); err != nil {
		t.Errorf("object passthrough rejected: %v", err)
	} else if got, ok := v.(map[string]any); !ok || got["a"] != json.Number("1") {
		t.Errorf("object passthrough altered the value: %v", v)
	}

	// Strict rejections.
	for _, bad := range []struct {
		name string
		v    any
	}{
		{"bad int8", map[string]any{"t": "int8", "v": "1.5"}},
		{"int8 overflow", map[string]any{"t": "int8", "v": "9223372036854775808"}},
		{"odd hex", map[string]any{"t": "bytea", "v": "0ff"}},
		{"non-hex", map[string]any{"t": "bytea", "v": "zz"}},
		{"unknown tag", map[string]any{"t": "bogus", "v": "x"}},
		{"empty payload", map[string]any{"t": "date", "v": ""}},
	} {
		if _, err = decodeWireValue(bad.v); err == nil {
			t.Errorf("%s should be rejected", bad.name)
		}
	}
}

func TestEncodeTaggedCellEdgeValues(t *testing.T) {
	cases := []struct {
		name string
		oid  uint32
		v    any
		want any
	}{
		{"timestamp BC keeps era after the time", oidTimestamp, time.Date(-43, 3, 15, 12, 0, 0, 0, time.UTC), taggedCell{"timestamp", "0044-03-15T12:00:00 BC"}},
		{"timestamptz BC", oidTimestamptz, time.Date(0, 1, 1, 0, 0, 0, 500000000, time.UTC), taggedCell{"timestamptz", "0001-01-01T00:00:00.5Z BC"}},
		{"year beyond 9999", oidDate, time.Date(12345, 6, 7, 0, 0, 0, 0, time.UTC), taggedCell{"date", "12345-06-07"}},
		{"date infinity", oidDate, pgtype.Infinity, taggedCell{"date", "infinity"}},
		{"timestamp -infinity", oidTimestamp, pgtype.NegativeInfinity, taggedCell{"timestamp", "-infinity"}},
		{"timestamptz infinity", oidTimestamptz, pgtype.Infinity, taggedCell{"timestamptz", "infinity"}},
		{"uuid canonical string", oidUUID, [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}, "12345678-9abc-def0-0123-456789abcdef"},
	}
	for _, tc := range cases {
		if got := encodeTaggedCell(tc.oid, tc.v); got != tc.want {
			t.Errorf("%s: encodeTaggedCell = %#v, want %#v", tc.name, got, tc.want)
		}
	}
}

func TestDecodeColumnValueStrict(t *testing.T) {
	col := func(oid uint32, name string) tableColumnMeta {
		return tableColumnMeta{Name: "c", TypeOID: oid, TypeName: name}
	}
	int8Col, int4Col, textCol := col(20, "int8"), col(23, "int4"), col(25, "text")
	jsonbCol, tsCol, byteaCol := col(3802, "jsonb"), col(1114, "timestamp"), col(17, "bytea")

	ok := []struct {
		name string
		col  tableColumnMeta
		raw  any
		want any
	}{
		{"int8 tagged", int8Col, map[string]any{"t": "int8", "v": "9007199254740993"}, int64(9007199254740993)},
		{"int4 number as exact literal", int4Col, json.Number("12"), "12"},
		{"text string", textCol, "", ""},
		{"text bool", textCol, true, true},
		{"jsonb JSON text", jsonbCol, `{"t":"int8","v":"1"}`, `{"t":"int8","v":"1"}`},
		{"jsonb JSON null text", jsonbCol, "null", "null"},
		{"timestamp tagged", tsCol, map[string]any{"t": "timestamp", "v": "2026-01-01T00:00:00.000001"}, "2026-01-01T00:00:00.000001"},
		{"SQL NULL", textCol, nil, nil},
	}
	for _, tc := range ok {
		got, err := decodeColumnValue(tc.col, tc.raw)
		if err != nil || got != tc.want {
			t.Errorf("%s: = %#v, %v; want %#v", tc.name, got, err, tc.want)
		}
	}
	b, err := decodeColumnValue(byteaCol, map[string]any{"t": "bytea", "v": "00ff"})
	if bs, isBytes := b.([]byte); err != nil || !isBytes || len(bs) != 2 || bs[1] != 0xff {
		t.Errorf("bytea tagged = %v %v", b, err)
	}

	bad := []struct {
		name string
		col  tableColumnMeta
		raw  any
	}{
		{"int8 bare number", int8Col, json.Number("9007199254740993")},
		{"int8 bare string", int8Col, "9007199254740993"},
		{"int8 wrong tag", int8Col, map[string]any{"t": "numeric", "v": "1"}},
		{"int4 tagged", int4Col, map[string]any{"t": "int8", "v": "1"}},
		{"text object", textCol, map[string]any{"a": "b"}},
		{"text array", textCol, []any{"a"}},
		{"jsonb object (ambiguous with tagged cells)", jsonbCol, map[string]any{"a": json.Number("1")}},
		{"jsonb invalid text", jsonbCol, "{nope"},
		{"timestamp untagged", tsCol, "2026-01-01"},
		{"bytea bad hex", byteaCol, map[string]any{"t": "bytea", "v": "0g"}},
		{"tag with extra field", int8Col, map[string]any{"t": "int8", "v": "1", "x": "y"}},
	}
	for _, tc := range bad {
		if got, err := decodeColumnValue(tc.col, tc.raw); err == nil {
			t.Errorf("%s: accepted as %#v", tc.name, got)
		}
	}
}

func TestDecodeStrictJSONBody(t *testing.T) {
	type body struct {
		A string `json:"a"`
		N any    `json:"n"`
	}
	decode := func(s string) (body, error) {
		var b body
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(s))
		return b, decodeStrictJSONBody(req, &b)
	}
	b, err := decode(`{"a":"x","n":9007199254740993}`)
	if err != nil || b.N != json.Number("9007199254740993") {
		t.Errorf("exact number decode = %#v %v", b, err)
	}
	for _, bad := range []string{`{"a":"x","extra":1}`, `{"a":"x"} {"a":"y"}`, `{"a":"x"}garbage`, ``} {
		if _, err := decode(bad); err == nil {
			t.Errorf("strict decode accepted %q", bad)
		}
	}
	if _, err := decode(`{"a":"x"}` + "\n  "); err != nil {
		t.Errorf("trailing whitespace must be accepted: %v", err)
	}
}
