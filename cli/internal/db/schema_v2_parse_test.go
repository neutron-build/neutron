package db

import "testing"

// S05: the designer's type/default text parsers accept exactly the
// contract's representable vocabulary and refuse the rest with precise
// messages. The classification rules must match introspection's own
// (literal vs expression), because a desired document built from these
// parsers is diffed against an introspected one.
func TestParseV2TypeText(t *testing.T) {
	ok := []struct {
		text string
		want V2ColumnType
	}{
		{"text", V2ColumnType{Name: "text", Codec: "string"}},
		{"integer", V2ColumnType{Name: "int4", Codec: "number"}},
		{"INT", V2ColumnType{Name: "int4", Codec: "number"}},
		{"bigint", V2ColumnType{Name: "int8", Codec: "bigint"}},
		{"double precision", V2ColumnType{Name: "float8", Codec: "number"}},
		{"numeric(12,4)", V2ColumnType{Name: "numeric", Params: map[string]int64{"precision": 12, "scale": 4}, Codec: "decimal-string"}},
		{"numeric", V2ColumnType{Name: "numeric", Codec: "decimal-string"}},
		{"varchar(80)", V2ColumnType{Name: "varchar", Params: map[string]int64{"length": 80}, Codec: "string"}},
		{"character varying(10)", V2ColumnType{Name: "varchar", Params: map[string]int64{"length": 10}, Codec: "string"}},
		{"varchar", V2ColumnType{Name: "varchar", Codec: "string"}},
		{"timestamptz(6)", V2ColumnType{Name: "timestamptz", Params: map[string]int64{"precision": 6}, Codec: "timestamptz-string"}},
		{"timestamp with time zone", V2ColumnType{Name: "timestamptz", Codec: "timestamptz-string"}},
		{"timestamp without time zone", V2ColumnType{Name: "timestamp", Codec: "timestamp-string"}},
		{"boolean", V2ColumnType{Name: "bool", Codec: "boolean"}},
		{"jsonb", V2ColumnType{Name: "jsonb", Codec: "json"}},
		{"text[]", V2ColumnType{Name: "text", Array: true, Codec: "array"}},
		{"integer[]", V2ColumnType{Name: "int4", Array: true, Codec: "array"}},
		{"vector(3)", V2ColumnType{Name: "vector", Params: map[string]int64{"dimensions": 3}, Codec: "vector"}},
	}
	for _, tc := range ok {
		got, err := ParseV2TypeText(tc.text)
		if err != nil {
			t.Errorf("%q: unexpected error %v", tc.text, err)
			continue
		}
		if got.Name != tc.want.Name || got.Array != tc.want.Array || got.Codec != tc.want.Codec || len(got.Params) != len(tc.want.Params) {
			t.Errorf("%q: got %+v want %+v", tc.text, got, tc.want)
			continue
		}
		for k, v := range tc.want.Params {
			if got.Params[k] != v {
				t.Errorf("%q: param %s = %d want %d", tc.text, k, got.Params[k], v)
			}
		}
	}

	bad := []string{
		"",              // empty
		"serial",        // sequence identity cannot be invented from text
		"bigserial",     // same
		"char(1)",       // bpchar is outside the contract vocabulary
		"time",          // not representable
		"interval",      // not representable
		"numeric(10)",   // numeric takes precision AND scale (or neither)
		"varchar(0)",    // positive length required
		"text(10)",      // text takes no parameters
		"numeric(4,12)", // scale > precision
		"timestamp(9)",  // precision 0-6
		"weirdtype",     // unknown name
		"varchar(80][]", // malformed array marker
	}
	for _, text := range bad {
		if _, err := ParseV2TypeText(text); err == nil {
			t.Errorf("%q: accepted", text)
		}
	}
}

func TestV2DefaultFromText(t *testing.T) {
	lit := func(sql string) *V2ColumnDefault { return &V2ColumnDefault{Kind: "literal", SQL: &sql} }
	ok := []struct {
		text string
		want *V2ColumnDefault
	}{
		{"42", lit("42")},
		{"-7", lit("-7")},
		{"3.14", lit("3.14")},
		{"true", lit("true")},
		{"'seed'", lit("'seed'")},
		{"now()", &V2ColumnDefault{Kind: "expression", SQL: strPtrV2("now()")}},
		{"(pos > 0)", &V2ColumnDefault{Kind: "expression", SQL: strPtrV2("(pos > 0)")}},
	}
	for _, tc := range ok {
		got, err := V2DefaultFromText(tc.text)
		if err != nil {
			t.Errorf("%q: unexpected error %v", tc.text, err)
			continue
		}
		if !got.SameAs(*tc.want) {
			t.Errorf("%q: got %+v want %+v", tc.text, got, tc.want)
		}
	}
	for _, text := range []string{"", "nextval('s'::regclass)", "'; DROP TABLE x; --"} {
		if _, err := V2DefaultFromText(text); err == nil {
			t.Errorf("%q: accepted", text)
		}
	}
}
