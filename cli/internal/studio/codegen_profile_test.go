package studio

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func scalarCols() []colInfo {
	var out []colInfo
	for _, oid := range []int64{21, 23, 20, 1700, 16, 25, 1043, 1042, 2950, 17} {
		v := readScalars[oid]
		out = append(out, colInfo{name: "v_" + v.name, udtName: v.name, dataType: v.name, typeOID: oid, typeNamespace: "pg_catalog", typeKind: "b"})
	}
	return out
}
func TestLosslessReadMatrix(t *testing.T) {
	cols := scalarCols()
	for i := range cols {
		cols[i].nullable = true
	}
	for _, lang := range []string{"ts", "python", "go"} {
		code, err := GenerateCodeProfile(LosslessReadProfile, lang, "samples", cols)
		if err != nil {
			t.Fatal(err)
		}
		again, _ := GenerateCodeProfile(LosslessReadProfile, lang, "samples", cols)
		if code != again {
			t.Fatal("nondeterministic output")
		}
		switch lang {
		case "ts":
			if !strings.Contains(code, "v_int8: string | null") || !strings.Contains(code, "v_bytea: Uint8Array | null") || strings.Contains(code, "?:") {
				t.Fatal(code)
			}
		case "python":
			if !strings.Contains(code, "Optional[Decimal]") || !strings.Contains(code, "ConfigDict(allow_inf_nan=True)") {
				t.Fatal(code)
			}
		case "go":
			if !strings.Contains(code, "VBytea *[]byte") || !strings.Contains(code, "VNumeric *string") {
				t.Fatal(code)
			}
		}
	}
}
func TestLosslessReadRefusesIdentityAndNames(t *testing.T) {
	base := scalarCols()[0]
	cases := []colInfo{base, base, base, base, base, base, base, base}
	cases[0].typeNamespace = "public"
	cases[1].typeKind = "d"
	cases[2].typeOID = 99999
	cases[3].udtName = "int8"
	cases[4].typeKind = "e"
	cases[5].typeNamespace = ""
	cases[6].name = "model_config"
	cases[7].name = "odd-name"
	for _, c := range cases {
		for _, lang := range []string{"go", "ts", "python"} {
			code, err := GenerateCodeProfile(LosslessReadProfile, lang, "samples", []colInfo{c})
			if err == nil || code != "" || !strings.Contains(err.Error(), c.name) || !strings.Contains(err.Error(), lang) {
				t.Fatalf("%+v %q %v", c, code, err)
			}
		}
	}
	a := base
	a.name = "a_b"
	b := base
	b.name = "a_B"
	if _, err := GenerateCodeProfile(LosslessReadProfile, "go", "samples", []colInfo{a, b}); err == nil {
		t.Fatal("field collision accepted")
	}
	for _, profile := range []string{"unknown", LosslessReadProfile} {
		if _, err := GenerateCodeProfile(profile, "rust", "samples", []colInfo{base}); err == nil {
			t.Fatal("invalid profile/lang accepted")
		}
	}
}
func TestLegacyProfileUnchanged(t *testing.T) {
	cols := []colInfo{{name: "id", dataType: "bigint", udtName: "int8"}, {name: "amount", dataType: "numeric", udtName: "numeric", nullable: true}, {name: "stamp", dataType: "timestamp", udtName: "timestamp"}}
	for _, lang := range []string{"go", "ts", "python", "rust", "elixir", "zig"} {
		want, err := GenerateCode(lang, "samples", cols)
		if err != nil {
			t.Fatal(err)
		}
		for _, profile := range []string{"", LegacyProfile} {
			got, err := GenerateCodeProfile(profile, lang, "samples", cols)
			if err != nil || got != want {
				t.Fatalf("legacy changed %s", lang)
			}
		}
	}
}

type profileRows struct {
	pgx.Rows
	count            int
	scanErr, rowsErr error
	closed           bool
}

func (r *profileRows) Next() bool {
	if r.count > 0 {
		r.count--
		return true
	}
	return false
}
func (r *profileRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	*dest[0].(*string) = "value"
	*dest[1].(*string) = "int8"
	*dest[2].(*string) = "pg_catalog"
	*dest[3].(*string) = "b"
	*dest[4].(*int64) = 20
	*dest[5].(*bool) = false
	return nil
}
func (r *profileRows) Err() error { return r.rowsErr }
func (r *profileRows) Close()     { r.closed = true }

type profileQuerier struct {
	rows pgx.Rows
	err  error
}

func (q profileQuerier) Query(_ context.Context, _ string, _ ...interface{}) (pgx.Rows, error) {
	return q.rows, q.err
}
func TestLosslessMetadataErrors(t *testing.T) {
	sentinel := errors.New("metadata unavailable")
	for _, q := range []profileQuerier{{err: sentinel}, {rows: &profileRows{count: 1, scanErr: sentinel}}, {rows: &profileRows{count: 1, rowsErr: sentinel}}} {
		_, err := FetchColsForProfile(context.Background(), q, "scope", "samples", LosslessReadProfile)
		if !errors.Is(err, sentinel) {
			t.Fatalf("not propagated: %v", err)
		}
	}
	cols, err := FetchColsForProfile(context.Background(), profileQuerier{rows: &profileRows{count: 1}}, "scope", "samples", LosslessReadProfile)
	if err != nil || len(cols) != 1 {
		t.Fatal(cols, err)
	}
	code, err := GenerateCodeProfile(LosslessReadProfile, "ts", "samples", cols)
	if err != nil || !strings.Contains(code, "value: string") {
		t.Fatal(code, err)
	}
	_, err = FetchColsForProfile(context.Background(), profileQuerier{rows: &profileRows{}}, "scope", "samples", LosslessReadProfile)
	if err == nil {
		t.Fatal("empty identity accepted")
	}
}

func TestReadProfileConditionalPythonImports(t *testing.T) {
	cols := scalarCols()
	code, err := GenerateCodeProfile(LosslessReadProfile, "python", "samples", cols)
	if err != nil || !strings.Contains(code, "from uuid import UUID") || !strings.Contains(code, "v_uuid: UUID") {
		t.Fatal(code, err)
	}
	code, err = GenerateCodeProfile(LosslessReadProfile, "python", "samples", cols[:1])
	if err != nil || strings.Contains(code, "ConfigDict") || strings.Contains(code, "import UUID") || strings.Contains(code, "import Decimal") {
		t.Fatal(code, err)
	}
}
