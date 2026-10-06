package nucleus

import (
	"github.com/jackc/pgx/v5/pgtype"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestScanRowNativeValues(t *testing.T) {
	type row struct {
		Note *string   `db:"note"`
		Data []byte    `db:"data"`
		Nums []int64   `db:"nums"`
		TS   time.Time `db:"ts"`
	}
	var got row
	rows := &mockScanRows{cols: []string{"note", "data", "nums", "ts"}, oids: []uint32{pgtype.TextOID, pgtype.ByteaOID, pgtype.Int8ArrayOID, pgtype.TimestamptzOID}, vals: []*string{strPtr("present"), strPtr(`\x00ff4142`), strPtr(`{9007199254740993,-9223372036854775808}`), strPtr("2026-09-30 12:34:56.123456+05:30")}}
	if err := scanRow(rows, &got); err != nil {
		t.Fatal(err)
	}
	if got.Note == nil || *got.Note != "present" || !reflect.DeepEqual(got.Data, []byte{0, 255, 65, 66}) || !reflect.DeepEqual(got.Nums, []int64{9007199254740993, -9223372036854775808}) {
		t.Fatalf("decoded %+v", got)
	}
	want := time.Date(2026, 9, 30, 7, 4, 56, 123456000, time.UTC)
	if !got.TS.Equal(want) {
		t.Fatalf("timestamp %v", got.TS)
	}
	rows.vals = []*string{nil, nil, nil, nil}
	if err := scanRow(rows, &got); err != nil {
		t.Fatal(err)
	}
	if got.Note != nil || got.Data != nil || got.Nums != nil || !got.TS.IsZero() {
		t.Fatalf("NULL left stale values: %+v", got)
	}
}

func TestScanRowErrors(t *testing.T) {
	cases := []struct {
		name string
		dest any
		raw  string
		oid  uint32
	}{
		{"int overflow", &struct {
			V int8 `db:"v"`
		}{}, "128", pgtype.Int2OID},
		{"uint overflow", &struct {
			V uint8 `db:"v"`
		}{}, "256", pgtype.Int2OID},
		{"invalid bool", &struct {
			V bool `db:"v"`
		}{}, "maybe", pgtype.BoolOID},
		{"unexported", &struct {
			v string `db:"v"`
		}{}, "hidden", pgtype.TextOID},
		{"unsupported", &struct {
			V map[string]int `db:"v"`
		}{}, "{}", pgtype.TextOID},
		{"unknown array OID", &struct {
			V []int64 `db:"v"`
		}{}, "{1}", 999999},
		{"malformed array", &struct {
			V []int64 `db:"v"`
		}{}, "{bad}", pgtype.Int8ArrayOID},
		{"null array element", &struct {
			V []int64 `db:"v"`
		}{}, "{NULL}", pgtype.Int8ArrayOID},
		{"invalid timestamp", &struct {
			V time.Time `db:"v"`
		}{}, "infinity", pgtype.TimestamptzOID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := scanRow(&mockScanRows{cols: []string{"v"}, oids: []uint32{tc.oid}, vals: []*string{strPtr(tc.raw)}}, tc.dest)
			if err == nil || !strings.Contains(err.Error(), `column "v"`) {
				t.Fatalf("expected qualified error, got %v", err)
			}
		})
	}
}

func TestScanRowArrayCodecSyntax(t *testing.T) {
	type row struct {
		V []*string `db:"v"`
	}
	var got row
	err := scanRow(&mockScanRows{cols: []string{"v"}, oids: []uint32{pgtype.TextArrayOID}, vals: []*string{strPtr(`{"comma,value","quote\"value",NULL,"NULL",""}`)}}, &got)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.V) != 5 || *got.V[0] != "comma,value" || *got.V[1] != `quote"value` || got.V[2] != nil || *got.V[3] != "NULL" || *got.V[4] != "" {
		t.Fatalf("array decoded incorrectly: %+v", got)
	}
}
