package orm

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestCatalogCodecMatrixAndIdentityRefusal(t *testing.T) {
	for _, sample := range []struct {
		value    any
		oid      uint32
		kind     string
		accepted bool
	}{
		{int64(0), pgtype.Int8OID, "b", true}, {float64(0), pgtype.NumericOID, "b", false},
		{Decimal{}, pgtype.NumericOID, "b", true}, {"", pgtype.Int8OID, "b", false},
		{Enum{}, 999999, "e", true}, {"", 999999, "e", false}, {JSON{}, 999999, "c", false},
		{Range[int64]{}, pgtype.Int8rangeOID, "r", true}, {Range[int32]{}, pgtype.Int8rangeOID, "r", false},
		{Date{}, pgtype.DateOID, "b", true}, {TimeOfDay{}, pgtype.TimetzOID, "b", false},
	} {
		if actual := qualifiedCatalogCodec(reflect.TypeOf(sample.value), catalogCodec{oid: sample.oid, kind: sample.kind}); actual != sample.accepted {
			t.Fatal("matrix mismatch", sample)
		}
	}
	e := &CodecError{Schema: "tenant", Table: "records", Column: "value", TypeSchema: "custom", TypeName: "opaque", OID: 123456}
	if !errors.Is(e, ErrCodecUnsupported) || !strings.Contains(e.Error(), "OID 123456") || !strings.Contains(e.Error(), `"custom"."opaque"`) {
		t.Fatal("refusal lacks identity", e)
	}
	if _, err := (Enum{}).Value(); !errors.Is(err, ErrScalarValue) {
		t.Fatal("zero enum accepts write")
	}
	if value, err := NewEnum("").Value(); err != nil || value != "" {
		t.Fatal("valid empty enum refused", err)
	}
}
