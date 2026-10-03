package orm

import (
	"errors"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestImmutableArrayDimensionsNullElementsAndBounds(t *testing.T) {
	x := int64(9223372036854775807)
	dims := []pgtype.ArrayDimension{{Length: 2, LowerBound: -3}, {Length: 2, LowerBound: 7}}
	a, err := NewArray(dims, []*int64{&x, nil, &x, &x})
	if err != nil {
		t.Fatal(err)
	}
	x = 0
	dims[0].LowerBound = 0
	if *a.Elements()[0] != 9223372036854775807 || a.Elements()[1] != nil || a.Dimensions()[0].LowerBound != -3 {
		t.Fatal("constructor input alias")
	}
	exported := a.Elements()
	*exported[0] = 4
	exportedDimensions := a.Dimensions()
	exportedDimensions[0].LowerBound = 2
	if a.Index(0).(int64) != 9223372036854775807 || a.Index(1) != nil {
		t.Fatal("native nullable index not flattened")
	}
	if *a.Elements()[0] != 9223372036854775807 || a.Dimensions()[0].LowerBound != -3 {
		t.Fatal("array accessor alias")
	}
	for _, dimensions := range [][]pgtype.ArrayDimension{{{Length: 0}}, {{Length: -1}}, {{Length: 2, LowerBound: 2147483647}}, {{Length: MaxArrayElements + 1}}} {
		if _, err := NewArray[int64](dimensions, nil); err == nil {
			t.Fatal("invalid dimension accepted", dimensions)
		}
	}
	if _, err := NewArray([]pgtype.ArrayDimension{{Length: 2}}, []int64{1}); err == nil {
		t.Fatal("wrong cardinality accepted")
	}
	if _, err := NewArray([]pgtype.ArrayDimension{{Length: 1}}, []Decimal{{}}); !errors.Is(err, ErrScalarValue) {
		t.Fatal("invalid element admitted", err)
	}
	type arbitrary struct{ X int }
	if _, err := NewArray[arbitrary](nil, nil); err == nil {
		t.Fatal("unqualified codec admitted")
	}
	empty, err := NewArray[int64](nil, nil)
	if err != nil || empty.Dimensions() == nil || len(empty.Elements()) != 0 {
		t.Fatal("empty conflates SQL NULL", err)
	}
	if !errors.Is(validateScalarValue(Array[int64]{}), ErrScalarValue) {
		t.Fatal("zero array encodes SQL NULL")
	}
	registry := pgtype.NewMap()
	encoded, err := registry.Encode(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Array[*int64]
	if err := registry.Scan(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, encoded, scanDestination(reflect.ValueOf(&decoded).Elem())); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.Dimensions(), a.Dimensions()) || !reflect.DeepEqual(decoded.Elements(), a.Elements()) {
		t.Fatal("native array shape/NULL loss")
	}
	var nullable *Array[*int64]
	if err := registry.Scan(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, encoded, scanDestination(reflect.ValueOf(&nullable).Elem())); err != nil || nullable == nil {
		t.Fatal("nullable native array", err)
	}
	if err := registry.Scan(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, nil, scanDestination(reflect.ValueOf(&nullable).Elem())); err != nil || nullable != nil {
		t.Fatal("array SQL NULL", err)
	}
	if err := registry.Scan(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, nil, scanDestination(reflect.ValueOf(&decoded).Elem())); !errors.Is(err, ErrScalarValue) {
		t.Fatal("nonnullable array SQL NULL", err)
	}
}

func TestByteaImmutableEmptyAndNull(t *testing.T) {
	input := []byte{0, 255, 92, 1}
	b := NewBytea(input)
	input[1] = 0
	output, err := b.BytesValue()
	if err != nil || output[1] != 255 {
		t.Fatal("bytea constructor alias", err)
	}
	output[1] = 0
	if b.Bytes()[1] != 255 {
		t.Fatal("bytea output alias")
	}
	if encoded, err := NewBytea(nil).BytesValue(); err != nil || encoded == nil || len(encoded) != 0 {
		t.Fatal("empty bytea SQL NULL", err)
	}
	if err := b.ScanBytes(nil); !errors.Is(err, ErrScalarValue) {
		t.Fatal("nonnullable bytea accepts SQL NULL")
	}
}
