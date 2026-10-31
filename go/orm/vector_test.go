package orm

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestVectorImmutableFiniteFloat32AndExtensionIdentity(t *testing.T) {
	input := []float32{math.SmallestNonzeroFloat32, math.MaxFloat32, -0.5}
	vector, err := NewVector(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = 0
	output := vector.Elements()
	output[1] = 0
	text, err := vector.Value()
	if err != nil {
		t.Fatal(err)
	}
	var decoded Vector
	if err := decoded.Scan(text); err != nil || !reflect.DeepEqual(decoded.Elements(), vector.Elements()) {
		t.Fatal("float32 round trip", err)
	}
	for _, input := range [][]float32{nil, make([]float32, MaxVectorDimensions+1), {float32(math.NaN())}, {float32(math.Inf(1))}} {
		if _, err := NewVector(input); !errors.Is(err, ErrScalarValue) {
			t.Fatal("invalid vector", err)
		}
	}
	for _, text := range []string{"[]", "[NaN]", "[Infinity]", "[1e100]", "[1,]", "1,2"} {
		if err := decoded.Scan(text); !errors.Is(err, ErrScalarValue) {
			t.Fatal("invalid text vector", err)
		}
	}
	typ := reflect.TypeOf(Vector{})
	for _, codec := range []catalogCodec{{oid: 42, kind: "b", name: "vector"}, {oid: 42, kind: "b", name: "halfvec", extension: "vector"}, {oid: 42, kind: "c", name: "vector", extension: "vector"}} {
		if qualifiedCatalogCodec(typ, codec) {
			t.Fatal("uncertified extension family")
		}
	}
	if !qualifiedCatalogCodec(typ, catalogCodec{oid: 42, kind: "b", name: "vector", extension: "vector"}) {
		t.Fatal("qualified vector refused")
	}
}
