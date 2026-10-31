package typed

import (
	"reflect"
	"testing"
)

func TestMetadataRejectsNilAndEmptyModel(t *testing.T) {
	if err := ValidateModel(nil, nil); err == nil {
		t.Fatal("nil reflect type accepted")
	}
	if err := ValidateModel(reflect.TypeOf(struct{}{}), nil); err == nil {
		t.Fatal("zero mapped fields accepted")
	}
	if err := ValidateModel(reflect.TypeOf(struct{ Name string }{}), nil); err == nil {
		t.Fatal("untagged field accepted")
	}
}
