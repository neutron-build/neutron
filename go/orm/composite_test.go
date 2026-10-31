package orm

import (
	"errors"
	"reflect"
	"testing"
)

func TestCompositeExactTextNullsEscapingAndBudgets(t *testing.T) {
	fields := []Nullable[string]{{Valid: true, Value: "9223372036854775807"}, {Valid: true, Value: "9007199254740993.12345678901234567890"}, {Valid: true, Value: ""}, {}, {Valid: true, Value: "comma, quote\" slash\\ ()"}}
	value, err := NewComposite(fields)
	if err != nil {
		t.Fatal(err)
	}
	fields[0].Value = "0"
	output := value.Fields()
	output[1].Value = "0"
	text, err := value.Value()
	if err != nil {
		t.Fatal(err)
	}
	var decoded Composite
	if err := decoded.Scan(text); err != nil || !reflect.DeepEqual(decoded.Fields(), value.Fields()) {
		t.Fatal("record text round trip", err)
	}
	if err := decoded.Scan(`(plain,"a""b",,"",\\)`); err != nil || len(decoded.Fields()) != 5 || decoded.Fields()[1].Value != "a\"b" || decoded.Fields()[2].Valid || !decoded.Fields()[3].Valid || decoded.Fields()[3].Value != "" || decoded.Fields()[4].Value != "\\" {
		t.Fatal("native record syntax", err)
	}
	for _, text := range []string{"", "(\"unterminated)", "(\"x\"trailing)", "(bad\"quote)", "(slash\\)", "(embedded(inner))"} {
		if err := decoded.Scan(text); !errors.Is(err, ErrScalarValue) {
			t.Fatal("malformed record", text, err)
		}
	}
	if _, err := NewComposite(nil); !errors.Is(err, ErrScalarValue) {
		t.Fatal("empty record profile", err)
	}
	if _, err := NewComposite(make([]Nullable[string], MaxCompositeFields+1)); !errors.Is(err, ErrScalarValue) {
		t.Fatal("field budget", err)
	}
	if err := decoded.Scan(nil); !errors.Is(err, ErrScalarValue) {
		t.Fatal("nonnullable SQL NULL", err)
	}
	if !errors.Is(validateScalarValue(Composite{}), ErrScalarValue) {
		t.Fatal("zero composite accepted")
	}
}

func TestCompositeRefusesSessionDependentByteaFieldText(t *testing.T) {
	if qualifiedCompositeField(17) {
		t.Fatal("bytea_output-dependent field text admitted")
	}
}
