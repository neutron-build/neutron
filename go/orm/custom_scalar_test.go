package orm

import (
	"database/sql/driver"
	"errors"
	"testing"
)

type customBytes struct{ bytes []byte }

func (v customBytes) Value() (driver.Value, error) { return v.bytes, nil }
func (v *customBytes) Scan(source any) error {
	bytes, ok := source.([]byte)
	if !ok {
		return ErrScalarValue
	}
	v.bytes = append([]byte{}, bytes...)
	return nil
}

type customNull struct{}

func (customNull) Value() (driver.Value, error) { return nil, nil }
func (*customNull) Scan(any) error              { return nil }

func TestCustomScannerValuerFreezesMutableState(t *testing.T) {
	input := customBytes{[]byte{0, 255, 1}}
	value, err := NewSQLValue(input)
	if err != nil {
		t.Fatal(err)
	}
	input.bytes[1] = 0
	decoded, err := value.Decode()
	if err != nil || decoded.bytes[1] != 255 {
		t.Fatal("valuer input alias", err)
	}
	decoded.bytes[1] = 0
	raw, err := value.Value()
	if err != nil || raw.([]byte)[1] != 255 {
		t.Fatal("decoder output alias", err)
	}
	raw.([]byte)[1] = 0
	decoded, err = value.Decode()
	if err != nil || decoded.bytes[1] != 255 {
		t.Fatal("valuer output alias", err)
	}
	if _, err := NewSQLValue(customNull{}); !errors.Is(err, ErrScalarValue) {
		t.Fatal("nonnull custom adapter silently SQL NULL", err)
	}
	if _, err := NewSQLValue("no interfaces"); err == nil {
		t.Fatal("uncertified custom type accepted")
	}
	if err := value.Scan(nil); !errors.Is(err, ErrScalarValue) {
		t.Fatal("nonnull adapter SQL NULL", err)
	}
	if _, err := (SQLValue[customBytes]{}).Value(); !errors.Is(err, ErrScalarValue) {
		t.Fatal("zero adapter admitted")
	}
}
