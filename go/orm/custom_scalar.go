package orm

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
)

// SQLValue adapts an explicit database/sql Scanner+Valuer pair. It freezes the
// Valuer's native driver value at construction/bind time; scanner reconstruction
// never exposes retained mutable application state. The adapter author owns
// codec semantics; native catalog admission requires an explicit CodecContract.
// SQL NULL is a nil *SQLValue[T], not a Valuer that returns nil.
type SQLValue[T any] struct {
	raw   driver.Value
	valid bool
}

func customScalarType[T any]() bool {
	var value T
	_, valuer := any(value).(driver.Valuer)
	_, scanner := any(&value).(sql.Scanner)
	return reflect.TypeOf((*T)(nil)).Elem().Kind() != reflect.Pointer && valuer && scanner
}
func freezeDriverValue(value driver.Value) (driver.Value, error) {
	if value == nil || !driver.IsValue(value) {
		return nil, ErrScalarValue
	}
	if err := validateScalarValue(value); err != nil {
		return nil, err
	}
	if bytes, ok := value.([]byte); ok {
		return append([]byte{}, bytes...), nil
	}
	return value, nil
}
func NewSQLValue[T any](value T) (SQLValue[T], error) {
	if !customScalarType[T]() {
		return SQLValue[T]{}, fmt.Errorf("orm: custom value requires value Valuer and pointer Scanner")
	}
	raw, err := any(value).(driver.Valuer).Value()
	if err != nil {
		return SQLValue[T]{}, err
	}
	raw, err = freezeDriverValue(raw)
	if err != nil {
		return SQLValue[T]{}, err
	}
	return SQLValue[T]{raw, true}, nil
}
func (s SQLValue[T]) Value() (driver.Value, error) {
	if !s.valid {
		return nil, ErrScalarValue
	}
	return freezeDriverValue(s.raw)
}
func (s SQLValue[T]) Decode() (T, error) {
	var value T
	if !s.valid || !customScalarType[T]() {
		return value, ErrScalarValue
	}
	raw, err := s.Value()
	if err != nil {
		return value, err
	}
	if err := any(&value).(sql.Scanner).Scan(raw); err != nil {
		return value, err
	}
	return value, nil
}
func (s *SQLValue[T]) Scan(source any) error {
	if source == nil || !customScalarType[T]() {
		return ErrScalarValue
	}
	var value T
	if err := any(&value).(sql.Scanner).Scan(source); err != nil {
		return err
	}
	next, err := NewSQLValue(value)
	if err != nil {
		return err
	}
	*s = next
	return nil
}
func (s SQLValue[T]) ormScalarValid() bool { return s.valid }
func (s SQLValue[T]) ormScalarType() bool  { return customScalarType[T]() }
func (s SQLValue[T]) ormCustomType()       {}

// CodecContract pins one mapped SQLValue field to exact native type identity.
// The contract is explicit caller qualification, not automatic certification of
// an arbitrary Scanner's semantic fidelity or behavior.
type CodecContract[M any] struct {
	field, typeSchema, typeName string
	oid                         uint32
	typ                         reflect.Type
}

func CodecFor[M, T any](goField, typeSchema, typeName string, oid uint32) (CodecContract[M], error) {
	if !customScalarType[T]() || goField == "" || oid == 0 {
		return CodecContract[M]{}, ErrScalarValue
	}
	if err := identifier(typeSchema); err != nil {
		return CodecContract[M]{}, err
	}
	if err := identifier(typeName); err != nil {
		return CodecContract[M]{}, err
	}
	return CodecContract[M]{goField, typeSchema, typeName, oid, reflect.TypeOf(SQLValue[T]{})}, nil
}

var _ sql.Scanner = (*SQLValue[Enum])(nil)
var _ driver.Valuer = SQLValue[Enum]{}
