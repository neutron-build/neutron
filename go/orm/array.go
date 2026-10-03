package orm

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5/pgtype"
)

// Bytea owns immutable binary data. Its zero value is invalid; NewBytea(nil)
// represents non-NULL empty bytea. SQL NULL uses a nil *Bytea mapped field.
type Bytea struct {
	data  string
	valid bool
}

func NewBytea(data []byte) Bytea { return Bytea{string(data), true} }
func (b Bytea) Bytes() []byte    { return []byte(b.data) }
func (b Bytea) BytesValue() ([]byte, error) {
	if !b.valid {
		return nil, ErrScalarValue
	}
	return b.Bytes(), nil
}
func (b *Bytea) ScanBytes(data []byte) error {
	if data == nil {
		return ErrScalarValue
	}
	*b = NewBytea(data)
	return nil
}

// Array preserves dimensions, lower bounds and nullable scalar elements.
// Values returned by Elements/Dimensions/Index are detached from its storage.
// Its zero value is invalid; a valid empty array has zero dimensions. SQL NULL
// is represented only by a nil *Array[T] on a nullable mapped field.
type Array[T any] struct {
	elements   []T
	dimensions []pgtype.ArrayDimension
	valid      bool
}

// MaxArrayElements bounds materialized decoding and constructor allocation.
const MaxArrayElements = 1_000_000

func arrayElementType[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }
func validArrayElement(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	// Nested PostgreSQL arrays use dimensions of a single flat array, never
	// nested Array values. Only the immutable scalar family is admitted here.
	if t == reflect.TypeOf(Bytea{}) || t == reflect.TypeOf(Date{}) || t == reflect.TypeOf(TimeOfDay{}) || t == reflect.TypeOf(Interval{}) {
		return true
	}
	return supportedBuiltinScalar(t)
}
func arrayCardinality(dimensions []pgtype.ArrayDimension) (int, error) {
	if len(dimensions) > 6 {
		return 0, fmt.Errorf("orm: array dimension count exceeds PostgreSQL maximum")
	}
	if len(dimensions) == 0 {
		return 0, nil
	}
	n := int64(1)
	for _, dimension := range dimensions {
		if dimension.Length <= 0 {
			return 0, fmt.Errorf("orm: nonempty array dimensions require positive lengths")
		}
		end := int64(dimension.LowerBound) + int64(dimension.Length) - 1
		if end > 2147483647 {
			return 0, fmt.Errorf("orm: array bound overflow")
		}
		n *= int64(dimension.Length)
		if n > MaxArrayElements {
			return 0, fmt.Errorf("orm: array element budget exceeded")
		}
	}
	return int(n), nil
}
func cloneElement[T any](element T) T {
	v := reflect.ValueOf(&element).Elem()
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		next := reflect.New(v.Type().Elem())
		next.Elem().Set(v.Elem())
		v.Set(next)
	}
	return element
}
func NewArray[T any](dimensions []pgtype.ArrayDimension, elements []T) (Array[T], error) {
	if !validArrayElement(arrayElementType[T]()) {
		return Array[T]{}, fmt.Errorf("orm: array element codec not qualified")
	}
	count, err := arrayCardinality(dimensions)
	if err != nil {
		return Array[T]{}, err
	}
	if count != len(elements) {
		return Array[T]{}, fmt.Errorf("orm: array dimensions/cardinality mismatch")
	}
	value := Array[T]{elements: make([]T, count), dimensions: append([]pgtype.ArrayDimension{}, dimensions...), valid: true}
	for i, element := range elements {
		if err := validateScalarValue(snapshot(element)); err != nil {
			return Array[T]{}, err
		}
		value.elements[i] = cloneElement(element)
	}
	return value, nil
}
func (a Array[T]) Dimensions() []pgtype.ArrayDimension {
	if !a.valid {
		return nil
	}
	return append([]pgtype.ArrayDimension{}, a.dimensions...)
}
func (a Array[T]) Elements() []T {
	if !a.valid {
		return nil
	}
	result := make([]T, len(a.elements))
	for i, value := range a.elements {
		result[i] = cloneElement(value)
	}
	return result
}
func (a Array[T]) Index(i int) any               { return cloneElement(a.elements[i]) }
func (a Array[T]) IndexType() any                { var zero T; return zero }
func (a Array[T]) ormScalarValid() bool          { return a.valid }
func (a Array[T]) ormScalarType() bool           { return validArrayElement(arrayElementType[T]()) }
func (a Array[T]) ormArrayType()                 {}
func (a Array[T]) ormArrayElement() reflect.Type { return arrayElementType[T]() }
func (a *Array[T]) ormDestination() any          { return &arrayDestination[T]{a} }

type arrayDestination[T any] struct{ target *Array[T] }

// Nullable array decoding must bypass pgx's pointer unwrapping so the private
// dimension-preserving destination remains active for non-NULL values.
type nullableArrayDestination struct {
	target reflect.Value
	inner  pgtype.ArraySetter
}

func (d *nullableArrayDestination) SetDimensions(dimensions []pgtype.ArrayDimension) error {
	if dimensions == nil {
		d.target.Set(reflect.Zero(d.target.Type()))
		return nil
	}
	value := reflect.New(d.target.Type().Elem())
	d.inner = value.Interface().(interface{ ormDestination() any }).ormDestination().(pgtype.ArraySetter)
	if err := d.inner.SetDimensions(dimensions); err != nil {
		return err
	}
	d.target.Set(value)
	return nil
}
func (d *nullableArrayDestination) ScanIndex(i int) any { return d.inner.ScanIndex(i) }
func (d *nullableArrayDestination) ScanIndexType() any {
	value := reflect.New(d.target.Type().Elem())
	return value.Interface().(interface{ ormDestination() any }).ormDestination().(pgtype.ArraySetter).ScanIndexType()
}

func (d *arrayDestination[T]) SetDimensions(dimensions []pgtype.ArrayDimension) error {
	if dimensions == nil {
		return ErrScalarValue
	}
	count, err := arrayCardinality(dimensions)
	if err != nil {
		return err
	}
	*d.target = Array[T]{make([]T, count), append([]pgtype.ArrayDimension{}, dimensions...), true}
	return nil
}
func (d *arrayDestination[T]) ScanIndex(i int) any {
	return scanDestination(reflect.ValueOf(&d.target.elements[i]).Elem())
}
func (d *arrayDestination[T]) ScanIndexType() any {
	var element T
	return scanDestination(reflect.ValueOf(&element).Elem())
}

var _ pgtype.BytesValuer = Bytea{}
var _ pgtype.BytesScanner = (*Bytea)(nil)
var _ pgtype.ArrayGetter = Array[int64]{}
var _ pgtype.ArraySetter = (*arrayDestination[int64])(nil)
