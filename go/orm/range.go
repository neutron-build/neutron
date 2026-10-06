package orm

import (
	"reflect"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type RangeBound = pgtype.BoundType

const (
	Inclusive RangeBound = pgtype.Inclusive
	Exclusive RangeBound = pgtype.Exclusive
	Unbounded RangeBound = pgtype.Unbounded
	Empty     RangeBound = pgtype.Empty
)

// Range retains native bounds and empty/unbounded distinctions. PostgreSQL may
// canonicalize discrete ranges on write; returned values describe that canonical
// database value. SQL NULL is a nil *Range[T], not an empty range.
type Range[T any] struct {
	lower, upper         T
	lowerType, upperType RangeBound
	valid                bool
}

func rangeElementType[T any]() bool {
	t := reflect.TypeOf((*T)(nil)).Elem()
	return t == reflect.TypeOf(int32(0)) || t == reflect.TypeOf(int64(0)) || t == reflect.TypeOf(Decimal{}) || t == reflect.TypeOf(time.Time{}) || t == reflect.TypeOf(Date{})
}
func validRangeBounds(lower, upper RangeBound) bool {
	if lower == Empty || upper == Empty {
		return lower == Empty && upper == Empty
	}
	allowed := func(bound RangeBound) bool { return bound == Inclusive || bound == Exclusive || bound == Unbounded }
	return allowed(lower) && allowed(upper)
}
func NewRange[T any](lower T, lowerType RangeBound, upper T, upperType RangeBound) (Range[T], error) {
	if !rangeElementType[T]() || !validRangeBounds(lowerType, upperType) {
		return Range[T]{}, ErrScalarValue
	}
	if lowerType == Inclusive || lowerType == Exclusive {
		if err := validateScalarValue(lower); err != nil {
			return Range[T]{}, err
		}
	}
	if upperType == Inclusive || upperType == Exclusive {
		if err := validateScalarValue(upper); err != nil {
			return Range[T]{}, err
		}
	}
	var zero T
	if lowerType == Unbounded || lowerType == Empty {
		lower = zero
	}
	if upperType == Unbounded || upperType == Empty {
		upper = zero
	}
	return Range[T]{lower, upper, lowerType, upperType, true}, nil
}
func (r Range[T]) Lower() (T, RangeBound)                           { return r.lower, r.lowerType }
func (r Range[T]) Upper() (T, RangeBound)                           { return r.upper, r.upperType }
func (r Range[T]) IsEmpty() bool                                    { return r.valid && r.lowerType == Empty }
func (r Range[T]) IsNull() bool                                     { return !r.valid }
func (r Range[T]) BoundTypes() (pgtype.BoundType, pgtype.BoundType) { return r.lowerType, r.upperType }
func (r Range[T]) Bounds() (any, any)                               { return r.lower, r.upper }
func (r Range[T]) ormScalarValid() bool                             { return r.valid }
func (r Range[T]) ormScalarType() bool                              { return rangeElementType[T]() }
func (r Range[T]) ormRangeType()                                    {}
func (r Range[T]) ormRangeElement() reflect.Type                    { return reflect.TypeOf((*T)(nil)).Elem() }
func (r *Range[T]) ormDestination() any                             { return &rangeDestination[T]{target: r} }

type rangeDestination[T any] struct {
	target       *Range[T]
	lower, upper T
}

func (d *rangeDestination[T]) ScanNull() error { return ErrScalarValue }
func (d *rangeDestination[T]) ScanBounds() (any, any) {
	return scanDestination(reflect.ValueOf(&d.lower).Elem()), scanDestination(reflect.ValueOf(&d.upper).Elem())
}
func (d *rangeDestination[T]) SetBoundTypes(lower, upper pgtype.BoundType) error {
	next, err := NewRange(d.lower, lower, d.upper, upper)
	if err != nil {
		return err
	}
	*d.target = next
	return nil
}

type nullableRangeDestination struct {
	target reflect.Value
	inner  pgtype.RangeScanner
	value  reflect.Value
}

func (d *nullableRangeDestination) ScanNull() error {
	d.target.Set(reflect.Zero(d.target.Type()))
	return nil
}
func (d *nullableRangeDestination) ScanBounds() (any, any) {
	d.value = reflect.New(d.target.Type().Elem())
	d.inner = d.value.Interface().(interface{ ormDestination() any }).ormDestination().(pgtype.RangeScanner)
	return d.inner.ScanBounds()
}
func (d *nullableRangeDestination) SetBoundTypes(lower, upper pgtype.BoundType) error {
	// Empty/unbounded ranges can bypass ScanBounds in a native driver plan.
	if d.inner == nil {
		d.ScanBounds()
	}
	if err := d.inner.SetBoundTypes(lower, upper); err != nil {
		return err
	}
	d.target.Set(d.value)
	return nil
}

var _ pgtype.RangeValuer = Range[int64]{}
var _ pgtype.RangeScanner = (*rangeDestination[int64])(nil)
