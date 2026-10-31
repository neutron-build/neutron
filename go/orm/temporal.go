package orm

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// Date preserves a finite Gregorian calendar date in years 1 through 9999.
// It has no timezone. PostgreSQL infinity and dates outside that profile refuse.
type Date struct {
	year  int
	month time.Month
	day   int
	valid bool
}

func NewDate(year int, month time.Month, day int) (Date, error) {
	value := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if year < 1 || year > 9999 || value.Year() != year || value.Month() != month || value.Day() != day {
		return Date{}, ErrScalarValue
	}
	return Date{year, month, day, true}, nil
}
func (d Date) Parts() (int, time.Month, int) { return d.year, d.month, d.day }
func (d Date) DateValue() (pgtype.Date, error) {
	if !d.valid {
		return pgtype.Date{}, ErrScalarValue
	}
	return pgtype.Date{Time: time.Date(d.year, d.month, d.day, 0, 0, 0, 0, time.UTC), Valid: true}, nil
}
func (d *Date) ScanDate(value pgtype.Date) error {
	if !value.Valid || value.InfinityModifier != pgtype.Finite {
		return ErrScalarValue
	}
	next, err := NewDate(value.Time.Year(), value.Time.Month(), value.Time.Day())
	if err != nil {
		return err
	}
	*d = next
	return nil
}
func (d Date) ormScalarValid() bool { return d.valid }

// TimeOfDay preserves PostgreSQL time without zone as microseconds since
// midnight, including the distinct 24:00:00 endpoint. It does not normalize that
// endpoint into the following date. timetz is outside this codec profile.
type TimeOfDay struct {
	microseconds int64
	valid        bool
}

func NewTimeOfDay(microseconds int64) (TimeOfDay, error) {
	if microseconds < 0 || microseconds > 86400000000 {
		return TimeOfDay{}, ErrScalarValue
	}
	return TimeOfDay{microseconds, true}, nil
}
func (t TimeOfDay) Microseconds() int64 { return t.microseconds }
func (t TimeOfDay) TimeValue() (pgtype.Time, error) {
	if !t.valid {
		return pgtype.Time{}, ErrScalarValue
	}
	return pgtype.Time{Microseconds: t.microseconds, Valid: true}, nil
}
func (t *TimeOfDay) ScanTime(value pgtype.Time) error {
	if !value.Valid {
		return ErrScalarValue
	}
	next, err := NewTimeOfDay(value.Microseconds)
	if err != nil {
		return err
	}
	*t = next
	return nil
}
func (t TimeOfDay) ormScalarValid() bool { return t.valid }

// Interval retains native months, days and microseconds independently. It
// never converts calendar months to a time.Duration or fixed number of days.
type Interval struct {
	months, days int32
	microseconds int64
	valid        bool
}

func NewInterval(months, days int32, microseconds int64) Interval {
	return Interval{months, days, microseconds, true}
}
func (i Interval) Parts() (int32, int32, int64) { return i.months, i.days, i.microseconds }
func (i Interval) IntervalValue() (pgtype.Interval, error) {
	if !i.valid {
		return pgtype.Interval{}, ErrScalarValue
	}
	return pgtype.Interval{Months: i.months, Days: i.days, Microseconds: i.microseconds, Valid: true}, nil
}
func (i *Interval) ScanInterval(value pgtype.Interval) error {
	if !value.Valid {
		return ErrScalarValue
	}
	*i = NewInterval(value.Months, value.Days, value.Microseconds)
	return nil
}
func (i Interval) ormScalarValid() bool { return i.valid }

var _ pgtype.DateValuer = Date{}
var _ pgtype.DateScanner = (*Date)(nil)
var _ pgtype.TimeValuer = TimeOfDay{}
var _ pgtype.TimeScanner = (*TimeOfDay)(nil)
var _ pgtype.IntervalValuer = Interval{}
var _ pgtype.IntervalScanner = (*Interval)(nil)
