package orm

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestRangeNativeEmptyUnboundedAndNullable(t *testing.T) {
	registry := pgtype.NewMap()
	for _, bounds := range [][2]RangeBound{{Inclusive, Exclusive}, {Empty, Empty}, {Unbounded, Unbounded}, {Unbounded, Exclusive}} {
		value, err := NewRange(int64(0), bounds[0], int64(9223372036854775807), bounds[1])
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := registry.Encode(pgtype.Int8rangeOID, pgtype.BinaryFormatCode, value, nil)
		if err != nil {
			t.Fatal(err)
		}
		var decoded Range[int64]
		if err := registry.Scan(pgtype.Int8rangeOID, pgtype.BinaryFormatCode, encoded, scanDestination(reflect.ValueOf(&decoded).Elem())); err != nil || decoded != value {
			t.Fatal("native range loss", err)
		}
		var optional *Range[int64]
		if err := registry.Scan(pgtype.Int8rangeOID, pgtype.BinaryFormatCode, encoded, scanDestination(reflect.ValueOf(&optional).Elem())); err != nil || optional == nil || *optional != value {
			t.Fatal("nullable range", err)
		}
		if err := registry.Scan(pgtype.Int8rangeOID, pgtype.BinaryFormatCode, nil, scanDestination(reflect.ValueOf(&optional).Elem())); err != nil || optional != nil {
			t.Fatal("SQL NULL range", err)
		}
	}
	if _, err := NewRange(int64(0), Empty, int64(0), Unbounded); !errors.Is(err, ErrScalarValue) {
		t.Fatal("partial empty bound accepted")
	}
	if _, err := NewRange("a", Inclusive, "b", Exclusive); !errors.Is(err, ErrScalarValue) {
		t.Fatal("uncertified range subtype accepted")
	}
	if _, err := NewRange(Decimal{}, Inclusive, Decimal{}, Exclusive); !errors.Is(err, ErrScalarValue) {
		t.Fatal("invalid finite bound accepted")
	}
	if _, err := NewRange(time.Unix(0, 1), Inclusive, time.Unix(0, 1000), Exclusive); !errors.Is(err, ErrScalarValue) {
		t.Fatal("submicrosecond range bound silently truncated")
	}
	if !errors.Is(validateScalarValue(Range[int64]{}), ErrScalarValue) {
		t.Fatal("zero range admitted")
	}
}

func TestFiniteTemporalNativeComponentFidelity(t *testing.T) {
	date, err := NewDate(2024, time.February, 29)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewDate(2023, time.February, 29); !errors.Is(err, ErrScalarValue) {
		t.Fatal("invalid Gregorian date normalized")
	}
	for _, microseconds := range []int64{0, 123456789, 86400000000} {
		value, err := NewTimeOfDay(microseconds)
		if err != nil {
			t.Fatal(err)
		}
		native, err := value.TimeValue()
		if err != nil || native.Microseconds != microseconds {
			t.Fatal("time precision/24h loss", err)
		}
	}
	if _, err := NewTimeOfDay(86400000001); !errors.Is(err, ErrScalarValue) {
		t.Fatal("time out of range")
	}
	registry := pgtype.NewMap()
	clock, err := NewTimeOfDay(86400000000)
	if err != nil {
		t.Fatal(err)
	}
	interval := NewInterval(-14, 3, -123456789)
	for _, sample := range []struct {
		oid                uint32
		value, destination any
	}{{pgtype.DateOID, date, new(Date)}, {pgtype.TimeOID, clock, new(TimeOfDay)}, {pgtype.IntervalOID, interval, new(Interval)}} {
		encoded, err := registry.Encode(sample.oid, pgtype.BinaryFormatCode, sample.value, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Scan(sample.oid, pgtype.BinaryFormatCode, encoded, sample.destination); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(sample.value, reflect.ValueOf(sample.destination).Elem().Interface()) {
			t.Fatal("temporal component loss")
		}
	}
	var decoded Date
	if err := decoded.ScanDate(pgtype.Date{Valid: true, InfinityModifier: pgtype.Infinity}); !errors.Is(err, ErrScalarValue) {
		t.Fatal("date infinity accepted")
	}
	if !errors.Is(validateScalarValue(Interval{}), ErrScalarValue) || !errors.Is(validateScalarValue(TimeOfDay{}), ErrScalarValue) || !errors.Is(validateScalarValue(Date{}), ErrScalarValue) {
		t.Fatal("zero temporal codecs admitted")
	}
}
