package orm

import (
	"errors"
	"strings"
	"testing"
	"time"
)

type rangeTemporalModel struct {
	ID       int64          `db:"id"`
	Span     Range[int64]   `db:"span"`
	Exact    Range[Decimal] `db:"exact"`
	Calendar Date           `db:"calendar"`
	Clock    TimeOfDay      `db:"clock"`
	Duration Interval       `db:"duration"`
	Optional *Range[int64]  `db:"optional,nullable"`
}

func TestPostgresRangesAndFiniteTemporalComponents(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	name := schemaSQL + ".range_temporal"
	if _, err := admin.Exec(ctx, "CREATE TABLE "+name+` (id bigint PRIMARY KEY,span int8range NOT NULL,exact numrange NOT NULL,calendar date NOT NULL,clock time NOT NULL,duration interval NOT NULL,optional int8range); INSERT INTO `+name+` VALUES (1,'[0,9223372036854775807)','[9007199254740993.123456789,9007199254740994.987654321)','2024-02-29','24:00:00','-14 months 3 days -00:02:03.456789',NULL),(2,'empty','(,)','2000-01-01','00:00:00.000001','0 seconds','empty')`); err != nil {
		t.Fatal(err)
	}
	table, err := NewPostgresTable[rangeTemporalModel](ctx, admin, schema, "range_temporal")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewColumn[rangeTemporalModel, int64](table, "ID")
	if err != nil {
		t.Fatal(err)
	}
	span, err := NewColumn[rangeTemporalModel, Range[int64]](table, "Span")
	if err != nil {
		t.Fatal(err)
	}
	exact, err := NewColumn[rangeTemporalModel, Range[Decimal]](table, "Exact")
	if err != nil {
		t.Fatal(err)
	}
	calendar, err := NewColumn[rangeTemporalModel, Date](table, "Calendar")
	if err != nil {
		t.Fatal(err)
	}
	clock, err := NewColumn[rangeTemporalModel, TimeOfDay](table, "Clock")
	if err != nil {
		t.Fatal(err)
	}
	duration, err := NewColumn[rangeTemporalModel, Interval](table, "Duration")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := Select(ctx, admin, table, Query[rangeTemporalModel]{}.OrderBy(id.Asc()))
	if err != nil || len(loaded) != 2 {
		t.Fatal("native range/temporal read", err)
	}
	lo, lt := loaded[0].Span.Lower()
	hi, ut := loaded[0].Span.Upper()
	if lo != 0 || hi != 9223372036854775807 || lt != Inclusive || ut != Exclusive || loaded[0].Clock.Microseconds() != 86400000000 || loaded[0].Optional != nil || !loaded[1].Span.IsEmpty() || loaded[1].Optional == nil || !loaded[1].Optional.IsEmpty() {
		t.Fatal("range/24h/NULL/empty loss")
	}
	for _, row := range loaded {
		var nativeLower, nativeUpper *string
		var empty, lowerInfinite, upperInfinite bool
		var nativeDay string
		var micros, months, days, intervalMicros int64
		if err := admin.QueryRow(ctx, "SELECT lower(exact)::text,upper(exact)::text,isempty(exact),lower_inf(exact),upper_inf(exact),calendar::text,(extract(epoch FROM clock)*1000000)::bigint,(extract(year FROM duration)*12+extract(month FROM duration))::bigint,extract(day FROM duration)::bigint,(extract(hour FROM duration)*3600000000+extract(minute FROM duration)*60000000+extract(second FROM duration)*1000000)::bigint FROM "+name+" WHERE id=$1", row.ID).Scan(&nativeLower, &nativeUpper, &empty, &lowerInfinite, &upperInfinite, &nativeDay, &micros, &months, &days, &intervalMicros); err != nil {
			t.Fatal(err)
		}
		lower, lowerType := row.Exact.Lower()
		upper, upperType := row.Exact.Upper()
		if (nativeLower != nil && lower.String() != *nativeLower) || (nativeUpper != nil && upper.String() != *nativeUpper) || row.Exact.IsEmpty() != empty || (lowerType == Unbounded) != lowerInfinite || (upperType == Unbounded) != upperInfinite || row.Clock.Microseconds() != micros {
			t.Fatal("native independent range oracle mismatch")
		}
		y, m, d := row.Calendar.Parts()
		nativeDate, err := time.Parse("2006-01-02", nativeDay)
		if err != nil || y != nativeDate.Year() || m != nativeDate.Month() || d != nativeDate.Day() {
			t.Fatal("native calendar oracle", err)
		}
		gotMonths, gotDays, gotMicros := row.Duration.Parts()
		if int64(gotMonths) != months || int64(gotDays) != days || gotMicros != intervalMicros {
			t.Fatal("native interval component oracle")
		}
	}
	lower, err := ParseDecimal("9007199254740993.000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	upper, err := ParseDecimal("9007199254740994.999999999999999999")
	if err != nil {
		t.Fatal(err)
	}
	writeExact, err := NewRange(lower, Inclusive, upper, Exclusive)
	if err != nil {
		t.Fatal(err)
	}
	writeSpan, err := NewRange(int64(0), Unbounded, int64(0), Unbounded)
	if err != nil {
		t.Fatal(err)
	}
	date, err := NewDate(2026, time.November, 1)
	if err != nil {
		t.Fatal(err)
	}
	tod, err := NewTimeOfDay(123456)
	if err != nil {
		t.Fatal(err)
	}
	result, err := InsertOne(ctx, admin, table, Set(id, Some(int64(3))), Set(span, Some(writeSpan)), Set(exact, Some(writeExact)), Set(calendar, Some(date)), Set(clock, Some(tod)), Set(duration, Some(NewInterval(1, -2, 123456))))
	if err != nil {
		t.Fatal("native range/temporal write", err)
	}
	if result.Exact != writeExact || result.Span != writeSpan || result.Clock != tod || result.Calendar != date {
		t.Fatal("range temporal RETURNING loss")
	}
	var nativeExact, nativeSpan string
	if err := admin.QueryRow(ctx, "SELECT exact::text,span::text FROM "+name+" WHERE id=3").Scan(&nativeExact, &nativeSpan); err != nil {
		t.Fatal(err)
	}
	if nativeExact != "[9007199254740993.000000000000000001,9007199254740994.999999999999999999)" || nativeSpan != "(,)" {
		t.Fatal("independent native range write oracle", nativeExact, nativeSpan)
	}
	if _, err := admin.Exec(ctx, "UPDATE "+name+" SET calendar='infinity' WHERE id=3"); err != nil {
		t.Fatal(err)
	}
	if result, err := Select(ctx, admin, table, Query[rangeTemporalModel]{}.Where(id.Eq(3))); !errors.Is(err, ErrScalarValue) || result != nil {
		t.Fatal("unsupported date infinity exposed partial model", err)
	}
}
