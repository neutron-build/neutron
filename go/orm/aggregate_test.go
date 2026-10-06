package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestTypedAggregateCompilationAndOwnership(t *testing.T) {
	table, id, tenant, _, score, _, _ := setupMetadata(t)
	sum := SumInt64(score)
	threshold, _ := ParseDecimal("9007199254740993")
	sql, args, err := aggregateSQL(sum, &tenant.field, Query[testModel]{}.Where(id.Gt(0)).OrderBy(tenant.Asc()).Limit(2), []AggregatePredicate[testModel]{sum.Compare(Greater, Nullable[Decimal]{Valid: true, Value: threshold})})
	if err != nil || !strings.Contains(sql, `SELECT "tenant", SUM("score")`) || !strings.Contains(sql, `GROUP BY "tenant" HAVING (SUM("score") > $2)`) || !reflect.DeepEqual(args, []any{int64(0), threshold, 2}) {
		t.Fatal(sql, args, err)
	}
	if _, _, err := aggregateSQL(sum, nil, Query[testModel]{}.Limit(1), nil); err == nil {
		t.Fatal("hidden aggregate cardinality pagination accepted")
	}
	if _, _, err := aggregateSQL(sum, &tenant.field, Query[testModel]{}.OrderBy(id.Asc()), nil); err == nil {
		t.Fatal("ungrouped order column accepted")
	}
	other, _, _, _, otherScore, _, _ := setupMetadata(t)
	_ = other
	if _, _, err := aggregateSQL(sum, &tenant.field, Query[testModel]{}, []AggregatePredicate[testModel]{SumInt64(otherScore).Compare(Equal, Nullable[Decimal]{})}); err == nil {
		t.Fatal("foreign HAVING binding accepted")
	}
	if _, _, err := aggregateSQL(CountAll(table), nil, Query[testModel]{}, nil); err != nil {
		t.Fatal(err)
	}
}
