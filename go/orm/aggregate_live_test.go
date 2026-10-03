package orm

import (
	"strings"
	"testing"
)

func TestPostgresExactTypedAggregatesAndHaving(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	table, tenant, id, _, version, _ := policyMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+table.info.sqlName()+" (tenant text NOT NULL,id bigint NOT NULL,value text NOT NULL,version bigint NOT NULL,deleted timestamptz,PRIMARY KEY(tenant,id)); INSERT INTO "+table.info.sqlName()+" VALUES ('a',1,'x',9223372036854775807,NULL),('a',2,'y',9223372036854775807,NULL),('b',3,'z',1,NULL)"); err != nil {
		t.Fatal(err)
	}
	sum := SumInt64(version)
	actual, err := AggregateOne(ctx, pool, sum, Query[policyModel]{}.Where(tenant.Eq("a")))
	var oracle string
	if err != nil || !actual.Valid {
		t.Fatal(actual, err)
	}
	if err := admin.QueryRow(ctx, "SELECT sum(version)::text FROM "+table.info.sqlName()+" WHERE tenant='a'").Scan(&oracle); err != nil || actual.Value.String() != oracle || oracle != "18446744073709551614" {
		t.Fatal("SUM(bigint) narrowed or rounded", actual.Value.String(), oracle, err)
	}
	avg, err := AggregateOne(ctx, pool, AvgInt64(version), Query[policyModel]{}.Where(tenant.Eq("a")))
	if err != nil || !avg.Valid {
		t.Fatal(avg, err)
	}
	if err := admin.QueryRow(ctx, "SELECT avg(version)::text FROM "+table.info.sqlName()+" WHERE tenant='a'").Scan(&oracle); err != nil || avg.Value.String() != oracle {
		t.Fatal(avg.Value.String(), oracle, err)
	}
	empty, err := AggregateOne(ctx, pool, sum, Query[policyModel]{}.Where(id.Lt(0)))
	if err != nil || empty.Valid {
		t.Fatal("empty SUM became numeric zero", empty, err)
	}
	count, err := AggregateOne(ctx, pool, CountAll(table), Query[policyModel]{}.Where(id.Lt(0)))
	if err != nil || count != 0 {
		t.Fatal(count, err)
	}
	distinct, err := AggregateOne(ctx, pool, CountDistinct(tenant), Query[policyModel]{})
	if err != nil || distinct != 2 {
		t.Fatal(distinct, err)
	}
	minimum, err := AggregateOne(ctx, pool, Min(id), Query[policyModel]{})
	if err != nil || !minimum.Valid || minimum.Value != 1 {
		t.Fatal(minimum, err)
	}
	threshold, _ := ParseDecimal("9007199254740993")
	groups, err := SelectGrouped(ctx, pool, tenant, sum, Query[policyModel]{}.OrderBy(tenant.Asc()), sum.Compare(Greater, Nullable[Decimal]{Valid: true, Value: threshold}))
	if err != nil || len(groups) != 1 || groups[0].First != "a" || !groups[0].Second.Valid || groups[0].Second.Value.String() != "18446744073709551614" {
		t.Fatal(groups, err)
	}
	var nativeTenant string
	var nativeSum string
	if err := admin.QueryRow(ctx, "SELECT tenant,sum(version)::text FROM "+table.info.sqlName()+" GROUP BY tenant HAVING sum(version)>9007199254740993 ORDER BY tenant").Scan(&nativeTenant, &nativeSum); err != nil || nativeTenant != groups[0].First || nativeSum != groups[0].Second.Value.String() {
		t.Fatal(nativeTenant, nativeSum, err)
	}
}
