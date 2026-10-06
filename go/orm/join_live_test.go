package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type joinedChildModel struct {
	Tenant   string  `db:"tenant"`
	ParentID int64   `db:"parent_id"`
	ID       int64   `db:"id"`
	Active   bool    `db:"active"`
	Score    int64   `db:"score"`
	Note     *string `db:"note,nullable"`
	Document *JSON   `db:"document,nullable"`
	Amount   Decimal `db:"amount"`
}

func TestPostgresTypedInnerLeftJoinProjections(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	parentSQL := strings.TrimSuffix(records, ".records")
	parentSchema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(parentSQL, `"`), `"`), `""`, `"`)
	childSchema := parentSchema + "_right"
	childSQL := quote(childSchema)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+childSQL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(clean, "DROP SCHEMA "+childSQL+" CASCADE"); err != nil {
			t.Error("owned joined schema cleanup failed")
		}
	})
	if _, err := admin.Exec(ctx, "CREATE TABLE "+parentSQL+`.join_rows (tenant text NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id));CREATE TABLE `+childSQL+`.join_rows (tenant text NOT NULL,parent_id bigint NOT NULL,id bigint NOT NULL,active boolean NOT NULL,score bigint NOT NULL,note text,document jsonb,amount numeric NOT NULL,PRIMARY KEY(tenant,id));INSERT INTO `+parentSQL+`.join_rows VALUES ('a',1,'two children'),('b',1,'other tenant'),('a',2,'missing'),('a',3,'matched NULL'),('a',4,'actual zeros');INSERT INTO `+childSQL+`.join_rows VALUES ('a',1,10,false,0,NULL,'{"n":9007199254740993}',1.2500),('a',1,11,true,1,'note','null',2.5),('b',1,20,true,2,'other',NULL,3.5),('a',3,30,false,0,NULL,NULL,'NaN'),('a',4,0,false,0,NULL,'null',0),('a',9,99,true,99,'orphan','null',99)`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE TEMP TABLE join_rows (tenant text,id bigint);INSERT INTO join_rows VALUES ('a',999);SET search_path TO pg_temp`); err != nil {
		t.Fatal(err)
	}
	parent, err := NewTable[assocParent](parentSchema, "join_rows")
	if err != nil {
		t.Fatal(err)
	}
	child, err := NewTable[joinedChildModel](childSchema, "join_rows")
	if err != nil {
		t.Fatal(err)
	}
	pt, err := NewColumn[assocParent, string](parent, "Tenant")
	if err != nil {
		t.Fatal(err)
	}
	pi, err := NewColumn[assocParent, int64](parent, "ID")
	if err != nil {
		t.Fatal(err)
	}
	ct, err := NewColumn[joinedChildModel, string](child, "Tenant")
	if err != nil {
		t.Fatal(err)
	}
	cp, err := NewColumn[joinedChildModel, int64](child, "ParentID")
	if err != nil {
		t.Fatal(err)
	}
	ci, err := NewColumn[joinedChildModel, int64](child, "ID")
	if err != nil {
		t.Fatal(err)
	}
	active, err := NewColumn[joinedChildModel, bool](child, "Active")
	if err != nil {
		t.Fatal(err)
	}
	score, err := NewColumn[joinedChildModel, int64](child, "Score")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := NewColumn[joinedChildModel, *JSON](child, "Document")
	if err != nil {
		t.Fatal(err)
	}
	amount, err := NewColumn[joinedChildModel, Decimal](child, "Amount")
	if err != nil {
		t.Fatal(err)
	}
	relation, err := NewRelation(parent, child, Join(pt, ct), Join(pi, cp))
	if err != nil {
		t.Fatal(err)
	}
	inner, err := NewInnerJoin(relation)
	if err != nil {
		t.Fatal(err)
	}
	ip, err := JoinParentField(inner, pi)
	if err != nil {
		t.Fatal(err)
	}
	ic, err := InnerChildField(inner, ci)
	if err != nil {
		t.Fatal(err)
	}
	iq := inner.Query().OrderParent(pt.Asc(), pi.Asc()).OrderChild(ci.Asc())
	pairs, err := SelectJoinedPair(ctx, admin, ip, ic, iq)
	if err != nil {
		t.Fatal("inner native join", err)
	}
	oracleSQL := "SELECT p.id,c.id FROM " + parentSQL + ".join_rows p INNER JOIN " + childSQL + ".join_rows c ON p.tenant=c.tenant AND p.id=c.parent_id ORDER BY p.tenant,p.id,c.id"
	native, err := admin.Query(ctx, oracleSQL)
	if err != nil {
		t.Fatal(err)
	}
	expected := []Pair[int64, int64]{}
	for native.Next() {
		var row Pair[int64, int64]
		if err := native.Scan(&row.First, &row.Second); err != nil {
			native.Close()
			t.Fatal(err)
		}
		expected = append(expected, row)
	}
	native.Close()
	if err := native.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pairs, expected) || len(pairs) != 5 {
		t.Fatal("inner native row oracle", pairs, expected)
	}
	left, err := NewLeftJoin(relation)
	if err != nil {
		t.Fatal(err)
	}
	lp, err := JoinParentField(left, pi)
	if err != nil {
		t.Fatal(err)
	}
	lc, err := LeftChildField(left, ci)
	if err != nil {
		t.Fatal(err)
	}
	lq := left.Query().OrderParent(pt.Asc(), pi.Asc()).OrderChild(ci.Asc())
	outer, err := SelectJoinedPair(ctx, admin, lp, lc, lq)
	if err != nil {
		t.Fatal("left native join", err)
	}
	native, err = admin.Query(ctx, strings.Replace(oracleSQL, "INNER JOIN", "LEFT JOIN", 1))
	if err != nil {
		t.Fatal(err)
	}
	expectedOuter := []Pair[int64, Nullable[int64]]{}
	for native.Next() {
		var parentID int64
		var childID *int64
		if err := native.Scan(&parentID, &childID); err != nil {
			native.Close()
			t.Fatal(err)
		}
		row := Pair[int64, Nullable[int64]]{First: parentID}
		if childID != nil {
			row.Second = Nullable[int64]{Valid: true, Value: *childID}
		}
		expectedOuter = append(expectedOuter, row)
	}
	native.Close()
	if err := native.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(outer, expectedOuter) || len(outer) != 6 {
		t.Fatal("left native row/NULL oracle", outer, expectedOuter)
	}
	zero, err := SelectJoinedOne(ctx, admin, lc, left.Query().WhereParent(And(pt.Eq("a"), pi.Eq(4))))
	if err != nil || !zero.Valid || zero.Value != 0 {
		t.Fatal("actual zero converted to missing", zero, err)
	}
	missing, err := SelectJoinedPairOne(ctx, admin, lp, lc, left.Query().WhereParent(And(pt.Eq("a"), pi.Eq(2))))
	if err != nil || missing.First != 2 || missing.Second.Valid {
		t.Fatal("unmatched child converted to zero/cardinality missing", missing, err)
	}
	flag, err := LeftChildField(left, active)
	if err != nil {
		t.Fatal(err)
	}
	boolean, err := SelectJoinedOne(ctx, admin, flag, left.Query().WhereParent(And(pt.Eq("a"), pi.Eq(4))))
	if err != nil || !boolean.Valid || boolean.Value {
		t.Fatal("actual false lost", err)
	}
	documents, err := LeftChildField(left, doc)
	if err != nil {
		t.Fatal(err)
	}
	jsonNull, err := SelectJoinedOne(ctx, admin, documents, left.Query().WhereParent(And(pt.Eq("a"), pi.Eq(4))))
	if err != nil || !jsonNull.Valid || jsonNull.Value == nil || !jsonNull.Value.IsNull() {
		t.Fatal("outer JSONnull conflated with SQLNULL", err)
	}
	sqlNull, err := SelectJoinedOne(ctx, admin, documents, left.Query().WhereParent(And(pt.Eq("a"), pi.Eq(3))))
	if err != nil || sqlNull.Valid || sqlNull.Value != nil {
		t.Fatal("matched SQLNULL falsely claimed row presence", err)
	}
	// WHERE-child filtering intentionally removes unmatched parents. Parameter
	// order and pagination are checked against independently authored native SQL.
	filteredQuery := lq.WhereParent(pt.Eq("a")).WhereChild(score.Gte(0)).Limit(2).Offset(1)
	filtered, err := SelectJoinedPair(ctx, admin, lp, lc, filteredQuery)
	if err != nil {
		t.Fatal(err)
	}
	native, err = admin.Query(ctx, "SELECT p.id,c.id FROM "+parentSQL+".join_rows p LEFT JOIN "+childSQL+".join_rows c ON p.tenant=c.tenant AND p.id=c.parent_id WHERE p.tenant=$1 AND c.score >= $2 ORDER BY p.tenant,p.id,c.id LIMIT $3 OFFSET $4", "a", int64(0), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	filteredExpected := []Pair[int64, Nullable[int64]]{}
	for native.Next() {
		var p int64
		var c *int64
		if err := native.Scan(&p, &c); err != nil {
			native.Close()
			t.Fatal(err)
		}
		if c == nil {
			native.Close()
			t.Fatal("native WHERE unexpectedly retained NULL")
		}
		filteredExpected = append(filteredExpected, Pair[int64, Nullable[int64]]{p, Nullable[int64]{true, *c}})
	}
	native.Close()
	if err := native.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(filtered, filteredExpected) {
		t.Fatal("native filtered/paginated join oracle", filtered, filteredExpected)
	}
	if _, err := SelectJoinedOne(ctx, admin, lc, left.Query().WhereParent(pi.Eq(1))); !errors.Is(err, ErrCardinality) {
		t.Fatal("joined cardinality hidden", err)
	}
	if _, err := SelectJoinedOne(ctx, admin, lc, left.Query().WhereParent(pt.Eq("absent"))); !errors.Is(err, ErrNotFound) {
		t.Fatal("joined not-found hidden", err)
	}
	// The same executor Scope lease is used; successful projection closes rows.
	if err := WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		if _, err := SelectJoinedPair(ctx, scope, lp, lc, lq); err != nil {
			return err
		}
		_, err := scope.Exec(ctx, "SELECT 1")
		return err
	}); err != nil {
		t.Fatal("joined Scope rows lease leaked", err)
	}
	poisonField, err := LeftChildField(left, amount)
	if err != nil {
		t.Fatal(err)
	}
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		if _, err := scope.Exec(ctx, "INSERT INTO "+records+" VALUES (1,'joined decode must roll back')"); err != nil {
			return err
		}
		if _, err := SelectJoinedOne(ctx, scope, poisonField, left.Query().WhereParent(And(pt.Eq("a"), pi.Eq(3)))); !errors.Is(err, ErrScalarValue) {
			return fmt.Errorf("expected joined decode failure: %w", err)
		}
		return nil
	})
	if !errors.Is(err, ErrScopeDecode) {
		t.Fatal("swallowed joined decode failure committed", err)
	}
	ids, err := nativeIDs(ctx, admin, records)
	if err != nil || len(ids) != 0 {
		t.Fatal("native joined decode rollback oracle", ids, err)
	}
	requirePoolReuse(t, ctx, pool)
	absent, err := NewTable[joinedChildModel](childSchema, "absent_join_rows")
	if err != nil {
		t.Fatal(err)
	}
	absentTenant, err := NewColumn[joinedChildModel, string](absent, "Tenant")
	if err != nil {
		t.Fatal(err)
	}
	absentParent, err := NewColumn[joinedChildModel, int64](absent, "ParentID")
	if err != nil {
		t.Fatal(err)
	}
	absentID, err := NewColumn[joinedChildModel, int64](absent, "ID")
	if err != nil {
		t.Fatal(err)
	}
	absentRelation, err := NewRelation(parent, absent, Join(pt, absentTenant), Join(pi, absentParent))
	if err != nil {
		t.Fatal(err)
	}
	absentScope, err := NewInnerJoin(absentRelation)
	if err != nil {
		t.Fatal(err)
	}
	absentField, err := InnerChildField(absentScope, absentID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = SelectJoinedColumn(ctx, admin, absentField, absentScope.Query())
	var state *pgconn.PgError
	if !errors.As(err, &state) || state.Code != "42P01" {
		t.Fatal("joined SQLSTATE not preserved", err)
	}
}
