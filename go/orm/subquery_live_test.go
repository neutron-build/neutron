package orm

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPostgresTypedScalarAndCorrelatedSubqueries(t *testing.T) {
	ctx, pool, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	relation, childID := associationMetadata(t, schema)
	parentID, err := NewColumn[assocParent, int64](relation.parent, "ID")
	if err != nil {
		t.Fatal(err)
	}
	parentTenant, err := NewColumn[assocParent, string](relation.parent, "Tenant")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE TABLE "+schemaSQL+`.parents (tenant text NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); CREATE TABLE `+schemaSQL+`.children (tenant text NOT NULL,parent_id bigint NOT NULL,id bigint NOT NULL,name text NOT NULL,PRIMARY KEY(tenant,id)); INSERT INTO `+schemaSQL+`.parents VALUES ('a',1,'A1'),('a',2,'A2'),('a',3,'A3'),('b',1,'B1'); INSERT INTO `+schemaSQL+`.children VALUES ('a',1,10,'first'),('a',1,11,'second'),('a',2,30,'third'),('b',1,20,'foreign')`); err != nil {
		t.Fatal(err)
	}
	q := Query[assocParent]{}.Where(And(parentTenant.Eq("a"), ExistsRelated(relation, Query[assocChild]{}.Where(childID.Gt(10))))).OrderBy(parentID.Asc())
	actual, err := SelectColumn(ctx, admin, parentID, q)
	if err != nil {
		t.Fatal(err)
	}
	native, err := admin.Query(ctx, "SELECT p.id FROM "+schemaSQL+".parents AS p WHERE p.tenant=$1 AND EXISTS (SELECT 1 FROM "+schemaSQL+".children AS c WHERE c.id>$2 AND c.tenant=p.tenant AND c.parent_id=p.id) ORDER BY p.id", "a", int64(10))
	if err != nil {
		t.Fatal(err)
	}
	expected := []int64{}
	for native.Next() {
		var id int64
		if err := native.Scan(&id); err != nil {
			native.Close()
			t.Fatal(err)
		}
		expected = append(expected, id)
	}
	native.Close()
	if err := native.Err(); err != nil || !reflect.DeepEqual(actual, expected) {
		t.Fatal("correlated native EXISTS oracle", actual, expected, err)
	}
	pairs, err := SelectRelatedScalar(ctx, admin, relation, parentID, childID, Query[assocParent]{}.Where(parentTenant.Eq("a")).OrderBy(parentID.Asc()), Query[assocChild]{}.OrderBy(childID.Asc()).Limit(1))
	if err != nil || len(pairs) != 3 {
		t.Fatal("native scalar projection", err)
	}
	if pairs[0].Second != (Nullable[int64]{Value: 10, Valid: true}) || pairs[1].Second != (Nullable[int64]{Value: 30, Valid: true}) || pairs[2].Second.Valid {
		t.Fatal("correlated scalar paging/NULL loss", pairs)
	}
	// The default unpaged scalar preserves native multiple-row failure. An owned
	// Scope rolls back earlier work even if the SQL error is swallowed.
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		if _, err := scope.Exec(ctx, "UPDATE "+schemaSQL+".parents SET name='must rollback' WHERE tenant='a' AND id=3"); err != nil {
			return err
		}
		partial, err := SelectRelatedScalar(ctx, scope, relation, parentID, childID, Query[assocParent]{}.Where(parentTenant.Eq("a")).OrderBy(parentID.Desc()), Query[assocChild]{})
		var nativeError *Error
		if partial != nil || !errors.As(err, &nativeError) || nativeError.SQLState() != "21000" {
			return errors.New("native scalar cardinality failure missing")
		}
		return nil
	})
	if err == nil {
		t.Fatal("scalar cardinality error committed prior writes")
	}
	var name string
	if err := admin.QueryRow(ctx, "SELECT name FROM "+schemaSQL+".parents WHERE tenant='a' AND id=3").Scan(&name); err != nil || name != "A3" {
		t.Fatal("scalar cardinality rollback", err)
	}
	self, selfID, selfName := selfJoinMetadata(t, schema)
	if _, err := admin.Exec(ctx, "CREATE TABLE "+self.parent.info.sqlName()+" (id bigint PRIMARY KEY,manager bigint NOT NULL,name text NOT NULL); INSERT INTO "+self.parent.info.sqlName()+" VALUES (1,0,'root'),(2,1,'child'),(3,2,'grand'),(4,0,'alone')"); err != nil {
		t.Fatal(err)
	}
	nested := ExistsRelated(self, Query[selfModel]{}.Where(ExistsRelated(self, Query[selfModel]{}.Where(selfName.Eq("grand")))))
	actual, err = SelectColumn(ctx, admin, selfID, Query[selfModel]{}.Where(nested).OrderBy(selfID.Asc()))
	if err != nil || !reflect.DeepEqual(actual, []int64{1}) {
		t.Fatal("nested self correlation shadowing", actual, err)
	}
	requirePoolReuse(t, ctx, pool)
}
