package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5/pgconn"
)

// UniqueConstraint is a point-in-time qualified nondeferred native UNIQUE/PK
// contract. Reconstruct it after DDL. It is not an inferred arbitrary index.
type UniqueConstraint[M any] struct {
	table   Table[M]
	name    string
	columns []BoundColumn[M]
}

func NewUniqueConstraint[M any](ctx context.Context, db Executor, table Table[M], name string, columns ...BoundColumn[M]) (UniqueConstraint[M], error) {
	if err := ready(ctx, db); err != nil {
		return UniqueConstraint[M]{}, err
	}
	if err := identifier(name); err != nil {
		return UniqueConstraint[M]{}, err
	}
	wanted, err := boundColumns(table, columns)
	if err != nil {
		return UniqueConstraint[M]{}, err
	}
	rows, err := db.Query(ctx, `SELECT c.condeferrable,array_agg(a.attname::text ORDER BY k.ordinality) FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class t ON t.oid=c.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY k(attnum,ordinality) JOIN pg_catalog.pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum WHERE n.nspname=$1 AND t.relname=$2 AND c.conname=$3 AND c.contype IN ('p','u') GROUP BY c.oid,c.condeferrable`, table.info.schema, table.info.name, name)
	if err != nil {
		return UniqueConstraint[M]{}, wrap("unique constraint", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return UniqueConstraint[M]{}, err
		}
		return UniqueConstraint[M]{}, fmt.Errorf("orm: named native unique constraint missing")
	}
	var deferred bool
	var actual []string
	if err := rows.Scan(&deferred, &actual); err != nil {
		return UniqueConstraint[M]{}, err
	}
	if deferred || !reflect.DeepEqual(actual, wanted) || rows.Next() {
		return UniqueConstraint[M]{}, fmt.Errorf("orm: native unique key must match exact nondeferred column contract")
	}
	if err := rows.Err(); err != nil {
		return UniqueConstraint[M]{}, err
	}
	return UniqueConstraint[M]{table, name, append([]BoundColumn[M]{}, columns...)}, nil
}
func connectCreatePlans[P, C any](relation NullableRelation[P, C], key UniqueConstraint[C], values, create []Assignment[C], parent P) (Predicate[C], []Assignment[C], error) {
	r := relation.relation
	if key.table.info == nil || key.table.info != r.child.info || len(values) != len(key.columns) {
		return Predicate[C]{}, nil, fmt.Errorf("orm: connect-or-create requires exact unique key values")
	}
	byField := map[int]Assignment[C]{}
	for _, value := range values {
		if value.info != r.child.info || value.mode != supplied || value.value == nil {
			return Predicate[C]{}, nil, fmt.Errorf("orm: explicit non-NULL unique key values required")
		}
		if _, duplicate := byField[value.field.index]; duplicate {
			return Predicate[C]{}, nil, fmt.Errorf("orm: duplicate unique key value")
		}
		byField[value.field.index] = value
	}
	parts := make([]Predicate[C], len(key.columns))
	for i, column := range key.columns {
		value, ok := byField[column.field.index]
		if !ok {
			return Predicate[C]{}, nil, fmt.Errorf("orm: missing unique key value")
		}
		parts[i] = Predicate[C]{&expression{kind: "=", info: value.info, field: value.field, value: value.value}}
	}
	selector := And(parts...)
	result := append([]Assignment[C]{}, create...)
	provided := map[int]bool{}
	for _, assignment := range create {
		if keyValue, ok := byField[assignment.field.index]; ok {
			if assignment.info != keyValue.info || assignment.mode != supplied || !reflect.DeepEqual(assignment.value, keyValue.value) {
				return Predicate[C]{}, nil, fmt.Errorf("orm: create changes declared unique identity")
			}
		}
		for _, part := range r.parts {
			if assignment.info == part.child && assignment.field.index == part.childField.index {
				return Predicate[C]{}, nil, fmt.Errorf("orm: relation owns create foreign keys")
			}
		}
		provided[assignment.field.index] = true
	}
	parentValue := reflect.ValueOf(parent)
	for _, column := range key.columns {
		assignment := byField[column.field.index]
		foreign := false
		for _, part := range r.parts {
			if column.field.index == part.childField.index {
				foreign = true
				if !reflect.DeepEqual(assignment.value, parentValue.Field(part.parentField.index).Interface()) {
					return Predicate[C]{}, nil, ErrAssociationOwner
				}
			}
		}
		if !foreign && !provided[column.field.index] {
			result = append(result, assignment)
		}
	}
	for _, part := range r.parts {
		result = append(result, Assignment[C]{info: part.child, field: part.childField, value: parentValue.Field(part.parentField.index).Interface(), mode: supplied})
	}
	if _, _, err := insertSQL(r.child, result); err != nil {
		return Predicate[C]{}, nil, err
	}
	return selector, result, nil
}

// ConnectOrCreateNullable locks the parent, looks up one explicit unique
// identity, and creates under a child savepoint if absent. It retries exactly
// one lookup after a competing writer wins the named native UNIQUE constraint;
// unrelated hook/constraint errors never trigger a retry. An existing child
// must be unowned or already belong to this parent. Hook create events from the
// failed attempt are discarded; externally visible effects belong AfterCommit.
func ConnectOrCreateNullable[P, C any](ctx context.Context, session *WriteSession, relation NullableRelation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, key UniqueConstraint[C], keyValues, create []Assignment[C], budget GraphBudget) (result C, err error) {
	selector, _, err := connectCreatePlans(relation, key, keyValues, create, parent)
	if err != nil {
		return result, wrap("connect-or-create", err)
	}
	if err := validateNullableMutation(relation, parentRepo, childRepo, selector, budget); err != nil {
		return result, wrap("connect-or-create", err)
	}
	if budget.MaxCoreStatements < 5 {
		return result, wrap("connect-or-create", ErrGraphBudget)
	}
	err = session.Savepoint(ctx, func(owned *WriteSession) error {
		locked, err := lockGraphParent(ctx, owned, relation.relation, parent)
		if err != nil {
			return err
		}
		selector, assignments, err := connectCreatePlans(relation, key, keyValues, create, locked)
		if err != nil {
			return err
		}
		existing, err := lockNullableChild(ctx, owned, relation, selector)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if errors.Is(err, ErrNotFound) {
			createErr := owned.Savepoint(ctx, func(attempt *WriteSession) error {
				var err error
				result, err = HookInsert(ctx, attempt, childRepo, assignments...)
				return err
			})
			if createErr == nil {
				return nil
			}
			var native *pgconn.PgError
			if !errors.As(createErr, &native) || native.Code != "23505" || native.ConstraintName != key.name || native.SchemaName != key.table.info.schema || native.TableName != key.table.info.name || errors.Is(createErr, ErrTransactionBroken) {
				return createErr
			}
			existing, err = lockNullableChild(ctx, owned, relation, selector)
			if err != nil {
				return err
			}
		}
		if err := nullableOwnership(relation, locked, existing, false); err != nil {
			return err
		}
		count, err := HookUpdate(ctx, owned, childRepo, selector, nullableAssignments(relation, locked, false)...)
		if err != nil {
			return err
		}
		if count != 1 {
			return ErrCardinality
		}
		// The return is the locked statement snapshot with derived FK fields,
		// matching HookInsert's snapshot semantics rather than after-hook SQL.
		result = existing
		v := reflect.ValueOf(&result).Elem()
		p := reflect.ValueOf(locked)
		for _, part := range relation.relation.parts {
			if part.childField.nullable {
				value := reflect.New(part.parentField.typ)
				value.Elem().Set(p.Field(part.parentField.index))
				v.Field(part.childField.index).Set(value)
			}
		}
		return nil
	})
	if err != nil {
		var zero C
		result = zero
	}
	return result, wrap("connect-or-create", err)
}
