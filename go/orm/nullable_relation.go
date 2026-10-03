package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// NullableJoinPart explicitly separates required shared identity (e.g. tenant)
// from nullable foreign-key components. Parent identity remains nonnullable.
type NullableJoinPart[P, C any] struct{ part JoinPart[P, C] }

func JoinNullable[P, C, T any](parent Column[P, T], child Column[C, *T]) NullableJoinPart[P, C] {
	return NullableJoinPart[P, C]{JoinPart[P, C]{parent.info, child.info, parent.field, child.field}}
}
func JoinRequired[P, C, T any](parent Column[P, T], child Column[C, T]) NullableJoinPart[P, C] {
	return NullableJoinPart[P, C]{Join(parent, child)}
}

type NullableRelation[P, C any] struct{ relation Relation[P, C] }

func NewNullableRelation[P, C any](parent Table[P], child Table[C], parts ...NullableJoinPart[P, C]) (NullableRelation[P, C], error) {
	if parent.info == nil || child.info == nil || len(parts) == 0 {
		return NullableRelation[P, C]{}, fmt.Errorf("orm: nullable relation requires initialized tables and keys")
	}
	seenParent, seenChild := map[int]bool{}, map[int]bool{}
	nullable := false
	result := Relation[P, C]{parent: parent, child: child, parts: make([]JoinPart[P, C], len(parts))}
	for i, item := range parts {
		part := item.part
		typ := part.childField.typ
		if typ == nil {
			return NullableRelation[P, C]{}, fmt.Errorf("orm: zero nullable relation key")
		}
		if part.childField.nullable {
			typ = typ.Elem()
			nullable = true
		}
		if part.parent != parent.info || part.child != child.info || part.parentField.nullable || part.parentField.typ != typ || !associationKeyType(typ) || seenParent[part.parentField.index] || seenChild[part.childField.index] {
			return NullableRelation[P, C]{}, fmt.Errorf("orm: nullable relation key binding/type/ownership conflict")
		}
		seenParent[part.parentField.index] = true
		seenChild[part.childField.index] = true
		result.parts[i] = part
	}
	if !nullable {
		return NullableRelation[P, C]{}, fmt.Errorf("orm: nullable relation needs a nullable foreign-key component")
	}
	return NullableRelation[P, C]{result}, nil
}
func LoadNullableMany[P, C any](ctx context.Context, db Executor, relation NullableRelation[P, C], parents []P, query Query[C], budget LoadBudget) ([]Association[P, C], error) {
	return loadAssociation(ctx, db, relation.relation, parents, query, budget, false)
}
func LoadNullableOne[P, C any](ctx context.Context, db Executor, relation NullableRelation[P, C], parents []P, query Query[C], budget LoadBudget) ([]Association[P, C], error) {
	return loadAssociation(ctx, db, relation.relation, parents, query, budget, true)
}

var ErrAssociationOwner = errors.New("orm: child belongs to a different association owner")

func validateNullableMutation[P, C any](relation NullableRelation[P, C], parent HookRepository[P], child HookRepository[C], selector Predicate[C], budget GraphBudget) error {
	r := relation.relation
	if r.parent.info == nil || r.child.info == nil || parent.table.info != r.parent.info || child.table.info != r.child.info || len(r.parts) == 0 {
		return fmt.Errorf("orm: nullable mutation repositories require exact relation binding")
	}
	if r.parent.info.schema == r.child.info.schema && r.parent.info.name == r.child.info.name {
		return fmt.Errorf("orm: nullable self graph mutation requires deferred-key plan")
	}
	if budget.MaxChildren < 1 || budget.MaxCoreStatements < 3 {
		return ErrGraphBudget
	}
	args := []any{}
	_, err := renderPredicate(r.child.info, selector.expr, &args)
	return err
}
func lockNullableChild[P, C any](ctx context.Context, session *WriteSession, relation NullableRelation[P, C], selector Predicate[C]) (C, error) {
	var zero C
	r := relation.relation
	sql, args, err := selectSQL(r.child, r.child.info.columns(), Query[C]{}.Where(selector).Limit(2))
	if err != nil {
		return zero, err
	}
	rows, err := session.Query(ctx, sql+" FOR UPDATE", args...)
	if err != nil {
		return zero, err
	}
	models, err := scanModels[C](rows, r.child.info)
	if err != nil {
		return zero, err
	}
	if len(models) == 0 {
		return zero, ErrNotFound
	}
	if len(models) != 1 {
		return zero, ErrCardinality
	}
	return models[0], nil
}
func nullableOwnership[P, C any](relation NullableRelation[P, C], parent P, child C, reparent bool) error {
	p, c := reflect.ValueOf(parent), reflect.ValueOf(child)
	nullableCount, nullCount := 0, 0
	different := false
	for _, part := range relation.relation.parts {
		pv, cv := p.Field(part.parentField.index), c.Field(part.childField.index)
		if part.childField.nullable {
			nullableCount++
			if cv.IsNil() {
				nullCount++
				continue
			}
			cv = cv.Elem()
			different = different || !reflect.DeepEqual(pv.Interface(), cv.Interface())
		} else if !reflect.DeepEqual(pv.Interface(), cv.Interface()) {
			return ErrAssociationOwner
		}
	}
	if nullCount > 0 && nullCount != nullableCount {
		return fmt.Errorf("%w: partially NULL composite foreign key", ErrAssociationOwner)
	}
	if nullCount == 0 && different && !reparent {
		return ErrAssociationOwner
	}
	return nil
}
func nullableAssignments[P, C any](relation NullableRelation[P, C], parent P, disconnect bool) []Assignment[C] {
	result := []Assignment[C]{}
	value := reflect.ValueOf(parent)
	for _, part := range relation.relation.parts {
		if part.childField.nullable {
			var bound any
			if !disconnect {
				bound = value.Field(part.parentField.index).Interface()
			}
			result = append(result, Assignment[C]{info: part.child, field: part.childField, value: bound, mode: supplied})
		}
	}
	return result
}
func mutateNullable[P, C any](ctx context.Context, session *WriteSession, relation NullableRelation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, selector Predicate[C], assignments []Assignment[C], operation string, budget GraphBudget) (result int64, err error) {
	if err := validateNullableMutation(relation, parentRepo, childRepo, selector, budget); err != nil {
		return 0, wrap(operation, err)
	}
	if operation == "update nullable relation" {
		for _, assignment := range assignments {
			for _, part := range relation.relation.parts {
				if assignment.info == part.child && assignment.field.index == part.childField.index {
					return 0, wrap(operation, fmt.Errorf("orm: relation owns foreign-key updates"))
				}
			}
		}
		if _, _, err := updateSQL(relation.relation.child, selector, assignments); err != nil {
			return 0, wrap(operation, err)
		}
	}
	err = session.Savepoint(ctx, func(owned *WriteSession) error {
		locked, err := lockGraphParent(ctx, owned, relation.relation, parent)
		if err != nil {
			return err
		}
		condition := selector
		if operation != "connect nullable relation" && operation != "reparent nullable relation" {
			condition = And(condition, childIdentity(relation.relation, locked))
		}
		child, err := lockNullableChild(ctx, owned, relation, condition)
		if err != nil {
			return err
		}
		if err := nullableOwnership(relation, locked, child, operation == "reparent nullable relation"); err != nil {
			return err
		}
		switch operation {
		case "connect nullable relation", "reparent nullable relation":
			result, err = HookUpdate(ctx, owned, childRepo, condition, nullableAssignments(relation, locked, false)...)
		case "disconnect nullable relation":
			result, err = HookUpdate(ctx, owned, childRepo, condition, nullableAssignments(relation, locked, true)...)
		case "update nullable relation":
			result, err = HookUpdate(ctx, owned, childRepo, condition, assignments...)
		case "delete nullable relation child":
			result, err = HookDelete(ctx, owned, childRepo, condition)
		default:
			return fmt.Errorf("orm: invalid nullable mutation")
		}
		if err != nil {
			return err
		}
		if result != 1 {
			return ErrCardinality
		}
		return nil
	})
	if err != nil {
		return 0, wrap(operation, err)
	}
	return result, nil
}
func ConnectNullable[P, C any](ctx context.Context, session *WriteSession, relation NullableRelation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, selector Predicate[C], budget GraphBudget) (int64, error) {
	return mutateNullable(ctx, session, relation, parentRepo, childRepo, parent, selector, nil, "connect nullable relation", budget)
}

// ReparentNullable explicitly moves nullable FK components; required shared
// identity fields such as tenant must already match and are never changed.
func ReparentNullable[P, C any](ctx context.Context, session *WriteSession, relation NullableRelation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, selector Predicate[C], budget GraphBudget) (int64, error) {
	return mutateNullable(ctx, session, relation, parentRepo, childRepo, parent, selector, nil, "reparent nullable relation", budget)
}
func DisconnectNullable[P, C any](ctx context.Context, session *WriteSession, relation NullableRelation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, selector Predicate[C], budget GraphBudget) (int64, error) {
	return mutateNullable(ctx, session, relation, parentRepo, childRepo, parent, selector, nil, "disconnect nullable relation", budget)
}
func UpdateNullable[P, C any](ctx context.Context, session *WriteSession, relation NullableRelation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, selector Predicate[C], budget GraphBudget, assignments ...Assignment[C]) (int64, error) {
	return mutateNullable(ctx, session, relation, parentRepo, childRepo, parent, selector, assignments, "update nullable relation", budget)
}
func DeleteNullableChild[P, C any](ctx context.Context, session *WriteSession, relation NullableRelation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, selector Predicate[C], budget GraphBudget) (int64, error) {
	return mutateNullable(ctx, session, relation, parentRepo, childRepo, parent, selector, nil, "delete nullable relation child", budget)
}
