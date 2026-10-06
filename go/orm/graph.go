package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// GraphBudget bounds the explicit one-level relation workflows. Core statements
// include parent locking, child budget reads and writes. Savepoint/transaction
// control and hook extra SQL are not included and remain separate overhead.
// MaxChildren bounds both
// proposed and removed child counts, including duplicate input rows.
type GraphBudget struct{ MaxChildren, MaxCoreStatements int }

var ErrGraphBudget = errors.New("orm: relation mutation budget exceeded")

func validateGraph[P, C any](relation Relation[P, C], parent HookRepository[P], child HookRepository[C], children [][]Assignment[C], budget GraphBudget, statements int) error {
	if relation.parent.info == nil || relation.child.info == nil || len(relation.parts) == 0 || parent.table.info != relation.parent.info || child.table.info != relation.child.info {
		return fmt.Errorf("orm: graph repositories require the exact relation binding")
	}
	if relation.parent.info.schema == relation.child.info.schema && relation.parent.info.name == relation.child.info.name {
		return fmt.Errorf("orm: cyclic/self relation mutation requires a deferred-key plan, which is unsupported")
	}
	if budget.MaxChildren <= 0 || budget.MaxCoreStatements <= 0 || budget.MaxChildren == int(^uint(0)>>1) {
		return fmt.Errorf("orm: positive finite graph budgets required")
	}
	if len(children) > budget.MaxChildren || statements > budget.MaxCoreStatements {
		return ErrGraphBudget
	}
	var zero P
	for _, assignments := range children {
		if _, err := derivedChildAssignments(relation, zero, assignments); err != nil {
			return err
		}
	}
	return nil
}
func derivedChildAssignments[P, C any](relation Relation[P, C], parent P, assignments []Assignment[C]) ([]Assignment[C], error) {
	for _, assignment := range assignments {
		for _, part := range relation.parts {
			if assignment.info == part.child && assignment.field.index == part.childField.index {
				return nil, fmt.Errorf("orm: relation owns child foreign-key assignments")
			}
		}
	}
	result := append([]Assignment[C](nil), assignments...)
	value := reflect.ValueOf(parent)
	for _, part := range relation.parts {
		result = append(result, Assignment[C]{info: part.child, field: part.childField, value: value.Field(part.parentField.index).Interface(), mode: supplied})
	}
	if _, _, err := insertSQL(relation.child, result); err != nil {
		return nil, err
	}
	return result, nil
}
func parentIdentity[P, C any](relation Relation[P, C], parent P) Predicate[P] {
	value := reflect.ValueOf(parent)
	parts := make([]Predicate[P], len(relation.parts))
	for i, part := range relation.parts {
		parts[i] = Predicate[P]{&expression{kind: "=", info: part.parent, field: part.parentField, value: value.Field(part.parentField.index).Interface()}}
	}
	return And(parts...)
}
func childIdentity[P, C any](relation Relation[P, C], parent P) Predicate[C] {
	value := reflect.ValueOf(parent)
	parts := make([]Predicate[C], len(relation.parts))
	for i, part := range relation.parts {
		parts[i] = Predicate[C]{&expression{kind: "=", info: part.child, field: part.childField, value: value.Field(part.parentField.index).Interface()}}
	}
	return And(parts...)
}
func lockGraphParent[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], parent P) (P, error) {
	var zero P
	sql, args, err := selectSQL(relation.parent, relation.parent.info.columns(), Query[P]{}.Where(parentIdentity(relation, parent)).Limit(2))
	if err != nil {
		return zero, err
	}
	rows, err := session.Query(ctx, sql+" FOR UPDATE", args...)
	if err != nil {
		return zero, err
	}
	models, err := scanModels[P](rows, relation.parent.info)
	if err == nil && len(models) == 0 {
		err = ErrNotFound
	}
	if err == nil && len(models) != 1 {
		err = ErrCardinality
	}
	if err != nil {
		session.fail(err)
		return zero, err
	}
	return models[0], nil
}
func insertGraphChildren[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], repo HookRepository[C], parent P, children [][]Assignment[C]) ([]C, error) {
	result := make([]C, 0, len(children))
	for _, assignments := range children {
		derived, err := derivedChildAssignments(relation, parent, assignments)
		if err != nil {
			session.fail(err)
			return nil, err
		}
		child, err := HookInsert(ctx, session, repo, derived...)
		if err != nil {
			session.fail(err)
			return nil, err
		}
		result = append(result, child)
	}
	return result, nil
}

// CreateRelated inserts one parent and a bounded list of children atomically in
// the owned hook transaction. All static child write plans are validated before
// any parent hook or SQL. Child composite foreign keys come only from the actual
// INSERT RETURNING parent; explicit key overrides and self cycles are refused.
func createRelated[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parentAssignments []Assignment[P], children [][]Assignment[C], budget GraphBudget) (Association[P, C], error) {
	var zero Association[P, C]
	if err := validateGraph(relation, parentRepo, childRepo, children, budget, 1+len(children)); err != nil {
		return zero, wrap("create relation", err)
	}
	if _, _, err := insertSQL(relation.parent, parentAssignments); err != nil {
		return zero, wrap("create relation", err)
	}
	parent, err := HookInsert(ctx, session, parentRepo, parentAssignments...)
	if err != nil {
		return zero, err
	}
	models, err := insertGraphChildren(ctx, session, relation, childRepo, parent, children)
	if err != nil {
		return zero, wrap("create relation", err)
	}
	return Association[P, C]{Parent: parent, Children: models}, nil
}

// AppendRelated locks and checks exactly one parent composite identity then
// inserts children with derived foreign keys. Existing children are retained.
func appendRelated[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, children [][]Assignment[C], budget GraphBudget) (Association[P, C], error) {
	var zero Association[P, C]
	if err := validateGraph(relation, parentRepo, childRepo, children, budget, 1+len(children)); err != nil {
		return zero, wrap("append relation", err)
	}
	locked, err := lockGraphParent(ctx, session, relation, parent)
	if err != nil {
		return zero, wrap("append relation", err)
	}
	models, err := insertGraphChildren(ctx, session, relation, childRepo, locked, children)
	if err != nil {
		return zero, wrap("append relation", err)
	}
	return Association[P, C]{Parent: locked, Children: models}, nil
}

func removeGraphChildren[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], repo HookRepository[C], parent P, budget GraphBudget) (int64, error) {
	// Check an explicit read budget before destructive writes. The post-delete
	// count is checked again: unconstrained external writers may race the read.
	models, err := Select(ctx, session, relation.child, Query[C]{}.Where(childIdentity(relation, parent)).Limit(budget.MaxChildren+1))
	if err != nil {
		session.fail(err)
		return 0, err
	}
	if len(models) > budget.MaxChildren {
		session.fail(ErrGraphBudget)
		return 0, ErrGraphBudget
	}
	count, err := HookDelete(ctx, session, repo, childIdentity(relation, parent))
	if err != nil {
		session.fail(err)
		return 0, err
	}
	if count > int64(budget.MaxChildren) {
		session.fail(ErrGraphBudget)
		return 0, ErrGraphBudget
	}
	return count, nil
}

// ReplaceRelated explicitly removes all children of the locked parent before
// inserting replacements. This is orphan deletion, not disconnection or an
// inferred database cascade. Hooks run for the child DELETE statement and each
// child INSERT. Any later failure rolls back and drops post-commit events.
func replaceRelated[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, children [][]Assignment[C], budget GraphBudget) (Association[P, C], error) {
	var zero Association[P, C]
	if err := validateGraph(relation, parentRepo, childRepo, children, budget, 3+len(children)); err != nil {
		return zero, wrap("replace relation", err)
	}
	locked, err := lockGraphParent(ctx, session, relation, parent)
	if err != nil {
		return zero, wrap("replace relation", err)
	}
	if _, err := removeGraphChildren(ctx, session, relation, childRepo, locked, budget); err != nil {
		return zero, wrap("replace relation", err)
	}
	models, err := insertGraphChildren(ctx, session, relation, childRepo, locked, children)
	if err != nil {
		return zero, wrap("replace relation", err)
	}
	return Association[P, C]{Parent: locked, Children: models}, nil
}

// DeleteRelated explicitly deletes bounded children then exactly one locked
// parent. Database cascades and nullable disconnection are separate policies.
func deleteRelated[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, budget GraphBudget) (int64, error) {
	if err := validateGraph(relation, parentRepo, childRepo, nil, budget, 4); err != nil {
		return 0, wrap("delete relation", err)
	}
	locked, err := lockGraphParent(ctx, session, relation, parent)
	if err != nil {
		return 0, wrap("delete relation", err)
	}
	children, err := removeGraphChildren(ctx, session, relation, childRepo, locked, budget)
	if err != nil {
		return 0, wrap("delete relation", err)
	}
	count, err := HookDelete(ctx, session, parentRepo, parentIdentity(relation, locked))
	if err == nil && count != 1 {
		err = ErrCardinality
	}
	if err != nil {
		session.fail(err)
		return 0, wrap("delete relation", err)
	}
	return children, nil
}

// CreateRelated creates a savepoint-owned relation graph. A swallowed operation
// error still rolls back that graph; unrelated parent work may continue after
// successful savepoint cleanup. Only released graph events merge into the parent.
func CreateRelated[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parentAssignments []Assignment[P], children [][]Assignment[C], budget GraphBudget) (result Association[P, C], err error) {
	if err = validateGraph(relation, parentRepo, childRepo, children, budget, 1+len(children)); err != nil {
		return result, wrap("create relation", err)
	}
	if _, _, err = insertSQL(relation.parent, parentAssignments); err != nil {
		return result, wrap("create relation", err)
	}
	err = session.Savepoint(ctx, func(child *WriteSession) error {
		var failure error
		result, failure = createRelated(ctx, child, relation, parentRepo, childRepo, parentAssignments, children, budget)
		return failure
	})
	if err != nil {
		result = Association[P, C]{}
	}
	return
}

// AppendRelated inserts new children under an exactly-one locked parent, inside
// an operation-owned savepoint. Its result contains only the appended children.
func AppendRelated[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, children [][]Assignment[C], budget GraphBudget) (result Association[P, C], err error) {
	if err = validateGraph(relation, parentRepo, childRepo, children, budget, 1+len(children)); err != nil {
		return result, wrap("append relation", err)
	}
	err = session.Savepoint(ctx, func(child *WriteSession) error {
		var failure error
		result, failure = appendRelated(ctx, child, relation, parentRepo, childRepo, parent, children, budget)
		return failure
	})
	if err != nil {
		result = Association[P, C]{}
	}
	return
}

// ReplaceRelated deletes all bounded children of a locked parent and inserts
// replacements inside an operation-owned savepoint. This is explicit orphan
// deletion, not nullable disconnection or an inferred database cascade.
func ReplaceRelated[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, children [][]Assignment[C], budget GraphBudget) (result Association[P, C], err error) {
	if err = validateGraph(relation, parentRepo, childRepo, children, budget, 3+len(children)); err != nil {
		return result, wrap("replace relation", err)
	}
	err = session.Savepoint(ctx, func(child *WriteSession) error {
		var failure error
		result, failure = replaceRelated(ctx, child, relation, parentRepo, childRepo, parent, children, budget)
		return failure
	})
	if err != nil {
		result = Association[P, C]{}
	}
	return
}

// DeleteRelated deletes bounded children and exactly one locked parent inside an
// operation-owned savepoint. Its count is the number of child rows removed.
func DeleteRelated[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, budget GraphBudget) (result int64, err error) {
	if err = validateGraph(relation, parentRepo, childRepo, nil, budget, 4); err != nil {
		return 0, wrap("delete relation", err)
	}
	err = session.Savepoint(ctx, func(child *WriteSession) error {
		var failure error
		result, failure = deleteRelated(ctx, child, relation, parentRepo, childRepo, parent, budget)
		return failure
	})
	if err != nil {
		result = 0
	}
	return
}

// SaveRelated updates explicit non-key parent assignments and appends new child
// rows in an operation-owned savepoint. It does not infer per-child upserts or
// tracked dirty state. Zero values remain supplied values. Composite parent keys
// cannot be changed; use an explicit database migration for identity changes.
func SaveRelated[P, C any](ctx context.Context, session *WriteSession, relation Relation[P, C], parentRepo HookRepository[P], childRepo HookRepository[C], parent P, parentAssignments []Assignment[P], children [][]Assignment[C], budget GraphBudget) (result Association[P, C], err error) {
	if err = validateGraph(relation, parentRepo, childRepo, children, budget, 3+len(children)); err != nil {
		return result, wrap("save relation", err)
	}
	for _, assignment := range parentAssignments {
		for _, part := range relation.parts {
			if assignment.info == part.parent && assignment.field.index == part.parentField.index {
				return result, wrap("save relation", fmt.Errorf("orm: graph parent identity is immutable"))
			}
		}
	}
	if _, _, err = updateSQL(relation.parent, parentIdentity(relation, parent), parentAssignments); err != nil {
		return result, wrap("save relation", err)
	}
	err = session.Savepoint(ctx, func(child *WriteSession) error {
		locked, failure := lockGraphParent(ctx, child, relation, parent)
		if failure != nil {
			return failure
		}
		count, failure := HookUpdate(ctx, child, parentRepo, parentIdentity(relation, locked), parentAssignments...)
		if failure == nil && count != 1 {
			failure = ErrCardinality
		}
		if failure != nil {
			child.fail(failure)
			return failure
		}
		locked, failure = SelectOne(ctx, child, relation.parent, Query[P]{}.Where(parentIdentity(relation, locked)))
		if failure != nil {
			child.fail(failure)
			return failure
		}
		models, failure := insertGraphChildren(ctx, child, relation, childRepo, locked, children)
		if failure != nil {
			return failure
		}
		result = Association[P, C]{Parent: locked, Children: models}
		return nil
	})
	if err != nil {
		result = Association[P, C]{}
	}
	return
}
