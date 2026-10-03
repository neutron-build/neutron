package orm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// Nullable is a left-join projection's SQL NULL wrapper. Valid=false means
// SQL NULL, whether it came from an unmatched row or a matched nullable field;
// it is not evidence of matched-row presence. Value's zero is not substituted
// for a non-NULL scalar. Nullable is a projection result, not a mapped codec.
type Nullable[T any] struct {
	Valid bool
	Value T
}

type joinedBinding[P, C any] struct {
	relation                Relation[P, C]
	left                    bool
	parentAlias, childAlias string
	lateral                 bool
	childQuery              Query[C]
}

// JoinedScope is sealed to validated InnerJoin/LeftJoin handles in this package.
type JoinedScope[P, C any] interface{ joinedBinding() *joinedBinding[P, C] }
type InnerJoin[P, C any] struct{ binding *joinedBinding[P, C] }
type LeftJoin[P, C any] struct{ binding *joinedBinding[P, C] }

func (s InnerJoin[P, C]) joinedBinding() *joinedBinding[P, C] { return s.binding }
func (s LeftJoin[P, C]) joinedBinding() *joinedBinding[P, C]  { return s.binding }

func newJoinedBinding[P, C any](relation Relation[P, C], left bool) (*joinedBinding[P, C], error) {
	validated, err := NewRelation(relation.parent, relation.child, relation.parts...)
	if err != nil {
		return nil, err
	}
	parent, child := validated.parent.info, validated.child.info
	if parent.schema == child.schema && parent.name == child.name {
		return nil, fmt.Errorf("orm: duplicate physical join table requires an explicit alias API, which is unsupported")
	}
	return &joinedBinding[P, C]{relation: validated, left: left}, nil
}

// NewInnerJoin joins two distinct qualified physical tables using exact typed
// relation keys. Use NewAliasedInnerJoin for aliases/self joins.
func NewInnerJoin[P, C any](relation Relation[P, C]) (InnerJoin[P, C], error) {
	binding, err := newJoinedBinding(relation, false)
	return InnerJoin[P, C]{binding}, err
}

// NewLeftJoin preserves left rows and requires nullable right projections.
func NewLeftJoin[P, C any](relation Relation[P, C]) (LeftJoin[P, C], error) {
	binding, err := newJoinedBinding(relation, true)
	return LeftJoin[P, C]{binding}, err
}

func newAliasedBinding[P, C any](relation Relation[P, C], left bool, parentAlias, childAlias string) (*joinedBinding[P, C], error) {
	validated, err := NewRelation(relation.parent, relation.child, relation.parts...)
	if err != nil {
		return nil, err
	}
	if err := identifier(parentAlias); err != nil {
		return nil, err
	}
	if err := identifier(childAlias); err != nil {
		return nil, err
	}
	if parentAlias == childAlias {
		return nil, fmt.Errorf("orm: join aliases must differ")
	}
	return &joinedBinding[P, C]{relation: validated, left: left, parentAlias: parentAlias, childAlias: childAlias}, nil
}

// NewAliasedInnerJoin explicitly distinguishes parent/child roles, including
// self relations using the same Table metadata. Aliases are quoted identifiers.
func NewAliasedInnerJoin[P, C any](relation Relation[P, C], parentAlias, childAlias string) (InnerJoin[P, C], error) {
	binding, err := newAliasedBinding(relation, false, parentAlias, childAlias)
	return InnerJoin[P, C]{binding}, err
}
func NewAliasedLeftJoin[P, C any](relation Relation[P, C], parentAlias, childAlias string) (LeftJoin[P, C], error) {
	binding, err := newAliasedBinding(relation, true, parentAlias, childAlias)
	return LeftJoin[P, C]{binding}, err
}

func newLateralBinding[P, C any](relation Relation[P, C], left bool, parentAlias, childAlias string, childQuery Query[C]) (*joinedBinding[P, C], error) {
	binding, err := newAliasedBinding(relation, left, parentAlias, childAlias)
	if err != nil {
		return nil, err
	}
	if !childQuery.limited || childQuery.limit < 0 || childQuery.limit == int(^uint(0)>>1) {
		return nil, fmt.Errorf("orm: lateral child query requires a finite explicit per-parent LIMIT")
	}
	if _, _, err := selectSQL(relation.child, relation.child.info.columns(), childQuery); err != nil {
		return nil, err
	}
	binding.lateral = true
	binding.childQuery = childQuery
	return binding, nil
}

// NewLateralInnerJoin correlates exact composite child keys to each parent and
// applies the child filter/order/LIMIT/OFFSET independently per parent.
func NewLateralInnerJoin[P, C any](relation Relation[P, C], parentAlias, childAlias string, childQuery Query[C]) (InnerJoin[P, C], error) {
	binding, err := newLateralBinding(relation, false, parentAlias, childAlias, childQuery)
	return InnerJoin[P, C]{binding}, err
}

// NewLateralLeftJoin additionally preserves parents with no qualifying child;
// every projected right field remains Nullable. Outer filters retain SQL WHERE
// semantics and can deliberately exclude unmatched parents.
func NewLateralLeftJoin[P, C any](relation Relation[P, C], parentAlias, childAlias string, childQuery Query[C]) (LeftJoin[P, C], error) {
	binding, err := newLateralBinding(relation, true, parentAlias, childAlias, childQuery)
	return LeftJoin[P, C]{binding}, err
}

// JoinedField retains scalar and model ownership at compile time and the exact
// join/table binding at runtime. Fields from another equally named scope fail.
type JoinedField[P, C, T any] struct {
	binding     *joinedBinding[P, C]
	info        *modelInfo
	field       fieldInfo
	outer       bool
	child       bool
	destination func(*T, bool) any
}

func plainJoinedField[P, C, T any](binding *joinedBinding[P, C], column Column[any, T]) JoinedField[P, C, T] {
	return JoinedField[P, C, T]{binding: binding, info: column.info, field: column.field, destination: func(value *T, _ bool) any { return scanDestination(reflect.ValueOf(value).Elem()) }}
}

// JoinParentField accepts the left table of either join kind; it preserves its
// original scalar nullability rather than wrapping it as an outer field.
func JoinParentField[P, C, T any](scope JoinedScope[P, C], column Column[P, T]) (JoinedField[P, C, T], error) {
	var zero JoinedField[P, C, T]
	if scope == nil || (reflect.ValueOf(scope).Kind() == reflect.Pointer && reflect.ValueOf(scope).IsNil()) {
		return zero, fmt.Errorf("orm: uninitialized join scope")
	}
	binding := scope.joinedBinding()
	if binding == nil || column.info != binding.relation.parent.info {
		return zero, fmt.Errorf("orm: parent projection outside join binding")
	}
	return plainJoinedField[P, C](binding, Column[any, T]{column.info, column.field}), nil
}
func InnerChildField[P, C, T any](scope InnerJoin[P, C], column Column[C, T]) (JoinedField[P, C, T], error) {
	var zero JoinedField[P, C, T]
	binding := scope.binding
	if binding == nil || column.info != binding.relation.child.info {
		return zero, fmt.Errorf("orm: child projection outside join binding")
	}
	field := plainJoinedField[P, C](binding, Column[any, T]{column.info, column.field})
	field.child = true
	return field, nil
}

// LeftChildField always wraps the right value as Nullable[T], including when
// the physical mapped scalar T is already a nullable pointer.
func LeftChildField[P, C, T any](scope LeftJoin[P, C], column Column[C, T]) (JoinedField[P, C, Nullable[T]], error) {
	var zero JoinedField[P, C, Nullable[T]]
	binding := scope.binding
	if binding == nil || column.info != binding.relation.child.info {
		return zero, fmt.Errorf("orm: child projection outside join binding")
	}
	return JoinedField[P, C, Nullable[T]]{binding: binding, info: column.info, field: column.field, outer: true, child: true, destination: func(value *Nullable[T], sqlNull bool) any {
		if sqlNull {
			*value = Nullable[T]{}
			return nil
		}
		value.Valid = true
		return scanDestination(reflect.ValueOf(&value.Value).Elem())
	}}, nil
}

type joinedFilter struct {
	info  *modelInfo
	expr  *expression
	child bool
}
type joinedOrder struct {
	info       *modelInfo
	order      fieldInfo
	descending bool
	child      bool
	nulls      string
}

// JoinQuery is an immutable value-style two-table query. Child filters belong
// to WHERE, not ON; a filter excluding NULL removes unmatched left-join rows.
type JoinQuery[P, C any] struct {
	binding            *joinedBinding[P, C]
	filters            []joinedFilter
	order              []joinedOrder
	limit, offset      int
	limited, offsetSet bool
}

func (s InnerJoin[P, C]) Query() JoinQuery[P, C] { return JoinQuery[P, C]{binding: s.binding} }
func (s LeftJoin[P, C]) Query() JoinQuery[P, C]  { return JoinQuery[P, C]{binding: s.binding} }
func (q JoinQuery[P, C]) WhereParent(predicate Predicate[P]) JoinQuery[P, C] {
	var info *modelInfo
	if q.binding != nil {
		info = q.binding.relation.parent.info
	}
	q.filters = append(append([]joinedFilter(nil), q.filters...), joinedFilter{info, predicate.expr, false})
	return q
}
func (q JoinQuery[P, C]) WhereChild(predicate Predicate[C]) JoinQuery[P, C] {
	var info *modelInfo
	if q.binding != nil {
		info = q.binding.relation.child.info
	}
	q.filters = append(append([]joinedFilter(nil), q.filters...), joinedFilter{info, predicate.expr, true})
	return q
}
func (q JoinQuery[P, C]) OrderParent(order ...Order[P]) JoinQuery[P, C] {
	q.order = append([]joinedOrder(nil), q.order...)
	for _, o := range order {
		q.order = append(q.order, joinedOrder{o.info, o.field, o.descending, false, o.nulls})
	}
	return q
}
func (q JoinQuery[P, C]) OrderChild(order ...Order[C]) JoinQuery[P, C] {
	q.order = append([]joinedOrder(nil), q.order...)
	for _, o := range order {
		q.order = append(q.order, joinedOrder{o.info, o.field, o.descending, true, o.nulls})
	}
	return q
}
func (q JoinQuery[P, C]) Limit(count int) JoinQuery[P, C] {
	q.limit = count
	q.limited = true
	return q
}
func (q JoinQuery[P, C]) Offset(count int) JoinQuery[P, C] {
	q.offset = count
	q.offsetSet = true
	return q
}
func qualifiedColumn(info *modelInfo, field fieldInfo) string {
	return info.sqlName() + "." + quote(field.name)
}

type joinedProjection struct {
	info  *modelInfo
	field fieldInfo
	outer bool
	child bool
}

func joinedSQL[P, C any](q JoinQuery[P, C], fields []joinedProjection) (string, []any, error) {
	if q.binding == nil || len(fields) == 0 {
		return "", nil, fmt.Errorf("orm: initialized join and nonempty projection required")
	}
	if q.limit < 0 || q.offset < 0 {
		return "", nil, fmt.Errorf("orm: negative join pagination")
	}
	parent, child := q.binding.relation.parent.info, q.binding.relation.child.info
	if parent == nil || child == nil {
		return "", nil, fmt.Errorf("orm: uninitialized join tables")
	}
	qualify := func(right bool, field fieldInfo) string {
		info, alias := parent, q.binding.parentAlias
		if right {
			info, alias = child, q.binding.childAlias
		}
		if alias != "" {
			return quote(alias) + "." + quote(field.name)
		}
		return qualifiedColumn(info, field)
	}
	columns := make([]string, len(fields))
	for i, f := range fields {
		expected := parent
		if f.child {
			expected = child
		}
		if f.info != expected {
			return "", nil, fmt.Errorf("orm: projection outside join tables")
		}
		if f.outer != (q.binding.left && f.child) {
			return "", nil, fmt.Errorf("orm: left child projection requires Nullable wrapper only on the right")
		}
		columns[i] = qualify(f.child, f.field)
	}
	parts := make([]string, len(q.binding.relation.parts))
	for i, p := range q.binding.relation.parts {
		parts[i] = qualify(false, p.parentField) + " = " + qualify(true, p.childField)
	}
	kind := " INNER JOIN "
	if q.binding.left {
		kind = " LEFT JOIN "
	}
	parentSQL, childSQL := parent.sqlName(), child.sqlName()
	if q.binding.parentAlias != "" {
		parentSQL += " AS " + quote(q.binding.parentAlias)
		childSQL += " AS " + quote(q.binding.childAlias)
	}
	args := []any{}
	joinClause := childSQL + " ON (" + strings.Join(parts, " AND ") + ")"
	if q.binding.lateral {
		innerAlias := "__neutron_lateral_source"
		for innerAlias == q.binding.parentAlias || innerAlias == q.binding.childAlias {
			innerAlias += "x"
		}
		innerColumn := func(field fieldInfo) string { return quote(innerAlias) + "." + quote(field.name) }
		innerFields := make([]string, len(child.fields))
		for i, field := range child.fields {
			innerFields[i] = innerColumn(field)
		}
		correlation := make([]string, len(q.binding.relation.parts))
		for i, part := range q.binding.relation.parts {
			correlation[i] = innerColumn(part.childField) + " = " + qualify(false, part.parentField)
		}
		subquery := "SELECT " + strings.Join(innerFields, ", ") + " FROM " + child.sqlName() + " AS " + quote(innerAlias) + " WHERE (" + strings.Join(correlation, " AND ") + ")"
		childQuery := q.binding.childQuery
		if childQuery.whereSet {
			predicate, err := renderPredicateColumns(child, childQuery.predicate.expr, &args, innerColumn)
			if err != nil {
				return "", nil, err
			}
			subquery += " AND (" + predicate + ")"
		}
		childOrders := make([]string, len(childQuery.order))
		for i, order := range childQuery.order {
			if order.info != child {
				return "", nil, fmt.Errorf("orm: lateral order outside child binding")
			}
			dir := " ASC"
			if order.descending {
				dir = " DESC"
			}
			childOrders[i] = innerColumn(order.field) + dir + order.nulls
		}
		if len(childOrders) > 0 {
			subquery += " ORDER BY " + strings.Join(childOrders, ", ")
		}
		args = append(args, childQuery.limit)
		subquery += fmt.Sprintf(" LIMIT $%d", len(args))
		if childQuery.offset > 0 {
			args = append(args, childQuery.offset)
			subquery += fmt.Sprintf(" OFFSET $%d", len(args))
		}
		joinClause = "LATERAL (" + subquery + ") AS " + quote(q.binding.childAlias) + " ON TRUE"
	}
	sql := "SELECT " + strings.Join(columns, ", ") + " FROM " + parentSQL + kind + joinClause
	where := make([]string, len(q.filters))
	for i, filter := range q.filters {
		expected := parent
		if filter.child {
			expected = child
		}
		if filter.info != expected {
			return "", nil, fmt.Errorf("orm: predicate outside join tables")
		}
		rendered, err := renderPredicateColumns(filter.info, filter.expr, &args, func(field fieldInfo) string { return qualify(filter.child, field) })
		if err != nil {
			return "", nil, err
		}
		where[i] = rendered
	}
	if len(where) > 0 {
		sql += " WHERE (" + strings.Join(where, ") AND (") + ")"
	}
	order := make([]string, len(q.order))
	for i, o := range q.order {
		expected := parent
		if o.child {
			expected = child
		}
		if o.info != expected {
			return "", nil, fmt.Errorf("orm: ordering outside its join table slot")
		}
		direction := " ASC"
		if o.descending {
			direction = " DESC"
		}
		order[i] = qualify(o.child, o.order) + direction + o.nulls
	}
	if len(order) > 0 {
		sql += " ORDER BY " + strings.Join(order, ", ")
	}
	if q.limited {
		args = append(args, q.limit)
		sql += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if q.offsetSet {
		args = append(args, q.offset)
		sql += fmt.Sprintf(" OFFSET $%d", len(args))
	}
	if len(args) > 65535 {
		return "", nil, fmt.Errorf("orm: join exceeds PostgreSQL parameter limit")
	}
	return sql, args, nil
}

func validateJoinedField[P, C, T any](q JoinQuery[P, C], field JoinedField[P, C, T]) error {
	if field.binding == nil || field.binding != q.binding || field.destination == nil {
		return fmt.Errorf("orm: projection belongs to another join binding")
	}
	return nil
}

func SelectJoinedColumn[P, C, T any](ctx context.Context, db Executor, field JoinedField[P, C, T], query JoinQuery[P, C]) ([]T, error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("join project", err)
	}
	if err := validateJoinedField(query, field); err != nil {
		return nil, wrap("join project", err)
	}
	sql, args, err := joinedSQL(query, []joinedProjection{{field.info, field.field, field.outer, field.child}})
	if err != nil {
		return nil, wrap("join project", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("join project", err)
	}
	defer rows.Close()
	result := []T{}
	for rows.Next() {
		var value T
		raw := rows.RawValues()
		if len(raw) != 1 {
			return nil, wrap("join project", fmt.Errorf("unexpected join projection shape"))
		}
		if err := rows.Scan(field.destination(&value, raw[0] == nil)); err != nil {
			return nil, wrap("join project", err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap("join project", err)
	}
	return result, nil
}
func SelectJoinedPair[P, C, A, B any](ctx context.Context, db Executor, first JoinedField[P, C, A], second JoinedField[P, C, B], query JoinQuery[P, C]) ([]Pair[A, B], error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("join pair", err)
	}
	for _, err := range []error{validateJoinedField(query, first), validateJoinedField(query, second)} {
		if err != nil {
			return nil, wrap("join pair", err)
		}
	}
	sql, args, err := joinedSQL(query, []joinedProjection{{first.info, first.field, first.outer, first.child}, {second.info, second.field, second.outer, second.child}})
	if err != nil {
		return nil, wrap("join pair", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("join pair", err)
	}
	defer rows.Close()
	result := []Pair[A, B]{}
	for rows.Next() {
		var value Pair[A, B]
		raw := rows.RawValues()
		if len(raw) != 2 {
			return nil, wrap("join pair", fmt.Errorf("unexpected join projection shape"))
		}
		if err := rows.Scan(first.destination(&value.First, raw[0] == nil), second.destination(&value.Second, raw[1] == nil)); err != nil {
			return nil, wrap("join pair", err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap("join pair", err)
	}
	return result, nil
}
func SelectJoinedOne[P, C, T any](ctx context.Context, db Executor, field JoinedField[P, C, T], query JoinQuery[P, C]) (T, error) {
	var zero T
	if query.limited || query.offsetSet {
		return zero, wrap("join one", fmt.Errorf("pagination refused for join cardinality check"))
	}
	rows, err := SelectJoinedColumn(ctx, db, field, query.Limit(2))
	if err != nil {
		return zero, err
	}
	if len(rows) == 0 {
		return zero, wrap("join one", ErrNotFound)
	}
	if len(rows) != 1 {
		return zero, wrap("join one", ErrCardinality)
	}
	return rows[0], nil
}
func SelectJoinedPairOne[P, C, A, B any](ctx context.Context, db Executor, first JoinedField[P, C, A], second JoinedField[P, C, B], query JoinQuery[P, C]) (Pair[A, B], error) {
	var zero Pair[A, B]
	if query.limited || query.offsetSet {
		return zero, wrap("join pair one", fmt.Errorf("pagination refused for join cardinality check"))
	}
	rows, err := SelectJoinedPair(ctx, db, first, second, query.Limit(2))
	if err != nil {
		return zero, err
	}
	if len(rows) == 0 {
		return zero, wrap("join pair one", ErrNotFound)
	}
	if len(rows) != 1 {
		return zero, wrap("join pair one", ErrCardinality)
	}
	return rows[0], nil
}
