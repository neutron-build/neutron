package orm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
)

// JoinPart pairs fields with the same exact Go type. Nullable and floating-point
// keys are deliberately unsupported: a relation must have an exact identity.
type JoinPart[P, C any] struct {
	parent, child           *modelInfo
	parentField, childField fieldInfo
}

func Join[P, C, T any](parent Column[P, T], child Column[C, T]) JoinPart[P, C] {
	return JoinPart[P, C]{parent.info, child.info, parent.field, child.field}
}

// Relation describes the direction from input parents to matching children.
// It neither infers foreign keys nor performs cascades or associated writes.
type Relation[P, C any] struct {
	parent Table[P]
	child  Table[C]
	parts  []JoinPart[P, C]
}

func NewRelation[P, C any](parent Table[P], child Table[C], parts ...JoinPart[P, C]) (Relation[P, C], error) {
	var zero Relation[P, C]
	if parent.info == nil || child.info == nil || len(parts) == 0 {
		return zero, fmt.Errorf("orm: relation requires initialized tables and keys")
	}
	seenParent, seenChild := map[int]bool{}, map[int]bool{}
	for _, part := range parts {
		if part.parent != parent.info || part.child != child.info {
			return zero, fmt.Errorf("orm: relation key belongs to another table scope")
		}
		if part.parentField.typ != part.childField.typ || !associationKeyType(part.parentField.typ) || part.parentField.nullable || part.childField.nullable {
			return zero, fmt.Errorf("orm: relation requires identical nonnullable integer, string, bool or UUID keys")
		}
		if seenParent[part.parentField.index] || seenChild[part.childField.index] {
			return zero, fmt.Errorf("orm: duplicate relation key field")
		}
		seenParent[part.parentField.index] = true
		seenChild[part.childField.index] = true
	}
	return Relation[P, C]{parent, child, append([]JoinPart[P, C](nil), parts...)}, nil
}

// Inverse changes ownership explicitly, preserving the exact composite key.
func Inverse[P, C any](relation Relation[P, C]) Relation[C, P] {
	parts := make([]JoinPart[C, P], len(relation.parts))
	for i, p := range relation.parts {
		parts[i] = JoinPart[C, P]{p.child, p.parent, p.childField, p.parentField}
	}
	return Relation[C, P]{relation.child, relation.parent, parts}
}

func associationKeyType(t reflect.Type) bool {
	if t == reflect.TypeOf(UUID{}) {
		return true
	}
	if t == nil || t.PkgPath() != "" {
		return false
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64, reflect.String, reflect.Bool:
		return true
	}
	return false
}

// LoadBudget is mandatory. MaxParents counts all input slots, including
// duplicate keys. MaxRows bounds expanded output child slots, including duplicates;
// BatchSize bounds distinct composite keys in each PostgreSQL statement.
type LoadBudget struct{ MaxParents, MaxRows, BatchSize int }

var ErrLoadBudget = errors.New("orm: association load budget exceeded")

// Association preserves each parent's input position, including duplicates.
// Missing relations have an empty Children slice. Children follow the supplied
// child query order within each key; without OrderBy their order is unspecified.
type Association[P, C any] struct {
	Parent   P
	Children []C
}

// LoadMany reads bounded batches of qualified children. The child query may
// filter and order, but cannot apply a global limit/offset as per-parent paging.
// Multiple statements do not imply a consistent snapshot; use an owned
// RepeatableRead/Serializable Scope when cross-batch consistency is required.
func LoadMany[P, C any](ctx context.Context, db Executor, relation Relation[P, C], parents []P, query Query[C], budget LoadBudget) ([]Association[P, C], error) {
	return loadAssociation(ctx, db, relation, parents, query, budget, false)
}

// LoadOne checks at most one matching child per composite key, rather than
// hiding additional matches. A missing child remains an empty Children slice.
func LoadOne[P, C any](ctx context.Context, db Executor, relation Relation[P, C], parents []P, query Query[C], budget LoadBudget) ([]Association[P, C], error) {
	return loadAssociation(ctx, db, relation, parents, query, budget, true)
}

func relationKey(value reflect.Value, fields []fieldInfo) string {
	// Type tags and length-prefixed strings preserve tuple boundaries and exact
	// types; no fmt/string conversion or float equality participates in identity.
	key := make([]byte, 0, len(fields)*9)
	for _, field := range fields {
		v := value.Field(field.index)
		key = append(key, byte(v.Kind()))
		if v.Type() == reflect.TypeOf(UUID{}) {
			uuid := v.Interface().(UUID)
			key = append(key, uuid.bytes[:]...)
			continue
		}
		switch v.Kind() {
		case reflect.String:
			text := v.String()
			key = binary.BigEndian.AppendUint64(key, uint64(len(text)))
			key = append(key, text...)
		case reflect.Bool:
			if v.Bool() {
				key = append(key, 1)
			} else {
				key = append(key, 0)
			}
		default:
			key = binary.BigEndian.AppendUint64(key, uint64(v.Int()))
		}
	}
	return string(key)
}

func loadAssociation[P, C any](ctx context.Context, db Executor, relation Relation[P, C], parents []P, query Query[C], budget LoadBudget, singular bool) ([]Association[P, C], error) {
	fail := func(err error) ([]Association[P, C], error) { return nil, wrap("load association", err) }
	if err := ready(ctx, db); err != nil {
		return fail(err)
	}
	if relation.parent.info == nil || relation.child.info == nil || len(relation.parts) == 0 {
		return fail(fmt.Errorf("uninitialized relation"))
	}
	if budget.MaxParents <= 0 || budget.MaxRows <= 0 || budget.BatchSize <= 0 || budget.MaxRows == int(^uint(0)>>1) {
		return fail(fmt.Errorf("positive finite association budgets required"))
	}
	if len(parents) > budget.MaxParents {
		return fail(ErrLoadBudget)
	}
	if query.limited || query.offset != 0 {
		return fail(fmt.Errorf("association limit/offset requires per-parent pagination, which is unsupported"))
	}
	// Validate the whole query before even an empty input succeeds.
	if _, _, err := selectSQL(relation.child, relation.child.info.columns(), query); err != nil {
		return fail(err)
	}
	parentFields, childFields := make([]fieldInfo, len(relation.parts)), make([]fieldInfo, len(relation.parts))
	for i, p := range relation.parts {
		parentFields[i] = p.parentField
		childFields[i] = p.childField
	}
	keys := make([]string, len(parents))
	distinct := make([]int, 0, len(parents))
	seen := map[string]bool{}
	multiplicity := map[string]int{}
	for i, parent := range parents {
		keys[i] = relationKey(reflect.ValueOf(parent), parentFields)
		multiplicity[keys[i]]++
		if !seen[keys[i]] {
			seen[keys[i]] = true
			distinct = append(distinct, i)
		}
	}
	byKey := map[string][]C{}
	total := 0
	for start := 0; start < len(distinct); {
		end := len(distinct)
		if budget.BatchSize < end-start {
			end = start + budget.BatchSize
		}
		groups := make([]Predicate[C], 0, end-start)
		requested := map[string]bool{}
		for _, index := range distinct[start:end] {
			parent := reflect.ValueOf(parents[index])
			children := make([]Predicate[C], len(relation.parts))
			for i, p := range relation.parts {
				children[i] = Predicate[C]{&expression{kind: "=", info: p.child, field: p.childField, value: parent.Field(p.parentField.index).Interface()}}
			}
			groups = append(groups, And(children...))
			requested[keys[index]] = true
		}
		combined := Or(groups...)
		if query.whereSet {
			combined = And(query.predicate, combined)
		}
		batch := query.Where(combined).Limit(budget.MaxRows - total + 1)
		sql, args, err := selectSQL(relation.child, relation.child.info.columns(), batch)
		if err != nil {
			return fail(err)
		}
		if len(args) > 65535 {
			return fail(fmt.Errorf("association batch exceeds PostgreSQL parameter limit"))
		}
		rows, err := db.Query(ctx, sql, args...)
		if err != nil {
			return fail(err)
		}
		children, err := scanModels[C](rows, relation.child.info)
		if err != nil {
			return fail(err)
		}
		if len(children) > budget.MaxRows-total {
			return fail(ErrLoadBudget)
		}
		for _, child := range children {
			key := relationKey(reflect.ValueOf(child), childFields)
			if !requested[key] {
				return fail(fmt.Errorf("returned association key was not requested"))
			}
			if multiplicity[key] > budget.MaxRows-total {
				return fail(ErrLoadBudget)
			}
			total += multiplicity[key]
			byKey[key] = append(byKey[key], child)
			if singular && len(byKey[key]) > 1 {
				return fail(ErrCardinality)
			}
		}
		start = end
	}
	result := make([]Association[P, C], len(parents))
	for i, parent := range parents {
		children := make([]C, len(byKey[keys[i]]))
		copy(children, byKey[keys[i]])
		result[i] = Association[P, C]{parent, children}
	}
	return result, nil
}
