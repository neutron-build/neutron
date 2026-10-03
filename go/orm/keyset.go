package orm

import "fmt"

// SeekAfter constructs lexicographic pagination for a fully specified order.
// uniqueKey declares a database-enforced nonnullable unique key present in that
// order; metadata does not create or certify the constraint. Cursor assignments
// must supply exactly one value for each ordered column, in order. PostgreSQL
// default ASC NULLS LAST/DESC NULLS FIRST and explicit NULL placement are handled.
// The query/order and cursor inputs remain unchanged. OFFSET is refused.
func SeekAfter[M any](table Table[M], query Query[M], uniqueKey []BoundColumn[M], cursor []Assignment[M]) (Query[M], error) {
	if _, err := boundColumns(table, uniqueKey); err != nil {
		return Query[M]{}, err
	}
	if len(query.order) == 0 || len(cursor) != len(query.order) || query.offset != 0 {
		return Query[M]{}, fmt.Errorf("orm: keyset requires complete ordered cursor and no OFFSET")
	}
	seen := map[int]bool{}
	for i, order := range query.order {
		if order.info != table.info || seen[order.field.index] {
			return Query[M]{}, fmt.Errorf("orm: duplicate or foreign keyset order")
		}
		seen[order.field.index] = true
		assignment := cursor[i]
		if assignment.info != table.info || assignment.field.index != order.field.index || assignment.mode != supplied {
			return Query[M]{}, fmt.Errorf("orm: keyset cursor must supply ordered columns exactly")
		}
		if assignment.value == nil && !order.field.nullable {
			return Query[M]{}, fmt.Errorf("orm: NULL keyset value for nonnullable order")
		}
		if err := validateScalarValue(assignment.value); err != nil {
			return Query[M]{}, err
		}
	}
	for _, column := range uniqueKey {
		if column.field.nullable || !seen[column.field.index] {
			return Query[M]{}, fmt.Errorf("orm: complete nonnullable unique key must appear in keyset order")
		}
	}
	equalities := make([]Predicate[M], len(cursor))
	for i, assignment := range cursor {
		equalities[i] = Predicate[M]{&expression{kind: "=", info: table.info, field: assignment.field, value: assignment.value}}
	}
	branches := make([]Predicate[M], len(cursor))
	for i, assignment := range cursor {
		order := query.order[i]
		nullsFirst := order.nulls == " NULLS FIRST" || (order.nulls == "" && order.descending)
		var after Predicate[M]
		if assignment.value == nil {
			if nullsFirst {
				after = Predicate[M]{&expression{kind: "<>", info: table.info, field: assignment.field, value: nil}}
			} else {
				after = Predicate[M]{&expression{kind: "IN", info: table.info, field: assignment.field}}
			}
		} else {
			op := ">"
			if order.descending {
				op = "<"
			}
			after = Predicate[M]{&expression{kind: op, info: table.info, field: assignment.field, value: assignment.value}}
			if order.field.nullable && !nullsFirst {
				after = Or(after, Predicate[M]{&expression{kind: "=", info: table.info, field: assignment.field, value: nil}})
			}
		}
		prefix := append([]Predicate[M](nil), equalities[:i]...)
		branches[i] = And(append(prefix, after)...)
	}
	after := Or(branches...)
	if query.whereSet {
		after = And(query.predicate, after)
	}
	result := query.Where(after)
	if _, err := CompileSelect(table, result); err != nil {
		return Query[M]{}, err
	}
	return result, nil
}
