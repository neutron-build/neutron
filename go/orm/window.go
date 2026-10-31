package orm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// Window preserves the aggregate's actual typed result/nullability. PostgreSQL's
// default frame is retained unless RowsBetween is explicit. DISTINCT aggregate
// windows are refused because PostgreSQL does not support them.
type Window[M, T any] struct {
	aggregate Aggregate[M, T]
	partition []BoundColumn[M]
	order     []Order[M]
	frame     *windowFrame
	rowNumber bool
}

func Over[M, T any](aggregate Aggregate[M, T], partition []BoundColumn[M], order ...Order[M]) Window[M, T] {
	return Window[M, T]{aggregate: aggregate, partition: append([]BoundColumn[M](nil), partition...), order: append([]Order[M](nil), order...)}
}
func RowNumber[M any](table Table[M], partition []BoundColumn[M], order ...Order[M]) Window[M, int64] {
	window := Over(CountAll(table), partition, order...)
	window.rowNumber = true
	return window
}

type FrameBoundary struct {
	kind string
	rows int
}

func UnboundedPreceding() FrameBoundary { return FrameBoundary{kind: "UNBOUNDED PRECEDING"} }
func UnboundedFollowing() FrameBoundary { return FrameBoundary{kind: "UNBOUNDED FOLLOWING"} }
func CurrentRow() FrameBoundary         { return FrameBoundary{kind: "CURRENT ROW"} }
func Preceding(rows int) FrameBoundary  { return FrameBoundary{kind: "PRECEDING", rows: rows} }
func Following(rows int) FrameBoundary  { return FrameBoundary{kind: "FOLLOWING", rows: rows} }

type windowFrame struct{ start, end FrameBoundary }

func (w Window[M, T]) RowsBetween(start, end FrameBoundary) Window[M, T] {
	w.frame = &windowFrame{start, end}
	return w
}
func frameBoundary(boundary FrameBoundary, args *[]any) (string, error) {
	switch boundary.kind {
	case "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING", "CURRENT ROW":
		return boundary.kind, nil
	case "PRECEDING", "FOLLOWING":
		if boundary.rows < 0 || boundary.rows == int(^uint(0)>>1) {
			return "", fmt.Errorf("orm: finite nonnegative window frame offset required")
		}
		*args = append(*args, boundary.rows)
		return fmt.Sprintf("$%d %s", len(*args), boundary.kind), nil
	default:
		return "", fmt.Errorf("orm: uninitialized window frame boundary")
	}
}
func (w Window[M, T]) sql(args *[]any) (string, error) {
	info := w.aggregate.info
	expr, err := w.aggregate.sql(info)
	if err != nil {
		return "", err
	}
	if w.aggregate.distinct {
		return "", fmt.Errorf("orm: DISTINCT aggregate windows unsupported by PostgreSQL")
	}
	if w.rowNumber {
		expr = "ROW_NUMBER()"
	}
	clauses := []string{}
	if len(w.partition) > 0 {
		names, err := boundColumns(Table[M]{info}, w.partition)
		if err != nil {
			return "", err
		}
		for i, name := range names {
			names[i] = quote(name)
		}
		clauses = append(clauses, "PARTITION BY "+strings.Join(names, ", "))
	}
	if len(w.order) > 0 {
		parts := make([]string, len(w.order))
		for i, order := range w.order {
			if order.info != info {
				return "", fmt.Errorf("orm: window order outside table binding")
			}
			dir := " ASC"
			if order.descending {
				dir = " DESC"
			}
			parts[i] = quote(order.field.name) + dir + order.nulls
		}
		clauses = append(clauses, "ORDER BY "+strings.Join(parts, ", "))
	}
	if w.frame != nil {
		if w.frame.start.kind == "UNBOUNDED FOLLOWING" || w.frame.end.kind == "UNBOUNDED PRECEDING" {
			return "", fmt.Errorf("orm: invalid unbounded window frame direction")
		}
		start, err := frameBoundary(w.frame.start, args)
		if err != nil {
			return "", err
		}
		end, err := frameBoundary(w.frame.end, args)
		if err != nil {
			return "", err
		}
		clauses = append(clauses, "ROWS BETWEEN "+start+" AND "+end)
	}
	return expr + " OVER (" + strings.Join(clauses, " ") + ")", nil
}

// SelectWindowPair returns a typed original scalar plus a typed window value.
// WHERE filters input rows before the window; Query orders/paginates resulting
// rows. Every partition/order/projection retains exact table/model binding.
func SelectWindowPair[M, A, T any](ctx context.Context, db Executor, column Column[M, A], window Window[M, T], query Query[M]) ([]Pair[A, T], error) {
	if err := ready(ctx, db); err != nil {
		return nil, wrap("window", err)
	}
	if column.info == nil || column.info != window.aggregate.info {
		return nil, wrap("window", fmt.Errorf("orm: projection outside window table binding"))
	}
	args := []any{}
	expr, err := window.sql(&args)
	if err != nil {
		return nil, wrap("window", err)
	}
	sql, args, err := selectSQLArgs(Table[M]{column.info}, quote(column.field.name)+", "+expr, query, args)
	if err != nil {
		return nil, wrap("window", err)
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap("window", err)
	}
	defer rows.Close()
	result := []Pair[A, T]{}
	for rows.Next() {
		raw := rows.RawValues()
		if len(raw) != 2 {
			return nil, wrap("window", fmt.Errorf("orm: unexpected window projection shape"))
		}
		var pair Pair[A, T]
		if err := rows.Scan(scanDestination(reflect.ValueOf(&pair.First).Elem()), window.aggregate.destination(&pair.Second, raw[1] == nil)); err != nil {
			return nil, wrap("window", err)
		}
		result = append(result, pair)
	}
	return result, wrap("window", rows.Err())
}
