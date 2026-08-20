package nucleus

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// ColumnarModel provides columnar analytics operations over Nucleus SQL functions.
type ColumnarModel struct {
	pool   querier
	client *Client
}

// Insert inserts a row into a columnar table. values is a map of column->value.
// COLUMNAR_INSERT is variadic: (table, col1, val1, col2, val2, ...) and
// returns the text 'OK'.
func (c *ColumnarModel) Insert(ctx context.Context, table string, values map[string]any) (bool, error) {
	if err := c.client.requireNucleus("Columnar.Insert"); err != nil {
		return false, err
	}
	if !isValidIdentifier(table) {
		return false, fmt.Errorf("nucleus: columnar insert: invalid table name %q", table)
	}
	if len(values) == 0 {
		return false, fmt.Errorf("nucleus: columnar insert: at least one column is required")
	}
	cols := make([]string, 0, len(values))
	for col := range values {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	args := make([]any, 0, 1+len(values)*2)
	args = append(args, table)
	for _, col := range cols {
		args = append(args, col, values[col])
	}
	placeholders := make([]string, len(args))
	for i := range args {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	q := "SELECT COLUMNAR_INSERT(" + strings.Join(placeholders, ", ") + ")"
	var result string
	err := c.pool.QueryRow(ctx, q, args...).Scan(&result)
	if err != nil {
		return false, wrapErr("columnar insert", err)
	}
	return result == "OK", nil
}

// Count returns the number of rows in a columnar table.
func (c *ColumnarModel) Count(ctx context.Context, table string) (int64, error) {
	if err := c.client.requireNucleus("Columnar.Count"); err != nil {
		return 0, err
	}
	if !isValidIdentifier(table) {
		return 0, fmt.Errorf("nucleus: columnar count: invalid table name %q", table)
	}
	var n int64
	err := c.pool.QueryRow(ctx, "SELECT COLUMNAR_COUNT($1)", table).Scan(&n)
	return n, wrapErr("columnar count", err)
}

// Sum returns the sum of a column in a columnar table.
func (c *ColumnarModel) Sum(ctx context.Context, table, column string) (float64, error) {
	if err := c.client.requireNucleus("Columnar.Sum"); err != nil {
		return 0, err
	}
	if !isValidIdentifier(table) {
		return 0, fmt.Errorf("nucleus: columnar sum: invalid table name %q", table)
	}
	var val float64
	err := c.pool.QueryRow(ctx, "SELECT COLUMNAR_SUM($1, $2)", table, column).Scan(&val)
	return val, wrapErr("columnar sum", err)
}

// Avg returns the average of a column in a columnar table.
func (c *ColumnarModel) Avg(ctx context.Context, table, column string) (float64, error) {
	if err := c.client.requireNucleus("Columnar.Avg"); err != nil {
		return 0, err
	}
	if !isValidIdentifier(table) {
		return 0, fmt.Errorf("nucleus: columnar avg: invalid table name %q", table)
	}
	var val float64
	err := c.pool.QueryRow(ctx, "SELECT COLUMNAR_AVG($1, $2)", table, column).Scan(&val)
	return val, wrapErr("columnar avg", err)
}

// Min returns the minimum value of a column in a columnar table.
func (c *ColumnarModel) Min(ctx context.Context, table, column string) (any, error) {
	if err := c.client.requireNucleus("Columnar.Min"); err != nil {
		return nil, err
	}
	if !isValidIdentifier(table) {
		return nil, fmt.Errorf("nucleus: columnar min: invalid table name %q", table)
	}
	var val any
	err := c.pool.QueryRow(ctx, "SELECT COLUMNAR_MIN($1, $2)", table, column).Scan(&val)
	return val, wrapErr("columnar min", err)
}

// Max returns the maximum value of a column in a columnar table.
func (c *ColumnarModel) Max(ctx context.Context, table, column string) (any, error) {
	if err := c.client.requireNucleus("Columnar.Max"); err != nil {
		return nil, err
	}
	if !isValidIdentifier(table) {
		return nil, fmt.Errorf("nucleus: columnar max: invalid table name %q", table)
	}
	var val any
	err := c.pool.QueryRow(ctx, "SELECT COLUMNAR_MAX($1, $2)", table, column).Scan(&val)
	return val, wrapErr("columnar max", err)
}
