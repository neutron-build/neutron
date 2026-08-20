package nucleus

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/neutron-dev/neutron-go/neutron"
)

var timeType = reflect.TypeOf(time.Time{})

// querier is the interface satisfied by both *pgxpool.Pool and txQuerier.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// SQLModel provides type-safe SQL query execution.
type SQLModel struct {
	pool querier
}

// Query executes a query and scans all rows into a slice of T.
// T must be a struct with `db` tags matching column names.
func Query[T any](ctx context.Context, sql *SQLModel, query string, args ...any) ([]T, error) {
	// Use SimpleProtocol for SELECT queries. Nucleus extended protocol
	// returns incorrect results for COUNT/SUM aggregates.
	queryArgs := make([]any, 0, len(args)+1)
	queryArgs = append(queryArgs, pgx.QueryExecModeSimpleProtocol)
	queryArgs = append(queryArgs, args...)
	rows, err := sql.pool.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("nucleus: query: %w", err)
	}
	defer rows.Close()

	// Initialise as empty slice (not nil) so handlers that pass results
	// straight back to JSON encoders emit `[]` for zero-row results
	// rather than `null`. Saves every consumer from writing the same
	// nil-to-empty shim.
	results := make([]T, 0)
	for rows.Next() {
		var item T
		if err := scanRow(rows, &item); err != nil {
			return nil, fmt.Errorf("nucleus: scan: %w", err)
		}
		results = append(results, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("nucleus: rows: %w", err)
	}

	return results, nil
}

// QueryOne executes a query and scans exactly one row into T.
// Returns ErrNotFound if no rows are returned.
func QueryOne[T any](ctx context.Context, sql *SQLModel, query string, args ...any) (T, error) {
	var result T
	queryArgs := make([]any, 0, len(args)+1)
	queryArgs = append(queryArgs, pgx.QueryExecModeSimpleProtocol)
	queryArgs = append(queryArgs, args...)
	rows, err := sql.pool.Query(ctx, query, queryArgs...)
	if err != nil {
		return result, fmt.Errorf("nucleus: query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return result, fmt.Errorf("nucleus: rows: %w", err)
		}
		return result, neutron.ErrNotFound("no rows returned")
	}

	if err := scanRow(rows, &result); err != nil {
		return result, fmt.Errorf("nucleus: scan: %w", err)
	}

	return result, nil
}

// Exec executes a non-query SQL statement and returns the number of affected rows.
// Uses SimpleProtocol for Nucleus compatibility — its extended protocol types
// all parameters as TEXT which breaks arithmetic on BIGINT columns.
func (s *SQLModel) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	execArgs := make([]any, 0, len(args)+1)
	execArgs = append(execArgs, pgx.QueryExecModeSimpleProtocol)
	execArgs = append(execArgs, args...)
	tag, err := s.pool.Exec(ctx, query, execArgs...)
	if err != nil {
		return 0, fmt.Errorf("nucleus: exec: %w", err)
	}
	return tag.RowsAffected(), nil
}

// scanRow scans a pgx row into a struct using `db` tags.
func scanRow(rows pgx.Rows, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer to struct")
	}
	rv = rv.Elem()
	rt := rv.Type()

	if rt.Kind() != reflect.Struct {
		// For non-struct types, try direct scan
		return rows.Scan(dest)
	}

	// Build column-name to field-index map
	fieldDescs := rows.FieldDescriptions()
	colNames := make([]string, len(fieldDescs))
	for i, fd := range fieldDescs {
		colNames[i] = fd.Name
	}

	// Map db tags to field indices
	tagMap := make(map[string]int, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		tag := f.Tag.Get("db")
		if tag == "" || tag == "-" {
			// Fall back to json tag
			tag = f.Tag.Get("json")
			if tag != "" {
				if idx := strings.Index(tag, ","); idx != -1 {
					tag = tag[:idx]
				}
			}
		}
		if tag == "" || tag == "-" {
			tag = strings.ToLower(f.Name)
		}
		tagMap[tag] = i
	}

	// Scan all columns as nullable strings, then convert to target types.
	// Nucleus pgwire may send binary format indicators with text data, so
	// direct scanning into typed fields fails. Nullable string intermediary
	// handles all cases including NULL values.
	rawVals := make([]any, len(colNames))
	rawPtrs := make([]*string, len(colNames))
	for i := range rawVals {
		rawVals[i] = &rawPtrs[i] // **string — allows NULL scanning
	}

	if err := rows.Scan(rawVals...); err != nil {
		return err
	}

	for i, col := range colNames {
		fieldIdx, ok := tagMap[col]
		if !ok {
			continue
		}
		field := rv.Field(fieldIdx)

		// NULL → zero value
		if rawPtrs[i] == nil {
			continue
		}
		raw := *rawPtrs[i]

		// Handle time.Time before the Kind switch — it's a struct, not a
		// primitive, so Kind() returns reflect.Struct.
		if field.Type() == timeType {
			t, err := parseTimeValue(raw)
			if err != nil {
				return fmt.Errorf("column %q: parse time %q: %w", col, raw, err)
			}
			field.Set(reflect.ValueOf(t))
			continue
		}

		switch field.Kind() {
		case reflect.String:
			field.SetString(raw)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if raw == "" {
				field.SetInt(0)
			} else {
				n, err := strconv.ParseInt(raw, 10, 64)
				if err != nil {
					return fmt.Errorf("column %q: parse int %q: %w", col, raw, err)
				}
				field.SetInt(n)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if raw == "" {
				field.SetUint(0)
			} else {
				n, err := strconv.ParseUint(raw, 10, 64)
				if err != nil {
					return fmt.Errorf("column %q: parse uint %q: %w", col, raw, err)
				}
				field.SetUint(n)
			}
		case reflect.Float32, reflect.Float64:
			if raw == "" {
				field.SetFloat(0)
			} else {
				f, err := strconv.ParseFloat(raw, 64)
				if err != nil {
					return fmt.Errorf("column %q: parse float %q: %w", col, raw, err)
				}
				field.SetFloat(f)
			}
		case reflect.Bool:
			field.SetBool(raw == "true" || raw == "t" || raw == "1" || raw == "TRUE")
		default:
			field.SetString(raw)
		}
	}

	return nil
}

// parseTimeValue parses a time value from a string. Supports:
//   - Unix milliseconds: "1712000000000"
//   - RFC3339: "2024-04-01T12:00:00Z"
//   - RFC3339Nano: "2024-04-01T12:00:00.000000Z"
//   - Empty or "0": zero time
func parseTimeValue(s string) (time.Time, error) {
	if s == "" || s == "0" {
		return time.Time{}, nil
	}
	// Try epoch milliseconds first (most common in MergeTree/Nucleus)
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(ms).UTC(), nil
	}
	// Try RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	// Try RFC3339Nano
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unrecognized time format: %s", s)
}
