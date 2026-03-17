package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// VectorModel provides vector similarity search over Nucleus SQL functions.
type VectorModel struct {
	pool   querier
	client *Client
}

// DistanceMetric defines the vector distance function.
type DistanceMetric int

const (
	Cosine     DistanceMetric = iota
	Euclidean                         // L2
	DotProduct                        // inner product
)

func (m DistanceMetric) String() string {
	switch m {
	case Cosine:
		return "cosine"
	case Euclidean:
		return "l2"
	case DotProduct:
		return "inner"
	default:
		return "l2"
	}
}

// VectorSearchResult holds a search result with its distance/score.
type VectorSearchResult[T any] struct {
	Item     T
	Score    float64
	Distance float64
}

// VectorOption configures vector search.
type VectorOption func(*vectorOpts)

type vectorOpts struct {
	limit  int
	metric DistanceMetric
	filter map[string]any
}

// WithLimit sets the max number of results.
func WithLimit(n int) VectorOption {
	return func(o *vectorOpts) { o.limit = n }
}

// WithMetric sets the distance metric.
func WithMetric(m DistanceMetric) VectorOption {
	return func(o *vectorOpts) { o.metric = m }
}

// WithFilter adds metadata filters to the search.
func WithFilter(filter map[string]any) VectorOption {
	return func(o *vectorOpts) { o.filter = filter }
}

func defaultVectorOpts() vectorOpts {
	return vectorOpts{limit: 10, metric: Cosine}
}

// Search performs a vector similarity search on a collection (table).
// The table must have columns: id TEXT, embedding VECTOR, metadata JSONB.
func (v *VectorModel) Search(ctx context.Context, collection string, query []float32, opts ...VectorOption) ([]VectorSearchResult[map[string]any], error) {
	if err := v.client.requireNucleus("Vector.Search"); err != nil {
		return nil, err
	}
	if !isValidIdentifier(collection) {
		return nil, fmt.Errorf("nucleus: vector search: invalid collection name %q", collection)
	}

	o := defaultVectorOpts()
	for _, fn := range opts {
		fn(&o)
	}

	vecJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("nucleus: vector marshal query: %w", err)
	}

	// Build filter WHERE clause
	var filterClause string
	var filterArgs []any
	if len(o.filter) > 0 {
		var clauses []string
		paramIdx := 4 // $1=vecJSON, $2=metric, $3=limit; filters start at $4
		for k, val := range o.filter {
			clauses = append(clauses, fmt.Sprintf("metadata->>$%d = $%d", paramIdx, paramIdx+1))
			filterArgs = append(filterArgs, k, fmt.Sprintf("%v", val))
			paramIdx += 2
		}
		filterClause = " WHERE " + strings.Join(clauses, " AND ")
	}

	q := fmt.Sprintf(
		"SELECT id, metadata, VECTOR_DISTANCE(embedding, VECTOR($1), $2) AS distance FROM %s%s ORDER BY distance LIMIT $3",
		collection, filterClause,
	)

	args := []any{string(vecJSON), o.metric.String(), o.limit}
	args = append(args, filterArgs...)

	rows, err := v.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("nucleus: vector search: %w", err)
	}
	defer rows.Close()

	var results []VectorSearchResult[map[string]any]
	for rows.Next() {
		var id string
		var metaJSON []byte
		var dist float64
		if err := rows.Scan(&id, &metaJSON, &dist); err != nil {
			return nil, fmt.Errorf("nucleus: vector scan: %w", err)
		}
		meta := make(map[string]any)
		if len(metaJSON) > 0 {
			_ = json.Unmarshal(metaJSON, &meta)
		}
		meta["id"] = id
		// Score is the inverse of distance (closer = higher score)
		score := 0.0
		if dist > 0 {
			score = 1.0 / dist
		}
		results = append(results, VectorSearchResult[map[string]any]{Item: meta, Score: score, Distance: dist})
	}
	return results, rows.Err()
}

// SearchTyped performs a vector search and scans results into type T.
func SearchTyped[T any](ctx context.Context, v *VectorModel, collection string, query []float32, opts ...VectorOption) ([]VectorSearchResult[T], error) {
	if err := v.client.requireNucleus("Vector.SearchTyped"); err != nil {
		return nil, err
	}
	if !isValidIdentifier(collection) {
		return nil, fmt.Errorf("nucleus: vector search: invalid collection name %q", collection)
	}

	o := defaultVectorOpts()
	for _, fn := range opts {
		fn(&o)
	}

	vecJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("nucleus: vector marshal query: %w", err)
	}

	// Build filter WHERE clause
	var filterClause string
	var filterArgs []any
	if len(o.filter) > 0 {
		var clauses []string
		paramIdx := 4
		for k, val := range o.filter {
			clauses = append(clauses, fmt.Sprintf("metadata->>$%d = $%d", paramIdx, paramIdx+1))
			filterArgs = append(filterArgs, k, fmt.Sprintf("%v", val))
			paramIdx += 2
		}
		filterClause = " WHERE " + strings.Join(clauses, " AND ")
	}

	q := fmt.Sprintf(
		"SELECT id, metadata, VECTOR_DISTANCE(embedding, VECTOR($1), $2) AS distance FROM %s%s ORDER BY distance LIMIT $3",
		collection, filterClause,
	)

	args := []any{string(vecJSON), o.metric.String(), o.limit}
	args = append(args, filterArgs...)

	rows, err := v.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("nucleus: vector search: %w", err)
	}
	defer rows.Close()

	var results []VectorSearchResult[T]
	for rows.Next() {
		var id string
		var metaJSON []byte
		var dist float64
		if err := rows.Scan(&id, &metaJSON, &dist); err != nil {
			return nil, fmt.Errorf("nucleus: vector scan: %w", err)
		}
		var item T
		if len(metaJSON) > 0 {
			if err := json.Unmarshal(metaJSON, &item); err != nil {
				return nil, fmt.Errorf("nucleus: vector unmarshal: %w", err)
			}
		}
		score := 0.0
		if dist > 0 {
			score = 1.0 / dist
		}
		results = append(results, VectorSearchResult[T]{Item: item, Score: score, Distance: dist})
	}
	return results, rows.Err()
}

// Insert adds a vector with metadata to a collection.
func (v *VectorModel) Insert(ctx context.Context, collection string, id string, vector []float32, metadata map[string]any) error {
	if err := v.client.requireNucleus("Vector.Insert"); err != nil {
		return err
	}
	if !isValidIdentifier(collection) {
		return fmt.Errorf("nucleus: vector insert: invalid collection name %q", collection)
	}
	vecJSON, err := json.Marshal(vector)
	if err != nil {
		return fmt.Errorf("nucleus: vector marshal vector: %w", err)
	}
	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("nucleus: vector marshal metadata: %w", err)
	}
	q := fmt.Sprintf(
		"INSERT INTO %s (id, embedding, metadata) VALUES ($1, VECTOR($2), $3)",
		collection,
	)
	_, err = v.pool.Exec(ctx, q, id, string(vecJSON), string(metaJSON))
	return wrapErr("vector insert", err)
}

// Delete removes a vector by ID.
func (v *VectorModel) Delete(ctx context.Context, collection string, id string) error {
	if err := v.client.requireNucleus("Vector.Delete"); err != nil {
		return err
	}
	if !isValidIdentifier(collection) {
		return fmt.Errorf("nucleus: vector delete: invalid collection name %q", collection)
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE id = $1", collection)
	_, err := v.pool.Exec(ctx, q, id)
	return wrapErr("vector delete", err)
}

// CreateCollection creates a table with vector columns and an index.
func (v *VectorModel) CreateCollection(ctx context.Context, name string, dimension int, metric DistanceMetric) error {
	if err := v.client.requireNucleus("Vector.CreateCollection"); err != nil {
		return err
	}
	if !isValidIdentifier(name) {
		return fmt.Errorf("nucleus: vector create collection: invalid name %q", name)
	}
	createSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY, embedding VECTOR(%d), metadata JSONB DEFAULT '{}')",
		name, dimension,
	)
	if _, err := v.pool.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("nucleus: create collection: %w", err)
	}

	// Validate metric string against known values to prevent SQL injection
	metricStr := metric.String()
	validMetrics := map[string]bool{"cosine": true, "l2": true, "inner": true}
	if !validMetrics[metricStr] {
		return fmt.Errorf("nucleus: vector create collection: invalid metric %q", metricStr)
	}

	indexSQL := fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS idx_%s_embedding ON %s USING VECTOR (embedding) WITH (metric = '%s')",
		name, name, metricStr,
	)
	_, err := v.pool.Exec(ctx, indexSQL)
	return wrapErr("vector create index", err)
}

// Dims returns the dimensionality of a vector.
func (v *VectorModel) Dims(ctx context.Context, vec []float32) (int64, error) {
	if err := v.client.requireNucleus("Vector.Dims"); err != nil {
		return 0, err
	}
	vecJSON, err := json.Marshal(vec)
	if err != nil {
		return 0, fmt.Errorf("nucleus: vector marshal: %w", err)
	}
	var n int64
	err = v.pool.QueryRow(ctx, "SELECT VECTOR_DIMS(VECTOR($1))", string(vecJSON)).Scan(&n)
	return n, wrapErr("vector dims", err)
}

// Distance computes the distance between two vectors.
func (v *VectorModel) Distance(ctx context.Context, a, b []float32, metric DistanceMetric) (float64, error) {
	if err := v.client.requireNucleus("Vector.Distance"); err != nil {
		return 0, err
	}
	aJSON, err := json.Marshal(a)
	if err != nil {
		return 0, fmt.Errorf("nucleus: vector marshal: %w", err)
	}
	bJSON, err := json.Marshal(b)
	if err != nil {
		return 0, fmt.Errorf("nucleus: vector marshal: %w", err)
	}
	var d float64
	err = v.pool.QueryRow(ctx, "SELECT VECTOR_DISTANCE(VECTOR($1), VECTOR($2), $3)",
		string(aJSON), string(bJSON), metric.String()).Scan(&d)
	return d, wrapErr("vector distance", err)
}

func float32SliceToSQL(v []float32) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = fmt.Sprintf("%g", f)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
