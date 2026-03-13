package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// DocumentModel provides document/JSON operations over Nucleus SQL functions.
type DocumentModel struct {
	pool   querier
	client *Client
}

// DocOption configures document queries.
type DocOption func(*docOpts)

type docOpts struct {
	sortField string
	sortAsc   bool
	skip      int
	limit     int
	fields    []string
}

// WithSort sets the sort field and direction.
func WithSort(field string, asc bool) DocOption {
	return func(o *docOpts) { o.sortField = field; o.sortAsc = asc }
}

// WithProjection limits which fields are returned.
func WithProjection(fields ...string) DocOption {
	return func(o *docOpts) { o.fields = fields }
}

// WithSkip skips the first n results.
func WithSkip(n int) DocOption {
	return func(o *docOpts) { o.skip = n }
}

// WithDocLimit limits the number of results.
func WithDocLimit(n int) DocOption {
	return func(o *docOpts) { o.limit = n }
}

func applyDocOpts(opts []DocOption) docOpts {
	var o docOpts
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// Insert stores a document and returns its ID.
func (d *DocumentModel) Insert(ctx context.Context, _ string, doc any) (int64, error) {
	if err := d.client.requireNucleus("Document.Insert"); err != nil {
		return 0, err
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return 0, fmt.Errorf("nucleus: doc marshal: %w", err)
	}
	var id int64
	err = d.pool.QueryRow(ctx, "SELECT DOC_INSERT($1)", string(data)).Scan(&id)
	return id, wrapErr("doc insert", err)
}

// Get retrieves a document by ID.
func (d *DocumentModel) Get(ctx context.Context, id int64) (map[string]any, error) {
	if err := d.client.requireNucleus("Document.Get"); err != nil {
		return nil, err
	}
	var raw *string
	err := d.pool.QueryRow(ctx, "SELECT DOC_GET($1)", id).Scan(&raw)
	if err != nil {
		return nil, wrapErr("doc get", err)
	}
	if raw == nil {
		return nil, nil
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(*raw), &result); err != nil {
		return nil, fmt.Errorf("nucleus: doc unmarshal: %w", err)
	}
	return result, nil
}

// DocGetTyped retrieves a document by ID and unmarshals into T.
func DocGetTyped[T any](ctx context.Context, d *DocumentModel, id int64) (T, error) {
	var result T
	if err := d.client.requireNucleus("Document.GetTyped"); err != nil {
		return result, err
	}
	var raw *string
	err := d.pool.QueryRow(ctx, "SELECT DOC_GET($1)", id).Scan(&raw)
	if err != nil {
		return result, wrapErr("doc get", err)
	}
	if raw == nil {
		return result, fmt.Errorf("nucleus: doc %d not found", id)
	}
	if err := json.Unmarshal([]byte(*raw), &result); err != nil {
		return result, fmt.Errorf("nucleus: doc unmarshal: %w", err)
	}
	return result, nil
}

// QueryDocs queries documents matching a JSON query and returns matching IDs.
func (d *DocumentModel) QueryDocs(ctx context.Context, filter map[string]any) ([]int64, error) {
	if err := d.client.requireNucleus("Document.QueryDocs"); err != nil {
		return nil, err
	}
	q, err := json.Marshal(filter)
	if err != nil {
		return nil, fmt.Errorf("nucleus: doc query marshal: %w", err)
	}
	var raw string
	err = d.pool.QueryRow(ctx, "SELECT DOC_QUERY($1)", string(q)).Scan(&raw)
	if err != nil {
		return nil, wrapErr("doc query", err)
	}
	if raw == "" {
		return nil, nil
	}
	// Parse comma-separated IDs
	parts := strings.Split(raw, ",")
	ids := make([]int64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var id int64
		if _, err := fmt.Sscanf(p, "%d", &id); err == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// Path extracts a nested value from a document using a key path.
func (d *DocumentModel) Path(ctx context.Context, id int64, keys ...string) (*string, error) {
	if err := d.client.requireNucleus("Document.Path"); err != nil {
		return nil, err
	}
	args := make([]any, 0, 1+len(keys))
	args = append(args, id)
	placeholders := make([]string, len(keys))
	for i, k := range keys {
		args = append(args, k)
		placeholders[i] = fmt.Sprintf("$%d", i+2)
	}
	q := fmt.Sprintf("SELECT DOC_PATH($1, %s)", strings.Join(placeholders, ", "))
	var val *string
	err := d.pool.QueryRow(ctx, q, args...).Scan(&val)
	return val, wrapErr("doc path", err)
}

// Count returns the total number of documents.
func (d *DocumentModel) Count(ctx context.Context) (int64, error) {
	if err := d.client.requireNucleus("Document.Count"); err != nil {
		return 0, err
	}
	var n int64
	err := d.pool.QueryRow(ctx, "SELECT DOC_COUNT()").Scan(&n)
	return n, wrapErr("doc count", err)
}

// applyProjection filters a document to only include specified fields.
func applyProjection(doc map[string]any, fields []string) map[string]any {
	if len(fields) == 0 {
		return doc
	}
	projected := make(map[string]any, len(fields))
	for _, f := range fields {
		if val, ok := doc[f]; ok {
			projected[f] = val
		}
	}
	return projected
}

// Find queries documents and returns full documents matching a filter.
// Supports WithSort, WithProjection, WithSkip, and WithDocLimit options.
func (d *DocumentModel) Find(ctx context.Context, collection string, filter map[string]any, opts ...DocOption) ([]map[string]any, error) {
	o := applyDocOpts(opts)

	ids, err := d.QueryDocs(ctx, filter)
	if err != nil {
		return nil, err
	}

	var results []map[string]any
	for _, id := range ids {
		doc, err := d.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if doc != nil {
			results = append(results, doc)
		}
	}

	// Apply sort
	if o.sortField != "" {
		sort.Slice(results, func(i, j int) bool {
			vi, _ := results[i][o.sortField]
			vj, _ := results[j][o.sortField]
			si := fmt.Sprintf("%v", vi)
			sj := fmt.Sprintf("%v", vj)
			if o.sortAsc {
				return si < sj
			}
			return si > sj
		})
	}

	// Apply skip
	if o.skip > 0 && o.skip < len(results) {
		results = results[o.skip:]
	} else if o.skip >= len(results) {
		return nil, nil
	}

	// Apply limit
	if o.limit > 0 && o.limit < len(results) {
		results = results[:o.limit]
	}

	// Apply projection
	if len(o.fields) > 0 {
		for i, doc := range results {
			results[i] = applyProjection(doc, o.fields)
		}
	}

	return results, nil
}

// FindTyped queries documents and returns typed results.
// Supports WithSort, WithSkip, and WithDocLimit options (projection is not applicable for typed results).
func FindTyped[T any](ctx context.Context, d *DocumentModel, collection string, filter map[string]any, opts ...DocOption) ([]T, error) {
	o := applyDocOpts(opts)

	ids, err := d.QueryDocs(ctx, filter)
	if err != nil {
		return nil, err
	}

	var results []T
	for _, id := range ids {
		item, err := DocGetTyped[T](ctx, d, id)
		if err != nil {
			continue // skip missing docs
		}
		results = append(results, item)
	}

	// Apply skip
	if o.skip > 0 && o.skip < len(results) {
		results = results[o.skip:]
	} else if o.skip >= len(results) {
		return nil, nil
	}

	// Apply limit
	if o.limit > 0 && o.limit < len(results) {
		results = results[:o.limit]
	}

	return results, nil
}

// FindOne returns the first document matching a filter.
func (d *DocumentModel) FindOne(ctx context.Context, collection string, filter map[string]any) (map[string]any, error) {
	docs, err := d.Find(ctx, collection, filter)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil
	}
	return docs[0], nil
}

// FindOneTyped returns the first typed result matching a filter.
func FindOneTyped[T any](ctx context.Context, d *DocumentModel, collection string, filter map[string]any) (T, error) {
	var zero T
	results, err := FindTyped[T](ctx, d, collection, filter)
	if err != nil {
		return zero, err
	}
	if len(results) == 0 {
		return zero, fmt.Errorf("nucleus: doc not found")
	}
	return results[0], nil
}

// Update updates documents matching a filter by applying the update map.
// Uses jsonb_set to apply each key in the update map.
// Returns the number of documents updated.
func (d *DocumentModel) Update(ctx context.Context, collection string, filter map[string]any, update map[string]any) (int64, error) {
	if err := d.client.requireNucleus("Document.Update"); err != nil {
		return 0, err
	}
	ids, err := d.QueryDocs(ctx, filter)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	var count int64
	for _, id := range ids {
		// Get current doc
		doc, err := d.Get(ctx, id)
		if err != nil || doc == nil {
			continue
		}
		// Apply updates
		for k, v := range update {
			doc[k] = v
		}
		// Re-serialize and store
		data, err := json.Marshal(doc)
		if err != nil {
			continue
		}
		// Use DOC_INSERT to replace (Nucleus DOC_INSERT with existing ID upserts)
		// Alternatively, update via SQL on the underlying documents table
		_, err = d.pool.Exec(ctx,
			"UPDATE documents SET data = $1::jsonb WHERE id = $2",
			string(data), id)
		if err == nil {
			count++
		}
	}
	return count, nil
}

// Delete removes documents matching a filter.
// Returns the number of documents deleted.
func (d *DocumentModel) Delete(ctx context.Context, collection string, filter map[string]any) (int64, error) {
	if err := d.client.requireNucleus("Document.Delete"); err != nil {
		return 0, err
	}
	ids, err := d.QueryDocs(ctx, filter)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	var count int64
	for _, id := range ids {
		_, err := d.pool.Exec(ctx, "DELETE FROM documents WHERE id = $1", id)
		if err == nil {
			count++
		}
	}
	return count, nil
}
