package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
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

// Insert stores a document in a collection and returns its ID.
//
// An empty collection is the default (unnamed) one, which is where documents
// written before collections existed live. A named collection isolates the
// document: only calls naming the same collection can read, update or delete
// it.
func (d *DocumentModel) Insert(ctx context.Context, collection string, doc any) (int64, error) {
	if err := d.client.requireNucleus("Document.Insert"); err != nil {
		return 0, err
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return 0, fmt.Errorf("nucleus: doc marshal: %w", err)
	}
	var id int64
	if collection == "" {
		// The one-argument form, so a client that names no collection still
		// works against a server that predates them.
		err = d.pool.QueryRow(ctx, "SELECT DOC_INSERT($1)", string(data)).Scan(&id)
	} else {
		err = d.pool.QueryRow(ctx, "SELECT DOC_INSERT($1, $2)", collection, string(data)).Scan(&id)
	}
	return id, wrapErr("doc insert", err)
}

// Get retrieves a document by ID from the default collection.
//
// A document in a NAMED collection is reported as absent — use GetIn. That is
// the isolation: holding an id is not enough to read across a collection
// boundary.
func (d *DocumentModel) Get(ctx context.Context, id int64) (map[string]any, error) {
	return d.GetIn(ctx, "", id)
}

// GetIn retrieves a document by ID from a specific collection.
func (d *DocumentModel) GetIn(ctx context.Context, collection string, id int64) (map[string]any, error) {
	if err := d.client.requireNucleus("Document.Get"); err != nil {
		return nil, err
	}
	raw, err := d.rawDoc(ctx, collection, id)
	if err != nil {
		return nil, err
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

// rawDoc fetches a document's JSON text, scoped to a collection.
//
// An empty collection uses the one-argument call, so a client that never names
// one keeps working against a server that predates collections; naming one uses
// the two-argument call, which such a server rejects outright rather than
// silently ignoring the scope.
func (d *DocumentModel) rawDoc(ctx context.Context, collection string, id int64) (*string, error) {
	var raw *string
	var err error
	if collection == "" {
		err = d.pool.QueryRow(ctx, "SELECT DOC_GET($1)", docID(id)).Scan(&raw)
	} else {
		err = d.pool.QueryRow(ctx, "SELECT DOC_GET($1, $2)", collection, docID(id)).Scan(&raw)
	}
	if err != nil {
		return nil, wrapErr("doc get", err)
	}
	return raw, nil
}

// DocGetTyped retrieves a document by ID and unmarshals into T.
func DocGetTyped[T any](ctx context.Context, d *DocumentModel, id int64) (T, error) {
	return DocGetTypedIn[T](ctx, d, "", id)
}

// DocGetTypedIn retrieves a document by ID from a collection and unmarshals
// into T.
func DocGetTypedIn[T any](ctx context.Context, d *DocumentModel, collection string, id int64) (T, error) {
	var result T
	if err := d.client.requireNucleus("Document.GetTyped"); err != nil {
		return result, err
	}
	raw, err := d.rawDoc(ctx, collection, id)
	if err != nil {
		return result, err
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
	return d.QueryDocsIn(ctx, "", filter)
}

// QueryDocsIn queries one collection and returns the matching IDs. Matches in
// other collections are not returned.
func (d *DocumentModel) QueryDocsIn(ctx context.Context, collection string, filter map[string]any) ([]int64, error) {
	if err := d.client.requireNucleus("Document.QueryDocs"); err != nil {
		return nil, err
	}
	q, err := json.Marshal(filter)
	if err != nil {
		return nil, fmt.Errorf("nucleus: doc query marshal: %w", err)
	}
	var raw string
	if collection == "" {
		err = d.pool.QueryRow(ctx, "SELECT DOC_QUERY($1)", string(q)).Scan(&raw)
	} else {
		err = d.pool.QueryRow(ctx, "SELECT DOC_QUERY($1, $2)", collection, string(q)).Scan(&raw)
	}
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

// Path extracts a nested value from a document in the default collection.
func (d *DocumentModel) Path(ctx context.Context, id int64, keys ...string) (*string, error) {
	return d.PathIn(ctx, "", id, keys...)
}

// PathIn extracts a nested value from a document in a specific collection.
//
// Calling with no keys is refused rather than sent: the engine requires at
// least one, and building the call without one produced a malformed statement
// (a trailing comma) that surfaced as a syntax error naming nothing useful.
func (d *DocumentModel) PathIn(ctx context.Context, collection string, id int64, keys ...string) (*string, error) {
	if err := d.client.requireNucleus("Document.Path"); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("nucleus: doc path requires at least one key")
	}
	// The scoped form is a distinct FUNCTION, not an extra argument: the key
	// tail is variadic, so a leading collection could not be told apart from a
	// leading id.
	args := make([]any, 0, 2+len(keys))
	fn, base := "DOC_PATH", 2
	if collection != "" {
		fn, base = "DOC_PATH_IN", 3
		args = append(args, collection)
	}
	args = append(args, docID(id))
	placeholders := make([]string, len(keys))
	for i, k := range keys {
		args = append(args, k)
		placeholders[i] = fmt.Sprintf("$%d", i+base)
	}
	idArg := "$1"
	if collection != "" {
		idArg = "$1, $2"
	}
	q := fmt.Sprintf("SELECT %s(%s, %s)", fn, idArg, strings.Join(placeholders, ", "))
	var val *string
	err := d.pool.QueryRow(ctx, q, args...).Scan(&val)
	return val, wrapErr("doc path", err)
}

// Count returns the number of documents in the default collection.
func (d *DocumentModel) Count(ctx context.Context) (int64, error) {
	return d.CountIn(ctx, "")
}

// CountIn returns the number of documents in a specific collection.
func (d *DocumentModel) CountIn(ctx context.Context, collection string) (int64, error) {
	if err := d.client.requireNucleus("Document.Count"); err != nil {
		return 0, err
	}
	var n int64
	var err error
	if collection == "" {
		err = d.pool.QueryRow(ctx, "SELECT DOC_COUNT()").Scan(&n)
	} else {
		err = d.pool.QueryRow(ctx, "SELECT DOC_COUNT($1)", collection).Scan(&n)
	}
	return n, wrapErr("doc count", err)
}

// docID renders a document id the way the engine expects it to arrive over
// pgwire.
//
// Nucleus reports a parameter whose type it cannot infer as TEXT, and the
// document functions take their id in a position it does not infer. pgx then
// refuses to encode an int64 into a TEXT parameter ("cannot find encode plan"),
// so passing the id as a number made every DOC_GET/DOC_UPDATE/DOC_DELETE/
// DOC_PATH call fail outright — Document.Get has never worked against a live
// Nucleus over the wire. The engine parses a text-encoded integer id for
// exactly this reason (see `val_to_u64`), so sending the digits is the
// supported encoding, not a workaround.
//
// The underlying gap is the engine's parameter-type inference, which is not
// function-signature aware; fixing it there would let every SDK send a native
// integer.
func docID(id int64) string {
	return strconv.FormatInt(id, 10)
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

	ids, err := d.QueryDocsIn(ctx, collection, filter)
	if err != nil {
		return nil, err
	}

	var results []map[string]any
	for _, id := range ids {
		doc, err := d.GetIn(ctx, collection, id)
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

	ids, err := d.QueryDocsIn(ctx, collection, filter)
	if err != nil {
		return nil, err
	}

	var results []T
	for _, id := range ids {
		item, err := DocGetTypedIn[T](ctx, d, collection, id)
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
	ids, err := d.QueryDocsIn(ctx, collection, filter)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	var count int64
	for _, id := range ids {
		// Get current doc
		doc, err := d.GetIn(ctx, collection, id)
		if err != nil {
			return count, err
		}
		if doc == nil {
			continue
		}
		// Apply updates
		for k, v := range update {
			doc[k] = v
		}
		// Re-serialize and replace in place via DOC_UPDATE
		data, err := json.Marshal(doc)
		if err != nil {
			return count, fmt.Errorf("nucleus: doc marshal: %w", err)
		}
		var ok bool
		if collection == "" {
			err = d.pool.QueryRow(ctx, "SELECT DOC_UPDATE($1, $2)", docID(id), string(data)).Scan(&ok)
		} else {
			err = d.pool.QueryRow(ctx, "SELECT DOC_UPDATE($1, $2, $3)", collection, docID(id), string(data)).Scan(&ok)
		}
		if err != nil {
			return count, wrapErr("doc update", err)
		}
		if ok {
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
	ids, err := d.QueryDocsIn(ctx, collection, filter)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	var count int64
	for _, id := range ids {
		var ok bool
		var err error
		if collection == "" {
			err = d.pool.QueryRow(ctx, "SELECT DOC_DELETE($1)", docID(id)).Scan(&ok)
		} else {
			err = d.pool.QueryRow(ctx, "SELECT DOC_DELETE($1, $2)", collection, docID(id)).Scan(&ok)
		}
		if err != nil {
			return count, wrapErr("doc delete", err)
		}
		if ok {
			count++
		}
	}
	return count, nil
}
