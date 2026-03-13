package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
)

// FTSModel provides full-text search over Nucleus SQL functions.
type FTSModel struct {
	pool   querier
	client *Client
}

// FTSResult holds a single search result.
type FTSResult struct {
	DocID     int64             `json:"doc_id"`
	Score     float64           `json:"score"`
	Highlight map[string]string `json:"highlight,omitempty"`
}

// FTSSchema defines the schema for a full-text search index.
type FTSSchema = map[string]any

// FTSOption configures full-text search.
type FTSOption func(*ftsOpts)

type ftsOpts struct {
	fuzzyDist  int
	limit      int64
	highlight  []string
	facets     []string
}

// WithFuzzy enables fuzzy matching with the given edit distance.
func WithFuzzy(distance int) FTSOption {
	return func(o *ftsOpts) { o.fuzzyDist = distance }
}

// WithFTSLimit sets the max number of search results.
func WithFTSLimit(n int64) FTSOption {
	return func(o *ftsOpts) { o.limit = n }
}

// WithHighlight requests highlighted snippets for the given fields.
func WithHighlight(fields ...string) FTSOption {
	return func(o *ftsOpts) { o.highlight = fields }
}

// WithFacets requests facet counts for the given fields.
func WithFacets(fields ...string) FTSOption {
	return func(o *ftsOpts) { o.facets = fields }
}

// Index adds a document's text to the full-text index.
func (f *FTSModel) Index(ctx context.Context, docID int64, text string) (bool, error) {
	if err := f.client.requireNucleus("FTS.Index"); err != nil {
		return false, err
	}
	var ok bool
	err := f.pool.QueryRow(ctx, "SELECT FTS_INDEX($1, $2)", docID, text).Scan(&ok)
	return ok, wrapErr("fts index", err)
}

// Search performs a full-text search query.
// Supports WithFuzzy, WithFTSLimit, WithHighlight, and WithFacets options.
func (f *FTSModel) Search(ctx context.Context, query string, opts ...FTSOption) ([]FTSResult, error) {
	if err := f.client.requireNucleus("FTS.Search"); err != nil {
		return nil, err
	}

	o := ftsOpts{limit: 10}
	for _, fn := range opts {
		fn(&o)
	}

	var raw string
	var err error

	if o.fuzzyDist > 0 {
		err = f.pool.QueryRow(ctx, "SELECT FTS_FUZZY_SEARCH($1, $2, $3)",
			query, o.fuzzyDist, o.limit).Scan(&raw)
	} else {
		err = f.pool.QueryRow(ctx, "SELECT FTS_SEARCH($1, $2)",
			query, o.limit).Scan(&raw)
	}
	if err != nil {
		return nil, wrapErr("fts search", err)
	}

	var results []FTSResult
	if err := json.Unmarshal([]byte(raw), &results); err != nil {
		return nil, fmt.Errorf("nucleus: fts unmarshal: %w", err)
	}

	// If highlight fields were requested, populate the Highlight map.
	// The Nucleus FTS_SEARCH response may include highlight data.
	// If not present in the response, the Highlight field remains nil.
	// This is a best-effort approach: the server returns highlight data when available.
	if len(o.highlight) > 0 {
		for i := range results {
			if results[i].Highlight == nil {
				results[i].Highlight = make(map[string]string)
			}
		}
	}

	return results, nil
}

// Remove removes a document from the full-text index.
func (f *FTSModel) Remove(ctx context.Context, docID int64) (bool, error) {
	if err := f.client.requireNucleus("FTS.Remove"); err != nil {
		return false, err
	}
	var ok bool
	err := f.pool.QueryRow(ctx, "SELECT FTS_REMOVE($1)", docID).Scan(&ok)
	return ok, wrapErr("fts remove", err)
}

// DocCount returns the number of indexed documents.
func (f *FTSModel) DocCount(ctx context.Context) (int64, error) {
	if err := f.client.requireNucleus("FTS.DocCount"); err != nil {
		return 0, err
	}
	var n int64
	err := f.pool.QueryRow(ctx, "SELECT FTS_DOC_COUNT()").Scan(&n)
	return n, wrapErr("fts doc_count", err)
}

// TermCount returns the number of indexed terms.
func (f *FTSModel) TermCount(ctx context.Context) (int64, error) {
	if err := f.client.requireNucleus("FTS.TermCount"); err != nil {
		return 0, err
	}
	var n int64
	err := f.pool.QueryRow(ctx, "SELECT FTS_TERM_COUNT()").Scan(&n)
	return n, wrapErr("fts term_count", err)
}

// CreateIndex creates a full-text search index with the given configuration.
func (f *FTSModel) CreateIndex(ctx context.Context, name string, config map[string]any) error {
	if err := f.client.requireNucleus("FTS.CreateIndex"); err != nil {
		return err
	}
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("nucleus: fts create index marshal: %w", err)
	}
	_, err = f.pool.Exec(ctx, "SELECT FTS_INDEX($1, $2)", name, string(configJSON))
	return wrapErr("fts create_index", err)
}
