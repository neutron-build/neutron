// Example: RAG Search with Neutron
//
// Demonstrates:
//   - Nucleus Vector model for embedding storage and similarity search
//   - Nucleus FTS model for full-text search
//   - Nucleus Document model for metadata storage
//   - Combined vector + keyword search (hybrid RAG)
//   - Tiered caching for search results
//
// Run:
//
//	DATABASE_URL=postgres://localhost:5432/rag go run .
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/neutron-dev/neutron-go/neutron"
	"github.com/neutron-dev/neutron-go/neutroncache"
	"github.com/neutron-dev/neutron-go/nucleus"
)

// --- Models ---

type Document struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Content string `json:"content"`
	Source  string `json:"source"`
}

type SearchResult struct {
	ID      string  `json:"id"`
	Title   string  `json:"title"`
	Content string  `json:"content"`
	Score   float64 `json:"score"`
	Source  string  `json:"source"`
	Method  string  `json:"method"` // "vector", "fts", or "hybrid"
}

// --- Inputs ---

type IngestInput struct {
	ID        string    `json:"id" validate:"required"`
	Title     string    `json:"title" validate:"required"`
	Content   string    `json:"content" validate:"required"`
	Source    string    `json:"source"`
	Embedding []float32 `json:"embedding" validate:"required"`
}

type SearchInput struct {
	Query     string    `json:"query"`
	Embedding []float32 `json:"embedding"`
	Limit     int       `query:"limit"`
	Method    string    `query:"method"` // "vector", "fts", "hybrid"
}

type SearchResponse struct {
	Results []SearchResult `json:"results"`
	Elapsed string         `json:"elapsed"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://localhost:5432/rag"
	}

	db, err := nucleus.Connect(context.Background(), dbURL)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}

	cache := neutroncache.NewTiered(1000, nil) // L1 only for this example

	app := neutron.New(
		neutron.WithLogger(logger),
		neutron.WithLifecycle(db.LifecycleHook()),
		neutron.WithOpenAPIInfo("RAG Search API", "1.0.0"),
		neutron.WithMiddleware(
			neutron.Logger(logger),
			neutron.Recover(),
			neutron.RequestID(),
		),
	)

	api := app.Router().Group("/api")

	// Ingest document — stores content, embedding, and full-text index
	neutron.Post(api, "/documents", func(ctx context.Context, input IngestInput) (Document, error) {
		doc := Document{
			ID:      input.ID,
			Title:   input.Title,
			Content: input.Content,
			Source:  input.Source,
		}

		// Store document metadata
		db.Document().Insert(ctx, "documents", map[string]any{
			"id":      input.ID,
			"title":   input.Title,
			"content": input.Content,
			"source":  input.Source,
		})

		// Store embedding for vector search
		db.Vector().Insert(ctx, "doc_embeddings", input.ID, input.Embedding, map[string]any{
			"title": input.Title,
		})

		// Index for full-text search
		db.FTS().Index(ctx, int64(len(input.ID)), input.Title+" "+input.Content)

		// Invalidate search cache
		neutroncache.Invalidate(ctx, cache, "search:*")

		return doc, nil
	}, neutron.WithSummary("Ingest a document"), neutron.WithTags("documents"))

	// Hybrid search
	neutron.Post(api, "/search", func(ctx context.Context, input SearchInput) (SearchResponse, error) {
		start := time.Now()

		limit := input.Limit
		if limit <= 0 || limit > 50 {
			limit = 10
		}

		method := input.Method
		if method == "" {
			method = "hybrid"
		}

		var results []SearchResult

		switch method {
		case "vector":
			if len(input.Embedding) == 0 {
				return SearchResponse{}, neutron.ErrBadRequest("embedding required for vector search")
			}
			vResults, err := db.Vector().Search(ctx, "doc_embeddings", input.Embedding,
				nucleus.WithLimit(limit))
			if err != nil {
				return SearchResponse{}, err
			}
			for _, vr := range vResults {
				title, _ := vr.Item["title"].(string)
				results = append(results, SearchResult{
					Title:  title,
					Score:  vr.Distance,
					Method: "vector",
				})
			}

		case "fts":
			if input.Query == "" {
				return SearchResponse{}, neutron.ErrBadRequest("query required for FTS search")
			}
			ftsResults, err := db.FTS().Search(ctx, input.Query,
				nucleus.WithFTSLimit(int64(limit)))
			if err != nil {
				return SearchResponse{}, err
			}
			for _, fr := range ftsResults {
				results = append(results, SearchResult{
					ID:     fmt.Sprintf("%d", fr.DocID),
					Score:  float64(fr.Score),
					Method: "fts",
				})
			}

		case "hybrid":
			// Combine vector + FTS results
			if len(input.Embedding) > 0 {
				vResults, _ := db.Vector().Search(ctx, "doc_embeddings", input.Embedding,
					nucleus.WithLimit(limit))
				for _, vr := range vResults {
					title, _ := vr.Item["title"].(string)
					results = append(results, SearchResult{
						Title:  title,
						Score:  vr.Distance,
						Method: "vector",
					})
				}
			}

			if input.Query != "" {
				ftsResults, _ := db.FTS().Search(ctx, input.Query,
					nucleus.WithFTSLimit(int64(limit)))
				for _, fr := range ftsResults {
					results = append(results, SearchResult{
						ID:     fmt.Sprintf("%d", fr.DocID),
						Score:  float64(fr.Score),
						Method: "fts",
					})
				}
			}

		default:
			return SearchResponse{}, neutron.ErrBadRequest("method must be vector, fts, or hybrid")
		}

		return SearchResponse{
			Results: results,
			Elapsed: time.Since(start).String(),
		}, nil
	}, neutron.WithSummary("Search documents"), neutron.WithTags("search"))

	_ = cache // used in production for caching search results

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = "8080"
	}
	fmt.Println("RAG Search API starting on", addr)
	app.Run(":" + addr)
}
