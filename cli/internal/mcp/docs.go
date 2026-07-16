package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// DocsBaseURL is the site whose published documentation the docs tools query.
// Overridable for testing or a self-hosted docs mirror.
var DocsBaseURL = "https://neutron.build"

// docSection is one documentation page parsed from /llms-full.txt.
type docSection struct {
	Title string
	Slug  string
	URL   string
	Body  string
}

var (
	docsCacheMu   sync.Mutex
	docsCache     []docSection
	docsCacheTime time.Time
)

// docsSep is the delimiter the site's llms-full.txt uses between pages.
// Keeping it specific (heading marker included) avoids splitting on a
// horizontal rule inside a page body.
const docsSep = "\n---\n\n## "

// fetchDocs downloads and parses /llms-full.txt into per-page sections,
// cached for 10 minutes so repeated tool calls don't refetch.
func fetchDocs(ctx context.Context) ([]docSection, error) {
	docsCacheMu.Lock()
	defer docsCacheMu.Unlock()

	if docsCache != nil && time.Since(docsCacheTime) < 10*time.Minute {
		return docsCache, nil
	}

	url := strings.TrimRight(DocsBaseURL, "/") + "/llms-full.txt"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{Timeout: 15 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch docs from %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch docs from %s: HTTP %d", url, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	sections := parseDocs(string(data))
	if len(sections) == 0 {
		return nil, fmt.Errorf("no documentation sections parsed from %s", url)
	}
	docsCache = sections
	docsCacheTime = time.Now()
	return sections, nil
}

// parseDocs splits the llms-full.txt corpus into sections. The first chunk is
// the file header (skipped); each remaining chunk starts with the page title.
func parseDocs(corpus string) []docSection {
	parts := strings.Split(corpus, docsSep)
	if len(parts) < 2 {
		return nil
	}
	sections := make([]docSection, 0, len(parts)-1)
	for _, part := range parts[1:] {
		lines := strings.SplitN(part, "\n", 2)
		title := strings.TrimSpace(lines[0])
		body := ""
		if len(lines) > 1 {
			body = strings.TrimSpace(lines[1])
		}
		sec := docSection{Title: title, Body: body}
		for _, l := range strings.Split(body, "\n") {
			if strings.HasPrefix(l, "Source: ") {
				sec.URL = strings.TrimSpace(strings.TrimPrefix(l, "Source: "))
				if i := strings.Index(sec.URL, "/docs/"); i >= 0 {
					sec.Slug = sec.URL[i+len("/docs/"):]
				}
				break
			}
		}
		sections = append(sections, sec)
	}
	return sections
}

// normalizeSlug strips a leading slash, a /docs/ prefix, and a .md suffix so
// callers can pass "routing/app-routes", "/docs/routing/app-routes", etc.
func normalizeSlug(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "/")
	s = strings.TrimPrefix(s, "docs/")
	s = strings.TrimSuffix(s, ".md")
	s = strings.TrimSuffix(s, "/")
	return s
}

// handleSearchDocs returns documentation pages matching a free-text query,
// ranked by how often the terms appear (title matches weighted heavily).
func handleSearchDocs(ctx context.Context, _ *db.Client, args map[string]any) (string, error) {
	query, _ := args["query"].(string)
	query = strings.TrimSpace(query)
	if query == "" {
		return "", fmt.Errorf("query argument is required")
	}
	limit := 8
	if n, ok := args["limit"].(float64); ok && n > 0 {
		limit = int(n)
	}

	sections, err := fetchDocs(ctx)
	if err != nil {
		return "", err
	}

	terms := strings.Fields(strings.ToLower(query))
	type scored struct {
		sec   docSection
		score int
	}
	var hits []scored
	for _, sec := range sections {
		titleLC := strings.ToLower(sec.Title)
		bodyLC := strings.ToLower(sec.Body)
		score := 0
		for _, t := range terms {
			score += 5 * strings.Count(titleLC, t)
			score += strings.Count(bodyLC, t)
		}
		if score > 0 {
			hits = append(hits, scored{sec, score})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })

	results := make([]map[string]any, 0, limit)
	for i, h := range hits {
		if i >= limit {
			break
		}
		results = append(results, map[string]any{
			"title":   h.sec.Title,
			"url":     h.sec.URL,
			"slug":    h.sec.Slug,
			"snippet": snippet(h.sec.Body, terms),
		})
	}
	return marshalJSON(map[string]any{"query": query, "count": len(results), "results": results})
}

// handleGetDoc returns the full markdown of a single documentation page by slug.
func handleGetDoc(ctx context.Context, _ *db.Client, args map[string]any) (string, error) {
	slug, _ := args["slug"].(string)
	slug = normalizeSlug(slug)
	if slug == "" {
		return "", fmt.Errorf("slug argument is required (e.g. \"routing/app-routes\")")
	}

	sections, err := fetchDocs(ctx)
	if err != nil {
		return "", err
	}

	for _, sec := range sections {
		if sec.Slug == slug {
			return marshalJSON(map[string]any{
				"title":    sec.Title,
				"url":      sec.URL,
				"slug":     sec.Slug,
				"markdown": sec.Body,
			})
		}
	}

	// Not found — suggest the closest slugs so the agent can retry.
	var suggestions []string
	for _, sec := range sections {
		if sec.Slug != "" && strings.Contains(sec.Slug, slug) {
			suggestions = append(suggestions, sec.Slug)
		}
	}
	return "", fmt.Errorf("no doc with slug %q; try search_docs, or one of: %s", slug, strings.Join(suggestions, ", "))
}

// snippet returns a short excerpt of body around the first matching term.
func snippet(body string, terms []string) string {
	flat := strings.Join(strings.Fields(body), " ")
	lc := strings.ToLower(flat)
	idx := -1
	for _, t := range terms {
		if i := strings.Index(lc, t); i >= 0 && (idx == -1 || i < idx) {
			idx = i
		}
	}
	if idx == -1 {
		idx = 0
	}
	start := idx - 80
	if start < 0 {
		start = 0
	}
	end := start + 240
	if end > len(flat) {
		end = len(flat)
	}
	out := flat[start:end]
	if start > 0 {
		out = "..." + out
	}
	if end < len(flat) {
		out = out + "..."
	}
	return out
}

func marshalJSON(v any) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}
