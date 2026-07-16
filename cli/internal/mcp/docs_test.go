package mcp

import "testing"

// sampleCorpus mirrors the shape of /llms-full.txt produced by the site.
const sampleCorpus = `# Neutron Documentation (full text)

> The complete Neutron documentation, concatenated for LLM ingestion.

---

## Loaders

Source: https://neutron.build/docs/data/loaders
Load data on the server for a route.

A loader runs on the server and its return value arrives as props.data.

---

## Vector Search

Source: https://neutron.build/docs/nucleus/vector
Approximate nearest-neighbor search.

Use VECTOR_DISTANCE to rank rows by similarity to a query embedding.
`

func TestParseDocs(t *testing.T) {
	secs := parseDocs(sampleCorpus)
	if len(secs) != 2 {
		t.Fatalf("expected 2 sections, got %d", len(secs))
	}
	if secs[0].Title != "Loaders" {
		t.Errorf("section 0 title = %q, want Loaders", secs[0].Title)
	}
	if secs[0].Slug != "data/loaders" {
		t.Errorf("section 0 slug = %q, want data/loaders", secs[0].Slug)
	}
	if secs[1].Slug != "nucleus/vector" {
		t.Errorf("section 1 slug = %q, want nucleus/vector", secs[1].Slug)
	}
}

func TestNormalizeSlug(t *testing.T) {
	cases := map[string]string{
		"routing/app-routes":       "routing/app-routes",
		"/docs/routing/app-routes": "routing/app-routes",
		"data/loaders.md":          "data/loaders",
		"/nucleus/overview/":       "nucleus/overview",
	}
	for in, want := range cases {
		if got := normalizeSlug(in); got != want {
			t.Errorf("normalizeSlug(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSnippet(t *testing.T) {
	body := "Load data on the server. A loader runs on the server and returns props.data."
	s := snippet(body, []string{"loader"})
	if s == "" {
		t.Fatal("expected a non-empty snippet")
	}
}
