package studio

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"testing/fstest"
)

// studioSourceHash mirrors sourceHash in studio/scripts/embed-manifest.mjs:
// each build input contributes "<path>\x00<sha256 hex of its bytes with CRLF
// normalized to LF>\n" in byte order of the posix path.
func studioSourceHash(inputs map[string][]byte) string {
	paths := make([]string, 0, len(inputs))
	for p := range inputs {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		sum := sha256.Sum256(bytes.ReplaceAll(inputs[p], []byte("\r\n"), []byte("\n")))
		fmt.Fprintf(h, "%s\x00%s\n", p, hex.EncodeToString(sum[:]))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// studioBuildInputs selects the build inputs under a Studio checkout the way
// embed-manifest.mjs does: the root config files, and every file under src/
// and public/ except test files and dot-segments.
func studioBuildInputs(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	inputs := map[string][]byte{}
	for _, name := range []string{"index.html", "package.json", "package-lock.json", "tsconfig.json", "vite.config.ts"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err == nil {
			inputs[name] = b
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	for _, tree := range []string{"src", "public"} {
		root := filepath.Join(dir, tree)
		if _, err := os.Stat(root); os.IsNotExist(err) {
			continue
		}
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !d.Type().IsRegular() {
				return err
			}
			rel, err := filepath.Rel(dir, p)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			segments := strings.Split(rel, "/")
			for _, s := range segments {
				if strings.HasPrefix(s, ".") {
					return nil
				}
			}
			base := segments[len(segments)-1]
			if strings.Contains(base, ".test.") || base == "test-setup.ts" {
				return nil
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			inputs[rel] = b
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return inputs
}

func embeddedDist(t *testing.T) fs.FS {
	t.Helper()
	sub, err := fs.Sub(Dist, "dist")
	if err != nil {
		t.Fatal(err)
	}
	return sub
}

// The binary's embedded Studio is exactly one build: every file its
// manifest lists and nothing else.
func TestEmbeddedStudioMatchesManifest(t *testing.T) {
	m, err := VerifyEmbed(embeddedDist(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) < 2 {
		t.Fatalf("embedded Studio manifest lists %d files", len(m.Files))
	}
}

// V17: a stale cli/internal/studio/dist — built from sources other than the
// checkout's — fails instead of shipping an older UI.
func TestEmbeddedStudioIsCurrentBuild(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "studio")
	if _, err := os.Stat(filepath.Join(dir, "scripts", "embed-manifest.mjs")); err != nil {
		t.Skip("studio sources are not in this checkout")
	}
	m, err := VerifyEmbed(embeddedDist(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := studioSourceHash(studioBuildInputs(t, dir)); got != m.Source {
		t.Fatalf("embedded Studio was built from other sources (manifest %s, checkout %s): stale cli/internal/studio/dist — run npm run build in studio/ and copy dist/ to cli/internal/studio/dist", m.Source, got)
	}
}

// The Go and Node source hashes agree: the constant is sourceHash() of the
// same tree computed by studio/scripts/embed-manifest.mjs. It covers CRLF
// normalization, excluded test/dot files, a non-input root file and a
// non-ASCII path.
func TestStudioSourceHashMatchesBuildScript(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"index.html":        "<html>\r\n</html>\n",
		"package.json":      "{}\n",
		"vitest.config.ts":  "export default {}\n",
		"src/main.tsx":      "export {}\n",
		"src/a.test.ts":     "test\n",
		"src/test-setup.ts": "setup\n",
		"src/.hidden/x.ts":  "hidden\n",
		"src/zeta/é.ts":     "x",
		"public/logo.svg":   "<svg/>",
	}
	for rel, content := range files {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const want = "c92518df3804f21c9bc18531d5904cc009c47d4c8e1add49df0e41c95b015085"
	if got := studioSourceHash(studioBuildInputs(t, dir)); got != want {
		t.Fatalf("source hash = %s, want %s (embed-manifest.mjs)", got, want)
	}
}

func sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func manifestFS(files map[string]string, manifest string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for p, c := range files {
		fsys[p] = &fstest.MapFile{Data: []byte(c)}
	}
	if manifest != "" {
		fsys[embedManifestName] = &fstest.MapFile{Data: []byte(manifest)}
	}
	return fsys
}

func TestVerifyEmbedRejectsMismatchedTrees(t *testing.T) {
	index, app := "<html></html>", "console.log(1)"
	good := fmt.Sprintf(`{"files":{"assets/app.js":%q,"index.html":%q},"format":"neutron-studio-embed/1","source":"%s"}`, sum(app), sum(index), strings.Repeat("a", 64))
	files := map[string]string{"index.html": index, "assets/app.js": app}

	if _, err := VerifyEmbed(manifestFS(files, good)); err != nil {
		t.Fatalf("matching tree rejected: %v", err)
	}
	cases := []struct {
		name     string
		files    map[string]string
		manifest string
		want     string
	}{
		{"no manifest (pre-manifest dist)", files, "", "has no studio-manifest.json"},
		{"manifest not JSON", files, "{", "not valid JSON"},
		{"unknown format", files, strings.Replace(good, "neutron-studio-embed/1", "neutron-studio-embed/2", 1), "manifest format"},
		{"modified file", map[string]string{"index.html": index, "assets/app.js": "console.log(2)"}, good, "differs from the manifest: assets/app.js"},
		{"extra file", map[string]string{"index.html": index, "assets/app.js": app, "assets/old.js": "x"}, good, "not in the manifest: assets/old.js"},
		{"missing file", map[string]string{"index.html": index}, good, "missing: assets/app.js"},
		{"no index.html listed", map[string]string{"assets/app.js": app}, fmt.Sprintf(`{"files":{"assets/app.js":%q},"format":"neutron-studio-embed/1","source":""}`, sum(app)), "lists no index.html"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := VerifyEmbed(manifestFS(c.files, c.manifest))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %q", err, c.want)
			}
		})
	}
}

// The server refuses to serve a Studio tree that does not match its
// manifest, and serves a matching one with the SPA fallback.
func TestSPAHandlerVerifiesTheEmbed(t *testing.T) {
	index := "<html>studio</html>"
	manifest := fmt.Sprintf(`{"files":{"index.html":%q},"format":"neutron-studio-embed/1","source":""}`, sum(index))
	if _, err := spaHandler(manifestFS(map[string]string{"index.html": "<html>older</html>"}, manifest)); err == nil {
		t.Fatal("spaHandler served a tree that differs from its manifest")
	}
	h, err := spaHandler(manifestFS(map[string]string{"index.html": index}, manifest))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/", "/c/x/t/public/users"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK || rec.Body.String() != index {
			t.Fatalf("GET %s = %d %q", path, rec.Code, rec.Body.String())
		}
	}
}
