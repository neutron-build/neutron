package studio

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

// Dist contains the compiled Studio frontend (studio/dist → cli/internal/studio/dist).
// Run `npm run build` in the studio/ directory then copy dist/ here before building the CLI.
//
//go:embed all:dist
var Dist embed.FS

const (
	embedManifestName   = "studio-manifest.json"
	embedManifestFormat = "neutron-studio-embed/1"
)

// EmbedManifest is the build manifest `npm run build` writes into
// studio/dist (studio/scripts/embed-manifest.mjs): the hash of the Studio
// sources the build came from and the sha256 of every file it produced.
type EmbedManifest struct {
	Format string            `json:"format"`
	Source string            `json:"source"`
	Files  map[string]string `json:"files"`
}

// VerifyEmbed checks that dist holds exactly the files its manifest lists,
// byte for byte, so the CLI never serves a partial copy, a hand-edited file
// or a dist tree without a manifest. Whether the manifest's sources are the
// checkout's current ones is checked by this package's tests.
func VerifyEmbed(dist fs.FS) (*EmbedManifest, error) {
	const rebuild = "rebuild Studio (npm run build in studio/) and copy its dist/ to cli/internal/studio/dist before building the CLI"
	raw, err := fs.ReadFile(dist, embedManifestName)
	if err != nil {
		return nil, fmt.Errorf("embedded Studio has no %s: %s", embedManifestName, rebuild)
	}
	var m EmbedManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("embedded Studio %s is not valid JSON (%v): %s", embedManifestName, err, rebuild)
	}
	if m.Format != embedManifestFormat {
		return nil, fmt.Errorf("embedded Studio manifest format %q, want %q: %s", m.Format, embedManifestFormat, rebuild)
	}
	if _, ok := m.Files["index.html"]; !ok {
		return nil, fmt.Errorf("embedded Studio manifest lists no index.html: %s", rebuild)
	}

	var problems []string
	seen := map[string]bool{}
	err = fs.WalkDir(dist, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || p == embedManifestName {
			return nil
		}
		seen[p] = true
		want, listed := m.Files[p]
		if !listed {
			problems = append(problems, "not in the manifest: "+p)
			return nil
		}
		b, err := fs.ReadFile(dist, p)
		if err != nil {
			return err
		}
		if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != want {
			problems = append(problems, "differs from the manifest: "+p)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read embedded Studio: %w", err)
	}
	for p := range m.Files {
		if !seen[p] {
			problems = append(problems, "missing: "+p)
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return nil, fmt.Errorf("embedded Studio does not match its build manifest (%s): %s", strings.Join(problems, "; "), rebuild)
	}
	return &m, nil
}
