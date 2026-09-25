// Package scaffold generates new Neutron projects from embedded templates.
package scaffold

import (
	"crypto/rand"
	"embed"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/neutron-build/neutron/cli/internal/detect"
)

//go:embed all:templates
var templatesFS embed.FS

// TemplateData is passed to all scaffold templates.
type TemplateData struct {
	Name      string     // project name
	Module    string     // go module path / package name
	Ident     string     // the name as an identifier (Zig package name)
	SDK       Dependency // the Neutron SDK for this language
	CLI       Dependency // the language-specific dev tool, when it is a separate package (TypeScript)
	Port      int        // default dev server port
	Lang      string     // language name
	NextSteps []string   // commands to run after `cd <name>`; also printed by `neutron new`

	Fingerprint string // Zig package fingerprint for build.zig.zon
}

// Dependency is how a generated project obtains a Neutron package.
//
// Packages that are published under a usable name are pinned by Version. The
// others are fetched from the monorepo by Source (+ Subdir), because the bare
// name belongs to someone else on the registry (PyPI "neutron" is OpenStack,
// crates.io "neutron" is a Pulsar client) or was never published.
type Dependency struct {
	Name    string // package name as the ecosystem knows it
	Version string // registry version or constraint; empty when Source is used
	Source  string // repository or archive URL
	Subdir  string // directory inside Source that holds the package
	Hash    string // content hash of Source (Zig package manager)
	UUID    string // package UUID (Julia)
}

// Repo is the monorepo the unpublished SDKs are fetched from.
const Repo = "https://github.com/neutron-build/neutron"

// zigSDKCommit is the monorepo commit the Zig scaffold's build.zig.zon pins.
// The Zig package manager needs a content hash, so the pin is a fixed commit;
// bump both constants together (`zig fetch <archive URL>` prints the hash).
const (
	zigSDKCommit = "cf1f86a74fc7badec5fe88a35a9c25021051619d"
	zigSDKHash   = "N-V-__8AAHgD3AVMDUEHbuqIi70cy3xdrYy8uqWf4x5EIimG"
)

// sdkFor returns the SDK dependency (and, for TypeScript, the CLI package)
// that a new project in lang depends on.
func sdkFor(lang detect.Language) (sdk, cli Dependency) {
	switch lang {
	case detect.Go:
		sdk = Dependency{Name: "github.com/neutron-build/neutron/go", Version: "v0.1.0"}
	case detect.Python:
		sdk = Dependency{Name: "neutron-py", Source: "git+" + Repo + ".git", Subdir: "python"}
	case detect.TypeScript:
		sdk = Dependency{Name: "@neutron-build/core", Version: "^0.2.2"}
		cli = Dependency{Name: "@neutron-build/cli", Version: "^0.2.3"}
	case detect.Rust:
		sdk = Dependency{Name: "neutron", Source: Repo}
	case detect.Zig:
		sdk = Dependency{
			Name:   "neutron",
			Source: Repo + "/archive/" + zigSDKCommit + ".tar.gz",
			Subdir: "zig",
			Hash:   zigSDKHash,
		}
	case detect.Julia:
		sdk = Dependency{
			Name:   "NeutronJulia",
			Source: Repo,
			Subdir: "julia",
			UUID:   "3a5f8b2c-9e1d-4c7a-b6f0-2d4e8a1c3b5f",
		}
	}
	return sdk, cli
}

// NextSteps returns the commands to run inside a freshly scaffolded project,
// in order. `neutron new` prints them and the generated README lists them.
func NextSteps(lang detect.Language) []string {
	switch lang {
	case detect.Go:
		return []string{"go mod tidy", "neutron dev"}
	case detect.Python:
		return []string{"python3 -m venv .venv", ".venv/bin/pip install -e .", "neutron dev"}
	case detect.TypeScript:
		return []string{"npm install", "neutron dev"}
	case detect.Rust:
		return []string{"cargo build", "neutron dev"}
	case detect.Zig:
		// No `zig build` step: `zig` on PATH may be 0.16, which cannot compile
		// the SDK. `neutron dev` finds a 0.15 and builds before running.
		return []string{"neutron dev"}
	case detect.Julia:
		return []string{"julia --project=. -e 'using Pkg; Pkg.instantiate()'", "neutron dev"}
	default:
		return nil
	}
}

// TemplateFile maps a template path to its output path.
type TemplateFile struct {
	TemplatePath string // path within embedded FS (e.g., "templates/go/go.mod.tmpl")
	OutputPath   string // relative output path (e.g., "go.mod")
}

// Scaffolder defines the interface for language-specific scaffolding.
type Scaffolder interface {
	Files(data *TemplateData) []TemplateFile
}

// ScaffoldProject creates a new project directory with the given language templates.
func ScaffoldProject(name string, lang detect.Language) error {
	if err := validateName(name); err != nil {
		return err
	}

	if _, err := os.Stat(name); err == nil {
		return fmt.Errorf("directory %q already exists", name)
	}

	sdk, cli := sdkFor(lang)
	data := &TemplateData{
		Name:      name,
		Module:    name,
		Ident:     identifier(name),
		SDK:       sdk,
		CLI:       cli,
		Port:      defaultPort(lang),
		Lang:      string(lang),
		NextSteps: NextSteps(lang),
	}

	if lang == detect.Zig {
		fp, err := zigFingerprint(data.Ident)
		if err != nil {
			return err
		}
		data.Fingerprint = fp
	}

	scaffolder := getScaffolder(lang)
	if scaffolder == nil {
		return fmt.Errorf("unsupported language: %s", lang)
	}

	files := scaffolder.Files(data)

	// Always add neutron.toml
	files = append(files, TemplateFile{
		TemplatePath: "templates/neutron.toml.tmpl",
		OutputPath:   "neutron.toml",
	})

	for _, f := range files {
		outputPath := filepath.Join(name, f.OutputPath)

		// Create parent directories
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			return fmt.Errorf("mkdir %s: %w", filepath.Dir(outputPath), err)
		}

		// Read and execute template
		content, err := templatesFS.ReadFile(f.TemplatePath)
		if err != nil {
			return fmt.Errorf("read template %s: %w", f.TemplatePath, err)
		}

		tmpl, err := template.New(f.TemplatePath).Parse(string(content))
		if err != nil {
			return fmt.Errorf("parse template %s: %w", f.TemplatePath, err)
		}

		out, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("create %s: %w", outputPath, err)
		}

		if err := tmpl.Execute(out, data); err != nil {
			out.Close()
			return fmt.Errorf("execute template %s: %w", f.TemplatePath, err)
		}
		out.Close()
	}

	return nil
}

func getScaffolder(lang detect.Language) Scaffolder {
	switch lang {
	case detect.Python:
		return &pythonScaffolder{}
	case detect.TypeScript:
		return &typescriptScaffolder{}
	case detect.Go:
		return &goScaffolder{}
	case detect.Rust:
		return &rustScaffolder{}
	case detect.Zig:
		return &zigScaffolder{}
	case detect.Julia:
		return &juliaScaffolder{}
	default:
		return nil
	}
}

func defaultPort(lang detect.Language) int {
	switch lang {
	case detect.Python:
		return 8000
	case detect.TypeScript:
		return 3000
	case detect.Go:
		return 8080
	case detect.Rust:
		return 8080
	case detect.Zig:
		return 8080
	case detect.Julia:
		return 8080
	default:
		return 8080
	}
}

// identifier turns a project name into an identifier: hyphens become
// underscores and a leading digit gets an underscore prefix.
func identifier(name string) string {
	id := strings.ReplaceAll(name, "-", "_")
	if id != "" && id[0] >= '0' && id[0] <= '9' {
		id = "_" + id
	}
	return id
}

// zigFingerprint returns a new package fingerprint for build.zig.zon: the
// CRC-32 of the package name in the high half (what `zig build` checks the
// name against) and a random 32-bit id in the low half.
func zigFingerprint(name string) (string, error) {
	var b [4]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return "", err
		}
		id := binary.LittleEndian.Uint32(b[:])
		if id != 0 && id != 0xffffffff {
			return fmt.Sprintf("0x%016x", uint64(crc32.ChecksumIEEE([]byte(name)))<<32|uint64(id)), nil
		}
	}
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("project name cannot be empty")
	}
	for _, c := range name {
		if !isValidNameChar(c) {
			return fmt.Errorf("project name %q contains invalid character %q (use alphanumeric, hyphens, underscores)", name, string(c))
		}
	}
	if strings.HasPrefix(name, "-") || strings.HasPrefix(name, ".") {
		return fmt.Errorf("project name cannot start with %q", name[:1])
	}
	return nil
}

func isValidNameChar(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_'
}
