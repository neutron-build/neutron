// Package scaffold generates new Neutron projects from embedded templates.
package scaffold

import (
	"embed"
	"fmt"
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
	Name       string // project name
	Module     string // go module path / package name
	NeutronVer string // neutron framework version
	Port       int    // default dev server port
	Lang       string // language name
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

	data := &TemplateData{
		Name:       name,
		Module:     name,
		NeutronVer: "0.1.0",
		Port:       defaultPort(lang),
		Lang:       string(lang),
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
