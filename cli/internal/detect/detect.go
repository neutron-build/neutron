// Package detect identifies the programming language of a Neutron project.
package detect

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Language represents a supported Neutron framework language.
type Language string

const (
	Python     Language = "python"
	TypeScript Language = "typescript"
	Go         Language = "go"
	Rust       Language = "rust"
	Zig        Language = "zig"
	Julia      Language = "julia"
	Unknown    Language = "unknown"
)

// AllLanguages returns all supported languages.
func AllLanguages() []Language {
	return []Language{Python, TypeScript, Go, Rust, Zig, Julia}
}

// String returns the language name.
func (l Language) String() string {
	return string(l)
}

// DisplayName returns a human-friendly name.
func (l Language) DisplayName() string {
	switch l {
	case Python:
		return "Python"
	case TypeScript:
		return "TypeScript"
	case Go:
		return "Go"
	case Rust:
		return "Rust"
	case Zig:
		return "Zig"
	case Julia:
		return "Julia"
	default:
		return "Unknown"
	}
}

// ParseLanguage converts a string to a Language, returning Unknown if invalid.
func ParseLanguage(s string) Language {
	switch s {
	case "python", "py":
		return Python
	case "typescript", "ts", "javascript", "js":
		return TypeScript
	case "go", "golang":
		return Go
	case "rust", "rs":
		return Rust
	case "zig":
		return Zig
	case "julia", "jl":
		return Julia
	default:
		return Unknown
	}
}

// DetectLanguage scans dir for marker files and returns the project language.
// First match wins. Falls back to neutron.toml config if no markers found.
func DetectLanguage(dir string) Language {
	// Check explicit config first
	if lang := viper.GetString("project.lang"); lang != "" {
		if parsed := ParseLanguage(lang); parsed != Unknown {
			return parsed
		}
	}

	// Python markers
	if fileExists(dir, "pyproject.toml") || fileExists(dir, "requirements.txt") {
		return Python
	}

	// TypeScript/JavaScript markers
	if fileExists(dir, "package.json") {
		if hasNeutronJSDep(filepath.Join(dir, "package.json")) {
			return TypeScript
		}
		// Even without neutron dep, a package.json likely means JS/TS project
		return TypeScript
	}

	// Go markers
	if fileExists(dir, "go.mod") {
		return Go
	}

	// Rust markers
	if fileExists(dir, "Cargo.toml") {
		return Rust
	}

	// Julia markers
	if fileExists(dir, "Project.toml") {
		return Julia
	}

	// Zig markers
	if fileExists(dir, "build.zig") {
		return Zig
	}

	return Unknown
}

func fileExists(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}

func hasNeutronJSDep(pkgPath string) bool {
	data, err := os.ReadFile(pkgPath)
	if err != nil {
		return false
	}
	var pkg struct {
		Dependencies    map[string]string `json:"dependencies"`
		DevDependencies map[string]string `json:"devDependencies"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return false
	}
	if _, ok := pkg.Dependencies["neutron"]; ok {
		return true
	}
	if _, ok := pkg.Dependencies["@neutron-build/neutron"]; ok {
		return true
	}
	if _, ok := pkg.DevDependencies["neutron"]; ok {
		return true
	}
	if _, ok := pkg.DevDependencies["@neutron-build/neutron"]; ok {
		return true
	}
	return false
}
