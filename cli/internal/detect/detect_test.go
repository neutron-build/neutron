package detect

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
)

func TestDetectPython(t *testing.T) {
	viper.Reset()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "pyproject.toml"), []byte("[project]\nname=\"test\""), 0644)

	if got := DetectLanguage(dir); got != Python {
		t.Errorf("DetectLanguage() = %v, want Python", got)
	}
}

func TestDetectGo(t *testing.T) {
	viper.Reset()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module test"), 0644)

	if got := DetectLanguage(dir); got != Go {
		t.Errorf("DetectLanguage() = %v, want Go", got)
	}
}

func TestDetectRust(t *testing.T) {
	viper.Reset()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "Cargo.toml"), []byte("[package]\nname=\"test\""), 0644)

	if got := DetectLanguage(dir); got != Rust {
		t.Errorf("DetectLanguage() = %v, want Rust", got)
	}
}

func TestDetectTypeScript(t *testing.T) {
	viper.Reset()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"dependencies":{"neutron":"^1.0"}}`), 0644)

	if got := DetectLanguage(dir); got != TypeScript {
		t.Errorf("DetectLanguage() = %v, want TypeScript", got)
	}
}

func TestDetectZig(t *testing.T) {
	viper.Reset()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "build.zig"), []byte("const std = @import(\"std\");"), 0644)

	if got := DetectLanguage(dir); got != Zig {
		t.Errorf("DetectLanguage() = %v, want Zig", got)
	}
}

func TestDetectJulia(t *testing.T) {
	viper.Reset()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "Project.toml"), []byte("name = \"Test\""), 0644)

	if got := DetectLanguage(dir); got != Julia {
		t.Errorf("DetectLanguage() = %v, want Julia", got)
	}
}

func TestDetectUnknown(t *testing.T) {
	viper.Reset()
	dir := t.TempDir()

	if got := DetectLanguage(dir); got != Unknown {
		t.Errorf("DetectLanguage() = %v, want Unknown", got)
	}
}

func TestDetectFromConfig(t *testing.T) {
	viper.Reset()
	viper.Set("project.lang", "rust")
	dir := t.TempDir()

	if got := DetectLanguage(dir); got != Rust {
		t.Errorf("DetectLanguage() = %v, want Rust", got)
	}
}

func TestParseLanguageAliases(t *testing.T) {
	tests := []struct {
		input string
		want  Language
	}{
		{"python", Python},
		{"py", Python},
		{"typescript", TypeScript},
		{"ts", TypeScript},
		{"javascript", TypeScript},
		{"js", TypeScript},
		{"go", Go},
		{"golang", Go},
		{"rust", Rust},
		{"rs", Rust},
		{"zig", Zig},
		{"julia", Julia},
		{"jl", Julia},
		{"invalid", Unknown},
	}
	for _, tt := range tests {
		if got := ParseLanguage(tt.input); got != tt.want {
			t.Errorf("ParseLanguage(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
