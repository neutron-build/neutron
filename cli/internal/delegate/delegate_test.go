package delegate

import (
	"testing"

	"github.com/neutron-build/neutron/cli/internal/detect"
)

func TestRunDevServerUnsupportedLanguage(t *testing.T) {
	err := RunDevServer(detect.Unknown, "/tmp")
	if err == nil {
		t.Fatal("expected error for unsupported language")
	}
}

func TestRunDevServerAllLanguagesHaveHandlers(t *testing.T) {
	// Verify each supported language has a handler (won't actually run, just checks the switch)
	languages := detect.AllLanguages()

	for _, lang := range languages {
		// Each language should be handled without returning "unsupported language" error.
		// We can't actually run dev servers, but we verify the function doesn't
		// fall through to the default case. The actual error will be about
		// the binary not being found, not about unsupported language.
		err := RunDevServer(lang, "/nonexistent-dir-for-test")
		if err != nil && err.Error() == "unsupported language: "+string(lang) {
			t.Errorf("RunDevServer(%q) returned unsupported language error", lang)
		}
	}
}

func TestRunDevServerPythonCommand(t *testing.T) {
	// This tests that the function recognizes Python without panicking.
	// It will fail with a command execution error but not a panic.
	err := RunDevServer(detect.Python, t.TempDir())
	if err == nil {
		t.Log("Python dev server started unexpectedly (uvicorn may be installed)")
	}
	// No panic = pass
}

func TestRunDevServerGoCommand(t *testing.T) {
	// Tests that Go delegation doesn't panic
	err := RunDevServer(detect.Go, t.TempDir())
	if err == nil {
		t.Log("Go dev server started unexpectedly")
	}
}

func TestRunDevServerTypeScriptCommand(t *testing.T) {
	err := RunDevServer(detect.TypeScript, t.TempDir())
	if err == nil {
		t.Log("TypeScript dev server started unexpectedly")
	}
}

func TestRunDevServerRustCommand(t *testing.T) {
	err := RunDevServer(detect.Rust, t.TempDir())
	if err == nil {
		t.Log("Rust dev server started unexpectedly")
	}
}

func TestRunDevServerZigCommand(t *testing.T) {
	err := RunDevServer(detect.Zig, t.TempDir())
	if err == nil {
		t.Log("Zig dev server started unexpectedly")
	}
}

func TestRunDevServerJuliaCommand(t *testing.T) {
	err := RunDevServer(detect.Julia, t.TempDir())
	if err == nil {
		t.Log("Julia dev server started unexpectedly")
	}
}
