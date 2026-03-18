// Package delegate handles delegating commands to language-specific tooling.
package delegate

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/neutron-build/neutron/cli/internal/detect"
)

// RunDevServer delegates to the language-appropriate dev server.
func RunDevServer(lang detect.Language, dir string) error {
	switch lang {
	case detect.Python:
		return runPythonDev(dir)
	case detect.TypeScript:
		return runTypeScriptDev(dir)
	case detect.Go:
		return runGoDev(dir)
	case detect.Rust:
		return runRustDev(dir)
	case detect.Zig:
		return runZigDev(dir)
	case detect.Julia:
		return runJuliaDev(dir)
	default:
		return fmt.Errorf("unsupported language: %s", lang)
	}
}

func runPythonDev(dir string) error {
	// Try uvicorn first (most common for Neutron Python)
	if _, err := exec.LookPath("uvicorn"); err == nil {
		return runCmd(dir, "uvicorn", "app.main:app", "--reload", "--port", "8000")
	}
	// Fallback to python -m
	return runCmd(dir, "python3", "-m", "uvicorn", "app.main:app", "--reload", "--port", "8000")
}

func runTypeScriptDev(dir string) error {
	// Use npx to invoke the local neutron-ts binary from the project's node_modules
	if _, err := exec.LookPath("npx"); err == nil {
		return runCmd(dir, "npx", "neutron-ts", "dev")
	}
	return runCmd(dir, "npm", "run", "dev")
}

func runGoDev(dir string) error {
	// Try air (hot-reload) first
	if _, err := exec.LookPath("air"); err == nil {
		return runCmd(dir, "air")
	}
	// Fallback to direct go run
	return runCmd(dir, "go", "run", "./cmd/server")
}

func runRustDev(dir string) error {
	// Try cargo-watch first
	if _, err := exec.LookPath("cargo-watch"); err == nil {
		return runCmd(dir, "cargo", "watch", "-x", "run")
	}
	return runCmd(dir, "cargo", "run")
}

func runZigDev(dir string) error {
	return runCmd(dir, "zig", "build", "run")
}

func runJuliaDev(dir string) error {
	return runCmd(dir, "julia", "--project=.", "src/App.jl")
}

func runCmd(dir string, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}
