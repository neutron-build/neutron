// Package delegate handles delegating commands to language-specific tooling.
package delegate

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/neutron-build/neutron/cli/internal/detect"
)

// RunDevServer delegates to the language-appropriate dev server.
func RunDevServer(lang detect.Language, dir string) error {
	cmd, err := DevCommand(lang, dir)
	if err != nil {
		return err
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

// DevCommand returns the command `neutron dev` runs for a project in dir,
// without starting it. The server reads NEUTRON_HOST / NEUTRON_PORT from the
// environment (FRAMEWORK_CONTRACT §6); where the language's dev tool does not,
// they are translated into its flags here.
func DevCommand(lang detect.Language, dir string) (*exec.Cmd, error) {
	switch lang {
	case detect.Python:
		return pythonDev(dir)
	case detect.TypeScript:
		return typeScriptDev(dir)
	case detect.Go:
		return goDev(dir), nil
	case detect.Rust:
		return rustDev(dir), nil
	case detect.Zig:
		return zigDev(dir)
	case detect.Julia:
		return juliaDev(dir)
	default:
		return nil, fmt.Errorf("unsupported language: %s", lang)
	}
}

// pythonDev runs the SDK's own dev server (`python -m neutron dev`), which
// reads NEUTRON_HOST / NEUTRON_PORT (default port 8000). The bind host
// defaults to loopback for local development.
func pythonDev(dir string) (*exec.Cmd, error) {
	py := PythonFor(dir)
	if py == "" {
		return nil, fmt.Errorf("python3 not found — install Python 3.11+ and run: python3 -m venv .venv && .venv/bin/pip install -e .")
	}
	cmd := command(dir, py, "-m", "neutron", "dev", "app.main:app")
	if os.Getenv("NEUTRON_HOST") == "" {
		cmd.Env = append(cmd.Env, "NEUTRON_HOST=127.0.0.1")
	}
	return cmd, nil
}

// PythonFor returns the interpreter for the project in dir: its .venv when
// present, otherwise python3 (or python) from PATH. Empty if none is found.
func PythonFor(dir string) string {
	venv := filepath.Join(dir, ".venv", "bin", "python")
	if runtime.GOOS == "windows" {
		venv = filepath.Join(dir, ".venv", "Scripts", "python.exe")
	}
	if _, err := os.Stat(venv); err == nil {
		return venv
	}
	for _, name := range []string{"python3", "python"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return ""
}

// typeScriptDev runs the project's own neutron-ts (from @neutron-build/cli).
// It is never fetched on demand: an uninstalled project is an error, not a
// registry lookup for whatever package happens to own that name.
func typeScriptDev(dir string) (*exec.Cmd, error) {
	bin := filepath.Join(dir, "node_modules", ".bin", "neutron-ts")
	if runtime.GOOS == "windows" {
		bin += ".cmd"
	}
	if _, err := os.Stat(bin); err != nil {
		return nil, fmt.Errorf("neutron-ts not found in node_modules/.bin — run npm install (the project must depend on @neutron-build/cli)")
	}
	args := []string{"dev"}
	if port := os.Getenv("NEUTRON_PORT"); port != "" {
		args = append(args, "--port", port)
	}
	if host := os.Getenv("NEUTRON_HOST"); host != "" {
		args = append(args, "--host", host)
	}
	return command(dir, bin, args...), nil
}

func goDev(dir string) *exec.Cmd {
	// Try air (hot-reload) first
	if _, err := exec.LookPath("air"); err == nil {
		return command(dir, "air")
	}
	return command(dir, "go", "run", "./cmd/server")
}

func rustDev(dir string) *exec.Cmd {
	if _, err := exec.LookPath("cargo-watch"); err == nil {
		return command(dir, "cargo", "watch", "-x", "run")
	}
	return command(dir, "cargo", "run")
}

func zigDev(dir string) (*exec.Cmd, error) {
	zig, err := Zig()
	if err != nil {
		return nil, err
	}
	return command(dir, zig, "build", "run"), nil
}

// zigCandidates are checked in order; the Homebrew kegs cover a machine whose
// default `zig` is a newer release.
var zigCandidates = []string{"zig", "/opt/homebrew/opt/zig@0.15/bin/zig", "/usr/local/opt/zig@0.15/bin/zig"}

// Zig returns a Zig 0.15.x binary. The Zig SDK pins 0.15.2
// (zig/build.zig.zon); 0.16 removed std APIs it uses and cannot compile it.
func Zig() (string, error) {
	var found []string
	for _, name := range zigCandidates {
		path, err := exec.LookPath(name)
		if err != nil {
			continue
		}
		out, err := exec.Command(path, "version").Output()
		if err != nil {
			continue
		}
		version := strings.TrimSpace(string(out))
		if strings.HasPrefix(version, "0.15.") {
			return path, nil
		}
		found = append(found, path+" is "+version)
	}
	if len(found) == 0 {
		return "", fmt.Errorf("zig not found — the Neutron Zig SDK needs Zig 0.15.x (0.15.2)")
	}
	return "", fmt.Errorf("the Neutron Zig SDK needs Zig 0.15.x (0.15.2); found %s", strings.Join(found, ", "))
}

// juliaDev runs the project's script. The Julia SDK is a database client with
// no HTTP layer, so there is no server to start.
func juliaDev(dir string) (*exec.Cmd, error) {
	for _, script := range []string{"src/main.jl", "src/App.jl"} {
		if _, err := os.Stat(filepath.Join(dir, script)); err == nil {
			return command(dir, "julia", "--project=.", script), nil
		}
	}
	return nil, fmt.Errorf("no src/main.jl to run — the Julia SDK is a database client with no HTTP server, so `neutron dev` runs the project's script")
}

func command(dir string, name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	return cmd
}
