// Package doctor runs environment diagnostics.
package doctor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/neutron-build/neutron/cli/internal/config"
)

// CheckResult holds the result of a single diagnostic check.
type CheckResult struct {
	Name    string
	Status  Status
	Detail  string
	Version string
}

// Status represents a check result status.
type Status int

const (
	Pass Status = iota
	Warn
	Fail
)

// RunAll executes all diagnostic checks and returns results.
func RunAll() []CheckResult {
	var results []CheckResult

	results = append(results, checkRuntime("Go", "go", "version")...)
	results = append(results, checkRuntime("Python", "python3", "--version")...)
	results = append(results, checkRuntime("Node.js", "node", "--version")...)
	results = append(results, checkRuntime("Rust", "rustc", "--version")...)
	results = append(results, checkRuntime("Zig", "zig", "version")...)
	results = append(results, checkRuntime("Julia", "julia", "--version")...)
	results = append(results, checkGit())
	results = append(results, checkNucleusBinary())
	results = append(results, checkDatabase())
	results = append(results, checkConfigFile())

	return results
}

func checkRuntime(name, binary string, args ...string) []CheckResult {
	path, err := exec.LookPath(binary)
	if err != nil {
		return []CheckResult{{
			Name:   name,
			Status: Warn,
			Detail: "not installed",
		}}
	}

	out, err := exec.Command(path, args...).Output()
	if err != nil {
		return []CheckResult{{
			Name:   name,
			Status: Warn,
			Detail: fmt.Sprintf("found at %s but could not get version", path),
		}}
	}

	ver := strings.TrimSpace(string(out))
	// Extract just the version number for cleaner display
	if idx := strings.LastIndex(ver, " "); idx >= 0 {
		ver = ver[idx+1:]
	}

	return []CheckResult{{
		Name:    name,
		Status:  Pass,
		Detail:  path,
		Version: strings.TrimSpace(ver),
	}}
}

func checkGit() CheckResult {
	path, err := exec.LookPath("git")
	if err != nil {
		return CheckResult{Name: "Git", Status: Fail, Detail: "not installed"}
	}
	out, _ := exec.Command(path, "--version").Output()
	ver := strings.TrimSpace(string(out))
	ver = strings.TrimPrefix(ver, "git version ")
	return CheckResult{Name: "Git", Status: Pass, Detail: path, Version: ver}
}

func checkNucleusBinary() CheckResult {
	home, err := os.UserHomeDir()
	if err != nil {
		return CheckResult{Name: "Nucleus binary", Status: Warn, Detail: "could not find home directory"}
	}

	binDir := filepath.Join(home, ".neutron", "bin")
	entries, err := os.ReadDir(binDir)
	if err != nil || len(entries) == 0 {
		return CheckResult{
			Name:   "Nucleus binary",
			Status: Warn,
			Detail: "not cached (run 'neutron db start' to download)",
		}
	}

	// Find latest binary
	var latest string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "nucleus-") {
			latest = e.Name()
		}
	}
	if latest == "" {
		return CheckResult{Name: "Nucleus binary", Status: Warn, Detail: "no nucleus binary found in cache"}
	}

	return CheckResult{
		Name:   "Nucleus binary",
		Status: Pass,
		Detail: filepath.Join(binDir, latest),
	}
}

func checkDatabase() CheckResult {
	url := config.DatabaseURL()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return CheckResult{
			Name:   "Database",
			Status: Warn,
			Detail: fmt.Sprintf("could not connect to %s", url),
		}
	}
	defer conn.Close(ctx)

	var ver string
	if err := conn.QueryRow(ctx, "SELECT VERSION()").Scan(&ver); err != nil {
		return CheckResult{
			Name:   "Database",
			Status: Warn,
			Detail: "connected but VERSION() failed",
		}
	}

	isNucleus := strings.Contains(ver, "Nucleus")
	label := "PostgreSQL"
	if isNucleus {
		label = "Nucleus"
	}

	return CheckResult{
		Name:    "Database",
		Status:  Pass,
		Detail:  url,
		Version: label,
	}
}

func checkConfigFile() CheckResult {
	cwd, err := os.Getwd()
	if err != nil {
		return CheckResult{Name: "Config file", Status: Warn, Detail: "could not determine cwd"}
	}

	candidate := filepath.Join(cwd, "neutron.toml")
	if _, err := os.Stat(candidate); err == nil {
		return CheckResult{Name: "Config file", Status: Pass, Detail: candidate}
	}

	return CheckResult{
		Name:   "Config file",
		Status: Warn,
		Detail: "no neutron.toml found in current directory",
	}
}
