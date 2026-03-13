package neutroncli

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func cmdDev() int {
	// Check if air is available
	if _, err := exec.LookPath("air"); err == nil {
		return runAir()
	}

	// Fall back to built-in watch loop
	fmt.Println("'air' not found — using built-in watch mode")
	fmt.Println("Install air for better hot reload: go install github.com/air-verse/air@latest")
	fmt.Println()
	return runWatch()
}

func runAir() int {
	cmd := exec.Command("air")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

func runWatch() int {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	var cmd *exec.Cmd
	entrypoint := findEntrypoint()

	rebuild := func() *exec.Cmd {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGTERM)
			cmd.Wait()
		}

		fmt.Printf("[neutron-go dev] Building %s...\n", entrypoint)
		build := exec.Command("go", "build", "-o", "./tmp/main", entrypoint)
		build.Stdout = os.Stdout
		build.Stderr = os.Stderr
		if err := build.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "[neutron-go dev] Build failed: %v\n", err)
			return nil
		}

		fmt.Println("[neutron-go dev] Starting server...")
		c := exec.Command("./tmp/main")
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		c.Env = append(os.Environ(), "NEUTRON_DEV=1")
		if err := c.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "[neutron-go dev] Start failed: %v\n", err)
			return nil
		}
		return c
	}

	os.MkdirAll("tmp", 0o755)
	cmd = rebuild()

	// Simple poll-based watcher
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	lastMod := time.Now()

	for {
		select {
		case <-sig:
			if cmd != nil && cmd.Process != nil {
				cmd.Process.Signal(syscall.SIGTERM)
				cmd.Wait()
			}
			fmt.Println("\n[neutron-go dev] Stopped")
			return 0
		case <-ticker.C:
			mod := latestModTime(".")
			if mod.After(lastMod) {
				lastMod = mod
				cmd = rebuild()
			}
		}
	}
}

func findEntrypoint() string {
	candidates := []string{
		"./cmd/server",
		"./cmd/api",
		"./cmd/app",
		".",
	}
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && info.IsDir() {
			return c
		}
	}
	return "."
}

func latestModTime(dir string) time.Time {
	var latest time.Time
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		// Skip hidden dirs and tmp
		base := filepath.Base(path)
		if info.IsDir() && (strings.HasPrefix(base, ".") || base == "tmp" || base == "vendor" || base == "node_modules") {
			return filepath.SkipDir
		}
		if !info.IsDir() && strings.HasSuffix(path, ".go") {
			if info.ModTime().After(latest) {
				latest = info.ModTime()
			}
		}
		return nil
	})
	return latest
}
