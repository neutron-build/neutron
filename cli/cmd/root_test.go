package cmd

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestRootCommand(t *testing.T) {
	if rootCmd.Use != "neutron" {
		t.Errorf("rootCmd.Use = %q, want %q", rootCmd.Use, "neutron")
	}
	if rootCmd.Short == "" {
		t.Error("rootCmd.Short is empty")
	}
	if rootCmd.Long == "" {
		t.Error("rootCmd.Long is empty")
	}
}

func TestRootCommandSilenceConfig(t *testing.T) {
	if !rootCmd.SilenceUsage {
		t.Error("rootCmd.SilenceUsage should be true")
	}
	if !rootCmd.SilenceErrors {
		t.Error("rootCmd.SilenceErrors should be true")
	}
}

func TestRootPersistentFlags(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"config"},
		{"url"},
		{"verbose"},
		{"no-color"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag := rootCmd.PersistentFlags().Lookup(tt.name)
			if flag == nil {
				t.Errorf("rootCmd missing --%s persistent flag", tt.name)
			}
		})
	}
}

func TestAllSubcommandsRegistered(t *testing.T) {
	expectedCmds := []string{
		"version", "doctor", "init", "new", "dev",
		"db", "migrate", "seed", "native", "desktop",
		"studio", "mcp", "repl", "upgrade", "completion",
	}

	cmds := rootCmd.Commands()
	nameSet := make(map[string]bool)
	for _, c := range cmds {
		nameSet[c.Name()] = true
	}

	for _, name := range expectedCmds {
		if !nameSet[name] {
			t.Errorf("subcommand %q not registered on rootCmd", name)
		}
	}
}

func TestExecuteReturnsNoErrorForHelp(t *testing.T) {
	rootCmd.SetArgs([]string{"--help"})
	err := rootCmd.Execute()
	if err != nil {
		t.Errorf("Execute() with --help returned error: %v", err)
	}
}

// A command error reaches the user exactly once: studio's listen failure
// (its RunE returns the error without printing it) used to exit 1 with no
// output, and errors reportRunE already printed must not print twice.
func TestCommandErrorsArePrintedOnce(t *testing.T) {
	bin := buildCLIBinary(t)
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	port := busy.Addr().(*net.TCPAddr).Port
	home := t.TempDir()

	run := func(args ...string) (int, string) {
		c := exec.Command(bin, args...)
		c.Dir = home
		c.Env = []string{"HOME=" + home, "USERPROFILE=" + home, "SystemRoot=" + os.Getenv("SystemRoot")}
		out, err := c.CombinedOutput()
		code := 0
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else if err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
		return code, string(out)
	}

	code, out := run("studio", "--port", strconv.Itoa(port))
	want := fmt.Sprintf("listen on port %d", port)
	if code != 1 || strings.Count(out, want) != 1 {
		t.Fatalf("studio on a busy port: exit %d, want 1 and %q once in:\n%s", code, want, out)
	}
	if strings.Contains(out, "Studio is running") || strings.Contains(out, "Starting Studio") {
		t.Fatalf("studio on a busy port announced a running server:\n%s", out)
	}

	code, out = run("migrate", "generate", "--schema", filepath.Join(home, "absent.json"))
	if code != 1 || strings.Count(out, "read schema") != 1 {
		t.Fatalf("migrate generate without a schema: exit %d, want 1 and one report in:\n%s", code, out)
	}
}
