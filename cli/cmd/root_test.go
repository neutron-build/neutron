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

	var stdout string
	run := func(args ...string) (int, string) {
		c := exec.Command(bin, args...)
		c.Dir = home
		c.Env = []string{"HOME=" + home, "USERPROFILE=" + home, "SystemRoot=" + os.Getenv("SystemRoot")}
		var outBuf, errBuf strings.Builder
		c.Stdout, c.Stderr = &outBuf, &errBuf
		err := c.Run()
		code := 0
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else if err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
		stdout = outBuf.String()
		return code, outBuf.String() + errBuf.String()
	}

	code, out := run("studio", "--port", strconv.Itoa(port))
	want := fmt.Sprintf("listen on port %d", port)
	if code != 1 || strings.Count(out, want) != 1 {
		t.Fatalf("studio on a busy port: exit %d, want 1 and %q once in:\n%s", code, want, out)
	}
	// stdout may be a command's machine-readable output (--json): the error
	// goes to stderr.
	if strings.Contains(stdout, want) {
		t.Fatalf("the unreported error went to stdout:\n%s", stdout)
	}
	if strings.Contains(out, "Studio is running") || strings.Contains(out, "Starting Studio") {
		t.Fatalf("studio on a busy port announced a running server:\n%s", out)
	}

	code, out = run("migrate", "generate", "--schema", filepath.Join(home, "absent.json"))
	if code != 1 || strings.Count(out, "read schema") != 1 {
		t.Fatalf("migrate generate without a schema: exit %d, want 1 and one report in:\n%s", code, out)
	}

	// Spinner failures report themselves ("✗ Failed: ...") once.
	if err := os.Mkdir(filepath.Join(home, "exists"), 0o755); err != nil {
		t.Fatal(err)
	}
	code, out = run("new", "exists", "--lang", "go")
	if code != 1 || strings.Count(out, `"exists" already exists`) != 1 {
		t.Fatalf("new into an existing directory: exit %d, want 1 and one report in:\n%s", code, out)
	}
	if base := os.Getenv("NEUTRON_E2E_DATABASE_URL"); base != "" {
		seed := filepath.Join(home, "fail.sql")
		if err := os.WriteFile(seed, []byte("SELECT 1/0;\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		code, out = run("seed", "--url", base, "-f", seed)
		if code != 1 || strings.Count(out, "division by zero") != 1 {
			t.Fatalf("seed with a failing statement: exit %d, want 1 and one report in:\n%s", code, out)
		}

		// A failing migration reports its interruption boundary and cause
		// once (Q09 review-1: the long 55P04 fix-naming error printed twice).
		dbURL, _ := newM02CommandDB(t, "printonce")
		mig := filepath.Join(home, "mig")
		if err := os.Mkdir(mig, 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(mig, "001_init.up.sql"), "create type printonce_mood as enum ('sad', 'ok');\ncreate table printonce_t (tone printonce_mood);")
		writeFile(t, filepath.Join(mig, "002_mood.up.sql"), "alter type printonce_mood add value 'glad';\ncreate view printonce_glad as select tone from printonce_t where tone = 'glad';")
		code, out = run("migrate", "--url", dbURL, "--dir", mig)
		if code != 1 || !strings.Contains(out, "55P04") {
			t.Fatalf("migrate with a failing migration: exit %d, want 1 and the 55P04 report in:\n%s", code, out)
		}
		for _, once := range []string{"interrupted after 1 of 2 pending migration(s)", "delete this unapplied migration"} {
			if strings.Count(out, once) != 1 {
				t.Fatalf("migrate printed %q %d times, want once:\n%s", once, strings.Count(out, once), out)
			}
		}
	}

	// Application errors report themselves ("Application: ...") once.
	code, out = run("project", "check")
	i := strings.Index(out, "Application: ")
	if code != 1 || i < 0 {
		t.Fatalf("project check outside a project: exit %d, want 1 and an Application report in:\n%s", code, out)
	}
	msg := strings.TrimSpace(strings.SplitN(out[i+len("Application: "):], "\n", 2)[0])
	if strings.Count(out, msg) != 1 {
		t.Fatalf("project check printed its error more than once:\n%s", out)
	}
}

// A spinner failure line is the error's report: every such site must return
// through failSpinner (marked reported) or the Execute fallback prints the
// error a second time.
func TestSpinnerFailuresGoThroughFailSpinner(t *testing.T) {
	allowed := map[string]int{"root.go": 1}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if n := strings.Count(string(src), "StopWithMessage(ui.CrossMark"); n != allowed[f] {
			t.Errorf("%s: %d direct spinner failure line(s), want %d; return failSpinner(...) instead", f, n, allowed[f])
		}
	}
}
