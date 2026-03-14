package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNativeCommand(t *testing.T) {
	if nativeCmd.Use != "native" {
		t.Errorf("nativeCmd.Use = %q, want %q", nativeCmd.Use, "native")
	}
	if nativeCmd.Short == "" {
		t.Error("nativeCmd.Short is empty")
	}
}

func TestNativeSubcommands(t *testing.T) {
	expectedSubs := []string{"init", "run", "dev", "build"}
	cmds := nativeCmd.Commands()

	names := make(map[string]bool)
	for _, c := range cmds {
		names[c.Name()] = true
	}
	for _, name := range expectedSubs {
		if !names[name] {
			t.Errorf("native subcommand %q not registered", name)
		}
	}
}

func TestNativeDevPortFlag(t *testing.T) {
	flag := nativeDevCmd.Flags().Lookup("port")
	if flag == nil {
		t.Fatal("nativeDevCmd missing --port flag")
	}
	if flag.DefValue != "8081" {
		t.Errorf("--port default = %q, want %q", flag.DefValue, "8081")
	}
}

func TestNativeBuildFlags(t *testing.T) {
	tests := []struct {
		name     string
		defValue string
	}{
		{"ios", "false"},
		{"android", "false"},
		{"release", "true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag := nativeBuildCmd.Flags().Lookup(tt.name)
			if flag == nil {
				t.Fatalf("nativeBuildCmd missing --%s flag", tt.name)
			}
			if flag.DefValue != tt.defValue {
				t.Errorf("--%s default = %q, want %q", tt.name, flag.DefValue, tt.defValue)
			}
		})
	}
}

func TestNativeRunRequiresPlatformArg(t *testing.T) {
	err := nativeRunCmd.Args(nativeRunCmd, []string{})
	if err == nil {
		t.Error("expected error for 0 args")
	}
	err = nativeRunCmd.Args(nativeRunCmd, []string{"ios"})
	if err != nil {
		t.Errorf("expected no error for 1 arg, got: %v", err)
	}
}

func TestNativeRunInvalidPlatform(t *testing.T) {
	dir := resolveSymlinks(t, t.TempDir())
	os.WriteFile(filepath.Join(dir, "repack.config.ts"), []byte(""), 0644)

	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := runNativeRun(nativeRunCmd, []string{"windows"})
	if err == nil {
		t.Fatal("expected error for invalid platform")
	}
}

func TestNativeBuildRequiresPlatformFlag(t *testing.T) {
	dir := resolveSymlinks(t, t.TempDir())
	os.WriteFile(filepath.Join(dir, "repack.config.ts"), []byte(""), 0644)

	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	nativeBuildCmd.Flags().Set("ios", "false")
	nativeBuildCmd.Flags().Set("android", "false")

	err := runNativeBuild(nativeBuildCmd, nil)
	if err == nil {
		t.Fatal("expected error when neither --ios nor --android specified")
	}
}

func TestFindNativeRootWithRepackConfig(t *testing.T) {
	dir := resolveSymlinks(t, t.TempDir())
	os.WriteFile(filepath.Join(dir, "repack.config.ts"), []byte(""), 0644)

	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	root, err := findNativeRoot()
	if err != nil {
		t.Fatalf("findNativeRoot() error: %v", err)
	}
	if root != dir {
		t.Errorf("findNativeRoot() = %q, want %q", root, dir)
	}
}

func TestFindNativeRootWithRepackConfigJS(t *testing.T) {
	dir := resolveSymlinks(t, t.TempDir())
	os.WriteFile(filepath.Join(dir, "repack.config.js"), []byte(""), 0644)

	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	root, err := findNativeRoot()
	if err != nil {
		t.Fatalf("findNativeRoot() error: %v", err)
	}
	if root != dir {
		t.Errorf("findNativeRoot() = %q, want %q", root, dir)
	}
}

func TestFindNativeRootWithIOSAndAndroid(t *testing.T) {
	dir := resolveSymlinks(t, t.TempDir())
	os.MkdirAll(filepath.Join(dir, "ios"), 0755)
	os.MkdirAll(filepath.Join(dir, "android"), 0755)

	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	root, err := findNativeRoot()
	if err != nil {
		t.Fatalf("findNativeRoot() error: %v", err)
	}
	if root != dir {
		t.Errorf("findNativeRoot() = %q, want %q", root, dir)
	}
}

func TestFindNativeRootNotFound(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	_, err := findNativeRoot()
	if err == nil {
		t.Fatal("expected error when no native project found")
	}
}

// resolveSymlinks resolves symlinks in a path (needed for macOS /var -> /private/var).
func resolveSymlinks(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", path, err)
	}
	return resolved
}
