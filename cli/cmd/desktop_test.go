package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDesktopCommand(t *testing.T) {
	if desktopCmd.Use != "desktop" {
		t.Errorf("desktopCmd.Use = %q, want %q", desktopCmd.Use, "desktop")
	}
	if desktopCmd.Short == "" {
		t.Error("desktopCmd.Short is empty")
	}
}

func TestDesktopSubcommands(t *testing.T) {
	expectedSubs := []string{"dev", "build", "preview"}
	cmds := desktopCmd.Commands()

	names := make(map[string]bool)
	for _, c := range cmds {
		names[c.Name()] = true
	}
	for _, name := range expectedSubs {
		if !names[name] {
			t.Errorf("desktop subcommand %q not registered", name)
		}
	}
}

func TestDesktopDevFlags(t *testing.T) {
	flag := desktopDevCmd.Flags().Lookup("port")
	if flag == nil {
		t.Fatal("desktopDevCmd missing --port flag")
	}
	if flag.DefValue != "5173" {
		t.Errorf("--port default = %q, want %q", flag.DefValue, "5173")
	}
}

func TestDesktopBuildFlags(t *testing.T) {
	releaseFlag := desktopBuildCmd.Flags().Lookup("release")
	if releaseFlag == nil {
		t.Fatal("desktopBuildCmd missing --release flag")
	}
	if releaseFlag.DefValue != "true" {
		t.Errorf("--release default = %q, want %q", releaseFlag.DefValue, "true")
	}

	targetFlag := desktopBuildCmd.Flags().Lookup("target")
	if targetFlag == nil {
		t.Fatal("desktopBuildCmd missing --target flag")
	}
}

func TestFindDesktopRootWithSrcTauri(t *testing.T) {
	dir := evalSymlinks(t, t.TempDir())
	tauriDir := filepath.Join(dir, "src-tauri")
	os.MkdirAll(tauriDir, 0755)
	os.WriteFile(filepath.Join(tauriDir, "tauri.conf.json"), []byte("{}"), 0644)

	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	root, err := findDesktopRoot()
	if err != nil {
		t.Fatalf("findDesktopRoot() error: %v", err)
	}
	if root != dir {
		t.Errorf("findDesktopRoot() = %q, want %q", root, dir)
	}
}

func TestFindDesktopRootNotFound(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	_, err := findDesktopRoot()
	if err == nil {
		t.Fatal("expected error when no Tauri project found")
	}
}

func TestFindDesktopRootFromSubdir(t *testing.T) {
	dir := evalSymlinks(t, t.TempDir())
	tauriDir := filepath.Join(dir, "src-tauri")
	os.MkdirAll(tauriDir, 0755)
	os.WriteFile(filepath.Join(tauriDir, "tauri.conf.json"), []byte("{}"), 0644)

	subDir := filepath.Join(dir, "sub", "deep")
	os.MkdirAll(subDir, 0755)

	origDir, _ := os.Getwd()
	os.Chdir(subDir)
	defer os.Chdir(origDir)

	root, err := findDesktopRoot()
	if err != nil {
		t.Fatalf("findDesktopRoot() error: %v", err)
	}
	if root != dir {
		t.Errorf("findDesktopRoot() = %q, want %q", root, dir)
	}
}

func TestDesktopPreviewNoBuild(t *testing.T) {
	dir := evalSymlinks(t, t.TempDir())
	tauriDir := filepath.Join(dir, "src-tauri")
	os.MkdirAll(tauriDir, 0755)
	os.WriteFile(filepath.Join(tauriDir, "tauri.conf.json"), []byte("{}"), 0644)

	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := runDesktopPreview(desktopPreviewCmd, nil)
	if err == nil {
		t.Fatal("expected error when no build exists")
	}
}

// evalSymlinks resolves symlinks in a path (needed for macOS /var -> /private/var).
func evalSymlinks(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", path, err)
	}
	return resolved
}
