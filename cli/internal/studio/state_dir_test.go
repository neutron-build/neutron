package studio

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStudioStateDirectoryIsolatesStores(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "studio")
	t.Setenv("NEUTRON_STUDIO_DATA_DIR", dir)
	connections, err := newConnectionStore()
	if err != nil {
		t.Fatal(err)
	}
	saved, err := newSavedQueryStore()
	if err != nil {
		t.Fatal(err)
	}
	if connections.path != filepath.Join(dir, "studio.json") || saved.path != filepath.Join(dir, "studio-saved.json") {
		t.Fatal("stores did not use the configured instance directory")
	}
	if _, err = connections.Add("owned fixture", "postgres://fixture.invalid/owned"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(connections.path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("connection file permission: %v %v", info, err)
	}
	other := filepath.Join(t.TempDir(), "other")
	t.Setenv("NEUTRON_STUDIO_DATA_DIR", other)
	second, err := newConnectionStore()
	if err != nil || len(second.List()) != 0 {
		t.Fatalf("another instance read the first connections: %v", err)
	}
}

func TestStudioStateDirectoryRefusesInvalidConfiguration(t *testing.T) {
	t.Setenv("NEUTRON_STUDIO_DATA_DIR", "relative")
	if _, err := studioStateDir(); err == nil {
		t.Fatal("relative path admitted")
	}
	dir := filepath.Join(t.TempDir(), "public")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NEUTRON_STUDIO_DATA_DIR", dir)
	if _, err := studioStateDir(); err == nil {
		t.Fatal("public state directory admitted")
	}
	info, _ := os.Stat(dir)
	if info.Mode().Perm() != 0o755 {
		t.Fatal("operator directory permissions changed")
	}
}

func TestStudioSavedStoreRefusesCorruptState(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NEUTRON_STUDIO_DATA_DIR", dir)
	file := filepath.Join(dir, "studio-saved.json")
	if err := os.WriteFile(file, []byte("invalid JSON"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newSavedQueryStore(); err == nil {
		t.Fatal("corrupt saved queries silently ignored")
	}
	data, _ := os.ReadFile(file)
	if string(data) != "invalid JSON" {
		t.Fatal("corrupt saved queries overwritten")
	}
}
