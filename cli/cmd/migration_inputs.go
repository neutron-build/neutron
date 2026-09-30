package cmd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// migrationInputs owns one immutable local bundle for a run. Database locking
// serializes cooperating runners, not local editors. Companion files must never
// be reopened from the caller's directory after waiting for that lock.
type migrationInputs struct {
	dir   string
	files []db.MigrationFile
}

func readMigrationInputBytes(dir string) (map[string][]byte, error) {
	out := map[string][]byte{}
	for _, sub := range []string{"", db.SnapshotDir} {
		entries, err := os.ReadDir(filepath.Join(dir, sub))
		if sub != "" && os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			name := entry.Name()
			relevant := sub == "" && (strings.HasSuffix(name, ".up.sql") || strings.HasSuffix(name, ".down.sql") || strings.HasSuffix(name, ".plan.json")) || sub != "" && strings.HasSuffix(name, ".snapshot.json")
			if !relevant {
				continue
			}
			if !entry.Type().IsRegular() {
				return nil, fmt.Errorf("migration input %s must be a regular file", filepath.Join(sub, name))
			}
			data, err := os.ReadFile(filepath.Join(dir, sub, name))
			if err != nil {
				return nil, err
			}
			out[filepath.Join(sub, name)] = data
		}
	}
	return out, nil
}

func sameMigrationInputBytes(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for name, data := range a {
		other, ok := b[name]
		if !ok || !bytes.Equal(data, other) {
			return false
		}
	}
	return true
}

func captureMigrationInputs(dir string) (*migrationInputs, func(), error) {
	first, err := readMigrationInputBytes(dir)
	if err != nil {
		return nil, nil, err
	}
	second, err := readMigrationInputBytes(dir)
	if err != nil {
		return nil, nil, err
	}
	if !sameMigrationInputBytes(first, second) {
		return nil, nil, fmt.Errorf("migration inputs changed while being captured; stop local writers and retry")
	}
	captured, err := os.MkdirTemp("", "neutron-migration-inputs-")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { _ = os.RemoveAll(captured) }
	for name, data := range first {
		path := filepath.Join(captured, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			cleanup()
			return nil, nil, err
		}
		if err := os.WriteFile(path, data, 0400); err != nil {
			cleanup()
			return nil, nil, err
		}
	}
	files, err := db.ReadMigrationFiles(captured)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return &migrationInputs{dir: captured, files: files}, cleanup, nil
}
