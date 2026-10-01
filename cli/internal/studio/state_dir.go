package studio

import (
	"fmt"
	"os"
	"path/filepath"
)

// studioStateDir allows an operator to isolate one local Studio instance
// without changing the process home or another instance's saved connections.
func studioStateDir() (string, error) {
	dir := os.Getenv("NEUTRON_STUDIO_DATA_DIR")
	explicit := dir != ""
	if explicit {
		if !filepath.IsAbs(dir) {
			return "", fmt.Errorf("NEUTRON_STUDIO_DATA_DIR must be an absolute directory")
		}
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("home dir: %w", err)
		}
		dir = filepath.Join(home, ".neutron")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create Studio state directory: %w", err)
	}
	if explicit {
		info, err := os.Stat(dir)
		if err != nil {
			return "", err
		}
		if info.Mode().Perm()&0o077 != 0 {
			return "", fmt.Errorf("NEUTRON_STUDIO_DATA_DIR must be private (mode 0700)")
		}
	}
	return dir, nil
}
