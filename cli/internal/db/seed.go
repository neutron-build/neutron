package db

import (
	"context"
	"fmt"
	"os"
)

// DefaultSeedPaths are the default locations to look for seed files.
var DefaultSeedPaths = []string{
	"seeds/seed.sql",
	"seed.sql",
	"seeds/data.sql",
}

// FindSeedFile returns the path to the seed file, checking defaults if path is empty.
func FindSeedFile(path string) (string, error) {
	if path != "" {
		if _, err := os.Stat(path); err != nil {
			return "", fmt.Errorf("seed file not found: %s", path)
		}
		return path, nil
	}

	for _, p := range DefaultSeedPaths {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	return "", fmt.Errorf("no seed file found (tried: %v)", DefaultSeedPaths)
}

// RunSeedFile executes a SQL seed file against the database.
func (c *Client) RunSeedFile(ctx context.Context, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read seed file: %w", err)
	}

	if err := c.Exec(ctx, string(data)); err != nil {
		return fmt.Errorf("execute seed: %w", err)
	}

	return nil
}
