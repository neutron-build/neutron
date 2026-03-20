package nucleus

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	githubReleasesURL = "https://api.github.com/repos/neutron-build/neutron/releases"
	nucleusTagPrefix  = "nucleus/v"
	cacheTTL          = 24 * time.Hour
)

// ResolveVersion resolves "latest" to an actual version string, using a local cache.
func ResolveVersion(version string) (string, error) {
	if version != "latest" && version != "" {
		return strings.TrimPrefix(version, "v"), nil
	}

	// Check cache
	cached, err := readCachedVersion()
	if err == nil && cached != "" {
		return cached, nil
	}

	// Fetch from GitHub
	resolved, err := fetchLatestVersion()
	if err != nil {
		// If we can't reach GitHub, fall back to a default
		if cached != "" {
			return cached, nil
		}
		return "0.1.0", fmt.Errorf("could not resolve latest version: %w", err)
	}

	// Cache it
	_ = writeCachedVersion(resolved)
	return resolved, nil
}

func fetchLatestVersion() (string, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(githubReleasesURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("github API returned %d", resp.StatusCode)
	}

	var releases []struct {
		TagName string `json:"tag_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&releases); err != nil {
		return "", err
	}

	for _, r := range releases {
		if strings.HasPrefix(r.TagName, nucleusTagPrefix) {
			return strings.TrimPrefix(r.TagName, nucleusTagPrefix), nil
		}
	}

	return "", fmt.Errorf("no nucleus release found (no tag matching %s*)", nucleusTagPrefix)
}

func cacheFilePath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".neutron")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return filepath.Join(dir, "latest-version"), nil
}

func readCachedVersion() (string, error) {
	path, err := cacheFilePath()
	if err != nil {
		return "", err
	}

	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	// Check TTL
	if time.Since(info.ModTime()) > cacheTTL {
		return "", fmt.Errorf("cache expired")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(data)), nil
}

func writeCachedVersion(version string) error {
	path, err := cacheFilePath()
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(version), 0644)
}
