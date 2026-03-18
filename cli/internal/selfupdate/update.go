// Package selfupdate handles CLI self-update from GitHub releases.
package selfupdate

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	// Monorepo: list all releases and filter by cli/ tag prefix.
	githubReleasesAPI = "https://api.github.com/repos/neutron-build/neutron/releases?per_page=30"
	tagPrefix         = "cli/"
)

// Release represents a GitHub release.
type Release struct {
	TagName string  `json:"tag_name"`
	Body    string  `json:"body"`
	Assets  []Asset `json:"assets"`
}

// Asset represents a release asset.
type Asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// InstallMethod describes how the CLI was installed.
type InstallMethod int

const (
	InstallDirect   InstallMethod = iota // Direct binary (curl, manual)
	InstallHomebrew                      // Homebrew
)

// DetectInstallMethod checks whether the binary lives under a Homebrew Cellar.
func DetectInstallMethod() InstallMethod {
	execPath, err := os.Executable()
	if err != nil {
		return InstallDirect
	}
	resolved, err := filepath.EvalSymlinks(execPath)
	if err != nil {
		resolved = execPath
	}
	if strings.Contains(resolved, "/Cellar/") || strings.Contains(resolved, "/homebrew/") {
		return InstallHomebrew
	}
	return InstallDirect
}

// CheckForUpdate checks if a newer CLI version is available.
// It fetches all recent releases and finds the latest cli/vX.Y.Z tag.
func CheckForUpdate(currentVersion string) (*Release, bool, error) {
	if currentVersion == "dev" {
		return nil, false, nil
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(githubReleasesAPI)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("github API returned %d", resp.StatusCode)
	}

	var releases []Release
	if err := json.NewDecoder(resp.Body).Decode(&releases); err != nil {
		return nil, false, err
	}

	// Find the latest release with a cli/ tag prefix
	var latest *Release
	for i := range releases {
		if strings.HasPrefix(releases[i].TagName, tagPrefix) {
			latest = &releases[i]
			break // GitHub returns newest first
		}
	}

	if latest == nil {
		return nil, false, fmt.Errorf("no CLI releases found (looking for %s* tags)", tagPrefix)
	}

	latestVer := extractVersion(latest.TagName)
	currentVer := normalizeVersion(currentVersion)

	if compareSemver(latestVer, currentVer) > 0 {
		return latest, true, nil
	}

	return latest, false, nil
}

// DownloadAndReplace downloads the new binary, verifies checksum, and replaces the current one.
func DownloadAndReplace(release *Release) error {
	ver := extractVersion(release.TagName)
	archiveExt := "tar.gz"
	if runtime.GOOS == "windows" {
		archiveExt = "zip"
	}

	assetName := fmt.Sprintf("neutron_%s_%s_%s.%s", ver, runtime.GOOS, runtime.GOARCH, archiveExt)
	checksumName := "checksums.txt"

	var downloadURL, checksumURL string
	for _, asset := range release.Assets {
		switch asset.Name {
		case assetName:
			downloadURL = asset.BrowserDownloadURL
		case checksumName:
			checksumURL = asset.BrowserDownloadURL
		}
	}
	if downloadURL == "" {
		return fmt.Errorf("no release asset found for %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	// Download archive to temp file
	archivePath, err := downloadToTemp(downloadURL)
	if err != nil {
		return fmt.Errorf("download archive: %w", err)
	}
	defer os.Remove(archivePath)

	// Verify checksum (mandatory for safe updates)
	if checksumURL == "" {
		return fmt.Errorf("release is missing integrity checksum file; cannot verify download safely")
	}
	if err := verifyChecksum(archivePath, assetName, checksumURL); err != nil {
		return fmt.Errorf("checksum verification failed: %w", err)
	}

	// Extract the neutron binary from the archive
	binaryPath, err := extractBinary(archivePath)
	if err != nil {
		return fmt.Errorf("extract binary: %w", err)
	}
	defer os.Remove(binaryPath)

	// Get current binary path
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find current binary: %w", err)
	}
	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		return fmt.Errorf("resolve symlinks: %w", err)
	}

	// Atomic replace: rename current -> .old, rename new -> current
	oldPath := execPath + ".old"
	os.Remove(oldPath)

	if err := os.Rename(execPath, oldPath); err != nil {
		return fmt.Errorf("backup current binary: %w", err)
	}

	if err := copyFile(binaryPath, execPath); err != nil {
		// Restore backup
		os.Rename(oldPath, execPath)
		return fmt.Errorf("install new binary: %w", err)
	}

	if err := os.Chmod(execPath, 0755); err != nil {
		return fmt.Errorf("chmod: %w", err)
	}

	os.Remove(oldPath)
	return nil
}

// downloadToTemp downloads a URL to a temp file and returns its path.
func downloadToTemp(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	tmp, err := os.CreateTemp("", "neutron-update-*")
	if err != nil {
		return "", err
	}

	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	tmp.Close()
	return tmp.Name(), nil
}

// verifyChecksum downloads the checksums file and verifies the archive.
func verifyChecksum(archivePath, assetName, checksumURL string) error {
	resp, err := http.Get(checksumURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Parse checksums.txt: each line is "sha256hash  filename"
	var expectedHash string
	for _, line := range strings.Split(string(body), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[1] == assetName {
			expectedHash = parts[0]
			break
		}
	}
	if expectedHash == "" {
		return fmt.Errorf("no checksum found for %s", assetName)
	}

	// Hash the downloaded file
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}

	actualHash := hex.EncodeToString(h.Sum(nil))
	if actualHash != expectedHash {
		return fmt.Errorf("expected %s, got %s", expectedHash, actualHash)
	}
	return nil
}

// extractBinary extracts the "neutron" binary from a .tar.gz archive to a temp file.
func extractBinary(archivePath string) (string, error) {
	f, err := os.Open(archivePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		name := filepath.Base(header.Name)
		if name == "neutron" || name == "neutron.exe" {
			tmp, err := os.CreateTemp("", "neutron-bin-*")
			if err != nil {
				return "", err
			}
			if _, err := io.Copy(tmp, tr); err != nil {
				tmp.Close()
				os.Remove(tmp.Name())
				return "", err
			}
			tmp.Close()
			os.Chmod(tmp.Name(), 0755)
			return tmp.Name(), nil
		}
	}

	return "", fmt.Errorf("neutron binary not found in archive")
}

// copyFile copies src to dst (cross-device safe, unlike os.Rename).
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

// --- Semver helpers (no external dependency) ---

// extractVersion strips the "cli/v" or "cli/" or "v" prefix.
func extractVersion(tag string) string {
	tag = strings.TrimPrefix(tag, tagPrefix)
	tag = strings.TrimPrefix(tag, "v")
	return tag
}

// normalizeVersion strips a leading "v".
func normalizeVersion(v string) string {
	return strings.TrimPrefix(v, "v")
}

// compareSemver returns >0 if a > b, <0 if a < b, 0 if equal.
// Handles X.Y.Z format. Non-numeric parts are ignored (treated as 0).
func compareSemver(a, b string) int {
	aParts := parseSemver(a)
	bParts := parseSemver(b)

	for i := 0; i < 3; i++ {
		if aParts[i] != bParts[i] {
			return aParts[i] - bParts[i]
		}
	}
	return 0
}

// parseSemver splits "X.Y.Z" into [X, Y, Z]. Missing parts default to 0.
func parseSemver(v string) [3]int {
	var parts [3]int
	// Strip any pre-release suffix (e.g. "1.2.3-beta")
	if idx := strings.IndexByte(v, '-'); idx >= 0 {
		v = v[:idx]
	}
	segments := strings.SplitN(v, ".", 3)
	for i, s := range segments {
		if i >= 3 {
			break
		}
		n := 0
		for _, c := range s {
			if c >= '0' && c <= '9' {
				n = n*10 + int(c-'0')
			}
		}
		parts[i] = n
	}
	return parts
}
