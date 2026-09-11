// Package selfupdate handles CLI self-update from GitHub releases.
package selfupdate

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
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
//
// Prerelease policy: a prerelease tag is offered only when the current
// version is itself a prerelease, so stable installs stay on stable
// releases while an rc still upgrades to its own final.
func CheckForUpdate(currentVersion string) (*Release, bool, error) {
	if currentVersion == "dev" {
		return nil, false, nil
	}

	currentVer, ok := parseSemverVersion(normalizeVersion(currentVersion))
	if !ok {
		return nil, false, fmt.Errorf("current version %q is not valid semver", currentVersion)
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

	// Find the latest eligible release with a cli/ tag prefix. A malformed
	// version tag is skipped rather than mangled into a comparison.
	var latest *Release
	var latestVer semver
	for i := range releases {
		if !strings.HasPrefix(releases[i].TagName, tagPrefix) {
			continue
		}
		v, ok := parseSemverVersion(extractVersion(releases[i].TagName))
		if !ok {
			continue
		}
		if len(v.prerelease) > 0 && len(currentVer.prerelease) == 0 {
			continue
		}
		latest = &releases[i]
		latestVer = v
		break // GitHub returns newest first
	}

	if latest == nil {
		return nil, false, fmt.Errorf("no CLI releases found (looking for %s* tags)", tagPrefix)
	}

	if compareParsedSemver(latestVer, currentVer) > 0 {
		return latest, true, nil
	}

	return latest, false, nil
}

// Download and verify stage limits. The initial version check is bounded to
// ten seconds; the archive and checksum stages carry their own deadlines and
// explicit byte caps so an abnormal upstream response cannot stall the
// upgrade or consume unbounded disk and memory (audit neutron-11).
const (
	archiveDownloadTimeout  = 10 * time.Minute
	checksumDownloadTimeout = 30 * time.Second
	maxArchiveBytes         = int64(256 << 20)
	maxChecksumBytes        = int64(1 << 20)
)

// DownloadAndReplace downloads the new binary, verifies checksum, and replaces the current one.
func DownloadAndReplace(ctx context.Context, release *Release) error {
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
	archivePath, err := downloadToTemp(ctx, downloadURL, maxArchiveBytes, archiveDownloadTimeout)
	if err != nil {
		return fmt.Errorf("download archive: %w", err)
	}
	defer os.Remove(archivePath)

	// Verify checksum (mandatory for safe updates)
	if checksumURL == "" {
		return fmt.Errorf("release is missing integrity checksum file; cannot verify download safely")
	}
	if err := verifyChecksum(ctx, archivePath, assetName, checksumURL); err != nil {
		return fmt.Errorf("checksum verification failed: %w", err)
	}

	// Extract the neutron binary from the archive
	binaryPath, err := extractBinary(archivePath, archiveExt)
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

	// Copy-first swap: stage the new binary beside the live one, then rename
	// it into place. The previous order — renaming the live executable aside
	// before copying — left a window where a crash or copy failure meant no
	// executable at execPath at all.
	newPath := execPath + ".new"
	if err := copyFile(binaryPath, newPath); err != nil {
		os.Remove(newPath)
		return fmt.Errorf("install new binary: %w", err)
	}

	if err := swapBinary(execPath, newPath); err != nil {
		os.Remove(newPath)
		return fmt.Errorf("swap binary: %w", err)
	}

	// Clean up any backup left by an earlier update.
	os.Remove(execPath + ".old")
	return nil
}

// swapBinary atomically replaces the executable at execPath with the binary
// staged at newPath. The staged bytes are fsynced before the rename so a
// crash cannot leave a half-written executable; until the rename succeeds
// the original at execPath is untouched.
func swapBinary(execPath, newPath string) error {
	if err := os.Chmod(newPath, 0755); err != nil {
		return fmt.Errorf("chmod: %w", err)
	}

	f, err := os.Open(newPath)
	if err != nil {
		return fmt.Errorf("open staged binary: %w", err)
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return fmt.Errorf("fsync staged binary: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close staged binary: %w", closeErr)
	}

	if err := os.Rename(newPath, execPath); err != nil {
		return fmt.Errorf("replace binary: %w", err)
	}
	return nil
}

// downloadToTemp downloads a URL to a temp file and returns its path. The
// stage has an explicit deadline and byte cap, and the file is synced and
// closed with checked errors before the path is handed back.
func downloadToTemp(ctx context.Context, url string, maxBytes int64, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}
	if resp.ContentLength > maxBytes {
		return "", fmt.Errorf("response of %d bytes exceeds the %d-byte limit", resp.ContentLength, maxBytes)
	}

	tmp, err := os.CreateTemp("", "neutron-update-*")
	if err != nil {
		return "", err
	}

	// LimitReader with maxBytes+1 so an oversized body is detected by the
	// copy length even when ContentLength lied or was absent.
	n, err := io.Copy(tmp, io.LimitReader(resp.Body, maxBytes+1))
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	if n > maxBytes {
		os.Remove(tmp.Name())
		return "", fmt.Errorf("download exceeded the %d-byte limit", maxBytes)
	}
	if err := syncFile(tmp.Name()); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

// syncFile fsyncs a closed file's contents so a crash after download cannot
// leave a truncated archive to verify and install.
func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// verifyChecksum downloads the checksums file and verifies the archive.
func verifyChecksum(ctx context.Context, archivePath, assetName, checksumURL string) error {
	ctx, cancel := context.WithTimeout(ctx, checksumDownloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, checksumURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d fetching %s", resp.StatusCode, checksumURL)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxChecksumBytes+1))
	if err != nil {
		return err
	}
	if int64(len(body)) > maxChecksumBytes {
		return fmt.Errorf("checksum file exceeds the %d-byte limit", maxChecksumBytes)
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

// extractBinary extracts the "neutron" binary from a release archive to a
// temp file. Windows releases ship as ZIP; every other platform ships
// tar.gz — the extractor must match the format the asset name selected, or
// a valid Windows ZIP fails against the gzip reader (audit neutron-08).
func extractBinary(archivePath, format string) (string, error) {
	if format == "zip" {
		return extractBinaryFromZip(archivePath)
	}
	return extractBinaryFromTarGz(archivePath)
}

func extractBinaryFromTarGz(archivePath string) (string, error) {
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

func extractBinaryFromZip(archivePath string) (string, error) {
	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		return "", err
	}
	defer zr.Close()

	for _, f := range zr.File {
		name := filepath.Base(f.Name)
		if f.FileInfo().IsDir() {
			continue
		}
		if name == "neutron" || name == "neutron.exe" {
			rc, err := f.Open()
			if err != nil {
				return "", err
			}
			tmp, err := os.CreateTemp("", "neutron-bin-*")
			if err != nil {
				rc.Close()
				return "", err
			}
			_, copyErr := io.Copy(tmp, rc)
			rc.Close()
			if cerr := tmp.Close(); copyErr == nil {
				copyErr = cerr
			}
			if copyErr != nil {
				os.Remove(tmp.Name())
				return "", copyErr
			}
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

// semver is a parsed SemVer 2.0.0 version. Build metadata is dropped at
// parse time: the spec forbids it from participating in precedence.
type semver struct {
	major, minor, patch uint64
	// prerelease identifiers, or nil for a final release
	prerelease []string
}

// parseSemverVersion parses X.Y.Z with optional -prerelease and +build
// parts. Missing minor/patch default to 0 (historical leniency). Every
// present component must be valid: numeric identifiers are digits without
// leading zeros, prerelease identifiers are non-empty alphanumeric-plus
// runs. Malformed input returns ok=false instead of being mangled into a
// plausible number (audit neutron-10).
func parseSemverVersion(v string) (semver, bool) {
	if i := strings.IndexByte(v, '+'); i >= 0 {
		v = v[:i]
	}
	core, pre := v, ""
	if i := strings.IndexByte(v, '-'); i >= 0 {
		core, pre = v[:i], v[i+1:]
	}

	var s semver
	parts := strings.Split(core, ".")
	if len(parts) > 3 {
		return semver{}, false
	}
	dst := []*uint64{&s.major, &s.minor, &s.patch}
	for i, p := range parts {
		n, ok := parseUint64(p)
		if !ok {
			return semver{}, false
		}
		*dst[i] = n
	}

	if pre != "" {
		s.prerelease = strings.Split(pre, ".")
		for _, id := range s.prerelease {
			if !validPrereleaseIdentifier(id) {
				return semver{}, false
			}
		}
	}
	return s, true
}

// parseUint64 accepts digits only, with no leading zeros ("0" alone is
// fine). Anything else — empty, signed, hex, overflow — is rejected.
func parseUint64(s string) (uint64, bool) {
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return 0, false
	}
	var n uint64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if n > (^uint64(0)-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	return n, true
}

func validPrereleaseIdentifier(id string) bool {
	if id == "" {
		return false
	}
	allDigits := true
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '-':
			allDigits = false
		default:
			return false
		}
	}
	// A numeric identifier may not carry leading zeros.
	if allDigits {
		_, ok := parseUint64(id)
		return ok
	}
	return true
}

// compareParsedSemver implements SemVer 2.0.0 precedence over two parsed
// versions.
func compareParsedSemver(a, b semver) int {
	if a.major != b.major {
		return cmpUint64(a.major, b.major)
	}
	if a.minor != b.minor {
		return cmpUint64(a.minor, b.minor)
	}
	if a.patch != b.patch {
		return cmpUint64(a.patch, b.patch)
	}
	return comparePrerelease(a.prerelease, b.prerelease)
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// comparePrerelease follows spec section 11: a release outranks any
// prerelease of the same core; identifiers compare numerically when both
// are numeric, lexically otherwise, and numeric identifiers outrank
// lexical ones; a longer identifier list outranks a prefix of itself.
func comparePrerelease(a, b []string) int {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	if len(a) == 0 {
		return 1
	}
	if len(b) == 0 {
		return -1
	}
	for i := 0; i < len(a) && i < len(b); i++ {
		an, aNum := parseUint64(a[i])
		bn, bNum := parseUint64(b[i])
		switch {
		case aNum && bNum:
			if an != bn {
				return cmpUint64(an, bn)
			}
		case aNum:
			return -1
		case bNum:
			return 1
		default:
			if a[i] != b[i] {
				if a[i] < b[i] {
					return -1
				}
				return 1
			}
		}
	}
	return cmpInt(len(a), len(b))
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// compareSemver returns >0 if a > b, <0 if a < b, 0 if equal, following
// SemVer 2.0.0 precedence including prereleases; build metadata is
// ignored. A malformed version compares below any valid one, and equal to
// another malformed one, so a bad tag can never look "newer" by accident.
func compareSemver(a, b string) int {
	av, aok := parseSemverVersion(a)
	bv, bok := parseSemverVersion(b)
	switch {
	case aok && bok:
		return compareParsedSemver(av, bv)
	case aok:
		return 1
	case bok:
		return -1
	default:
		return 0
	}
}
