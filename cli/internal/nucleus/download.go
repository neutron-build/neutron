// Package nucleus manages Nucleus binary download and process lifecycle.
package nucleus

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	releaseURLBase         = "https://github.com/neutron-build/neutron/releases/download"
	binaryName             = "nucleus"
	defaultDownloadTimeout = 120 * time.Second
)

// downloadTimeout returns the HTTP timeout for binary downloads.
// Configurable via NEUTRON_DOWNLOAD_TIMEOUT (in seconds). Defaults to 120s.
func downloadTimeout() time.Duration {
	if v := os.Getenv("NEUTRON_DOWNLOAD_TIMEOUT"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultDownloadTimeout
}

// BinDir returns the path to the Nucleus binary cache directory.
func BinDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".neutron", "bin")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return dir, nil
}

// BinaryPath returns the path to the cached Nucleus binary for the given version.
func BinaryPath(version string) (string, error) {
	binDir, err := BinDir()
	if err != nil {
		return "", err
	}
	name := fmt.Sprintf("%s-%s-%s-%s", binaryName, version, platformOS(), platformArch())
	return filepath.Join(binDir, name), nil
}

// FindOrDownload returns the path to the Nucleus binary, downloading if needed.
func FindOrDownload(version string) (string, error) {
	path, err := BinaryPath(version)
	if err != nil {
		return "", err
	}

	if _, err := os.Stat(path); err == nil {
		return path, nil
	}

	if err := download(version, path); err != nil {
		return "", err
	}

	return path, nil
}

func download(version, destPath string) error {
	archiveURL := fmt.Sprintf("%s/v%s/%s-%s-%s-%s.tar.gz",
		releaseURLBase, version, binaryName, version, platformOS(), platformArch())
	checksumURL := fmt.Sprintf("%s/v%s/checksums.txt",
		releaseURLBase, version)
	archiveBaseName := fmt.Sprintf("%s-%s-%s-%s.tar.gz",
		binaryName, version, platformOS(), platformArch())

	client := &http.Client{Timeout: downloadTimeout()}

	// Download archive to a temp file so we can verify checksum before extraction
	tmpFile, err := os.CreateTemp("", "nucleus-download-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	resp, err := client.Get(archiveURL)
	if err != nil {
		tmpFile.Close()
		return fmt.Errorf("download nucleus: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		tmpFile.Close()
		return fmt.Errorf("download nucleus: HTTP %d from %s", resp.StatusCode, archiveURL)
	}

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		return fmt.Errorf("save archive: %w", err)
	}
	tmpFile.Close()

	// Attempt checksum verification
	checksumResp, checksumErr := client.Get(checksumURL)
	if checksumErr == nil {
		defer checksumResp.Body.Close()
		if checksumResp.StatusCode == http.StatusOK {
			body, err := io.ReadAll(checksumResp.Body)
			if err == nil {
				if err := verifyArchiveChecksum(tmpPath, archiveBaseName, string(body)); err != nil {
					return fmt.Errorf("checksum verification failed: %w", err)
				}
			}
		} else {
			log.Printf("Warning: checksum file not available for Nucleus %s (HTTP %d); skipping verification", version, checksumResp.StatusCode)
		}
	} else {
		log.Printf("Warning: could not fetch checksum file for Nucleus %s: %v; skipping verification", version, checksumErr)
	}

	// Extract binary from tar.gz
	archiveFile, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer archiveFile.Close()

	gz, err := gzip.NewReader(archiveFile)
	if err != nil {
		return fmt.Errorf("decompress: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar: %w", err)
		}

		// Find the nucleus binary in the archive
		if strings.HasSuffix(header.Name, binaryName) || header.Name == binaryName {
			out, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
			if err != nil {
				return fmt.Errorf("create binary: %w", err)
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return fmt.Errorf("extract binary: %w", err)
			}
			out.Close()
			return nil
		}
	}

	return fmt.Errorf("nucleus binary not found in archive")
}

// verifyArchiveChecksum checks a downloaded file against a checksums.txt body.
func verifyArchiveChecksum(filePath, assetName, checksums string) error {
	var expectedHash string
	for _, line := range strings.Split(checksums, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[1] == assetName {
			expectedHash = parts[0]
			break
		}
	}
	if expectedHash == "" {
		return fmt.Errorf("no checksum found for %s", assetName)
	}

	f, err := os.Open(filePath)
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

func platformOS() string {
	switch runtime.GOOS {
	case "darwin":
		return "darwin"
	case "linux":
		return "linux"
	case "windows":
		return "windows"
	default:
		return runtime.GOOS
	}
}

func platformArch() string {
	switch runtime.GOARCH {
	case "amd64":
		return "amd64"
	case "arm64":
		return "arm64"
	default:
		return runtime.GOARCH
	}
}
