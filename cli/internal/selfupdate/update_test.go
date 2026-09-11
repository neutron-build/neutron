package selfupdate

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExtractVersion(t *testing.T) {
	tests := []struct {
		tag  string
		want string
	}{
		{"cli/v1.2.3", "1.2.3"},
		{"cli/1.2.3", "1.2.3"},
		{"v1.2.3", "1.2.3"},
		{"1.2.3", "1.2.3"},
		{"cli/v0.1.0-beta", "0.1.0-beta"},
	}
	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			got := extractVersion(tt.tag)
			if got != tt.want {
				t.Errorf("extractVersion(%q) = %q, want %q", tt.tag, got, tt.want)
			}
		})
	}
}

func TestNormalizeVersion(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"v1.2.3", "1.2.3"},
		{"1.2.3", "1.2.3"},
		{"v0.1.0", "0.1.0"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizeVersion(tt.input)
			if got != tt.want {
				t.Errorf("normalizeVersion(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestCompareSemver(t *testing.T) {
	tests := []struct {
		a, b string
		want int // >0 means a>b, <0 means a<b, 0 means equal
	}{
		{"1.0.0", "1.0.0", 0},
		{"1.0.1", "1.0.0", 1},
		{"1.0.0", "1.0.1", -1},
		{"2.0.0", "1.0.0", 1},
		{"1.1.0", "1.0.0", 1},
		{"1.0.0", "2.0.0", -1},
		{"0.2.0", "0.1.0", 1},
		{"0.1.1", "0.1.0", 1},
		{"10.0.0", "9.0.0", 1},
		{"1.10.0", "1.9.0", 1},
		// Prerelease precedence: a release outranks its own prereleases,
		// so an rc upgrades to the final (audit neutron-10).
		{"1.2.3", "1.2.3-rc.1", 1},
		{"1.2.3-rc.1", "1.2.3", -1},
		{"1.2.3-rc.1", "1.2.3-rc.1", 0},
		{"1.2.3-rc.2", "1.2.3-rc.10", -1},
		{"1.2.3-alpha", "1.2.3-beta", -1},
		// SemVer spec section 11 ordering example.
		{"1.0.0-alpha", "1.0.0-alpha.1", -1},
		{"1.0.0-alpha.1", "1.0.0-alpha.beta", -1},
		{"1.0.0-alpha.beta", "1.0.0-beta", -1},
		{"1.0.0-beta", "1.0.0-beta.2", -1},
		{"1.0.0-beta.2", "1.0.0-beta.11", -1},
		{"1.0.0-beta.11", "1.0.0-rc.1", -1},
		{"1.0.0-rc.1", "1.0.0", -1},
		// Build metadata never affects ordering.
		{"1.2.3+build.7", "1.2.3", 0},
		{"1.2.3+build.7", "1.2.4+build.1", -1},
		{"1.2.3+1", "1.2.3+2", 0},
		// Malformed versions compare below valid ones so a bad tag can
		// never look "newer" by accident.
		{"1.0.0", "not-a-version", 1},
		{"not-a-version", "1.0.0", -1},
		{"not-a-version", "also junk", 0},
	}
	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			got := compareSemver(tt.a, tt.b)
			switch {
			case tt.want > 0 && got <= 0:
				t.Errorf("compareSemver(%q, %q) = %d, want >0", tt.a, tt.b, got)
			case tt.want < 0 && got >= 0:
				t.Errorf("compareSemver(%q, %q) = %d, want <0", tt.a, tt.b, got)
			case tt.want == 0 && got != 0:
				t.Errorf("compareSemver(%q, %q) = %d, want 0", tt.a, tt.b, got)
			}
		})
	}
}

func TestParseSemverVersion(t *testing.T) {
	valid := []struct {
		input string
		want  semver
	}{
		{"1.2.3", semver{1, 2, 3, nil}},
		{"0.1.0", semver{0, 1, 0, nil}},
		{"10.20.30", semver{10, 20, 30, nil}},
		{"1.0", semver{1, 0, 0, nil}},
		{"1", semver{1, 0, 0, nil}},
		{"1.2.3-beta", semver{1, 2, 3, []string{"beta"}}},
		{"0.1.0-rc1", semver{0, 1, 0, []string{"rc1"}}},
		{"1.2.3-rc.1", semver{1, 2, 3, []string{"rc", "1"}}},
		{"1.2.3-0.3.7", semver{1, 2, 3, []string{"0", "3", "7"}}},
		{"1.2.3-x.7.z.92", semver{1, 2, 3, []string{"x", "7", "z", "92"}}},
		{"1.2.3+build.7", semver{1, 2, 3, nil}},
		{"1.2.3-rc.1+build.5", semver{1, 2, 3, []string{"rc", "1"}}},
		{"18446744073709551615.0.0", semver{18446744073709551615, 0, 0, nil}},
	}
	for _, tt := range valid {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := parseSemverVersion(tt.input)
			if !ok {
				t.Fatalf("parseSemverVersion(%q) rejected a valid version", tt.input)
			}
			if got.major != tt.want.major || got.minor != tt.want.minor || got.patch != tt.want.patch {
				t.Errorf("core = %d.%d.%d, want %d.%d.%d", got.major, got.minor, got.patch, tt.want.major, tt.want.minor, tt.want.patch)
			}
			if len(got.prerelease) != len(tt.want.prerelease) {
				t.Fatalf("prerelease = %v, want %v", got.prerelease, tt.want.prerelease)
			}
			for i := range got.prerelease {
				if got.prerelease[i] != tt.want.prerelease[i] {
					t.Errorf("prerelease[%d] = %q, want %q", i, got.prerelease[i], tt.want.prerelease[i])
				}
			}
		})
	}

	invalid := []string{
		"",
		"junk",
		"1.2.3.4",
		"1.02.3",      // leading zero in numeric identifier
		"1.2.3-01",    // leading zero in numeric prerelease identifier
		"1.2.3-rc..1", // empty prerelease identifier
		"1.2.3-rc.1.", // trailing empty identifier
		"1.2.3-rc_1",  // underscore is not legal in identifiers
		"1.2.3-beta!", // punctuation outside the identifier set
		"1.2.x",
		"v1.2.3",                   // leading v must be stripped by the caller
		"1.2.3 ",                   // whitespace
		"18446744073709551616.0.0", // overflow
	}
	for _, tt := range invalid {
		t.Run("invalid:"+tt, func(t *testing.T) {
			if _, ok := parseSemverVersion(tt); ok {
				t.Errorf("parseSemverVersion(%q) accepted a malformed version", tt)
			}
		})
	}
}

func TestCheckForUpdateDevVersion(t *testing.T) {
	// "dev" version should always return no update available
	release, hasUpdate, err := CheckForUpdate("dev")
	if err != nil {
		t.Fatalf("CheckForUpdate(dev) error: %v", err)
	}
	if hasUpdate {
		t.Error("hasUpdate should be false for dev version")
	}
	if release != nil {
		t.Error("release should be nil for dev version")
	}
}

func TestDetectInstallMethod(t *testing.T) {
	method := DetectInstallMethod()
	// Should return either InstallDirect or InstallHomebrew
	if method != InstallDirect && method != InstallHomebrew {
		t.Errorf("DetectInstallMethod() = %d, want InstallDirect or InstallHomebrew", method)
	}
}

func TestInstallMethodConstants(t *testing.T) {
	if InstallDirect == InstallHomebrew {
		t.Error("InstallDirect == InstallHomebrew, should be different")
	}
}

func TestReleaseStruct(t *testing.T) {
	r := Release{
		TagName: "cli/v1.0.0",
		Body:    "Release notes",
		Assets: []Asset{
			{Name: "neutron_1.0.0_darwin_arm64.tar.gz", BrowserDownloadURL: "https://example.com/download"},
		},
	}
	if r.TagName != "cli/v1.0.0" {
		t.Errorf("TagName = %q", r.TagName)
	}
	if len(r.Assets) != 1 {
		t.Errorf("Assets len = %d", len(r.Assets))
	}
}

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	content := []byte("hello world")
	os.WriteFile(src, content, 0644)

	err := copyFile(src, dst)
	if err != nil {
		t.Fatalf("copyFile() error: %v", err)
	}

	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("ReadFile(dst) error: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("dst content = %q, want %q", string(data), string(content))
	}
}

func TestCopyFileSrcNotFound(t *testing.T) {
	dir := t.TempDir()
	err := copyFile(filepath.Join(dir, "nonexistent"), filepath.Join(dir, "dst"))
	if err == nil {
		t.Fatal("expected error for nonexistent source")
	}
}

func TestSwapBinaryReplacesExecutable(t *testing.T) {
	dir := t.TempDir()
	execPath := filepath.Join(dir, "neutron")
	newPath := filepath.Join(dir, "neutron.new")

	os.WriteFile(execPath, []byte("old binary"), 0755)
	os.WriteFile(newPath, []byte("new binary"), 0644)

	if err := swapBinary(execPath, newPath); err != nil {
		t.Fatalf("swapBinary() error: %v", err)
	}

	data, err := os.ReadFile(execPath)
	if err != nil {
		t.Fatalf("ReadFile(execPath) error: %v", err)
	}
	if string(data) != "new binary" {
		t.Errorf("execPath content = %q, want %q", data, "new binary")
	}

	info, err := os.Stat(execPath)
	if err != nil {
		t.Fatalf("Stat(execPath) error: %v", err)
	}
	if info.Mode().Perm() != 0755 {
		t.Errorf("execPath perms = %v, want 0755", info.Mode().Perm())
	}

	// The staged file must be gone — it became the executable.
	if _, err := os.Stat(newPath); !os.IsNotExist(err) {
		t.Errorf("staged file still present after swap: %v", err)
	}
}

// The crash-window guarantee: any failure before the rename must leave the
// original executable untouched.
func TestSwapBinaryFailureLeavesOriginalUntouched(t *testing.T) {
	dir := t.TempDir()
	execPath := filepath.Join(dir, "neutron")

	os.WriteFile(execPath, []byte("old binary"), 0755)

	err := swapBinary(execPath, filepath.Join(dir, "missing.new"))
	if err == nil {
		t.Fatal("expected error when staged binary does not exist")
	}

	data, readErr := os.ReadFile(execPath)
	if readErr != nil {
		t.Fatalf("ReadFile(execPath) error: %v", readErr)
	}
	if string(data) != "old binary" {
		t.Errorf("execPath content = %q after failed swap, want %q", data, "old binary")
	}
}

// Windows releases select ZIP assets, so extraction must handle a real
// ZIP, not only gzip/tar (audit neutron-08).
func TestExtractBinaryFromZip(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "neutron_1.0.0_windows_amd64.zip")

	f, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	files := map[string]string{
		"dist/neutron.exe": "windows binary bytes",
		"dist/README.txt":  "readme",
	}
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	binPath, err := extractBinary(zipPath, "zip")
	if err != nil {
		t.Fatalf("extractBinary(zip) error: %v", err)
	}
	defer os.Remove(binPath)

	data, err := os.ReadFile(binPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "windows binary bytes" {
		t.Errorf("extracted content = %q, want the neutron.exe entry", data)
	}
}

func TestExtractBinaryFromTarGz(t *testing.T) {
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "neutron_1.0.0_darwin_arm64.tar.gz")

	f, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	gzw := gzip.NewWriter(f)
	tw := tar.NewWriter(gzw)
	content := []byte("unix binary bytes")
	if err := tw.WriteHeader(&tar.Header{Name: "neutron", Mode: 0755, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	binPath, err := extractBinary(archivePath, "tar.gz")
	if err != nil {
		t.Fatalf("extractBinary(tar.gz) error: %v", err)
	}
	defer os.Remove(binPath)

	data, err := os.ReadFile(binPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "unix binary bytes" {
		t.Errorf("extracted content = %q, want the neutron entry", data)
	}
}

func TestExtractBinaryErrors(t *testing.T) {
	dir := t.TempDir()

	emptyZip := filepath.Join(dir, "empty.zip")
	zf, _ := os.Create(emptyZip)
	zw := zip.NewWriter(zf)
	zw.Close()
	zf.Close()
	if _, err := extractBinary(emptyZip, "zip"); err == nil {
		t.Error("a ZIP without the neutron binary was accepted")
	}

	notAZip := filepath.Join(dir, "not-a.zip")
	os.WriteFile(notAZip, []byte("definitely not zip"), 0644)
	if _, err := extractBinary(notAZip, "zip"); err == nil {
		t.Error("a non-ZIP archive was accepted by the ZIP extractor")
	}

	emptyTar := filepath.Join(dir, "empty.tar.gz")
	tf, _ := os.Create(emptyTar)
	gzw := gzip.NewWriter(tf)
	gzw.Close()
	tf.Close()
	if _, err := extractBinary(emptyTar, "tar.gz"); err == nil {
		t.Error("a tar.gz without the neutron binary was accepted")
	}
}

// Archive and checksum downloads carry explicit byte caps and stage
// deadlines; an abnormal upstream response must fail bounded, not stall or
// consume unbounded disk (audit neutron-11).
func TestDownloadToTempEnforcesByteCap(t *testing.T) {
	const cap = int64(16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(make([]byte, cap*4))
	}))
	defer srv.Close()

	if _, err := downloadToTemp(context.Background(), srv.URL, cap, time.Minute); err == nil {
		t.Error("an oversized body was downloaded despite the cap")
	}
}

func TestDownloadToTempChecksStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gone", http.StatusNotFound)
	}))
	defer srv.Close()

	if _, err := downloadToTemp(context.Background(), srv.URL, 1024, time.Minute); err == nil {
		t.Error("a non-200 response was accepted")
	}
}

func TestDownloadToTempHonorsContextDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Headers sent, body never arrives: the stage deadline must fire
		// rather than the copy blocking forever.
		w.Header().Set("Content-Length", "1024")
		w.WriteHeader(http.StatusOK)
		<-r.Context().Done()
	}))
	defer srv.Close()

	start := time.Now()
	if _, err := downloadToTemp(context.Background(), srv.URL, 4096, 50*time.Millisecond); err == nil {
		t.Fatal("a stalled body did not fail the download")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("download took %v to fail; the stage deadline did not bound it", elapsed)
	}
}

func TestVerifyChecksumRejectsNon200AndOversized(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "archive")
	os.WriteFile(archive, []byte("bytes"), 0644)

	notFound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer notFound.Close()
	if err := verifyChecksum(context.Background(), archive, "archive", notFound.URL); err == nil {
		t.Error("a non-200 checksum response was accepted")
	}

	huge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(make([]byte, 4*maxChecksumBytes))
	}))
	defer huge.Close()
	if err := verifyChecksum(context.Background(), archive, "archive", huge.URL); err == nil {
		t.Error("an oversized checksum body was accepted")
	}
}
