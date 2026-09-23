package scaffold

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/delegate"
	"github.com/neutron-build/neutron/cli/internal/detect"
)

func TestScaffoldPython(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := ScaffoldProject("test-app", detect.Python)
	if err != nil {
		t.Fatalf("ScaffoldProject(Python) error: %v", err)
	}

	// Verify key files exist
	expect := []string{
		"test-app/pyproject.toml",
		"test-app/app/main.py",
		"test-app/.gitignore",
		"test-app/neutron.toml",
		"test-app/migrations/001_init.up.sql",
	}
	for _, f := range expect {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("expected file %s to exist, got error: %v", f, err)
		}
	}

	// Verify neutron.toml contents
	data, _ := os.ReadFile(filepath.Join(dir, "test-app/neutron.toml"))
	if len(data) == 0 {
		t.Error("neutron.toml is empty")
	}
}

func TestScaffoldGo(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := ScaffoldProject("my-service", detect.Go)
	if err != nil {
		t.Fatalf("ScaffoldProject(Go) error: %v", err)
	}

	expect := []string{
		"my-service/go.mod",
		"my-service/cmd/server/main.go",
		"my-service/internal/handler/hello.go",
		"my-service/neutron.toml",
		"my-service/migrations/001_init.up.sql",
		"my-service/migrations/001_init.down.sql",
	}
	for _, f := range expect {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("expected file %s to exist, got error: %v", f, err)
		}
	}
}

func TestScaffoldTypeScript(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := ScaffoldProject("ts-app", detect.TypeScript)
	if err != nil {
		t.Fatalf("ScaffoldProject(TypeScript) error: %v", err)
	}

	expect := []string{
		"ts-app/package.json",
		"ts-app/tsconfig.json",
		"ts-app/src/main.tsx",
		"ts-app/src/routes/index.tsx",
		"ts-app/neutron.toml",
	}
	for _, f := range expect {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("expected file %s to exist, got error: %v", f, err)
		}
	}
}

func TestScaffoldRust(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	err := ScaffoldProject("rust-svc", detect.Rust)
	if err != nil {
		t.Fatalf("ScaffoldProject(Rust) error: %v", err)
	}

	expect := []string{
		"rust-svc/Cargo.toml",
		"rust-svc/src/main.rs",
		"rust-svc/neutron.toml",
	}
	for _, f := range expect {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("expected file %s to exist, got error: %v", f, err)
		}
	}
}

func TestScaffoldAlreadyExists(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	os.Mkdir("existing", 0755)
	err := ScaffoldProject("existing", detect.Python)
	if err == nil {
		t.Fatal("expected error for existing directory, got nil")
	}
}

func TestScaffoldInvalidName(t *testing.T) {
	err := ScaffoldProject("", detect.Python)
	if err == nil {
		t.Fatal("expected error for empty name")
	}

	err = ScaffoldProject("bad name", detect.Python)
	if err == nil {
		t.Fatal("expected error for name with spaces")
	}

	err = ScaffoldProject("-starts-with-dash", detect.Python)
	if err == nil {
		t.Fatal("expected error for name starting with dash")
	}
}

// A scaffold that does not compile ships silently if tests only check file
// names; this builds it against the SDK version the template pins.
func TestScaffoldGoCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("downloads the published Go SDK")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not available")
	}
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)
	if err := ScaffoldProject("compiles", detect.Go); err != nil {
		t.Fatal(err)
	}
	project := filepath.Join(dir, "compiles")
	for _, args := range [][]string{{"mod", "tidy"}, {"build", "./..."}, {"vet", "./..."}} {
		cmd := exec.Command("go", args...)
		cmd.Dir = project
		cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go %v: %v\n%s", args, err, out)
		}
	}
}

// ---------------------------------------------------------------------------
// Install, build and run tests.
//
// Each one scaffolds a project, installs it with the ecosystem's own tool
// (fetching the SDK the templates pin, so they need the network), builds or
// typechecks it and, for server languages, starts it the way `neutron dev`
// does (delegate.DevCommand) on a free NEUTRON_PORT and requires GET /health
// to return FRAMEWORK_CONTRACT §7. They skip under -short or when the
// toolchain is missing.
// ---------------------------------------------------------------------------

// testDatabaseURL, when set, is a reachable PostgreSQL/Nucleus the Go and Julia
// run tests use. The Go scaffold exits at startup without a database.
const testDatabaseURL = "NEUTRON_TEST_DATABASE_URL"

func requireTools(t *testing.T, tools ...string) {
	t.Helper()
	if testing.Short() {
		t.Skip("installs the SDK from the network")
	}
	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found on PATH", tool)
		}
	}
}

// scaffoldIn scaffolds name in a fresh temporary directory and returns the
// project path.
func scaffoldIn(t *testing.T, lang detect.Language, name string) string {
	t.Helper()
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(origDir)
	if err := ScaffoldProject(name, lang); err != nil {
		t.Fatalf("ScaffoldProject(%s): %v", lang, err)
	}
	return filepath.Join(dir, name)
}

// run runs a command in dir and fails the test on error or timeout.
func run(t *testing.T, dir string, timeout time.Duration, name string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	ownProcessGroup(cmd)
	cmd.Cancel = func() error { signalGroup(cmd, true); return nil }
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, tail(out))
	}
	return string(out)
}

func tail(out []byte) string {
	const max = 6000
	if len(out) > max {
		return "...\n" + string(out[len(out)-max:])
	}
	return string(out)
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

// server is a process tree started by startServer; it is stopped at cleanup.
type server struct {
	cmd    *exec.Cmd
	output *lockedBuffer
	done   chan struct{}
	err    error
}

func (s *server) exited() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// stop sends SIGTERM to the whole process group, then SIGKILL after 10s.
func (s *server) stop() {
	if s.exited() {
		return
	}
	signalGroup(s.cmd, false)
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		signalGroup(s.cmd, true)
		<-s.done
	}
}

func startServer(t *testing.T, cmd *exec.Cmd) *server {
	t.Helper()
	s := &server{cmd: cmd, output: &lockedBuffer{}, done: make(chan struct{})}
	cmd.Stdout = s.output
	cmd.Stderr = s.output
	ownProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", cmd.Path, err)
	}
	go func() {
		s.err = cmd.Wait()
		close(s.done)
	}()
	t.Cleanup(s.stop)
	return s
}

// startDev starts the project the way `neutron dev` does, on port.
func startDev(t *testing.T, lang detect.Language, project, port string) *server {
	t.Helper()
	t.Setenv("NEUTRON_PORT", port)
	cmd, err := delegate.DevCommand(lang, project)
	if err != nil {
		t.Fatalf("delegate.DevCommand(%s): %v", lang, err)
	}
	return startServer(t, cmd)
}

// get polls url until it answers or the server exits, and returns the first
// response's status and body.
func get(t *testing.T, s *server, url string, timeout time.Duration) (int, []byte) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(timeout)
	for {
		resp, err := client.Get(url)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp.StatusCode, body
		}
		if s.exited() {
			t.Fatalf("server exited before %s answered (%v)\n%s", url, s.err, tail(s.output.Bytes()))
		}
		if time.Now().After(deadline) {
			s.stop()
			t.Fatalf("no answer from %s after %s: %v\n%s", url, timeout, err, tail(s.output.Bytes()))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// requireContractHealth checks GET /health against FRAMEWORK_CONTRACT §7:
// 200 {"status": "ok", "nucleus": connected|unconfigured, "version": "X.Y.Z"}.
func requireContractHealth(t *testing.T, s *server, port string, timeout time.Duration) {
	t.Helper()
	checkHealth(t, s, port, timeout, true)
}

// checkHealth requires a 200 with exactly the §7 keys. strict also requires
// status "ok" with nucleus connected or unconfigured.
func checkHealth(t *testing.T, s *server, port string, timeout time.Duration, strict bool) {
	t.Helper()
	status, body := get(t, s, "http://localhost:"+port+"/health", timeout)
	if status != http.StatusOK {
		t.Fatalf("GET /health = %d, want 200\n%s\n%s", status, body, tail(s.output.Bytes()))
	}
	var health map[string]any
	if err := json.Unmarshal(body, &health); err != nil {
		t.Fatalf("GET /health body is not JSON: %v\n%s", err, body)
	}
	_, hasStatus := health["status"]
	_, hasNucleus := health["nucleus"]
	if len(health) != 3 || !hasStatus || !hasNucleus {
		t.Fatalf("GET /health = %s, want exactly {status, nucleus, version}", body)
	}
	if strict && (health["status"] != "ok" ||
		(health["nucleus"] != "connected" && health["nucleus"] != "unconfigured")) {
		t.Fatalf("GET /health = %s, want {status: ok, nucleus: connected|unconfigured, version}", body)
	}
	if v, ok := health["version"].(string); !ok || v == "" {
		t.Fatalf("GET /health version = %v, want a non-empty string", health["version"])
	}
	t.Logf("GET /health -> %d %s", status, body)
}

func TestScaffoldGoRuns(t *testing.T) {
	requireTools(t, "go")
	dbURL := os.Getenv(testDatabaseURL)
	if dbURL == "" {
		t.Skipf("%s not set; the Go scaffold connects to the database at startup", testDatabaseURL)
	}
	project := scaffoldIn(t, detect.Go, "go-runs")
	t.Setenv("GOFLAGS", "-mod=mod")
	run(t, project, 5*time.Minute, "go", "mod", "tidy")
	run(t, project, 5*time.Minute, "go", "build", "./...")
	t.Setenv("NEUTRON_DATABASE_URL", dbURL)
	port := freePort(t)
	s := startDev(t, detect.Go, project, port)
	// Shape only: the published Go SDK v0.1.0 that the scaffold pins reports
	// {"status": "ok", "nucleus": "disconnected"} for a reachable plain
	// PostgreSQL (feature detection, not health). The SDK on main is fixed;
	// the strict check applies once a release with the fix is pinned.
	checkHealth(t, s, port, 3*time.Minute, false)
}

func TestScaffoldPythonRuns(t *testing.T) {
	requireTools(t, "python3")
	if out, err := exec.Command("python3", "-c", "import sys; sys.exit(sys.version_info < (3, 11))").CombinedOutput(); err != nil {
		t.Skipf("python3 is older than 3.11 (%s)", out)
	}
	project := scaffoldIn(t, detect.Python, "py-runs")
	run(t, project, 2*time.Minute, "python3", "-m", "venv", ".venv")
	py := delegate.PythonFor(project)
	run(t, project, 10*time.Minute, py, "-m", "pip", "install", "--disable-pip-version-check", "-e", ".")
	run(t, project, time.Minute, py, "-c", "import app.main")
	port := freePort(t)
	s := startDev(t, detect.Python, project, port)
	requireContractHealth(t, s, port, 2*time.Minute)
}

func TestScaffoldTypeScriptRuns(t *testing.T) {
	requireTools(t, "node", "npm")
	project := scaffoldIn(t, detect.TypeScript, "ts-runs")
	run(t, project, 10*time.Minute, "npm", "install", "--no-audit", "--no-fund")
	bin := filepath.Join(project, "node_modules", ".bin")
	run(t, project, 2*time.Minute, filepath.Join(bin, "tsc"), "--noEmit")

	// neutron dev: the Vite dev server renders the file-routed page. It does
	// not serve /health; the production server below does.
	port := freePort(t)
	dev := startDev(t, detect.TypeScript, project, port)
	status, body := get(t, dev, "http://localhost:"+port+"/", 2*time.Minute)
	if status != http.StatusOK || !bytes.Contains(body, []byte("ts-runs")) {
		t.Fatalf("neutron dev GET / = %d, want 200 with the project name\n%s", status, tail(body))
	}
	dev.stop()

	run(t, project, 5*time.Minute, filepath.Join(bin, "neutron-ts"), "build")
	port = freePort(t)
	cmd := exec.Command(filepath.Join(bin, "neutron-ts"), "start", "--port", port)
	cmd.Dir = project
	start := startServer(t, cmd)
	requireContractHealth(t, start, port, time.Minute)
}

func TestScaffoldRustRuns(t *testing.T) {
	requireTools(t, "cargo")
	// A shared target directory keeps reruns from recompiling the SDK.
	if cache, err := os.UserCacheDir(); err == nil {
		t.Setenv("CARGO_TARGET_DIR", filepath.Join(cache, "neutron-cli-scaffold-test", "cargo-target"))
	}
	project := scaffoldIn(t, detect.Rust, "rust-runs")
	run(t, project, 15*time.Minute, "cargo", "build")
	port := freePort(t)
	s := startDev(t, detect.Rust, project, port)
	requireContractHealth(t, s, port, 5*time.Minute)
}

func TestScaffoldZigRuns(t *testing.T) {
	requireTools(t)
	zig, err := delegate.Zig()
	if err != nil {
		t.Skip(err)
	}
	project := scaffoldIn(t, detect.Zig, "zig-runs")
	run(t, project, 10*time.Minute, zig, "build")
	port := freePort(t)
	s := startDev(t, detect.Zig, project, port)
	requireContractHealth(t, s, port, 3*time.Minute)
}

// The Julia SDK is a database client with no HTTP layer, so `neutron dev`
// runs the project's script: it must connect and query, or fail clearly.
func TestScaffoldJuliaRuns(t *testing.T) {
	requireTools(t, "julia")
	if out, err := exec.Command("julia", "-e", "exit(VERSION >= v\"1.11\" ? 0 : 1)").CombinedOutput(); err != nil {
		t.Skipf("julia is older than 1.11, which [sources] in Project.toml needs (%s)", out)
	}
	project := scaffoldIn(t, detect.Julia, "jl-runs")
	run(t, project, 15*time.Minute, "julia", "--project=.", "-e", "using Pkg; Pkg.instantiate(); using NeutronJulia")

	dbURL := os.Getenv(testDatabaseURL)
	want := "connected: SELECT 1 returned 1"
	if dbURL == "" {
		dbURL = "postgres://127.0.0.1:" + freePort(t) + "/none"
		want = "could not connect to the database"
	}
	t.Setenv("NEUTRON_DATABASE_URL", dbURL)
	cmd, err := delegate.DevCommand(detect.Julia, project)
	if err != nil {
		t.Fatal(err)
	}
	s := startServer(t, cmd)
	select {
	case <-s.done:
	case <-time.After(5 * time.Minute):
		t.Fatalf("src/main.jl did not finish\n%s", tail(s.output.Bytes()))
	}
	out := s.output.Bytes()
	if !bytes.Contains(out, []byte(want)) {
		t.Fatalf("src/main.jl output lacks %q (exit: %v)\n%s", want, s.err, tail(out))
	}
	if (s.err == nil) != (want == "connected: SELECT 1 returned 1") {
		t.Fatalf("src/main.jl exit = %v with output\n%s", s.err, tail(out))
	}
	t.Logf("neutron dev (julia): %s", strings.TrimSpace(string(out)))
}

// The printed next steps and the README must agree, since both are what a
// user follows.
func TestReadmeListsNextSteps(t *testing.T) {
	for _, lang := range detect.AllLanguages() {
		project := scaffoldIn(t, lang, "readme-"+string(lang))
		readme, err := os.ReadFile(filepath.Join(project, "README.md"))
		if err != nil {
			t.Fatalf("%s: %v", lang, err)
		}
		steps := NextSteps(lang)
		if len(steps) == 0 {
			t.Fatalf("%s: no next steps", lang)
		}
		for _, step := range steps {
			if !bytes.Contains(readme, []byte(step+"\n")) {
				t.Errorf("%s: README.md does not list %q", lang, step)
			}
		}
		if dotenv, err := os.ReadFile(filepath.Join(project, ".env")); err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(dotenv)), "\n") {
				if !strings.HasPrefix(line, "NEUTRON_") {
					t.Errorf("%s: .env line %q lacks the NEUTRON_ prefix", lang, line)
				}
			}
		}
	}
}

// The names and ids the templates depend on must match the SDKs in this
// repository.
func TestSDKDependenciesMatchRepo(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	checks := []struct {
		lang detect.Language
		file string
		want func(Dependency) []string
	}{
		{detect.Python, "python/pyproject.toml", func(d Dependency) []string { return []string{`name = "` + d.Name + `"`} }},
		{detect.Rust, "rust/crates/neutron/Cargo.toml", func(d Dependency) []string { return []string{`name = "` + d.Name + `"`} }},
		{detect.TypeScript, "typescript/packages/neutron/package.json", func(d Dependency) []string { return []string{`"name": "` + d.Name + `"`} }},
		{detect.Julia, "julia/Project.toml", func(d Dependency) []string {
			return []string{`name = "` + d.Name + `"`, `uuid = "` + d.UUID + `"`}
		}},
		{detect.Zig, "zig/build.zig.zon", func(d Dependency) []string { return []string{".minimum_zig_version"} }},
	}
	for _, c := range checks {
		sdk, _ := sdkFor(c.lang)
		if sdk.Subdir != "" && !strings.HasPrefix(c.file, sdk.Subdir+"/") {
			t.Errorf("%s: Subdir %q does not hold %s", c.lang, sdk.Subdir, c.file)
		}
		data, err := os.ReadFile(filepath.Join(root, c.file))
		if err != nil {
			t.Fatalf("%s: %v", c.lang, err)
		}
		for _, want := range c.want(sdk) {
			if !bytes.Contains(data, []byte(want)) {
				t.Errorf("%s: %s does not contain %s", c.lang, c.file, want)
			}
		}
	}
	_, cli := sdkFor(detect.TypeScript)
	data, err := os.ReadFile(filepath.Join(root, "typescript/packages/neutron-cli/package.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"name": "`+cli.Name+`"`)) {
		t.Errorf("typescript CLI package is not %s", cli.Name)
	}
}
