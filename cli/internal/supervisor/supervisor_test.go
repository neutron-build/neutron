//go:build linux || darwin

package supervisor

import (
	"bytes"
	"context"
	"fmt"
	"github.com/neutron-build/neutron/cli/internal/project"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestHelperProcess(t *testing.T) {
	mode := os.Getenv("NEUTRON_SUPERVISOR_HELPER")
	if mode == "" {
		return
	}
	if marker := os.Getenv("REQUIRE_MARKER"); marker != "" {
		if _, err := os.Stat(marker); err != nil {
			os.Exit(9)
		}
	}
	switch mode {
	case "exit":
		os.Exit(0)
	case "fail":
		os.Exit(7)
	case "tree":
		cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
		cmd.Env = append(os.Environ(), "NEUTRON_SUPERVISOR_HELPER=http")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			os.Exit(8)
		}
		for {
			time.Sleep(time.Hour)
		}
	case "http":
		signal.Ignore(syscall.SIGTERM)
		delay, _ := time.ParseDuration(os.Getenv("READY_DELAY"))
		time.Sleep(delay)
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if marker := os.Getenv("MARKER"); marker != "" {
				_ = os.WriteFile(marker, []byte("ready"), 0600)
			}
			if os.Getenv("REDIRECT") == "1" {
				http.Redirect(w, r, "/health", http.StatusFound)
				return
			}
			fmt.Fprint(w, "ok")
		})
		if err := http.ListenAndServe(os.Getenv("ADDRESS"), handler); err != nil {
			os.Exit(6)
		}
	case "noise":
		fmt.Print(strings.Repeat("x", 100000))
		fmt.Fprintln(os.Stderr, "stderr is drained")
	}
	if started := os.Getenv("STARTED"); started != "" {
		_ = os.WriteFile(started, []byte("started"), 0600)
	}
	for {
		time.Sleep(time.Hour)
	}
}
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
func service(t *testing.T, name, mode, root string) project.Service {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return project.Service{Name: name, Dir: root, Command: []string{binary, "-test.run=^TestHelperProcess$"}, Env: map[string]string{"NEUTRON_SUPERVISOR_HELPER": mode}}
}
func httpService(t *testing.T, name, mode, root string) project.Service {
	s := service(t, name, mode, root)
	port := freePort(t)
	address := fmt.Sprintf("127.0.0.1:%d", port)
	s.Env["ADDRESS"] = address
	s.Ports = []int{port}
	s.Ready = &project.Readiness{HTTP: "http://" + address, Timeout: "2s"}
	return s
}
func waitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for " + path)
}
func waitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not finish")
		return nil
	}
}
func requireClosed(t *testing.T, address string) {
	t.Helper()
	// SIGKILL delivery to a grandchild is asynchronous; the supervisor can reap
	// its direct child only. Require bounded eventual closure, not same-tick closure.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err != nil {
			return
		}
		conn.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("child still accepts connections at " + address)
}
func TestReadinessOrderingCancellationAndLock(t *testing.T) {
	root := t.TempDir()
	api := httpService(t, "api", "http", root)
	api.Env["READY_DELAY"] = "120ms"
	marker := filepath.Join(root, "ready")
	api.Env["MARKER"] = marker
	web := service(t, "web", "stay", root)
	web.Env["REQUIRE_MARKER"] = marker
	web.Env["STARTED"] = filepath.Join(root, "web-started")
	web.DependsOn = []string{"api"}
	plan := &project.Plan{Root: root, Services: []project.Service{api, web}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs bytes.Buffer
	result := make(chan error, 1)
	go func() { result <- Run(ctx, plan, Options{Output: &logs, GracePeriod: 100 * time.Millisecond}) }()
	waitFile(t, web.Env["STARTED"])
	if err := Run(ctx, plan, Options{}); err == nil || !strings.Contains(err.Error(), "lock") {
		t.Fatalf("duplicate session: %v", err)
	}
	cancel()
	if err := waitResult(t, result); err != context.Canceled {
		t.Fatalf("cancel: %v", err)
	}
	text := logs.String()
	if strings.Index(text, "[api] ready") > strings.Index(text, "[web] running") {
		t.Fatal(text)
	}
	if strings.Index(text, "[web] stopping") > strings.Index(text, "[api] stopping") {
		t.Fatal(text)
	}
	requireClosed(t, api.Env["ADDRESS"])
	unlock, err := lockProject(root)
	if err != nil {
		t.Fatal(err)
	}
	unlock()
}
func TestUnexpectedExitCleansUpReadySibling(t *testing.T) {
	for _, mode := range []string{"exit", "fail"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			api := httpService(t, "api", "http", root)
			bad := service(t, "bad", mode, root)
			err := Run(context.Background(), &project.Plan{Root: root, Services: []project.Service{api, bad}}, Options{GracePeriod: 50 * time.Millisecond})
			if err == nil || !strings.Contains(err.Error(), "bad exited unexpectedly") {
				t.Fatalf("%v", err)
			}
			requireClosed(t, api.Env["ADDRESS"])
		})
	}
}
func TestReadinessFailureAndRedirects(t *testing.T) {
	for _, mode := range []string{"slow", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			api := httpService(t, "api", "http", root)
			api.Ready.Timeout = "100ms"
			if mode == "slow" {
				api.Env["READY_DELAY"] = "1s"
			} else {
				api.Env["REDIRECT"] = "1"
			}
			web := service(t, "web", "stay", root)
			web.Env["STARTED"] = filepath.Join(root, "must-not-start")
			err := Run(context.Background(), &project.Plan{Root: root, Services: []project.Service{api, web}}, Options{GracePeriod: 50 * time.Millisecond})
			if err == nil || !strings.Contains(err.Error(), "readiness timed out") {
				t.Fatalf("%v", err)
			}
			if _, err := os.Stat(web.Env["STARTED"]); !os.IsNotExist(err) {
				t.Fatal("dependent launched before readiness")
			}
			requireClosed(t, api.Env["ADDRESS"])
		})
	}
}
func TestGrandchildCleanup(t *testing.T) {
	root := t.TempDir()
	tree := httpService(t, "tree", "tree", root)
	tree.Env["MARKER"] = filepath.Join(root, "ready")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- Run(ctx, &project.Plan{Root: root, Services: []project.Service{tree}}, Options{GracePeriod: 100 * time.Millisecond})
	}()
	waitFile(t, tree.Env["MARKER"])
	cancel()
	if err := waitResult(t, result); err != context.Canceled {
		t.Fatal(err)
	}
	requireClosed(t, tree.Env["ADDRESS"])
}
func TestPreflightDoesNotStartAnything(t *testing.T) {
	root := t.TempDir()
	first := service(t, "first", "stay", root)
	first.Env["STARTED"] = filepath.Join(root, "started")
	bad := service(t, "missing", "stay", root)
	bad.Command = []string{"/no/such/program"}
	err := Run(context.Background(), &project.Plan{Root: root, Services: []project.Service{first, bad}}, Options{})
	if err == nil {
		t.Fatal("missing command accepted")
	}
	if _, err := os.Stat(first.Env["STARTED"]); !os.IsNotExist(err) {
		t.Fatal("partial startup")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	first.Ports = []int{l.Addr().(*net.TCPAddr).Port}
	if err := Run(context.Background(), &project.Plan{Root: root, Services: []project.Service{first}}, Options{}); err == nil {
		t.Fatal("occupied port accepted")
	}
}
func TestLongOutputDoesNotBlockLifecycle(t *testing.T) {
	root := t.TempDir()
	s := service(t, "noise", "noise", root)
	s.Env["STARTED"] = filepath.Join(root, "started")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs bytes.Buffer
	result := make(chan error, 1)
	go func() {
		result <- Run(ctx, &project.Plan{Root: root, Services: []project.Service{s}}, Options{Output: &logs, GracePeriod: 100 * time.Millisecond})
	}()
	waitFile(t, s.Env["STARTED"])
	cancel()
	_ = waitResult(t, result)
	if !strings.Contains(logs.String(), "stderr is drained") {
		t.Fatal("stderr lost")
	}
	for _, line := range strings.Split(logs.String(), "\n") {
		if len(line) > 8250 {
			t.Fatal("unbounded line")
		}
	}
}
