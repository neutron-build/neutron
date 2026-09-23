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
	"strconv"
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
	case "coordinator":
		// A coordinator in its own process, so the test can SIGKILL it.
		binary, _ := os.Executable()
		address := os.Getenv("SERVICE_ADDRESS")
		_, port, _ := net.SplitHostPort(address)
		portNumber, _ := strconv.Atoi(port)
		api := project.Service{Name: "api", Dir: os.Getenv("ROOT"), Command: []string{binary, "-test.run=^TestHelperProcess$"},
			Env:   map[string]string{"NEUTRON_SUPERVISOR_HELPER": "http", "ADDRESS": address},
			Ports: []int{portNumber}, Ready: &project.Readiness{HTTP: "http://" + address, Timeout: "5s"}}
		_ = Run(context.Background(), &project.Plan{Root: os.Getenv("ROOT"), Services: []project.Service{api}}, Options{GracePeriod: 200 * time.Millisecond})
		os.Exit(0)
	case "exit":
		os.Exit(0)
	case "record":
		f, err := os.OpenFile(os.Getenv("RECORD"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			os.Exit(5)
		}
		fmt.Fprintln(f, os.Getenv("NAME"))
		f.Close()
		os.Exit(0)
	case "barrier":
		// Succeeds only if a peer runs at the same time.
		_ = os.WriteFile(os.Getenv("STARTED"), []byte("started"), 0600)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(os.Getenv("PEER")); err == nil {
				os.Exit(0)
			}
			time.Sleep(10 * time.Millisecond)
		}
		os.Exit(3)
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

func waitAccepting(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond); err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("service never accepted connections at " + address)
}

// Process groups are not signalled when their parent dies; the shim's
// lifeline must stop the service however the coordinator ends.
func TestCoordinatorDeathStopsServices(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGKILL, syscall.SIGHUP} {
		t.Run(sig.String(), func(t *testing.T) {
			root := t.TempDir()
			address := fmt.Sprintf("127.0.0.1:%d", freePort(t))
			coordinator := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
			coordinator.Env = append(os.Environ(), "NEUTRON_SUPERVISOR_HELPER=coordinator", "ROOT="+root, "SERVICE_ADDRESS="+address)
			if err := coordinator.Start(); err != nil {
				t.Fatal(err)
			}
			waitAccepting(t, address)
			_ = coordinator.Process.Signal(sig)
			_ = coordinator.Wait()
			requireClosed(t, address)
			unlock, err := lockProject(root)
			if err != nil {
				t.Fatalf("lock not released after coordinator death: %v", err)
			}
			unlock()
		})
	}
}

func TestInterruptDuringReadiness(t *testing.T) {
	root := t.TempDir()
	api := httpService(t, "api", "http", root)
	api.Env["READY_DELAY"] = "3s"
	api.Ready.Timeout = "10s"
	web := service(t, "web", "stay", root)
	web.Env["STARTED"] = filepath.Join(root, "must-not-start")
	web.DependsOn = []string{"api"}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- Run(ctx, &project.Plan{Root: root, Services: []project.Service{api, web}}, Options{GracePeriod: 100 * time.Millisecond})
	}()
	time.Sleep(300 * time.Millisecond)
	started := time.Now()
	cancel()
	if err := waitResult(t, result); err != context.Canceled {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("interrupt during readiness took %s", elapsed)
	}
	if _, err := os.Stat(web.Env["STARTED"]); !os.IsNotExist(err) {
		t.Fatal("dependent started after interrupt")
	}
	requireClosed(t, api.Env["ADDRESS"])
}

func TestForceSkipsGracePeriod(t *testing.T) {
	root := t.TempDir()
	api := httpService(t, "api", "http", root) // ignores SIGTERM
	api.Env["MARKER"] = filepath.Join(root, "ready")
	ctx, cancel := context.WithCancel(context.Background())
	force := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- Run(ctx, &project.Plan{Root: root, Services: []project.Service{api}}, Options{GracePeriod: 30 * time.Second, Force: force})
	}()
	waitAccepting(t, api.Env["ADDRESS"])
	started := time.Now()
	cancel()
	time.Sleep(100 * time.Millisecond)
	close(force)
	if err := waitResult(t, result); err != context.Canceled {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("force did not skip grace: %s", elapsed)
	}
	requireClosed(t, api.Env["ADDRESS"])
}

// A stale server on the wildcard or ::1 must fail preflight; otherwise the
// readiness probe would succeed against it.
func TestPreflightRejectsListenerOnOtherAddresses(t *testing.T) {
	for _, host := range []string{"0.0.0.0", "[::]", "[::1]"} {
		t.Run(host, func(t *testing.T) {
			l, err := net.Listen("tcp", host+":0")
			if err != nil {
				t.Skipf("%s unavailable here: %v", host, err)
			}
			defer l.Close()
			root := t.TempDir()
			s := service(t, "api", "stay", root)
			s.Env["STARTED"] = filepath.Join(root, "started")
			s.Ports = []int{l.Addr().(*net.TCPAddr).Port}
			err = Run(context.Background(), &project.Plan{Root: root, Services: []project.Service{s}}, Options{})
			if err == nil || !strings.Contains(err.Error(), "unavailable") {
				t.Fatalf("occupied port accepted: %v", err)
			}
			if _, err := os.Stat(s.Env["STARTED"]); !os.IsNotExist(err) {
				t.Fatal("service started despite occupied port")
			}
		})
	}
}
