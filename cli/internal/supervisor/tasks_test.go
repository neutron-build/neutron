//go:build linux || darwin

package supervisor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/project"
)

func task(t *testing.T, name, mode, root string, deps ...string) project.Task {
	t.Helper()
	s := service(t, name, mode, root)
	s.Env["NAME"] = name
	s.Env["RECORD"] = filepath.Join(root, "record")
	return project.Task{Name: name, Dir: root, Command: s.Command, Env: s.Env, DependsOn: deps, Timeout: "10s"}
}

func statuses(r *TaskReport) map[string]TaskStatus {
	m := map[string]TaskStatus{}
	for _, t := range r.Tasks {
		m[t.Name] = t.Status
	}
	return m
}

func runTasks(t *testing.T, ctx context.Context, tasks []project.Task, jobs int) (*TaskReport, string) {
	t.Helper()
	var logs bytes.Buffer
	report, err := RunTasks(ctx, tasks, TaskOptions{Output: &logs, Jobs: jobs, GracePeriod: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return report, logs.String()
}

func TestTasksRunInDependencyOrder(t *testing.T) {
	root := t.TempDir()
	tasks := []project.Task{task(t, "build", "record", root), task(t, "test", "record", root, "build"), task(t, "package", "record", root, "test")}
	report, _ := runTasks(t, context.Background(), tasks, 4)
	if !report.Succeeded() || report.Version != 1 {
		t.Fatalf("%+v", report)
	}
	for _, r := range report.Tasks {
		if r.ExitCode == nil || *r.ExitCode != 0 {
			t.Fatalf("%s exit code %v", r.Name, r.ExitCode)
		}
	}
	record, _ := os.ReadFile(filepath.Join(root, "record"))
	if string(record) != "build\ntest\npackage\n" {
		t.Fatalf("order: %q", record)
	}
}

func TestTaskFailureSkipsTheRest(t *testing.T) {
	root := t.TempDir()
	build := task(t, "build", "fail", root)
	test := task(t, "test", "record", root, "build")
	lint := task(t, "lint", "record", root)
	report, logs := runTasks(t, context.Background(), []project.Task{build, test, lint}, 1)
	got := statuses(report)
	if got["build"] != TaskFailed || got["test"] != TaskSkipped || got["lint"] != TaskSkipped {
		t.Fatalf("%v\n%s", got, logs)
	}
	if code := report.Tasks[0].ExitCode; code == nil || *code != 7 {
		t.Fatalf("exit code %v", code)
	}
	if _, err := os.Stat(filepath.Join(root, "record")); !os.IsNotExist(err) {
		t.Fatal("a task ran after a failure")
	}
}

func TestTaskFailureCancelsRunningSibling(t *testing.T) {
	root := t.TempDir()
	slow := task(t, "slow", "stay", root)
	slow.Env["STARTED"] = filepath.Join(root, "slow-started")
	bad := task(t, "bad", "fail", root)
	bad.Env["REQUIRE_MARKER"] = slow.Env["STARTED"]
	started := time.Now()
	report, logs := runTasks(t, context.Background(), []project.Task{bad, slow}, 2)
	got := statuses(report)
	if got["bad"] != TaskFailed || got["slow"] != TaskCancelled {
		t.Fatalf("%v\n%s", got, logs)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("sibling was not stopped promptly")
	}
}

func TestTaskTimeout(t *testing.T) {
	root := t.TempDir()
	hang := task(t, "hang", "stay", root)
	hang.Timeout = "300ms"
	after := task(t, "after", "record", root, "hang")
	started := time.Now()
	report, logs := runTasks(t, context.Background(), []project.Task{hang, after}, 2)
	got := statuses(report)
	if got["hang"] != TaskTimedOut || got["after"] != TaskSkipped {
		t.Fatalf("%v\n%s", got, logs)
	}
	if !strings.Contains(logs, "exceeded 300ms") || time.Since(started) > 3*time.Second {
		t.Fatalf("%s after %s", logs, time.Since(started))
	}
}

func TestTaskMissingExecutable(t *testing.T) {
	root := t.TempDir()
	missing := task(t, "missing", "exit", root)
	missing.Command = []string{"neutron-no-such-tool"}
	next := task(t, "next", "record", root, "missing")
	report, _ := runTasks(t, context.Background(), []project.Task{missing, next}, 1)
	r := report.Tasks[0]
	if r.Status != TaskFailed || r.ExitCode != nil || !strings.Contains(r.Error, "not found") {
		t.Fatalf("%+v", r)
	}
	if report.Tasks[1].Status != TaskSkipped {
		t.Fatalf("%+v", report.Tasks[1])
	}
}

func TestTaskCancellation(t *testing.T) {
	root := t.TempDir()
	hang := task(t, "hang", "stay", root)
	hang.Env["STARTED"] = filepath.Join(root, "started")
	after := task(t, "after", "record", root, "hang")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		waitFile(t, hang.Env["STARTED"])
		cancel()
	}()
	report, logs := runTasks(t, ctx, []project.Task{hang, after}, 1)
	got := statuses(report)
	if got["hang"] != TaskCancelled || got["after"] != TaskSkipped {
		t.Fatalf("%v\n%s", got, logs)
	}
}

func TestTaskConcurrencyIsBounded(t *testing.T) {
	root := t.TempDir()
	left := task(t, "left", "barrier", root)
	right := task(t, "right", "barrier", root)
	left.Env["STARTED"], left.Env["PEER"] = filepath.Join(root, "left"), filepath.Join(root, "right")
	right.Env["STARTED"], right.Env["PEER"] = filepath.Join(root, "right"), filepath.Join(root, "left")
	report, logs := runTasks(t, context.Background(), []project.Task{left, right}, 2)
	if !report.Succeeded() {
		t.Fatalf("--jobs 2 did not run concurrently: %v\n%s", statuses(report), logs)
	}
	for _, f := range []string{"left", "right"} {
		os.Remove(filepath.Join(root, f))
	}
	left.Timeout, right.Timeout = "1s", "1s"
	report, _ = runTasks(t, context.Background(), []project.Task{left, right}, 1)
	if report.Succeeded() {
		t.Fatal("--jobs 1 ran two tasks at once")
	}
}
