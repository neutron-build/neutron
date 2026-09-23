package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/neutron-build/neutron/cli/internal/project"
)

type TaskOptions struct {
	Output      io.Writer
	Jobs        int
	GracePeriod time.Duration
	Force       <-chan struct{}
}

type TaskStatus string

const (
	TaskSucceeded TaskStatus = "succeeded"
	TaskFailed    TaskStatus = "failed"
	TaskTimedOut  TaskStatus = "timed_out"
	TaskCancelled TaskStatus = "cancelled"
	TaskSkipped   TaskStatus = "skipped"
)

type TaskResult struct {
	Name       string     `json:"name"`
	Status     TaskStatus `json:"status"`
	ExitCode   *int       `json:"exit_code,omitempty"`
	DurationMS int64      `json:"duration_ms"`
	Outputs    []string   `json:"outputs,omitempty"`
	Error      string     `json:"error,omitempty"`
}

type TaskReport struct {
	Version int          `json:"version"`
	Tasks   []TaskResult `json:"tasks"`
}

func (r *TaskReport) Succeeded() bool {
	for _, t := range r.Tasks {
		if t.Status != TaskSucceeded {
			return false
		}
	}
	return true
}

type runningTask struct {
	cmd     *exec.Cmd
	done    chan struct{}
	started time.Time
	timer   *time.Timer
}
type taskEvent struct {
	index   int
	err     error
	timeout bool
}

// RunTasks runs tasks (already in dependency order) with bounded concurrency.
// It is fail-fast: after the first failure, timeout or cancellation nothing
// new starts, running tasks are stopped, and unstarted tasks are skipped.
// The returned error covers setup only; task outcomes are in the report.
func RunTasks(ctx context.Context, tasks []project.Task, options TaskOptions) (*TaskReport, error) {
	if err := platformCheck(); err != nil {
		return nil, err
	}
	if options.Output == nil {
		options.Output = io.Discard
	}
	if options.GracePeriod <= 0 {
		options.GracePeriod = 2 * time.Second
	}
	if options.Jobs < 1 {
		options.Jobs = 1
	}
	out := &output{writer: options.Output}
	lifeline, lifelineWriter, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer lifelineWriter.Close()
	defer lifeline.Close()

	report := &TaskReport{Version: 1, Tasks: make([]TaskResult, len(tasks))}
	index := map[string]int{}
	for i, t := range tasks {
		report.Tasks[i] = TaskResult{Name: t.Name, Outputs: t.Outputs}
		index[t.Name] = i
	}
	events := make(chan taskEvent, 2*len(tasks))
	active := map[int]*runningTask{}
	stopping := false

	finish := func(i int, status TaskStatus, detail string) {
		r := &report.Tasks[i]
		r.Status = status
		r.Error = detail
		if status == TaskSucceeded {
			out.print("[%s] succeeded in %s\n", r.Name, time.Duration(r.DurationMS)*time.Millisecond)
		} else if detail != "" {
			out.print("[%s] %s: %s\n", r.Name, status, detail)
		} else {
			out.print("[%s] %s\n", r.Name, status)
		}
	}
	start := func(i int) error {
		t := tasks[i]
		s := project.Service{Name: t.Name, Dir: t.Dir, Command: t.Command, Env: t.Env}
		binary, err := executable(s)
		if err != nil {
			return err
		}
		cmd, err := shimCommand(binary, t.Command[1:], lifeline)
		if err != nil {
			return err
		}
		cmd.Dir = t.Dir
		cmd.Env = append(environment(t.Env), shimEnv+"="+options.GracePeriod.String())
		ownProcess(cmd)
		stdout := &logWriter{out: out, prefix: t.Name + " stdout"}
		stderr := &logWriter{out: out, prefix: t.Name + " stderr"}
		cmd.Stdout, cmd.Stderr = stdout, stderr
		cmd.WaitDelay = options.GracePeriod
		if err := cmd.Start(); err != nil {
			return err
		}
		timeout, _ := time.ParseDuration(t.Timeout)
		run := &runningTask{cmd: cmd, done: make(chan struct{}), started: time.Now()}
		run.timer = time.AfterFunc(timeout, func() { events <- taskEvent{index: i, timeout: true} })
		active[i] = run
		out.print("[%s] started\n", t.Name)
		go func() {
			err := cmd.Wait()
			stdout.flush()
			stderr.flush()
			close(run.done)
			events <- taskEvent{index: i, err: err}
		}()
		return nil
	}
	// stopAll signals every running task at once and shares one grace period.
	stopAll := func() {
		for _, run := range active {
			signalTree(run.cmd, false)
		}
		deadline := time.NewTimer(options.GracePeriod)
		defer deadline.Stop()
		for _, run := range active {
			select {
			case <-run.done:
			case <-deadline.C:
			case <-options.Force:
			}
		}
		for _, run := range active {
			signalTree(run.cmd, true)
			<-run.done
		}
	}
	halt := func(status TaskStatus, except int) {
		stopping = true
		for i := range active {
			if i != except && report.Tasks[i].Status == "" {
				report.Tasks[i].Status = status
			}
		}
		stopAll()
	}

	for {
		if !stopping && ctx.Err() != nil {
			halt(TaskCancelled, -1)
		}
		if !stopping {
			for i, t := range tasks {
				if len(active) >= options.Jobs {
					break
				}
				if report.Tasks[i].Status != "" || active[i] != nil {
					continue
				}
				ready := true
				for _, dep := range t.DependsOn {
					if report.Tasks[index[dep]].Status != TaskSucceeded {
						ready = false
					}
				}
				if !ready {
					continue
				}
				if err := start(i); err != nil {
					finish(i, TaskFailed, err.Error())
					halt(TaskCancelled, -1)
					break
				}
			}
		}
		if len(active) == 0 {
			break
		}
		cancelled := ctx.Done()
		if stopping {
			cancelled = nil
		}
		select {
		case <-cancelled:
			halt(TaskCancelled, -1)
		case e := <-events:
			run := active[e.index]
			if run == nil {
				continue // a timeout that fired after completion
			}
			if e.timeout {
				if !stopping {
					report.Tasks[e.index].Status = TaskTimedOut
					halt(TaskCancelled, e.index)
				}
				continue
			}
			run.timer.Stop()
			delete(active, e.index)
			r := &report.Tasks[e.index]
			r.DurationMS = time.Since(run.started).Milliseconds()
			code := 0
			var exitErr *exec.ExitError
			if errors.As(e.err, &exitErr) {
				code = exitErr.ExitCode()
			} else if e.err != nil {
				code = -1
			}
			r.ExitCode = &code
			switch {
			case r.Status == TaskTimedOut:
				finish(e.index, TaskTimedOut, fmt.Sprintf("exceeded %s", tasks[e.index].Timeout))
			case r.Status == TaskCancelled:
				finish(e.index, TaskCancelled, "")
			case e.err == nil:
				finish(e.index, TaskSucceeded, "")
			default:
				finish(e.index, TaskFailed, e.err.Error())
				if !stopping {
					halt(TaskCancelled, -1)
				}
			}
		}
	}
	for i := range report.Tasks {
		if report.Tasks[i].Status == "" {
			finish(i, TaskSkipped, "")
		}
	}
	return report, nil
}
