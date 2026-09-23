// Package supervisor runs an explicitly configured application in the foreground.
package supervisor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/neutron-build/neutron/cli/internal/project"
)

type Options struct {
	Output      io.Writer
	GracePeriod time.Duration
	// Force, when closed, skips remaining grace periods during shutdown
	// (a second interrupt). Nil means always wait the full grace period.
	Force <-chan struct{}
}
type process struct {
	service project.Service
	cmd     *exec.Cmd
	done    chan struct{}
}
type exit struct {
	name string
	err  error
}
type output struct {
	mu     sync.Mutex
	writer io.Writer
}

func (o *output) print(format string, args ...any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, _ = fmt.Fprintf(o.writer, format, args...)
}

// Bound partial lines while preserving complete draining of both output streams.
type logWriter struct {
	out     *output
	prefix  string
	pending []byte
}

func (w *logWriter) Write(data []byte) (int, error) {
	n := len(data)
	for len(data) > 0 {
		size := 8192 - len(w.pending)
		if size > len(data) {
			size = len(data)
		}
		w.pending = append(w.pending, data[:size]...)
		data = data[size:]
		for {
			index := bytes.IndexByte(w.pending, '\n')
			if index < 0 {
				break
			}
			w.out.print("[%s] %s\n", w.prefix, w.pending[:index])
			w.pending = w.pending[index+1:]
		}
		if len(w.pending) == 8192 {
			w.flush()
		}
	}
	return n, nil
}
func (w *logWriter) flush() {
	if len(w.pending) > 0 {
		w.out.print("[%s] %s\n", w.prefix, w.pending)
		w.pending = nil
	}
}

func environment(overrides map[string]string) []string {
	values := map[string]string{}
	for _, entry := range os.Environ() {
		k, v, ok := strings.Cut(entry, "=")
		if ok {
			values[k] = v
		}
	}
	for k, v := range overrides {
		values[k] = v
	}
	result := make([]string, 0, len(values))
	for k, v := range values {
		result = append(result, k+"="+v)
	}
	return result
}
func executable(s project.Service) (string, error) {
	name := s.Command[0]
	check := func(path string) (string, error) {
		info, err := os.Stat(path)
		if err != nil {
			return "", err
		}
		if info.IsDir() || info.Mode()&0111 == 0 {
			return "", fmt.Errorf("not executable: %s", path)
		}
		return path, nil
	}
	if strings.ContainsRune(name, filepath.Separator) {
		if !filepath.IsAbs(name) {
			name = filepath.Join(s.Dir, name)
		}
		return check(name)
	}
	path := os.Getenv("PATH")
	if value, ok := s.Env["PATH"]; ok {
		path = value
	}
	for _, dir := range filepath.SplitList(path) {
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(s.Dir, dir)
		}
		if found, err := check(filepath.Join(dir, name)); err == nil {
			return found, nil
		}
	}
	return "", fmt.Errorf("executable %q not found in PATH", name)
}

// portAvailable rejects a port that another process already serves. Binding
// 127.0.0.1 alone misses listeners on the wildcard or ::1 on macOS, and a
// readiness probe would then succeed against the stale process.
func portAvailable(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	_ = listener.Close()
	for _, host := range []string{"127.0.0.1", "::1"} {
		address := net.JoinHostPort(host, strconv.Itoa(port))
		if conn, err := net.DialTimeout("tcp", address, 250*time.Millisecond); err == nil {
			conn.Close()
			return fmt.Errorf("another process is accepting connections on %s", address)
		}
	}
	return nil
}

func unexpected(e exit) error {
	if e.err == nil {
		return fmt.Errorf("service %s exited unexpectedly (status 0)", e.name)
	}
	return fmt.Errorf("service %s exited unexpectedly: %w", e.name, e.err)
}

func Run(ctx context.Context, plan *project.Plan, options Options) error {
	if err := platformCheck(); err != nil {
		return err
	}
	if len(plan.Services) == 0 {
		return fmt.Errorf("application has no selected services")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if options.Output == nil {
		options.Output = io.Discard
	}
	if options.GracePeriod <= 0 {
		options.GracePeriod = 2 * time.Second
	}
	out := &output{writer: options.Output}
	unlock, err := lockProject(plan.Root)
	if err != nil {
		return err
	}
	defer unlock()
	// Preflight every selected command/port before starting any service.
	binaries := map[string]string{}
	for _, s := range plan.Services {
		binary, err := executable(s)
		if err != nil {
			return fmt.Errorf("%s: %w", s.Name, err)
		}
		binaries[s.Name] = binary
		for _, port := range s.Ports {
			if err := portAvailable(port); err != nil {
				return fmt.Errorf("%s: declared port %d unavailable: %w", s.Name, port, err)
			}
		}
	}
	lifeline, lifelineWriter, err := os.Pipe()
	if err != nil {
		return err
	}
	// Closed only after every service is stopped; closing it is what tells
	// shims their coordinator is gone.
	defer lifelineWriter.Close()
	defer lifeline.Close()
	exits := make(chan exit, len(plan.Services))
	running := []*process{}
	defer func() {
		for i := len(running) - 1; i >= 0; i-- {
			p := running[i]
			out.print("[%s] stopping\n", p.service.Name)
			signalTree(p.cmd, false)
			timer := time.NewTimer(options.GracePeriod)
			select {
			case <-p.done:
			case <-timer.C:
			case <-options.Force:
			}
			timer.Stop()
			// Also terminate descendants when the wrapper has already exited.
			signalTree(p.cmd, true)
			<-p.done
		}
	}()
	for _, s := range plan.Services {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-exits:
			return unexpected(e)
		default:
		}
		cmd, err := shimCommand(binaries[s.Name], s.Command[1:], lifeline)
		if err != nil {
			return err
		}
		cmd.Dir = s.Dir
		cmd.Env = append(environment(s.Env), shimEnv+"="+options.GracePeriod.String())
		ownProcess(cmd)
		stdout := &logWriter{out: out, prefix: s.Name + " stdout"}
		stderr := &logWriter{out: out, prefix: s.Name + " stderr"}
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		cmd.WaitDelay = options.GracePeriod
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("%s: start: %w", s.Name, err)
		}
		p := &process{service: s, cmd: cmd, done: make(chan struct{})}
		running = append(running, p)
		go func() {
			err := cmd.Wait()
			stdout.flush()
			stderr.flush()
			exits <- exit{name: p.service.Name, err: err}
			close(p.done)
		}()
		out.print("[%s] running\n", s.Name)
		if s.Ready != nil {
			if err := waitReady(ctx, p, exits); err != nil {
				return err
			}
			out.print("[%s] ready\n", s.Name)
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e := <-exits:
		return unexpected(e)
	}
}

func waitReady(parent context.Context, p *process, exits <-chan exit) error {
	timeout, _ := time.ParseDuration(p.service.Ready.Timeout)
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	client := &http.Client{Timeout: 250 * time.Millisecond, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case e := <-exits:
			return unexpected(e)
		case <-ctx.Done():
			if parent.Err() != nil {
				return parent.Err()
			}
			return fmt.Errorf("%s: readiness timed out after %s", p.service.Name, p.service.Ready.Timeout)
		default:
		}
		ready := false
		if address := p.service.Ready.HTTP; address != "" {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
			if err != nil {
				return err
			}
			response, err := client.Do(request)
			if err == nil {
				ready = response.StatusCode >= 200 && response.StatusCode < 300
				response.Body.Close()
			}
		} else {
			dialer := net.Dialer{Timeout: 250 * time.Millisecond}
			conn, err := dialer.DialContext(ctx, "tcp", p.service.Ready.TCP)
			if err == nil {
				ready = true
				conn.Close()
			}
		}
		if ready {
			select {
			case e := <-exits:
				return unexpected(e)
			case <-p.done:
				return fmt.Errorf("%s: exited during readiness", p.service.Name)
			default:
				return nil
			}
		}
		select {
		case e := <-exits:
			return unexpected(e)
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}
