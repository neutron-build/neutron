//go:build linux || darwin

package supervisor

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

// Each service runs under a shim: a re-exec of this binary that leads the
// service's process group and holds the read end of a lifeline pipe. The
// coordinator holds the only write end, so the pipe reaches EOF however the
// coordinator ends, including SIGKILL, and the shim then stops the group.
// Process groups are not signalled by the kernel when a parent dies.
const shimEnv = "NEUTRON_SUPERVISOR_SHIM"

func init() {
	grace := os.Getenv(shimEnv)
	if grace == "" {
		return
	}
	os.Exit(runShim(grace, os.Args[1:]))
}

func runShim(graceValue string, argv []string) int {
	grace, err := time.ParseDuration(graceValue)
	if err != nil || len(argv) == 0 {
		fmt.Fprintln(os.Stderr, "neutron supervisor shim: invalid invocation")
		return 125
	}
	_ = os.Unsetenv(shimEnv)
	lifeline := os.NewFile(3, "lifeline")
	syscall.CloseOnExec(3)
	// Handled, not ignored: handled dispositions reset to default on exec, so
	// the service still receives group signals normally.
	signals := make(chan os.Signal, 4)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start: %v\n", err)
		return 127
	}
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, lifeline)
		select {
		case <-done:
			return
		default:
		}
		_ = syscall.Kill(0, syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(grace):
		}
		_ = syscall.Kill(0, syscall.SIGKILL)
	}()
	err = cmd.Wait()
	close(done)
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return 128 + int(status.Signal())
		}
		return exitErr.ExitCode()
	}
	if err != nil {
		return 1
	}
	return 0
}

// shimCommand wraps a resolved service binary in the shim.
func shimCommand(binary string, args []string, lifeline *os.File) (*exec.Cmd, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate neutron executable: %w", err)
	}
	cmd := exec.Command(self, append([]string{binary}, args...)...)
	cmd.ExtraFiles = []*os.File{lifeline}
	return cmd, nil
}
