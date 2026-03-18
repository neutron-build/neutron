//go:build !windows

package nucleus

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// PIDFile returns the path to the Nucleus PID file.
func PIDFile() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".neutron")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return filepath.Join(dir, "nucleus.pid"), nil
}

// Start launches a Nucleus server as a background process.
func Start(binaryPath string, port int, dataDir string, memory bool) (int, error) {
	args := []string{"start", "--port", strconv.Itoa(port)}

	if memory {
		args = append(args, "--memory")
	} else {
		args = append(args, "--data", dataDir)
	}

	// Bind to localhost only for local development
	args = append(args, "--host", "127.0.0.1")

	cmd := exec.Command(binaryPath, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start nucleus: %w", err)
	}

	pid := cmd.Process.Pid

	// Write PID file (with symlink attack protection)
	pidFile, err := PIDFile()
	if err != nil {
		return pid, fmt.Errorf("pid file path: %w", err)
	}
	if info, statErr := os.Lstat(pidFile); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return pid, fmt.Errorf("PID file %s is a symlink; refusing to write", pidFile)
		}
		os.Remove(pidFile)
	}
	f, err := os.OpenFile(pidFile, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return pid, fmt.Errorf("create pid file: %w", err)
	}
	if _, err := f.WriteString(strconv.Itoa(pid)); err != nil {
		f.Close()
		return pid, fmt.Errorf("write pid file: %w", err)
	}
	f.Close()

	// Release the process so it runs independently
	cmd.Process.Release()

	// Wait for the port to become reachable
	if err := waitForPort(port, 10*time.Second); err != nil {
		return pid, fmt.Errorf("nucleus did not start within 10s: %w", err)
	}

	return pid, nil
}

// Stop sends SIGTERM to the Nucleus process and waits for shutdown.
func Stop() error {
	pidFile, err := PIDFile()
	if err != nil {
		return err
	}

	data, err := os.ReadFile(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no running Nucleus instance found (no PID file)")
		}
		return err
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		os.Remove(pidFile)
		return fmt.Errorf("invalid PID in file: %w", err)
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		os.Remove(pidFile)
		return fmt.Errorf("process %d not found: %w", pid, err)
	}

	// Send SIGTERM for graceful shutdown
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		os.Remove(pidFile)
		return fmt.Errorf("signal process %d: %w", pid, err)
	}

	// Wait for process to exit (up to 10 seconds)
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 100; i++ {
			if err := proc.Signal(syscall.Signal(0)); err != nil {
				done <- nil
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		done <- fmt.Errorf("process %d did not exit after SIGTERM", pid)
	}()

	if err := <-done; err != nil {
		// Force kill
		proc.Signal(syscall.SIGKILL)
	}

	os.Remove(pidFile)
	return nil
}

// IsRunning checks if a Nucleus process is currently running.
func IsRunning() (bool, int) {
	pidFile, err := PIDFile()
	if err != nil {
		return false, 0
	}

	data, err := os.ReadFile(pidFile)
	if err != nil {
		return false, 0
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return false, 0
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return false, 0
	}

	// Check if process is alive
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		os.Remove(pidFile)
		return false, 0
	}

	return true, pid
}

func waitForPort(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("port %d not reachable", port)
}
