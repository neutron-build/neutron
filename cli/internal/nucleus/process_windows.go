//go:build windows

package nucleus

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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

	args = append(args, "--host", "127.0.0.1")

	cmd := exec.Command(binaryPath, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start nucleus: %w", err)
	}

	pid := cmd.Process.Pid

	pidFile, err := PIDFile()
	if err != nil {
		return pid, fmt.Errorf("pid file path: %w", err)
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0644); err != nil {
		return pid, fmt.Errorf("write pid file: %w", err)
	}

	cmd.Process.Release()

	if err := waitForPort(port, 10*time.Second); err != nil {
		return pid, fmt.Errorf("nucleus did not start within 10s: %w", err)
	}

	return pid, nil
}

// Stop kills the Nucleus process on Windows.
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

	// On Windows, use taskkill for graceful termination
	killCmd := exec.Command("taskkill", "/PID", strconv.Itoa(pid))
	if err := killCmd.Run(); err != nil {
		// Try force kill
		exec.Command("taskkill", "/F", "/PID", strconv.Itoa(pid)).Run()
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

	// On Windows, FindProcess always succeeds; check if it's actually alive
	// by trying to open the process handle
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false, 0
	}

	// On Windows, Signal(os.Kill) with a zero-signal trick doesn't work.
	// Instead, check if tasklist shows the PID.
	checkCmd := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/NH")
	out, err := checkCmd.Output()
	if err != nil {
		os.Remove(pidFile)
		return false, 0
	}

	_ = proc
	if !strings.Contains(string(out), strconv.Itoa(pid)) {
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
