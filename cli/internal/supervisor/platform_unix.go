//go:build linux || darwin

package supervisor

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

func platformCheck() error     { return nil }
func ownProcess(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func signalTree(cmd *exec.Cmd, force bool) {
	signal := syscall.SIGTERM
	if force {
		signal = syscall.SIGKILL
	}
	_ = syscall.Kill(-cmd.Process.Pid, signal)
}
func lockProject(root string) (func(), error) {
	dir := filepath.Join(root, ".neutron")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "application.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another application session holds the project lock: %w", err)
	}
	// Keep the inode: unlinking a lock file allows concurrent owners of different inodes.
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}
