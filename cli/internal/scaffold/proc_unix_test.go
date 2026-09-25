//go:build unix

package scaffold

import (
	"os/exec"
	"syscall"
)

// ownProcessGroup starts cmd in its own process group so stopGroup reaches
// every process it spawns (uvicorn's reloader, cargo run, zig build run, npm).
func ownProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func signalGroup(cmd *exec.Cmd, kill bool) {
	sig := syscall.SIGTERM
	if kill {
		sig = syscall.SIGKILL
	}
	_ = syscall.Kill(-cmd.Process.Pid, sig)
}
