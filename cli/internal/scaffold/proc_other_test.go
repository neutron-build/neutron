//go:build !unix

package scaffold

import "os/exec"

func ownProcessGroup(cmd *exec.Cmd) {}

func signalGroup(cmd *exec.Cmd, kill bool) {
	_ = cmd.Process.Kill()
}
