//go:build !linux && !darwin

package supervisor

import (
	"fmt"
	"os/exec"
)

func platformCheck() error {
	return fmt.Errorf("application supervision is experimental and currently supports macOS and Linux only; planning is available on this platform")
}
func ownProcess(cmd *exec.Cmd)                {}
func signalTree(cmd *exec.Cmd, force bool)    {}
func lockProject(root string) (func(), error) { return nil, platformCheck() }
