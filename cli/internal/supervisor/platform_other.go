//go:build !linux && !darwin

package supervisor

import (
	"fmt"
	"os"
	"os/exec"
)

const shimEnv = "NEUTRON_SUPERVISOR_SHIM"

func platformCheck() error {
	return fmt.Errorf("application supervision is experimental and currently supports macOS and Linux only; planning is available on this platform")
}
func ownProcess(cmd *exec.Cmd)                {}
func signalTree(cmd *exec.Cmd, force bool)    {}
func lockProject(root string) (func(), error) { return nil, platformCheck() }
func shimCommand(binary string, args []string, _ *os.File) (*exec.Cmd, error) {
	return exec.Command(binary, args...), nil
}
