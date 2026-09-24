//go:build !linux

package provision

import (
	"fmt"
	"os"
	"syscall"
)

// sysProcAttr sets nothing: the compose path runs only on Linux, and Preconditions refuses every
// other host before anything is spawned.
func sysProcAttr(syscall.Signal) *syscall.SysProcAttr {
	return &syscall.SysProcAttr{}
}

// killGroup kills the child alone; no process group exists off Linux.
func killGroup(process *os.Process) error {
	if err := process.Kill(); err != nil {
		return fmt.Errorf("kill process %d: %w", process.Pid, err)
	}

	return nil
}
