//go:build linux

package provision

import (
	"fmt"
	"os"
	"syscall"
)

// sysProcAttr puts a child in its own process group, so a terminal's Ctrl-C reaches only Stutter,
// and has the kernel send it death when the thread that spawned it exits.
func sysProcAttr(death syscall.Signal) *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true, Pdeathsig: death}
}

// killGroup kills the child's whole process group: the child leads it, so its PID names the group.
func killGroup(process *os.Process) error {
	if err := syscall.Kill(-process.Pid, syscall.SIGKILL); err != nil {
		return fmt.Errorf("kill process group %d: %w", process.Pid, err)
	}

	return nil
}
