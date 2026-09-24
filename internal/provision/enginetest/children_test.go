//go:build linux

package enginetest_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// alive reports whether pid is a running process: present in /proc and not a zombie.
func alive(pid int) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}

	text := string(data)
	closing := strings.LastIndexByte(text, ')')

	return closing < 0 || !strings.HasPrefix(text[closing+1:], " Z")
}

// waitForPID polls path until it holds a PID.
func waitForPID(t *testing.T, path string) int {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && strings.HasSuffix(string(data), "\n") {
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil {
				t.Fatalf("pid file %q: %v", data, err)
			}

			return pid
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("the docker shim never wrote its PID")

	return 0
}

// A docker call Stutter is blocked in dies with Stutter. Without a parent-death signal the call
// keeps running on the host after a SIGKILL, holding whatever it was doing to the engine.
func TestNoDockerChildOutlivesStutter(t *testing.T) {
	t.Parallel()

	shimDir := t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "pid")
	shim := "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$STUTTER_SHIM_PID.tmp\"\n" +
		"mv \"$STUTTER_SHIM_PID.tmp\" \"$STUTTER_SHIM_PID\"\nexec sleep 300\n"

	writeShim(t, shimDir, shim)

	helper, _ := startHelper(t, "child", []string{
		"PATH=" + shimDir + ":/usr/bin:/bin",
		"STUTTER_SHIM_PID=" + pidFile,
	})

	pid := waitForPID(t, pidFile)
	if !alive(pid) {
		t.Fatalf("docker child %d was not alive at the kill", pid)
	}

	if err := helper.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill the helper: %v", err)
	}

	if err := helper.Wait(); err == nil {
		t.Fatal("the helper exited cleanly; it was meant to die by SIGKILL")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !alive(pid) {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Logf("kill %d: %v", pid, err)
	}

	t.Fatalf("docker child %d alive 1s after the helper died", pid)
}
