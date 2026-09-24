//go:build linux

package enginetest_test

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/testexec"
)

// pluginLimit bounds the wait for the compose plugin to start.
const pluginLimit = 30 * time.Second

// proc is one process as /proc/<pid>/stat reports it.
type proc struct {
	comm string
	pid  int
	ppid int
	pgid int
}

// procs lists every process /proc shows.
func procs() []proc {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}

	var out []proc

	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		data, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if err != nil {
			continue
		}

		text := string(data)
		open, closing := strings.IndexByte(text, '('), strings.LastIndexByte(text, ')')

		// After the command: state, ppid, pgrp.
		fields := strings.Fields(text[closing+1:])
		if open < 0 || closing < open || len(fields) < 3 {
			continue
		}

		ppid, errParent := strconv.Atoi(fields[1])
		pgid, errGroup := strconv.Atoi(fields[2])

		if errParent == nil && errGroup == nil {
			out = append(out, proc{comm: text[open+1 : closing], pid: pid, ppid: ppid, pgid: pgid})
		}
	}

	return out
}

// descendants lists the processes under root.
func descendants(root int) []proc {
	all := procs()
	parents := []int{root}

	var out []proc

	for len(parents) > 0 {
		var next []int

		for _, p := range all {
			if slices.Contains(parents, p.ppid) {
				out = append(out, p)
				next = append(next, p.pid)
			}
		}

		parents = next
	}

	return out
}

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

// composePlugin waits for the compose plugin under the helper to be reading model, and returns it
// with every process in its group: the descendant holding model in its argv that is neither the
// group-killer's shell nor the docker CLI. The CLI runs the plugin once before, to read its
// metadata; that run is not the one blocked.
func composePlugin(t *testing.T, helper int, model string) (proc, []proc) {
	t.Helper()

	for deadline := time.Now().Add(pluginLimit); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		for _, p := range descendants(helper) {
			argv, err := os.ReadFile("/proc/" + strconv.Itoa(p.pid) + "/cmdline")
			if err == nil && p.comm != "sh" && p.comm != "docker" && strings.Contains(string(argv), model) {
				return p, slices.DeleteFunc(procs(), func(q proc) bool { return q.pgid != p.pgid })
			}
		}
	}

	t.Fatal("the compose plugin never started under the helper")

	return proc{}, nil
}

// deafPlugin is a compose plugin that never learns its CLI died: it answers the CLI's metadata
// probe, then blocks reading the model, as a plugin without the CLI's exit handshake — an older one,
// or one early in its start — does.
const deafPlugin = `#!/bin/sh
if [ "$1" = docker-cli-plugin-metadata ]; then
	echo '{"SchemaVersion":"0.1.0","Vendor":"stutter tests","Version":"v2.29.7"}'
	exit 0
fi
while [ "$#" -gt 0 ] && [ "$1" != -f ]; do shift; done
exec cat "$2"
`

// A compose call Stutter is blocked in dies with Stutter, plugin included: the plugin is the CLI's
// child, not Stutter's, so only a killer holding the whole group can take it. The installed plugin
// also exits when its CLI does; a plugin that does not shows that Stutter does not rely on it.
func TestNoComposeChildOutlivesStutter(t *testing.T) {
	t.Parallel()

	gate := dockertest.Require(t)

	for _, plugin := range []string{"installed", "deaf"} {
		t.Run(plugin, func(t *testing.T) {
			t.Parallel()
			slot(t)

			dir := t.TempDir()
			fifo := filepath.Join(dir, "compose.yaml")

			if err := syscall.Mkfifo(fifo, 0o600); err != nil {
				t.Fatal(err)
			}

			userConfig := t.TempDir()
			if plugin == "deaf" {
				plugins := filepath.Join(userConfig, "cli-plugins")
				if err := os.Mkdir(plugins, 0o700); err != nil {
					t.Fatal(err)
				}

				testexec.WriteScript(t, filepath.Join(plugins, "docker-compose"), deafPlugin)
			}

			h := launch(t, "compose", helperSpec{StateDir: newStateDir(t), TempDir: t.TempDir(), Dir: dir, FIFO: fifo},
				helperEnv(gate, os.Getenv("PATH"), userConfig))

			blocked, group := composePlugin(t, h.cmd.Process.Pid, fifo)
			t.Logf("compose plugin %d (%s) in group %d with %d processes", blocked.pid, blocked.comm, blocked.pgid,
				len(group))

			h.kill(t)

			for deadline := time.Now().Add(dieLimit); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
				group = slices.DeleteFunc(group, func(p proc) bool { return !alive(p.pid) })
				if len(group) == 0 {
					return
				}
			}

			for _, p := range group {
				if err := syscall.Kill(p.pid, syscall.SIGKILL); err != nil {
					t.Logf("kill %d: %v", p.pid, err)
				}
			}

			t.Fatalf("compose plugin %d alive 1s after the helper died (group survivors %+v)", blocked.pid, group)
		})
	}
}
