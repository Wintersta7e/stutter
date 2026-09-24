//go:build linux

package provision

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// shimEnv returns an environment whose PATH finds a docker shim running script, followed by the
// system directories the shim's own commands need.
func shimEnv(t *testing.T, script string, extra ...string) []string {
	t.Helper()

	dir := t.TempDir()
	// `env` would show what the shell exports; /proc shows exactly what the runner passed.
	script = strings.ReplaceAll(script, "env\n", "tr '\\0' '\\n' < /proc/$$/environ\n")
	writeShim(t, dir, "#!/bin/sh\n"+script)

	return append([]string{"PATH=" + dir + ":/usr/bin:/bin"}, extra...)
}

// A user-mode call carries the user's environment, but never a context selection and never any
// endpoint but the pinned one: otherwise pull, build and run could land on two engines.
func TestUserModeDropsTheContextAndPinsTheHost(t *testing.T) {
	t.Parallel()

	env := shimEnv(t, "env\n", "DOCKER_CONTEXT=elsewhere", "DOCKER_HOST=tcp://10.0.0.9:2375", "KEPT=yes")
	runner := newRunner(env, nil)
	runner.pin = "unix:///pinned.sock"

	res, err := runner.call(t.Context(), request{verb: verbInfo, args: []arg{{val: infoTemplate}}})
	if err != nil {
		t.Fatalf("call: %v", err)
	}

	got := strings.Split(strings.TrimSpace(string(res.out)), "\n")
	hosts := 0

	for _, line := range got {
		switch {
		case strings.HasPrefix(line, "DOCKER_CONTEXT="):
			t.Errorf("the child saw %s", line)
		case strings.HasPrefix(line, "DOCKER_HOST="):
			hosts++

			if line != "DOCKER_HOST=unix:///pinned.sock" {
				t.Errorf("the child saw %s, want the pin", line)
			}
		default:
		}
	}

	if hosts != 1 {
		t.Errorf("the child saw DOCKER_HOST %d times, want once", hosts)
	}

	if !strings.Contains(string(res.out), "KEPT=yes\n") {
		t.Errorf("the user's own variables did not reach the child:\n%s", res.out)
	}
}

// A call past its deadline is a named failure, and it ends at the deadline rather than when the
// child chooses to exit.
func TestADeadlineIsANamedFailure(t *testing.T) {
	t.Parallel()

	runner := newRunner(shimEnv(t, "sleep 5\n"), map[deadline]time.Duration{deadlineShort: 50 * time.Millisecond})

	start := time.Now()
	_, err := runner.call(t.Context(), request{verb: verbVersion, args: []arg{{val: versionTemplate}}})
	elapsed := time.Since(start)

	if !errors.Is(err, ErrDeadline) {
		t.Fatalf("want ErrDeadline, got %v", err)
	}

	if !strings.Contains(err.Error(), "version") {
		t.Errorf("the deadline failure %q does not name the verb", err)
	}

	if elapsed >= time.Second {
		t.Errorf("the call took %s past a 50ms deadline", elapsed)
	}
}

// A runner that has not been made mutable refuses every mutating row before spawning anything:
// reading the preconditions, or a dry run, can never create or remove a thing.
func TestAReadOnlyRunnerRefusesAMutation(t *testing.T) {
	t.Parallel()

	marker := filepath.Join(t.TempDir(), "spawned")
	runner := newRunner(shimEnv(t, "touch '"+marker+"'\n"), nil)
	spec := verbs()[verbInfo]
	spec.mutates = true

	_, err := runner.run(t.Context(), spec, request{verb: verbInfo})
	if !errors.Is(err, ErrEngine) || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("want a read-only refusal, got %v", err)
	}

	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("the refused call spawned its child: %v", statErr)
	}

	runner.mutable = true

	if _, err := runner.run(t.Context(), spec, request{verb: verbInfo}); err != nil {
		t.Fatalf("a mutable runner refused the call: %v", err)
	}

	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the admitted call did not run: %v", err)
	}
}

// A held call runs to completion when its caller is cancelled: a mutation already issued is never
// abandoned half-recorded.
func TestAHeldCallOutlivesCancellation(t *testing.T) {
	t.Parallel()

	runner := newRunner(shimEnv(t, "sleep 0.3\n"), nil)
	spec := verbs()[verbInfo]
	spec.hold = true

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	res, err := runner.run(ctx, spec, request{verb: verbInfo})

	if err != nil || res.exit != 0 {
		t.Fatalf("held call = (exit %d, %v), want it completed with exit 0", res.exit, err)
	}

	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Errorf("the held call returned after %s, before its child finished", elapsed)
	}
}

// composeShim starts a grandchild, records both PIDs, and blocks — as the compose plugin does
// beneath the docker CLI.
const composeShim = `sleep 300 &
printf '%s %s\n' "$$" "$!" > "$STUTTER_SHIM_PIDS.tmp"
mv "$STUTTER_SHIM_PIDS.tmp" "$STUTTER_SHIM_PIDS"
wait
`

func composeSpec() verbSpec {
	return verbSpec{
		name: "compose-test", program: programCompose, prefix: []string{"compose"},
		mode: modeUser, deadline: deadlineShort, stderr: stderrCount,
	}
}

// startCompose runs a compose-shaped call in the background and returns the PIDs of the docker
// shim and its grandchild once both exist, and a channel that yields the call's error.
func startCompose(ctx context.Context, t *testing.T) ([]int, <-chan error) {
	t.Helper()

	pidFile := filepath.Join(t.TempDir(), "pids")
	runner := newRunner(shimEnv(t, composeShim, "STUTTER_SHIM_PIDS="+pidFile), nil)
	done := make(chan error, 1)

	go func() {
		_, err := runner.run(ctx, composeSpec(), request{})
		done <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(pidFile)
		if err == nil {
			return parsePIDs(t, string(data)), done
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("the compose shim never wrote its PIDs")

	return nil, nil
}

func parsePIDs(t *testing.T, text string) []int {
	t.Helper()

	fields := strings.Fields(text)
	pids := make([]int, 0, len(fields))

	for _, field := range fields {
		pid, err := strconv.Atoi(field)
		if err != nil {
			t.Fatalf("pid %q: %v", field, err)
		}

		pids = append(pids, pid)
	}

	return pids
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

// awaitGone fails naming every pid still alive after one second.
func awaitGone(t *testing.T, what string, pids []int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !anyAlive(pids) {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	for _, pid := range pids {
		if alive(pid) {
			t.Errorf("%s: process %d alive 1s later", what, pid)

			if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
				t.Logf("kill %d: %v", pid, err)
			}
		}
	}
}

func anyAlive(pids []int) bool {
	return slices.ContainsFunc(pids, alive)
}

// Cancelling a compose call ends the plugin too, not only the docker CLI it runs beneath.
func TestCancellingAComposeCallKillsItsWholeGroup(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	pids, done := startCompose(ctx, t)
	if !alive(pids[0]) || !alive(pids[1]) {
		t.Fatalf("the shim and its grandchild %v were not alive before the cancel", pids)
	}

	cancel()
	<-done
	awaitGone(t, "after cancel", pids)
}

// The parent-death signal reaches only the direct child. For a compose call that child must take
// the whole process group with it, because the compose plugin is a grandchild the signal never
// reaches: measured, the plugin outlived a killed docker CLI by seconds.
func TestAParentDeathSignalTakesTheWholeComposeGroup(t *testing.T) {
	t.Parallel()

	pids, done := startCompose(t.Context(), t)

	leader := processGroup(t, pids[0])
	if leader == pids[0] {
		t.Fatalf("the docker shim %d leads its own group: nothing stands between it and the parent", leader)
	}

	// What the kernel sends the direct child when Stutter dies.
	if err := syscall.Kill(leader, syscall.SIGTERM); err != nil {
		t.Fatalf("signal the direct child %d: %v", leader, err)
	}

	<-done
	awaitGone(t, "after the parent-death signal", pids)
}

// processGroup reads field 5 of /proc/<pid>/stat.
func processGroup(t *testing.T, pid int) int {
	t.Helper()

	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		t.Fatal(err)
	}

	text := string(data)
	fields := strings.Fields(text[strings.LastIndexByte(text, ')')+1:])

	group, err := strconv.Atoi(fields[2])
	if err != nil {
		t.Fatal(err)
	}

	return group
}

// A compose call's stderr can carry interpolated secrets, so it is counted and never quoted. The
// same shim on a first-line row is quoted, which is what makes the absence meaningful.
func TestComposeStderrIsNeverQuoted(t *testing.T) {
	t.Parallel()

	const sentinel = "SENTINEL-4f1c9a-interpolated"

	runner := newRunner(shimEnv(t, "echo '"+sentinel+"' >&2\nexit 1\n"), nil)

	res, err := runner.run(t.Context(), composeSpec(), request{})

	var callErr *CallError
	if !errors.As(err, &callErr) {
		t.Fatalf("want a *CallError, got %v", err)
	}

	if strings.Contains(err.Error(), sentinel) || callErr.Stderr != "" || res.stderrFirst != "" {
		t.Errorf("compose stderr was quoted: error %q, Stderr %q", err, callErr.Stderr)
	}

	if res.stderrLines != 1 || callErr.ExitCode() != 1 {
		t.Errorf("stderr lines = %d, exit = %d; want 1 and 1", res.stderrLines, callErr.ExitCode())
	}

	_, err = runner.call(t.Context(), request{verb: verbInfo, args: []arg{{val: infoTemplate}}})
	if err == nil || !strings.Contains(err.Error(), sentinel) {
		t.Errorf("a first-line row did not quote its stderr: %v", err)
	}
}
