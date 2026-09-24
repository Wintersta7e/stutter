//go:build linux

package enginetest_test

import (
	"bytes"
	"encoding/json"
	"os"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

const (
	// dieLimit is how soon a process must be gone after the signal that ends it.
	dieLimit = time.Second
	// childLimit bounds the wait for a helper's docker call to be in flight.
	childLimit = 5 * time.Second
)

// inFlight waits until the helper has a docker call running, and returns the process groups of its
// descendants.
func inFlight(t *testing.T, pid int) []int {
	t.Helper()

	for deadline := time.Now().Add(childLimit); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		var groups []int

		for _, p := range descendants(pid) {
			if !slices.Contains(groups, p.pgid) {
				groups = append(groups, p.pgid)
			}
		}

		if len(groups) > 0 {
			return groups
		}
	}

	t.Fatal("the helper never had a docker call in flight")

	return nil
}

// grouped lists the live processes in any of groups.
func grouped(groups []int) []proc {
	var out []proc

	for _, p := range procs() {
		if slices.Contains(groups, p.pgid) && alive(p.pid) {
			out = append(out, p)
		}
	}

	return out
}

// wholeLines reports whether a ledger holds only whole JSON lines, the last one ended.
func wholeLines(t *testing.T, path string) bool {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	lines := bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))

	return bytes.HasSuffix(data, []byte("\n")) && !slices.ContainsFunc(lines, func(line []byte) bool {
		return !json.Valid(line)
	})
}

// A second interrupt abandons the teardown at once and kills every docker call in flight; what is
// left is the next check's sweep's, or `stutter clean`'s, and either removes all of it.
func TestASecondInterruptLeavesTheRestToTheSweep(t *testing.T) {
	t.Parallel()

	gate := dockertest.Require(t)

	for _, finish := range []string{"open", "clean"} {
		t.Run(finish, func(t *testing.T) {
			t.Parallel()

			docker := gate.Docker(t)
			decoy, started := decoyContainer(t, docker)
			stateDir := newStateDir(t)

			h := launch(t, "interrupt", helperSpec{StateDir: stateDir, TempDir: t.TempDir()},
				helperEnv(gate, os.Getenv("PATH"), t.TempDir()))
			h.await(t, "ready")

			if err := h.cmd.Process.Signal(syscall.SIGINT); err != nil {
				t.Fatal(err)
			}

			h.await(t, "teardown")
			groups := inFlight(t, h.cmd.Process.Pid)

			sent := time.Now()

			if err := h.cmd.Process.Signal(syscall.SIGINT); err != nil {
				t.Fatal(err)
			}

			waitErr := h.cmd.Wait()
			took := time.Since(sent)
			status, ok := h.cmd.ProcessState.Sys().(syscall.WaitStatus)

			t.Logf("gone in %v: %v", took, waitErr)

			if !ok || !status.Signaled() || status.Signal() != syscall.SIGINT || h.cmd.ProcessState.ExitCode() != -1 {
				t.Errorf("the helper ended %v, not killed by the second SIGINT", h.cmd.ProcessState)
			}

			if took > dieLimit {
				t.Errorf("the helper took %v to die, want under %v", took, dieLimit)
			}

			left := grouped(groups)
			for deadline := time.Now().Add(dieLimit); len(left) > 0 && time.Now().Before(deadline); {
				time.Sleep(20 * time.Millisecond)

				left = grouped(groups)
			}

			if len(left) > 0 {
				t.Errorf("docker processes of the helper's groups %v alive 1s after it died: %+v", groups, left)
			}

			if !wholeLines(t, ledgerPath(stateDir, h.check)) {
				t.Error("the interrupted ledger does not end in a whole line")
			}

			if finish == "open" {
				sweep := openEngine(t, provision.Options{StateDir: stateDir}).Sweep()
				t.Logf("swept %v, failed %+v", sweep.Swept, sweep.Failed)
			} else {
				result, err := provision.Clean(t.Context(), provision.CleanOptions{StateDir: stateDir})
				t.Logf("cleaned %d, failed %+v: %v", len(result.Removed), result.Failed, err)
			}

			listing := docker.Listing(t, rules.LabelCheck+"="+h.check)
			remaining := len(listing.Containers) + len(listing.Networks) + len(listing.Volumes) + len(listing.Images)
			t.Logf("remaining with check %s=%d (processes in flight at the interrupt: %d groups)", h.check, remaining,
				len(groups))

			if remaining != 0 {
				t.Errorf("after the %s, the engine holds %+v", finish, listing)
			}

			if now := startedAt(t, docker, decoy); now == nil || now != started {
				t.Errorf("the decoy %s was touched: started %v, now %v", decoy, started, now)
			}
		})
	}
}
