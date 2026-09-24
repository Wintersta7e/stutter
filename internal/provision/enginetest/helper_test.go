//go:build linux

package enginetest_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision"
)

// helperMode turns this test binary into a helper process when set in its environment. A helper is
// the process a test kills or signals, standing in for a stutter invocation.
const helperMode = "STUTTER_PROVISION_HELPER"

func TestMain(m *testing.M) {
	if mode := os.Getenv(helperMode); mode != "" {
		// The helper exits with its mode's code; m.Run never runs in it.
		os.Exit(runHelper(mode))
	}

	m.Run()
}

// runHelper runs one helper mode and returns the process's exit code.
func runHelper(mode string) int {
	switch mode {
	case "child":
		// Blocks in whatever the first precondition call spawned, until the test kills this process.
		_, err := provision.Preconditions(context.Background())
		fmt.Fprintf(os.Stderr, "preconditions returned: %v\n", err)

		return 1
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q\n", mode)

		return 2
	}
}

// startHelper re-executes this test binary as a helper in mode, with exactly env as its
// environment, and returns it with a reader over its stdout. The helper is killed and reaped when
// the test ends.
func startHelper(t *testing.T, mode string, env []string) (*exec.Cmd, *bufio.Reader) {
	t.Helper()

	//nolint:gosec // re-executes this test binary; the only argument is fixed.
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^$")

	cmd.Env = append(append([]string(nil), env...), helperMode+"="+mode)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("helper stdout: %v", err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}

	t.Cleanup(func() {
		if cmd.ProcessState != nil {
			return
		}

		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("kill helper %s: %v", mode, err)
		}

		if err := cmd.Wait(); err != nil {
			t.Logf("helper %s ended: %v", mode, err)
		}
	})

	return cmd, bufio.NewReader(stdout)
}
