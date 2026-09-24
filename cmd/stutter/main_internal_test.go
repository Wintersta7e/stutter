//go:build unix

package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

const (
	// signalHelper turns this test binary into the helper process when set in its environment.
	signalHelper = "STUTTER_SIGNAL_HELPER"
	// teardownTime is how long the stand-in's teardown ignores cancellation.
	teardownTime = 3 * time.Second
	// atOnce is how soon a second interrupt must end the process.
	atOnce = 500 * time.Millisecond
	// helperExit is what the stand-in returns if its teardown is allowed to finish.
	helperExit = 3
)

func TestMain(m *testing.M) {
	if os.Getenv(signalHelper) != "" {
		//nolint:revive // The helper exits with what the wiring returns; m.Run never runs in it.
		os.Exit(interruptible(slowTeardown))
	}

	m.Run()
}

// slowTeardown stands in for a check whose teardown ignores cancellation, as unwinding containers
// can. It reports each stage on stdout so the test knows when to signal.
func slowTeardown(ctx context.Context) int {
	fmt.Println("ready")
	<-ctx.Done()
	fmt.Println("teardown")
	time.Sleep(teardownTime)

	return helperExit
}

// The first signal starts teardown. A second interrupt was being swallowed while that teardown ran,
// so the only way out of a stuck check was to wait or to kill it from another terminal. It has to
// end the process at once, by the signal, as it would any other program.
func TestSecondInterruptExitsAtOnce(t *testing.T) {
	t.Parallel()

	for _, first := range []syscall.Signal{syscall.SIGINT, syscall.SIGHUP} {
		t.Run(first.String(), func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 2*teardownTime)
			defer cancel()

			// Cancelling ctx kills the helper, so a failed assertion never leaves it running.
			//nolint:gosec // Re-executes this test binary as the helper, never outside input.
			helper := exec.CommandContext(ctx, os.Args[0])

			helper.Env = append(os.Environ(), signalHelper+"=1")

			stdout, err := helper.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}

			if err := helper.Start(); err != nil {
				t.Fatal(err)
			}

			lines := bufio.NewScanner(stdout)
			await(t, lines, "ready")
			send(t, helper, first)
			await(t, lines, "teardown")

			sent := time.Now()

			send(t, helper, syscall.SIGINT)
			waitErr := helper.Wait()
			elapsed := time.Since(sent)

			status, ok := helper.ProcessState.Sys().(syscall.WaitStatus)
			if !ok {
				t.Fatalf("wait status is %T", helper.ProcessState.Sys())
			}

			if !status.Signaled() || status.Signal() != syscall.SIGINT {
				t.Fatalf("after a second interrupt the helper ended with %v %v after %v, want killed by SIGINT",
					helper.ProcessState, waitErr, elapsed)
			}

			if elapsed > atOnce {
				t.Fatalf("the helper died %v after the second interrupt, want within %v", elapsed, atOnce)
			}

			t.Logf("killed by SIGINT %v after the second interrupt", elapsed)
		})
	}
}

func await(t *testing.T, lines *bufio.Scanner, want string) {
	t.Helper()

	for lines.Scan() {
		if lines.Text() == want {
			return
		}
	}

	t.Fatalf("the helper stopped before reporting %q (read error: %v)", want, lines.Err())
}

func send(t *testing.T, helper *exec.Cmd, sig syscall.Signal) {
	t.Helper()

	if err := helper.Process.Signal(sig); err != nil {
		t.Fatalf("send %v: %v", sig, err)
	}
}
