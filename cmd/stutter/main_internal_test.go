//go:build unix

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

const (
	// signalHelper turns this test binary into the helper process when set in its environment.
	signalHelper = "STUTTER_SIGNAL_HELPER"
	// underNohup starts the helper with hangups ignored, as nohup leaves a process it launches.
	underNohup = "nohup"
	// teardownTime is how long the stand-in's teardown ignores cancellation.
	teardownTime = 3 * time.Second
	// atOnce is how soon a second interrupt must end the process.
	atOnce = 500 * time.Millisecond
	// settle is how long an ignored signal is given to show any effect before it is judged to have none.
	settle = 500 * time.Millisecond
	// helperExit is what the stand-in returns if its teardown is allowed to finish.
	helperExit = 3
)

func TestMain(m *testing.M) {
	if mode := os.Getenv(signalHelper); mode != "" {
		if mode == underNohup {
			signal.Ignore(syscall.SIGHUP)
		}

		// The helper exits with what the wiring returns; m.Run never runs in it.
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
// end the process at once, by the signal, as it would any other program. A hangup the process was
// started ignoring, as under nohup, must stay ignored: a closed terminal must not cancel a check
// the user asked to outlive it.
func TestSecondInterruptExitsAtOnce(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mode    string
		ignored syscall.Signal
		first   syscall.Signal
	}{
		{name: "interrupt", mode: "plain", first: syscall.SIGINT},
		{name: "hangup", mode: "plain", first: syscall.SIGHUP},
		{name: "hangup under nohup", mode: underNohup, ignored: syscall.SIGHUP, first: syscall.SIGINT},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 2*teardownTime)
			defer cancel()

			// Cancelling ctx kills the helper, so a failed assertion never leaves it running.
			//nolint:gosec // Re-executes this test binary as the helper, never outside input.
			helper := exec.CommandContext(ctx, os.Args[0])

			helper.Env = append(os.Environ(), signalHelper+"="+testCase.mode)

			stdout, err := helper.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}

			if err := helper.Start(); err != nil {
				t.Fatal(err)
			}

			lines := readLines(stdout)
			await(t, lines, "ready")

			if testCase.ignored != 0 {
				send(t, helper, testCase.ignored)
				expectNothing(t, lines, testCase.ignored)
			}

			send(t, helper, testCase.first)
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

// readLines delivers the helper's stdout line by line, closing the channel when the helper exits.
func readLines(stdout io.Reader) <-chan string {
	lines := make(chan string, 4)

	go func() {
		defer close(lines)

		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()

	return lines
}

func await(t *testing.T, lines <-chan string, want string) {
	t.Helper()

	for line := range lines {
		if line == want {
			return
		}
	}

	t.Fatalf("the helper stopped before reporting %q", want)
}

// expectNothing fails if the helper reports anything, or exits, within settle of sig.
func expectNothing(t *testing.T, lines <-chan string, sig syscall.Signal) {
	t.Helper()

	select {
	case line, open := <-lines:
		if !open {
			t.Fatalf("an ignored %v ended the helper", sig)
		}

		t.Fatalf("an ignored %v reached the check: the helper reported %q", sig, line)
	case <-time.After(settle):
	}
}

func send(t *testing.T, helper *exec.Cmd, sig syscall.Signal) {
	t.Helper()

	if err := helper.Process.Signal(sig); err != nil {
		t.Fatalf("send %v: %v", sig, err)
	}
}
