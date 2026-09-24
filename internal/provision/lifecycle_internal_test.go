//go:build linux

package provision

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// createFixture creates one container of kind from the container fixture.
func createFixture(t *testing.T, kind rules.Kind) (*Engine, *fakeEngine, *Container) {
	t.Helper()

	engine, fake, spec := containerFixture(t)
	spec.Kind = kind

	container, err := engine.CreateContainer(t.Context(), spec)
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	return engine, fake, container
}

// verbsCalled lists the verbs of every call the fake answered, from the first of start.
func verbsCalled(fake *fakeEngine) []string {
	fake.mu.Lock()
	defer fake.mu.Unlock()

	out := make([]string, 0, len(fake.calls))
	for _, c := range fake.calls {
		out = append(out, c.verb)
	}

	return out
}

// State is read before the kill: afterwards every exit code is the kill's own 137, and a run that
// failed would read as one Stutter stopped.
func TestStopReadsStateBeforeTheKill(t *testing.T) {
	t.Parallel()

	engine, fake, container := createFixture(t, rules.KindTarget)

	if err := engine.Start(t.Context(), container); err != nil {
		t.Fatalf("Start: %v", err)
	}

	state, err := engine.Stop(t.Context(), container)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if !state.Running || state.ExitCode != 0 {
		t.Errorf("Stop = %+v, want the pre-kill state: running, exit 0", state)
	}

	if after := fake.find(ResourceContainer, container.ID()).inspect; after.ExitCode != 137 {
		t.Fatalf("the fake did not kill: exit %d", after.ExitCode)
	}

	if state.Log == "" {
		t.Error("a target's log was not copied")
	}
}

// Only a seed stops gracefully, with its compose signal and grace period; every other container is
// killed.
func TestAGracefulStopIsForSeedsOnly(t *testing.T) {
	t.Parallel()

	engine, fake, restore := createFixture(t, rules.KindRestore)
	before := len(fake.calls)

	if _, err := engine.StopGracefully(t.Context(), restore, Graceful{Signal: "SIGUSR1"}); err == nil {
		t.Error("a restore stopped gracefully")
	}

	if len(fake.calls) != before {
		t.Errorf("a refused graceful stop issued %v", verbsCalled(fake)[before:])
	}

	seedEngine, seedFake, seed := createFixture(t, rules.KindSeed)

	state, err := seedEngine.StopGracefully(t.Context(), seed, Graceful{
		Signal: "SIGUSR1", Grace: 3 * time.Second, HasGrace: true,
	})
	if err != nil {
		t.Fatalf("StopGracefully: %v", err)
	}

	stops := seedFake.verbCalls("stopGraceful")
	if len(stops) != 1 || strings.Join(stops[0], " ") != "stop --signal SIGUSR1 --timeout 3 "+seed.ID() {
		t.Errorf("graceful stop argv = %v", stops)
	}

	if state.Killed {
		t.Errorf("a seed that stopped by its signal reads Killed: %+v", state)
	}
}

// A seed still running when its grace period ends is killed by the engine; that is recorded.
func TestAKillAfterTheGracePeriodIsRecorded(t *testing.T) {
	t.Parallel()

	engine, fake, seed := createFixture(t, rules.KindSeed)
	fake.gracefulExit = 137

	state, err := engine.StopGracefully(t.Context(), seed, Graceful{})
	if err != nil {
		t.Fatalf("StopGracefully: %v", err)
	}

	if !state.Killed || state.ExitCode != 137 {
		t.Errorf("StopGracefully = %+v, want Killed", state)
	}

	if stops := seedArgs(fake); !slices.Equal(stops, []string{"stop", seed.ID()}) {
		t.Errorf("a stop with no compose signal or grace = %v, want the image's and the engine's", stops)
	}
}

func seedArgs(fake *fakeEngine) []string {
	stops := fake.verbCalls("stopGraceful")
	if len(stops) != 1 {
		return nil
	}

	return stops[0]
}

// Exited is a wait on the container, begun only once it has started: a wait on a created container
// returns at once, which would read as an exit.
func TestExitedFiresOnlyAfterStart(t *testing.T) {
	t.Parallel()

	engine, fake, container := createFixture(t, rules.KindTarget)

	if engine.Exited(container) != nil {
		t.Error("Exited is set before Start")
	}

	if waits := fake.verbCalls("wait"); len(waits) != 0 {
		t.Fatalf("a wait was issued before start: %v", waits)
	}

	if err := engine.Start(t.Context(), container); err != nil {
		t.Fatal(err)
	}

	exited := engine.Exited(container)

	select {
	case <-exited:
		t.Fatal("Exited fired while the container runs")
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := engine.Stop(t.Context(), container); err != nil {
		t.Fatal(err)
	}

	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Exited did not fire after the container stopped")
	}

	calls := verbsCalled(fake)
	if slices.Index(calls, "wait") < slices.Index(calls, "start") {
		t.Errorf("wait came before start: %v", calls)
	}
}

// Under --keep the check's final target survives: retiring it stops it, and it is removed only when
// the next run's target of the same name is created; at Close it is kept.
func TestUnderKeepTheLastTargetIsKeptStopped(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)
	engine.keep = true

	first, err := engine.CreateContainer(t.Context(), spec)
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	if _, retireErr := engine.Retire(t.Context(), first); retireErr != nil {
		t.Fatalf("Retire: %v", retireErr)
	}

	if removes := fake.verbCalls(removeVerb); len(removes) != 0 || fake.find(ResourceContainer, first.ID()) == nil {
		t.Fatalf("a kept target was removed on retire: %v", removes)
	}

	second, err := engine.CreateContainer(t.Context(), spec)
	if err != nil {
		t.Fatalf("the next target's create: %v", err)
	}

	if fake.find(ResourceContainer, first.ID()) != nil {
		t.Error("the held target was not removed before its successor was created")
	}

	if _, err := engine.Retire(t.Context(), second); err != nil {
		t.Fatal(err)
	}

	engine.Close(t.Context(), DiscardLogs)

	if fake.find(ResourceContainer, second.ID()) == nil {
		t.Error("the final target is gone after a kept check's Close")
	}

	if got := lastCheckWide(t, engine.book.led.path); got != opKept {
		t.Errorf("the ledger ends %q, want kept", got)
	}
}

// Output needs a deadline: a relay that never prints its ready line must not hold the check forever.
func TestOutputNeedsADeadline(t *testing.T) {
	t.Parallel()

	engine, fake, container := createFixture(t, rules.KindRelay)
	fake.find(ResourceContainer, container.ID()).output = "starting\nready 10.231.20.9:4222\nserving\n"
	before := len(fake.calls)

	if _, err := engine.Output(t.Context(), container, "ready"); err == nil {
		t.Error("Output without a deadline was accepted")
	}

	if len(fake.calls) != before {
		t.Errorf("Output without a deadline issued %v", verbsCalled(fake)[before:])
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	line, err := engine.Output(ctx, container, "ready")
	if err != nil || line != "ready 10.231.20.9:4222" {
		t.Errorf("Output = (%q, %v)", line, err)
	}

	if _, err := engine.Output(ctx, container, "verified"); err == nil {
		t.Error("a stream that ended without the line reported success")
	}

	fake.find(ResourceContainer, container.ID()).stderr = "listening\nrelay: dial refused\n"

	if last, err := engine.LastStderrLine(t.Context(), container); err != nil || last != "relay: dial refused" {
		t.Errorf("LastStderrLine = (%q, %v)", last, err)
	}
}

// A retired container's log is copied between its kill and its removal: after the kill its state is
// the kill's, after the removal its log is gone.
func TestLogsAreCopiedBeforeRemoval(t *testing.T) {
	t.Parallel()

	engine, fake, container := createFixture(t, rules.KindJob)
	fake.find(ResourceContainer, container.ID()).output = "migrated\n"
	before := len(fake.calls)

	state, err := engine.Retire(t.Context(), container)
	if err != nil {
		t.Fatalf("Retire: %v", err)
	}

	var order []string

	// The first read of the container, and each step after it; re-reads that verify are left out.
	for _, verb := range verbsCalled(fake)[before:] {
		steps := []string{"inspect", "kill", "logs", removeVerb}
		if slices.Contains(steps, verb) && !slices.Contains(order, verb) {
			order = append(order, verb)
		}
	}

	if want := []string{"inspect", "kill", "logs", removeVerb}; !slices.Equal(order, want) {
		t.Errorf("retire ran %v, want %v", order, want)
	}

	data, err := os.ReadFile(state.Log)
	if err != nil || !strings.Contains(string(data), "migrated") {
		t.Errorf("the copied log %s = (%q, %v)", state.Log, data, err)
	}

	if errors.Is(err, os.ErrNotExist) {
		t.Error("the log path names no file")
	}
}
