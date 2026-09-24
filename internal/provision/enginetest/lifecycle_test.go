//go:build linux

package enginetest_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

const (
	// ignoresTerm is a PID 1 that ignores SIGTERM, and SIGINT — the test image's stop signal — and runs
	// until killed.
	ignoresTerm = `trap "" INT TERM; while :; do sleep 1; done`
	// stopLimit is how long a stop may take: well under the engine's ten-second SIGTERM grace.
	stopLimit = 2 * time.Second
	// readLimit bounds a read that follows a running container.
	readLimit = 30 * time.Second
	// healthInterval is the test healthcheck's interval.
	healthInterval = 200 * time.Millisecond
	// seedGrace is the grace a seed that honours SIGTERM gets; one that ignores it gets a second.
	seedGrace = 10 * time.Second
)

// running creates a container of kind on network running script, and starts it.
func running(
	t *testing.T, engine *provision.Engine, network *provision.Network, kind rules.Kind, script string,
) *provision.Container {
	t.Helper()

	spec := shell(targetSpec(pinImage(t, engine, testImage), network), script)
	spec.Kind = kind

	c := createContainer(t, engine, spec)
	startContainer(t, engine, c)

	return c
}

// A stop is a SIGKILL: a process that ignores SIGTERM never holds the check up for the engine's
// grace period.
func TestAStopNeverWaitsOnSIGTERM(t *testing.T) {
	t.Parallel()

	requireEngine(t)

	engine := openEngine(t, provision.Options{})
	c := running(t, engine, freeNetwork(t, engine, serviceRole), rules.KindTarget, ignoresTerm)

	began := time.Now()
	state, err := engine.Stop(t.Context(), c)
	took := time.Since(began)

	t.Logf("stopped in %v", took)

	if err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if !state.Running {
		t.Errorf("the container was not running when stopped: %+v", state)
	}

	if took >= stopLimit {
		t.Errorf("the stop took %v, want under %v", took, stopLimit)
	}
}

// The state a stop returns is read before the kill: afterwards every exit code is the kill's.
func TestStateIsReadBeforeTheKill(t *testing.T) {
	t.Parallel()

	requireEngine(t)

	engine := openEngine(t, provision.Options{})
	network := freeNetwork(t, engine, serviceRole)

	live := running(t, engine, network, rules.KindTarget, ignoresTerm)

	state, err := engine.Stop(t.Context(), live)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if !state.Running || state.ExitCode != 0 {
		t.Errorf("a running container read %+v, want running with exit code 0", state)
	}

	killed, err := engine.Status(t.Context(), live)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}

	t.Logf("read before the kill %+v; after it %+v", state, killed)

	exited := running(t, engine, network, rules.KindJob, "exit 7")
	awaitExit(t, engine, exited)

	state, err = engine.Stop(t.Context(), exited)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if state.Running || state.ExitCode != 7 {
		t.Errorf("a container that exited 7 read %+v", state)
	}
}

// A seed stops with its own signal and grace period, so a database flushes what it seeded; one that
// outlives its grace is killed, and the state says so.
func TestASeedStopsGracefully(t *testing.T) {
	t.Parallel()

	requireEngine(t)

	engine := openEngine(t, provision.Options{})
	network := freeNetwork(t, engine, serviceRole)

	for _, tc := range []struct {
		script string
		grace  time.Duration
		killed bool
	}{
		{script: `trap "exit 0" TERM; while :; do sleep 1; done`, grace: seedGrace},
		{script: ignoresTerm, grace: time.Second, killed: true},
	} {
		seed := running(t, engine, network, rules.KindSeed, tc.script)

		state, err := engine.StopGracefully(t.Context(), seed,
			provision.Graceful{Signal: "SIGTERM", Grace: tc.grace, HasGrace: true})
		if err != nil {
			t.Fatalf("StopGracefully: %v", err)
		}

		t.Logf("seed %q stopped: %+v", tc.script, state)

		if state.Killed != tc.killed || state.Running {
			t.Errorf("seed %q read %+v, want killed=%v", tc.script, state, tc.killed)
		}

		if err := engine.Remove(t.Context(), seed); err != nil {
			t.Fatalf("Remove: %v", err)
		}
	}
}

// A relay's ready line is read from its running output, within the caller's deadline.
func TestTheReadyLineIsReadFromARunningContainer(t *testing.T) {
	t.Parallel()

	requireEngine(t)

	engine := openEngine(t, provision.Options{})
	c := running(t, engine, freeNetwork(t, engine, serviceRole), rules.KindTarget,
		"echo starting; echo ready 1.2.3.4; exec sleep 600")

	ctx, cancel := context.WithTimeout(t.Context(), readLimit)
	defer cancel()

	line, err := engine.Output(ctx, c, "ready")
	if err != nil {
		t.Fatalf("Output: %v", err)
	}

	if line != "ready 1.2.3.4" {
		t.Errorf("Output = %q, want the ready line", line)
	}
}

// Health is what the engine reports for the container's healthcheck; none reads empty.
func TestHealthIsReadFromTheEngine(t *testing.T) {
	t.Parallel()

	requireEngine(t)

	engine := openEngine(t, provision.Options{})
	image := pinImage(t, engine, testImage)
	network := freeNetwork(t, engine, serviceRole)

	checked := shell(targetSpec(image, network), "exec sleep 600")
	checked.Healthcheck = &provision.Healthcheck{Test: []string{"CMD-SHELL", "exit 0"}, Interval: healthInterval}
	healthy := createContainer(t, engine, checked)
	startContainer(t, engine, healthy)

	unchecked := shell(targetSpec(image, network), "exec sleep 600")
	unchecked.Kind = rules.KindJob
	none := createContainer(t, engine, unchecked)
	startContainer(t, engine, none)

	health := ""

	for deadline := time.Now().Add(readLimit); health != "healthy" && time.Now().Before(deadline); {
		time.Sleep(healthInterval)

		var err error
		if health, err = engine.Health(t.Context(), healthy); err != nil {
			t.Fatalf("Health: %v", err)
		}
	}

	if health != "healthy" {
		t.Errorf("a passing healthcheck reads %q", health)
	}

	if got, err := engine.Health(t.Context(), none); err != nil || got != "" {
		t.Errorf("no healthcheck reads %q, %v; want empty", got, err)
	}
}

// Both networks attach at create; each network's aliases are its own.
func TestTwoNetworksAtCreate(t *testing.T) {
	t.Parallel()

	docker := requireEngine(t).Docker(t)
	engine := openEngine(t, provision.Options{})
	first := freeNetwork(t, engine, serviceRole)
	second := freeNetwork(t, engine, "egress")

	spec := targetSpec(pinImage(t, engine, testImage), first)
	spec.Networks = []provision.NetworkAttach{
		{Network: first, Aliases: []string{"db", "postgres"}},
		{Network: second},
	}

	c := createContainer(t, engine, spec)
	networks := field(inspected(t, docker.Inspect(t, dockertest.ObjectContainer, c.ID())), "NetworkSettings",
		"Networks")

	aliases := func(n *provision.Network) []string {
		settings, ok := field(networks, n.Name()).(map[string]any)
		if !ok {
			t.Fatalf("the container is not attached to %s: %v", n.Name(), networks)
		}

		return texts(settings["Aliases"])
	}

	onFirst, onSecond := aliases(first), aliases(second)
	t.Logf("aliases on the first network %v, on the second %v", onFirst, onSecond)

	if !slices.Contains(onFirst, "db") || !slices.Contains(onFirst, "postgres") {
		t.Errorf("the first network's aliases %v miss db or postgres", onFirst)
	}

	if slices.Contains(onSecond, "db") || slices.Contains(onSecond, "postgres") {
		t.Errorf("the second network carries the first network's aliases: %v", onSecond)
	}
}

// A helper may run with no network at all, and the create is verified as such.
func TestAHelperRunsWithNoNetwork(t *testing.T) {
	t.Parallel()

	docker := requireEngine(t).Docker(t)
	engine := openEngine(t, provision.Options{})

	spec := targetSpec(pinImage(t, engine, testImage), nil)
	spec.Kind, spec.Networks, spec.NoNetwork = rules.KindHelper, nil, true

	c := createContainer(t, engine, spec)

	mode := field(inspected(t, docker.Inspect(t, dockertest.ObjectContainer, c.ID())), "HostConfig", "NetworkMode")
	if mode != "none" {
		t.Errorf("NetworkMode = %v, want none", mode)
	}
}
