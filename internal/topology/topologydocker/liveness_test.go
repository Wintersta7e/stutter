package topologydocker_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/topology"
)

// deathWait bounds how long a relay may take to exit once its dial has failed.
const deathWait = 10 * time.Second

// TestLivenessNamesADeadRelay kills a relay the way a run would: its host listener gone, a connection
// arrives, the dial fails, the relay exits. Liveness must name it, how it exited, and why.
func TestLivenessNamesADeadRelay(t *testing.T) {
	t.Parallel()

	checked := relayedRig(t, requireEngine(t), testLayout())

	if err := checked.topo.Live(t.Context()); err != nil {
		t.Fatalf("Live() before anything died = %v", err)
	}

	if err := checked.topo.Listeners().Close(context.Background()); err != nil {
		t.Fatalf("close the listeners: %v", err)
	}

	// Several connections, a second apart: the relay dies on the first whose dial is refused, and the
	// engine's host forwarding may not refuse at once for a listener that has only just closed.
	_, dialled := checked.target(t,
		`for i in 1 2 3 4 5; do nc -w 2 cache 6379 </dev/null; sleep 1; done; echo "RESULT dialled=$?"`)

	var (
		dead *topology.RelayError
		last error
	)

	deadline := time.Now().Add(deathWait)

	for time.Now().Before(deadline) {
		if last = checked.topo.Live(t.Context()); errors.As(last, &dead) {
			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	if dead == nil {
		t.Fatalf("no relay reported dead within %s (client %s; last liveness: %v)", deathWait, dialled, last)
	}

	if dead.Relay != "dependency cache" || dead.ExitCode != 4 || dead.Restarts != 0 ||
		!strings.Contains(dead.Stderr, "dial") {
		t.Errorf("RelayError = %+v, want dependency cache, exit 4, no restart, and its dial failure", dead)
	}

	for _, each := range checked.relaysOf(t, checked.networks.Service.Name()) {
		if each.labels[rules.LabelService] != cacheName {
			continue
		}

		state := inspect(t, checked.docker.Inspect(t, dockertest.ObjectContainer, each.id))
		if !state.is(false, "State", "Running") || state.at("State", "ExitCode") != float64(4) {
			t.Errorf("the engine reads the dead relay back as %v, want exited with code 4", state.at("State"))
		}
	}
}
