package topologydocker_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/topology"
)

// TestTheHostModeIsDetectedAndVerified proves, on the engine this host runs, that containers reach the
// listener set where the topology says they will — and that a check whose containers cannot is refused
// before a single relay or dependency exists.
func TestTheHostModeIsDetectedAndVerified(t *testing.T) {
	t.Parallel()

	t.Run("this-host", func(t *testing.T) {
		t.Parallel()

		engine := requireEngine(t)
		checked := openRig(t, engine, testLayout(), nil)

		if err := checked.topo.Verify(t.Context()); err != nil {
			t.Fatalf("Verify() error = %v", err)
		}

		want := expectedMode(checked.eng)
		if got := checked.topo.Listeners().Mode(); got != want || checked.topo.Mode() != want {
			t.Errorf("mode = %s, want %s from the engine's identity %q", got, want, checked.eng.Identity().Platform)
		}

		if _, open := checked.topo.Listeners().Port(harness.KeyVerify); open {
			t.Error("the verification listener is still open after Verify")
		}

		t.Logf("mode %s verified", want)
	})

	t.Run("wrong-advertised-address", func(t *testing.T) {
		t.Parallel()

		engine := requireEngine(t)
		checked := openRig(t, engine, testLayout(), func(cfg *topology.Config) { cfg.VerifyTarget = "192.0.2.1" })

		err := checked.topo.Verify(t.Context())

		var refused *topology.VerifyError
		if !errors.As(err, &refused) || !strings.Contains(err.Error(), "192.0.2.1") ||
			!strings.Contains(err.Error(), checked.topo.Mode().String()) {
			t.Fatalf("Verify() = %v, want a VerifyError naming the mode and 192.0.2.1", err)
		}

		counted := checked.kinds(t)
		t.Logf("containers by kind after the refusal: %v", counted)

		if counted[string(rules.KindRelay)] != 0 {
			t.Errorf("relay containers = %d, want 0", counted[string(rules.KindRelay)])
		}
	})
}
