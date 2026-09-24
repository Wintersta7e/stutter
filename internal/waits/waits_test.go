package waits_test

import (
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/waits"
)

// TestTheVerifierDialsWithinItsOwnWait: every setup wait is a positive bound, because a zero one fails
// the step it guards before it has started; and the verifier's dial has to fit inside the verifier's
// own wait, or a dial that times out is reported as the verifier timing out.
func TestTheVerifierDialsWithinItsOwnWait(t *testing.T) {
	t.Parallel()

	bounds := []struct {
		name string
		wait time.Duration
	}{
		{name: "DependencyReady", wait: waits.DependencyReady},
		{name: "Job", wait: waits.Job},
		{name: "PostgresRestore", wait: waits.PostgresRestore},
		{name: "RelayReady", wait: waits.RelayReady},
		{name: "Verifier", wait: waits.Verifier},
		{name: "VerifierDial", wait: waits.VerifierDial},
		{name: "ProxyDrain", wait: waits.ProxyDrain},
	}

	t.Logf("%d waits checked", len(bounds))

	if len(bounds) == 0 {
		t.Fatal("no waits to check")
	}

	for _, bound := range bounds {
		if bound.wait <= 0 {
			t.Errorf("%s = %s, want a positive bound", bound.name, bound.wait)
		}
	}

	if waits.VerifierDial >= waits.Verifier {
		t.Errorf("VerifierDial = %s, want it inside Verifier = %s", waits.VerifierDial, waits.Verifier)
	}
}
