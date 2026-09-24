package harness_test

import (
	"errors"
	"strings"
	"testing"

	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// TestATLSFirstBusClientStopsTheRun: a bus client that starts a TLS handshake leaves the proxy
// nothing to read. Counted as setup, it reported a handler that did nothing; it stops the run.
func TestATLSFirstBusClientStopsTheRun(t *testing.T) {
	t.Parallel()

	built, _ := quirkySandbox(t, observedConfig(), quirks{tlsFirst: true}, nil, "ORD-TLS-1")

	_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if !errors.Is(err, natsproxy.ErrUnsupportedBus) || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("Run() error = %v, want the run stopped as an unsupported bus client naming TLS", err)
	}
}
