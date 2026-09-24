package topologydocker_test

import (
	"net"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// TestTheSeedBusRelayLivesOnlyForTheSeedPhase gives a job the bus under the name it dials, unrecorded,
// and takes it away again: after the seed phase no relay of it is left and its listener refuses.
func TestTheSeedBusRelayLivesOnlyForTheSeedPhase(t *testing.T) {
	t.Parallel()

	store, err := corpus.Open(t.Context(), filepath.Join(t.TempDir(), "store"), "ORDERS")
	if err != nil {
		t.Fatalf("corpus.Open() error = %v", err)
	}

	t.Cleanup(store.Close)

	layout := testLayout()
	layout.SeedBusNames = []string{busName}

	checked := relayedRig(t, requireEngine(t), layout)
	relaysBefore := checked.kinds(t)[string(rules.KindRelay)]

	client := netip.MustParseAddrPort(strings.TrimPrefix(store.URL(), "nats://"))
	if err := checked.topo.SeedBus(t.Context(), client, store.MonitorAddr()); err != nil {
		t.Fatalf("SeedBus() error = %v", err)
	}

	port, open := checked.topo.Listeners().Port(harness.KeySeedBus)
	if !open {
		t.Fatal("the seed bus's listener is not open during the seed phase")
	}

	job, result := checked.runClient(t, rules.KindJob,
		`echo "RESULT varz=$(wget -qO- http://nats:8222/varz | grep -c server_id)"`,
		netip.Addr{}, provision.NetworkAttach{Network: checked.networks.Dependency})

	if got := fields(result)["varz"]; got == "" || got == "0" {
		t.Errorf("GET http://nats:8222/varz from a job: %q, want the server's JSON with server_id", result)
	}

	if err := checked.eng.Remove(t.Context(), job); err != nil {
		t.Fatalf("remove the job: %v", err)
	}

	if err := checked.topo.EndSeedBus(t.Context()); err != nil {
		t.Fatalf("EndSeedBus() error = %v", err)
	}

	if after := checked.kinds(t)[string(rules.KindRelay)]; after != relaysBefore {
		t.Errorf("relay containers = %d after the seed phase, want %d as before it", after, relaysBefore)
	}

	var dialer net.Dialer

	if conn, err := dialer.DialContext(t.Context(), "tcp4", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"),
		port).String()); err == nil {
		_ = conn.Close()

		t.Error("the seed bus's listener still accepts after the seed phase")
	}
}
