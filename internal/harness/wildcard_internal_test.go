package harness

import (
	"net"
	"net/netip"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/Wintersta7e/stutter/internal/corpus"
)

// localIPv4 is a non-loopback IPv4 address of this host, the shape a gateway-mode bind has, or false
// when the host has none.
func localIPv4(t *testing.T) (netip.Addr, bool) {
	t.Helper()

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("list the interface addresses: %v", err)
	}

	for _, entry := range addrs {
		network, isNetwork := entry.(*net.IPNet)
		if !isNetwork {
			continue
		}

		if addr, parsed := netip.AddrFromSlice(network.IP); parsed && addr.Unmap().Is4() && !addr.IsLoopback() {
			return addr.Unmap(), true
		}
	}

	return netip.Addr{}, false
}

// TestNoListenerBindsAWildcard holds every socket Stutter opens for a relayed check to one address:
// the listener set's to its bind address, the embedded bus's to loopback. A wildcard would answer on
// every network the host is on.
func TestNoListenerBindsAWildcard(t *testing.T) {
	t.Parallel()

	binds := []netip.Addr{loopbackAddr}
	if local, found := localIPv4(t); found {
		binds = append(binds, local)
	}

	checked := 0

	for _, bind := range binds {
		cfg := testListenerConfig(t)
		cfg.Bind = bind
		set := openTestSet(t, cfg)

		if _, err := set.OpenSeedBus(t.Context(), netip.MustParseAddrPort("127.0.0.1:1"),
			netip.MustParseAddrPort("127.0.0.1:2")); err != nil {
			t.Fatalf("OpenSeedBus() error = %v", err)
		}

		for key, opened := range set.endpoints {
			checked++

			if bound := netip.MustParseAddrPort(opened.listener.Addr().String()).Addr(); bound != bind {
				t.Errorf("bind %s: %s listens on %s", bind, key, bound)
			}
		}

		if err := set.CloseSeedBus(); err != nil {
			t.Errorf("CloseSeedBus() error = %v", err)
		}
	}

	store, err := corpus.Open(t.Context(), filepath.Join(t.TempDir(), "store"), "ORDERS")
	if err != nil {
		t.Fatalf("corpus.Open() error = %v", err)
	}

	t.Cleanup(store.Close)

	client, err := url.Parse(store.URL())
	if err != nil {
		t.Fatalf("parse the bus URL: %v", err)
	}

	servers := map[string]string{
		"bus client":  client.Hostname(),
		"bus monitor": store.MonitorAddr().Addr().String(),
	}

	for name, bound := range servers {
		checked++

		if bound != loopbackAddr.String() {
			t.Errorf("the %s listens on %s, want %s", name, bound, loopbackAddr)
		}
	}

	t.Logf("listeners checked %d", checked)

	if checked == 0 {
		t.Fatal("no listener was checked")
	}
}
