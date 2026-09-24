package relay

import (
	"net/netip"
	"strings"
	"testing"
)

// TestTheRelayBindsOnlyInsideItsPrefix is the relay's refusal to listen anywhere but the one network
// it serves. A relay is dual-homed; a listener on every interface would answer a container on the
// dependency network, or on the engine's default bridge, as if it were the service.
func TestTheRelayBindsOnlyInsideItsPrefix(t *testing.T) {
	t.Parallel()

	sys := system{addresses: fixedAddresses("127.0.0.1", "127.0.0.2", "::1")}
	token := testToken(t)
	host := listenForRelay(t, token)
	port := unusedPort(t)

	spec := func(prefix string) []string {
		return Spec{
			Bind:      netip.MustParsePrefix(prefix),
			Listeners: []Listener{{Upstream: host.address, Port: port}},
			Token:     token,
		}.Args()
	}

	t.Run("one address inside", func(t *testing.T) {
		t.Parallel()

		run := runSystem(t, sys, spec("127.0.0.2/32"))

		if bound := run.ready(t); bound != netip.MustParseAddr("127.0.0.2") {
			t.Fatalf("ready address = %s, want 127.0.0.2", bound)
		}

		if err := dialFrom(t, netip.AddrPortFrom(netip.MustParseAddr("127.0.0.2"), port)); err != nil {
			t.Fatalf("dial the bound address: %v", err)
		}

		if relayed := host.next(t); relayed.DestinationPort() != port {
			t.Errorf("relayed port = %d, want %d", relayed.DestinationPort(), port)
		}

		if err := dialFrom(t, netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)); err == nil {
			t.Error("a dial to 127.0.0.1, outside the prefix, connected: the relay bound a wildcard")
		}
	})

	for _, prefix := range []string{"127.0.0.0/8", "10.9.9.0/24"} {
		t.Run(prefix, func(t *testing.T) {
			t.Parallel()

			run := runSystem(t, sys, spec(prefix))

			if line, printed := run.line(t); printed {
				t.Errorf("printed %q, want no ready line", line)
			}

			if code := run.wait(t); code != exitFailure {
				t.Errorf("exit = %d, want %d", code, exitFailure)
			}

			if stderr := run.stderr.String(); !strings.Contains(stderr, prefix) {
				t.Errorf("stderr %q does not name the prefix %s", stderr, prefix)
			}
		})
	}
}
