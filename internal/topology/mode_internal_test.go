package topology

import (
	"net/netip"
	"testing"

	"github.com/Wintersta7e/stutter/internal/harness"
)

// TestTheModeFollowsTheGateway decides how containers reach the host from one fact: whether the
// dependency network's gateway is an address of the host's own.
func TestTheModeFollowsTheGateway(t *testing.T) {
	t.Parallel()

	local := []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("172.16.1.1")}

	if mode := detectMode(netip.MustParseAddr("172.16.1.1"), local); mode != harness.ModeGateway {
		t.Errorf("a local gateway: mode = %s, want gateway", mode)
	}

	if mode := detectMode(netip.MustParseAddr("172.16.2.1"), local); mode != harness.ModeHostAlias {
		t.Errorf("a gateway elsewhere: mode = %s, want host-alias", mode)
	}
}
