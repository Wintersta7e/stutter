package topology

import (
	"net/netip"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision"
)

// TestAGatewayTheEngineDidNotRecordIsDerived keeps a network usable on an engine that records no
// gateway for a network created with a subnet alone: measured on Docker 28, where 29 records one. The
// derived address is the one the engine gives the bridge; verification then proves it, never trust.
func TestAGatewayTheEngineDidNotRecordIsDerived(t *testing.T) {
	t.Parallel()

	subnet := netip.MustParsePrefix("172.16.4.0/24")
	first := netip.MustParseAddr("172.16.4.1")

	recorded, err := gatewayOf(provision.NetworkState{Subnet: subnet, Gateway: netip.MustParseAddr("172.16.4.254")})
	if err != nil || recorded != netip.MustParseAddr("172.16.4.254") {
		t.Errorf("a recorded gateway: %s, %v; want the recorded 172.16.4.254", recorded, err)
	}

	derived, err := gatewayOf(provision.NetworkState{Subnet: subnet})
	if err != nil || derived != first {
		t.Errorf("no recorded gateway: %s, %v; want the subnet's first address, %s", derived, err, first)
	}

	if _, err := gatewayOf(provision.NetworkState{}); err == nil {
		t.Error("no gateway and no subnet: err = <nil>, want a refusal")
	}
}
