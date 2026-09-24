//go:build linux

package enginetest_test

import (
	"errors"
	"fmt"
	"net/netip"
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// A subnet another network already holds is refused by the engine, and the refusal reads as taken
// from what the engine then holds: the caller picks another, and no network of the check is left.
func TestATakenSubnetIsRefusedByTheEngine(t *testing.T) {
	t.Parallel()

	docker := requireEngine(t).Docker(t)
	engine := openEngine(t, provision.Options{})

	// The helper picks the subnet from its own range, retrying while another test holds one.
	decoy := docker.CreateNetwork(t, "stutter-test-taken-"+randomHex(8), netip.Prefix{}, nil)

	configs := list(field(inspected(t, docker.Inspect(t, dockertest.ObjectNetwork, decoy)), "IPAM", "Config"))
	if len(configs) != 1 {
		t.Fatalf("the decoy network holds %d subnets, want 1", len(configs))
	}

	subnet, err := netip.ParsePrefix(fmt.Sprint(configs[0]["Subnet"]))
	if err != nil {
		t.Fatal(err)
	}

	network, err := engine.CreateNetwork(t.Context(), serviceRole, true, subnet)
	taken := errors.Is(err, provision.ErrSubnetTaken)
	t.Logf("engine refused taken subnet=%v (%v)", taken, err)

	if !taken {
		t.Fatalf("CreateNetwork on the taken %s = %v, %v; want ErrSubnetTaken", subnet, network, err)
	}

	if left := docker.Listing(t, rules.LabelCheck+"="+engine.CheckID()); len(left.Networks) != 0 {
		t.Errorf("the engine holds networks of the check: %v", left.Networks)
	}
}
