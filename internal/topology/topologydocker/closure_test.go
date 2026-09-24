package topologydocker_test

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
)

// listeningDecoy starts a test container on network that accepts on port 5432, so a connection that
// fails to reach it failed on the way, not at its door. It returns its address on network.
func listeningDecoy(t *testing.T, docker *dockertest.Docker, network string) netip.Addr {
	t.Helper()

	id := docker.Create(t, dockertest.CreateSpec{
		Image:   clientImage,
		Network: network,
		Cmd:     []string{"sh", "-c", "while true; do nc -l -p 5432 >/dev/null; done"},
	})
	docker.Start(t, id)

	var inspected inspectedContainer
	if err := json.Unmarshal(docker.Inspect(t, dockertest.ObjectContainer, id), &inspected); err != nil {
		t.Fatalf("decode the decoy: %v", err)
	}

	address, err := netip.ParseAddr(inspected.NetworkSettings.Networks[network].IPAddress)
	if err != nil {
		t.Fatalf("the decoy has no address on %s: %v", network, err)
	}

	return address
}

// TestTheServiceNetworkReachesOnlyTheStub is the service under test's whole view of the world: every
// name it resolves outside its dependencies is the stub relay, a port there reaches the stub's host
// side, and nothing else — the dependency network, a container on it, the internet — is reachable.
func TestTheServiceNetworkReachesOnlyTheStub(t *testing.T) {
	t.Parallel()

	checked := relayedRig(t, requireEngine(t), testLayout())
	decoy := listeningDecoy(t, checked.docker, checked.networks.Dependency.Name())
	before := checked.topo.Listeners().Counts()

	script := fmt.Sprintf(`resolved=$(getent hosts host.docker.internal | awk '{print $1}' | head -n 1)
nc -z -w 3 host.docker.internal 5555; stub=$?
nc -z -w 3 %s 5432; gateway=$?
nc -z -w 3 %s 5432; dependency=$?
nc -z -w 3 1.1.1.1 443; external=$?
echo "RESULT resolved=$resolved stub=$stub gateway=$gateway dependency=$dependency external=$external"
sleep 60`, checked.networks.Gateway, decoy)

	_, result := checked.target(t, script)
	got := fields(result)

	if got["resolved"] != checked.topo.Placement().DNS.String() {
		t.Errorf("host.docker.internal resolved to %q, want the stub relay's %s", got["resolved"],
			checked.topo.Placement().DNS)
	}

	failed := 0

	for _, attempt := range []string{"gateway", "dependency", "external"} {
		if got[attempt] != "0" {
			failed++
		}
	}

	if got["stub"] != "0" || failed != 3 {
		t.Errorf("reached: %s; want the stub reached and the other 3 failed (failed=%d)", result, failed)
	}

	// The stub side handled the connection: no start was attached, so the set counted it.
	if after := checked.topo.Listeners().Counts(); after.Unattached != before.Unattached+1 {
		t.Errorf("unattached connections %d then %d, want one more", before.Unattached, after.Unattached)
	}

	t.Logf("attempts=4 stub=%d failed=%d", map[bool]int{true: 1}[got["stub"] == "0"], failed)

	// The target is still up: the service network holds it beside the relays, which liveness refuses.
	if err := checked.topo.Live(t.Context()); err == nil || !strings.Contains(err.Error(), "membership") {
		t.Errorf("Live() with an extra member = %v, want a membership error", err)
	}
}
