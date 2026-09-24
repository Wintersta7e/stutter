package topologydocker_test

import (
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/topology"
)

// inspectedNetwork is what the engine reports about one network.
type inspectedNetwork struct {
	Name       string      `json:"Name"`       //nolint:tagliatelle // the engine's own field name
	IPAM       networkIPAM `json:"IPAM"`       //nolint:tagliatelle // the engine's own field name
	Internal   bool        `json:"Internal"`   //nolint:tagliatelle // the engine's own field name
	EnableIPv6 bool        `json:"EnableIPv6"` //nolint:tagliatelle // the engine's own field name
}

// networkIPAM is a network's address management, as the engine reports it.
type networkIPAM struct {
	Config []ipamConfig `json:"Config"` //nolint:tagliatelle // the engine's own field name
}

// ipamConfig is one of a network's address pools.
type ipamConfig struct {
	Subnet string `json:"Subnet"` //nolint:tagliatelle // the engine's own field name
}

func readNetwork(t *testing.T, docker *dockertest.Docker, network *provision.Network) inspectedNetwork {
	t.Helper()

	var inspected inspectedNetwork
	if err := json.Unmarshal(docker.Inspect(t, dockertest.ObjectNetwork, network.ID()), &inspected); err != nil {
		t.Fatalf("decode network %s: %v", network.Name(), err)
	}

	return inspected
}

// localPrefixes are this host's IPv4 interface prefixes, read by the test itself.
func localPrefixes(t *testing.T) []netip.Prefix {
	t.Helper()

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("list the interface addresses: %v", err)
	}

	var prefixes []netip.Prefix

	for _, entry := range addrs {
		if prefix, err := netip.ParsePrefix(entry.String()); err == nil && prefix.Addr().Unmap().Is4() {
			prefixes = append(prefixes, prefix.Masked())
		}
	}

	return prefixes
}

// TestEveryNetworkGetsAVerifiedFreeSubnet reads every network the topology creates back from the
// engine: a /24 of 172.16.0.0/12 that shadows no interface of this host and no other network, with the
// flags the topology asked for. Four checks create theirs at once, so the subnet pick meets real
// contention.
func TestEveryNetworkGetsAVerifiedFreeSubnet(t *testing.T) {
	t.Parallel()

	candidates := netip.MustParsePrefix("172.16.0.0/12")
	locals := localPrefixes(t)

	for check := range 4 {
		t.Run(fmt.Sprintf("check-%d", check+1), func(t *testing.T) {
			t.Parallel()

			engine := requireEngine(t)
			eng := openEngine(t)
			docker := engine.Docker(t)

			networks, err := topology.CreateNetworks(t.Context(), eng)
			if err != nil {
				t.Fatalf("CreateNetworks() error = %v", err)
			}

			occupied, err := eng.Occupied(t.Context())
			if err != nil {
				t.Fatalf("Occupied() error = %v", err)
			}

			checked := 0

			wantInternal := map[*provision.Network]bool{networks.Service: true, networks.Dependency: false}

			for network, internal := range wantInternal {
				read := readNetwork(t, docker, network)
				checked++

				if len(read.IPAM.Config) != 1 {
					t.Fatalf("%s: %d IPAM configs, want 1", read.Name, len(read.IPAM.Config))
				}

				subnet := netip.MustParsePrefix(read.IPAM.Config[0].Subnet)
				if subnet.Bits() != 24 || !candidates.Overlaps(subnet) || subnet.Addr().Is6() {
					t.Errorf("%s: subnet %s, want a /24 of %s", read.Name, subnet, candidates)
				}

				for _, local := range locals {
					if local.Overlaps(subnet) {
						t.Errorf("%s: subnet %s overlaps this host's interface prefix %s", read.Name, subnet, local)
					}
				}

				for _, other := range occupied {
					if strings.HasSuffix(other.By, read.Name) || !strings.HasPrefix(other.By, "network ") {
						continue
					}

					if other.Prefix.Overlaps(subnet) {
						t.Errorf("%s: subnet %s overlaps %s", read.Name, subnet, other.By)
					}
				}

				if read.Internal != internal || read.EnableIPv6 {
					t.Errorf("%s: Internal=%v EnableIPv6=%v, want Internal=%v EnableIPv6=false", read.Name,
						read.Internal, read.EnableIPv6, internal)
				}
			}

			t.Logf("networks checked: %d", checked)
		})
	}
}
