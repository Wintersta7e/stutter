package topology_test

import (
	"slices"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/topology"
)

// The bus's service, first name and second name, and the unparsed dependency's.
const (
	busName   = "nats"
	busAlias  = "queue"
	cacheName = "cache"
)

func endpoint(port uint16, protocol compose.Protocol) compose.Endpoint {
	return compose.Endpoint{Port: port, Protocol: protocol}
}

// classified is a model with every role the relay set must tell apart.
func classified(busPorts ...uint16) compose.Classification {
	bus := compose.Dependency{Service: busName, Role: compose.RoleBus, Names: []string{busName, busAlias}}
	for _, port := range busPorts {
		bus.Endpoints = append(bus.Endpoints, endpoint(port, compose.ProtocolNATS))
	}

	return compose.Classification{
		Deps: []compose.Dependency{
			{
				Service: "db", Role: compose.RoleDatastore, Names: []string{"db", "postgres"},
				Endpoints: []compose.Endpoint{endpoint(5432, compose.ProtocolPG)},
			},
			{
				Service: cacheName,
				Role:    compose.RoleOther,
				Names:   []string{cacheName},
				Endpoints: []compose.Endpoint{
					endpoint(6379, compose.ProtocolOpaque),
					endpoint(6380, compose.ProtocolUDP),
				},
			},
			{Service: "worker", Role: compose.RoleSibling, Names: []string{"worker"}},
			{Service: "admin", Role: compose.RoleUnused, Names: []string{"admin"}},
			{Service: "migrate", Role: compose.RoleJob, Names: []string{"migrate"}},
			bus,
		},
		SelfAliases:   []string{"orders"},
		BusNames:      []string{busName, busAlias},
		SetupBusNames: []string{busName},
	}
}

// TestLayoutFollowsTheClassification gives a relay to every dependency the service under test can
// reach and nothing else: a sibling, an unused service and a job are the stub's.
func TestLayoutFollowsTheClassification(t *testing.T) {
	t.Parallel()

	layout, err := topology.LayoutFrom(classified(4222))
	if err != nil {
		t.Fatalf("LayoutFrom() error = %v", err)
	}

	services := make([]string, 0, len(layout.Deps))
	for _, dep := range layout.Deps {
		services = append(services, dep.Service)
	}

	if !slices.Equal(services, []string{cacheName, "db"}) {
		t.Errorf("relayed dependencies = %v, want [cache db]", services)
	}

	if cache := layout.Deps[0]; !slices.Equal(cache.Ports, []uint16{6379}) {
		t.Errorf("cache ports = %v, want [6379]: a UDP port is not relayed", cache.Ports)
	}

	if db := layout.Deps[1]; !slices.Equal(db.Names, []string{"db", "postgres"}) {
		t.Errorf("db names = %v, want [db postgres]", db.Names)
	}

	if layout.BusService != busName || !slices.Equal(layout.BusPorts, []uint16{4222}) {
		t.Errorf("bus = %s %v, want nats [4222]", layout.BusService, layout.BusPorts)
	}

	if !slices.Equal(layout.BusNames, []string{busName, busAlias}) ||
		!slices.Equal(layout.SeedBusNames, []string{busName}) ||
		!slices.Equal(layout.SelfAliases, []string{"orders"}) {
		t.Errorf("names = bus %v, seed %v, self %v", layout.BusNames, layout.SeedBusNames, layout.SelfAliases)
	}

	if keys := layout.Keys(); !slices.Equal(keys, []string{"cache:6379", "db:5432"}) {
		t.Errorf("Keys() = %v, want [cache:6379 db:5432]", keys)
	}
}

func TestLayoutRefusesACollidingMonitorPort(t *testing.T) {
	t.Parallel()

	if _, err := topology.LayoutFrom(classified(4222, harness.MonitorPort)); err == nil {
		t.Errorf("a bus serving clients on %d was laid out, want a refusal", harness.MonitorPort)
	}

	withoutBus := classified(4222)
	withoutBus.BusNames = nil

	if _, err := topology.LayoutFrom(withoutBus); err == nil {
		t.Error("a service naming no bus was laid out, want a refusal")
	}

	portless := classified(4222)
	portless.Deps[1].Endpoints = []compose.Endpoint{endpoint(6380, compose.ProtocolUDP)}

	if _, err := topology.LayoutFrom(portless); err == nil {
		t.Error("a dependency with no TCP port was laid out, want a refusal")
	}
}

func TestTheBusPortDefaultsToNATS(t *testing.T) {
	t.Parallel()

	layout, err := topology.LayoutFrom(classified())
	if err != nil {
		t.Fatalf("LayoutFrom() error = %v", err)
	}

	if !slices.Equal(layout.BusPorts, []uint16{4222}) {
		t.Errorf("BusPorts = %v, want [4222]", layout.BusPorts)
	}
}
