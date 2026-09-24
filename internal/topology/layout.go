// Package topology builds a compose check's network topology on the engine: the relay image, the two
// networks, the host-address mode and its verification, the relays and their liveness. The service
// under test reaches nothing but relays on an internal network; each relay pipes to the check's
// listener set on the host, where the proxies observe it.
package topology

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/harness"
)

// defaultBusPort is the bus's client port when the bus service declares none: NATS's own default.
const defaultBusPort uint16 = 4222

// errLayout means the classification cannot be given relays.
var errLayout = errors.New("cannot lay out the relays")

// Dependency is one relayed dependency: its compose service, the names the service under test dials it
// by, and its TCP ports.
type Dependency struct {
	Service string
	Names   []string
	Ports   []uint16
}

// Layout is the relay set a classification calls for, identical for every start of the check.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Layout struct {
	// Deps are the relayed dependencies, by service name.
	Deps []Dependency
	// BusService is the compose service the bus names belong to; empty when every bus name is a host
	// outside the model.
	BusService string
	// BusNames are the names the service under test dials the bus by.
	BusNames []string
	// SeedBusNames are the names jobs dial the bus by during the seed phase.
	SeedBusNames []string
	// SelfAliases are the names the service under test answers to itself.
	SelfAliases []string
	// BusPorts are the bus's client ports.
	BusPorts []uint16
}

// LayoutFrom derives the relay set from a classification: one relay per datastore and other
// dependency, carrying its target-view names and its TCP ports, and the bus relay's names and ports.
// Siblings, unused services and jobs get no relay: their names reach the stub.
func LayoutFrom(c compose.Classification) (Layout, error) {
	if len(c.BusNames) == 0 {
		return Layout{}, fmt.Errorf("%w: the service under test names no bus", errLayout)
	}

	layout := Layout{
		BusNames:     slices.Clone(c.BusNames),
		SeedBusNames: slices.Clone(c.SetupBusNames),
		SelfAliases:  slices.Clone(c.SelfAliases),
	}

	for _, dep := range c.Deps {
		switch dep.Role {
		case compose.RoleDatastore, compose.RoleOther:
			relayed, err := relayedDependency(dep)
			if err != nil {
				return Layout{}, err
			}

			layout.Deps = append(layout.Deps, relayed)
		case compose.RoleBus:
			layout.BusService = dep.Service
			layout.BusPorts = portsOf(dep, compose.ProtocolNATS)
		case compose.RoleSibling, compose.RoleUnused, compose.RoleJob, compose.RoleAttached:
			// No relay: their names reach the stub. An attached target was refused long before this.
		default:
			// A dependency this cannot place would go unobserved, so it is refused rather than guessed at.
			return Layout{}, fmt.Errorf("%w: dependency %s has role %q", errLayout, dep.Service, dep.Role)
		}
	}

	if len(layout.BusPorts) == 0 {
		layout.BusPorts = []uint16{defaultBusPort}
	}

	if slices.Contains(layout.BusPorts, harness.MonitorPort) {
		return Layout{}, fmt.Errorf("%w: bus %s serves clients on %d, the port its monitoring is piped from",
			errLayout, layout.BusService, harness.MonitorPort)
	}

	slices.SortFunc(layout.Deps, func(a, b Dependency) int { return cmp.Compare(a.Service, b.Service) })

	return layout, nil
}

// relayedDependency is a dependency's relay: its names and every TCP port.
func relayedDependency(dep compose.Dependency) (Dependency, error) {
	ports := portsOf(dep, "")
	if len(ports) == 0 {
		return Dependency{}, fmt.Errorf("%w: dependency %s has no TCP port to relay", errLayout, dep.Service)
	}

	return Dependency{Service: dep.Service, Names: slices.Clone(dep.Names), Ports: ports}, nil
}

// portsOf lists a dependency's TCP ports, sorted: those of one protocol, or every protocol but UDP when
// protocol is empty.
func portsOf(dep compose.Dependency, protocol compose.Protocol) []uint16 {
	var ports []uint16

	for _, endpoint := range dep.Endpoints {
		wanted := endpoint.Protocol == protocol
		if protocol == "" {
			wanted = endpoint.Protocol != compose.ProtocolUDP
		}

		if wanted {
			ports = append(ports, endpoint.Port)
		}
	}

	slices.Sort(ports)

	return slices.Compact(ports)
}

// Keys are the endpoint keys of every relayed dependency's ports, sorted: the keys the listener set
// serves with a Postgres or an opaque proxy.
func (l Layout) Keys() []string {
	var keys []string

	for _, dep := range l.Deps {
		for _, port := range dep.Ports {
			keys = append(keys, harness.EndpointKey(dep.Service, port))
		}
	}

	slices.Sort(keys)

	return keys
}
