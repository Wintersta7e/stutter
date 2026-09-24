package topologydocker_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/topology"
)

// twoByTwo is a classification with two dependencies of two names each, and a bus of two names.
func twoByTwo(t *testing.T) topology.Layout {
	t.Helper()

	layout, err := topology.LayoutFrom(compose.Classification{
		Deps: []compose.Dependency{
			{
				Service: cacheName, Role: compose.RoleOther, Names: []string{cacheName, "kv"},
				Endpoints: []compose.Endpoint{{Port: 6379, Protocol: compose.ProtocolOpaque}},
			},
			{
				Service: "db", Role: compose.RoleDatastore, Names: []string{"db", "postgres"},
				Endpoints: []compose.Endpoint{{Port: 5432, Protocol: compose.ProtocolPG}},
			},
			{
				Service: busName, Role: compose.RoleBus, Names: []string{busName, "queue"},
				Endpoints: []compose.Endpoint{{Port: 4222, Protocol: compose.ProtocolNATS}},
			},
		},
		SelfAliases: []string{selfAlias},
		BusNames:    []string{busName, "queue"},
	})
	if err != nil {
		t.Fatalf("LayoutFrom() error = %v", err)
	}

	return layout
}

// TestEveryTargetViewNameResolvesToItsRelay resolves every name the service under test dials a
// dependency by, from where the service runs: each is its dependency's relay, never the stub, so no
// dependency's traffic is taken for egress to an unknown host.
func TestEveryTargetViewNameResolvesToItsRelay(t *testing.T) {
	t.Parallel()

	layout := twoByTwo(t)
	checked := relayedRig(t, requireEngine(t), layout)

	want := map[string]string{}

	for _, each := range checked.relaysOf(t, checked.networks.Service.Name()) {
		service := each.labels[rules.LabelService]

		for _, dep := range layout.Deps {
			if dep.Service == service {
				for _, name := range dep.Names {
					want[name] = each.address.String()
				}
			}
		}

		if service == layout.BusService {
			for _, name := range layout.BusNames {
				want[name] = each.address.String()
			}
		}
	}

	names := slices.Sorted(func(yield func(string) bool) {
		for name := range want {
			if !yield(name) {
				return
			}
		}
	})

	var script strings.Builder

	script.WriteString(`printf "RESULT"; `)

	for _, name := range names {
		fmt.Fprintf(&script, `printf " %s=%%s" "$(getent hosts %s | awk '{print $1}' | head -n 1)"; `, name, name)
	}

	script.WriteString("echo")

	_, result := checked.target(t, script.String())
	got := fields(result)

	for _, name := range names {
		if got[name] != want[name] || got[name] == checked.topo.Placement().DNS.String() {
			t.Errorf("%s resolved to %q, want its relay's %s (the stub is %s)", name, got[name], want[name],
				checked.topo.Placement().DNS)
		}
	}

	t.Logf("names checked: %d", len(names))

	if len(names) == 0 {
		t.Fatal("no name was checked")
	}
}
