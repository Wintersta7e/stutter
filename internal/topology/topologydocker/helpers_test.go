package topologydocker_test

import (
	"context"
	"encoding/json"
	"net/netip"
	"os"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/topology"
)

func TestMain(m *testing.M) {
	dockertest.Main(m)
}

// engineSlots bounds how many of this package's tests use the engine at once: every one runs docker
// CLIs and containers on the one engine the whole run shares.
var engineSlots = make(chan struct{}, 2)

// requireEngine is the gate every test here passes first, then its slot.
func requireEngine(t *testing.T) dockertest.Engine {
	t.Helper()

	engine := dockertest.Require(t)

	select {
	case engineSlots <- struct{}{}:
	case <-t.Context().Done():
		t.Fatal("the test ended waiting for an engine slot")
	}

	t.Cleanup(func() { <-engineSlots })

	return engine
}

// openEngine opens a check against the real engine. The test ends with it closed, and nothing
// carrying its label left behind.
func openEngine(t *testing.T) *provision.Engine {
	t.Helper()

	state := t.TempDir()
	//nolint:gosec // a directory needs its search bit; 0700 is Open's own requirement.
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}

	eng, err := provision.Open(t.Context(), provision.Options{StateDir: state, TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("provision.Open() error = %v", err)
	}

	t.Cleanup(func() {
		if down := eng.Close(context.Background(), provision.DiscardLogs); len(down.Listing) != 0 {
			t.Errorf("the engine still holds %d resources of check %s: %+v", len(down.Listing), eng.CheckID(),
				down.Listing)
		}
	})

	return eng
}

// staticUpstreams is an upstream source for tests whose starts never attach.
func staticUpstreams(context.Context) (map[string]netip.AddrPort, error) {
	return map[string]netip.AddrPort{}, nil
}

// testLayout is a layout with one unparsed dependency, cache:6379.
func testLayout() topology.Layout {
	return topology.Layout{
		Deps:        []topology.Dependency{{Service: "cache", Names: []string{"cache"}, Ports: []uint16{6379}}},
		BusService:  "nats",
		BusNames:    []string{"nats"},
		SelfAliases: []string{"orders"},
		BusPorts:    []uint16{4222},
	}
}

// rig is one check's topology on the real engine: its relay image, its networks and its listeners.
type rig struct {
	eng      *provision.Engine
	topo     *topology.Topology
	docker   *dockertest.Docker
	networks topology.Networks
	image    compose.Image
}

// openRig builds the relay image from the static binary, creates the networks, and opens the topology,
// with tune applied to its configuration first.
func openRig(t *testing.T, engine dockertest.Engine, layout topology.Layout, tune func(*topology.Config)) rig {
	t.Helper()

	eng := openEngine(t)

	image, err := topology.Image(t.Context(), eng, engine.Binary(t, "./cmd/stutter"))
	if err != nil {
		t.Fatalf("Image() error = %v", err)
	}

	networks, err := topology.CreateNetworks(t.Context(), eng)
	if err != nil {
		t.Fatalf("CreateNetworks() error = %v", err)
	}

	cfg := topology.Config{
		Engine: eng, Upstreams: staticUpstreams, Networks: networks, Layout: layout, Image: image,
		Hostname: "orders-under-test", Opaque: layout.Keys(),
	}

	if tune != nil {
		tune(&cfg)
	}

	topo, err := topology.Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("topology.Open() error = %v", err)
	}

	// Registered after the engine's close, so it runs first: the listeners go once the containers do,
	// and the engine removes whatever the test left.
	t.Cleanup(func() {
		if err := topo.Listeners().Close(context.Background()); err != nil {
			t.Errorf("close the listeners: %v", err)
		}
	})

	return rig{eng: eng, topo: topo, docker: engine.Docker(t), networks: networks, image: image}
}

// expectedMode is how containers reach this host, from the engine's own identity rather than from
// the topology's detection: Docker Desktop runs its engine in a VM of its own.
func expectedMode(eng *provision.Engine) harness.Mode {
	if strings.Contains(eng.Identity().Platform, "Docker Desktop") {
		return harness.ModeHostAlias
	}

	return harness.ModeGateway
}

// labelledContainer is a container's labels, as the engine reports them.
type labelledContainer struct {
	Config containerConfig `json:"Config"` //nolint:tagliatelle // the engine's own field name
}

// containerConfig is the part of a container's configuration the tests read.
type containerConfig struct {
	Labels map[string]string `json:"Labels"` //nolint:tagliatelle // the engine's own field name
}

// kinds counts the check's containers on the engine by kind, read back from the engine.
func (r rig) kinds(t *testing.T) map[string]int {
	t.Helper()

	counted := map[string]int{}

	for _, id := range r.docker.Listing(t, rules.LabelCheck+"="+r.eng.CheckID()).Containers {
		var inspected labelledContainer

		if err := json.Unmarshal(r.docker.Inspect(t, dockertest.ObjectContainer, id), &inspected); err != nil {
			t.Fatalf("decode container %s: %v", id, err)
		}

		counted[inspected.Config.Labels[rules.LabelKind]]++
	}

	return counted
}
