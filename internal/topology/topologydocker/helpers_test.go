package topologydocker_test

import (
	"context"
	"encoding/json"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/topology"
)

// The names the tests' compose model uses.
const (
	// cacheName is the unparsed dependency's service and first name.
	cacheName = "cache"
	// busName is the bus's service and first name.
	busName = "nats"
	// selfAlias is the service under test's name for itself.
	selfAlias = "orders"
	// clientImage is the image the tests' client containers run: it has a shell, nc, wget and getent.
	clientImage = "postgres:18-alpine"
	// resultWait bounds how long a client container may take to print its result.
	resultWait = time.Minute
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
		Deps:        []topology.Dependency{{Service: cacheName, Names: []string{cacheName}, Ports: []uint16{6379}}},
		BusService:  busName,
		BusNames:    []string{busName},
		SelfAliases: []string{selfAlias},
		BusPorts:    []uint16{4222},
	}
}

// rig is one check's topology on the real engine: its relay image, its networks and its listeners.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type rig struct {
	eng      *provision.Engine
	topo     *topology.Topology
	docker   *dockertest.Docker
	networks topology.Networks
	image    compose.Image
	// client is the pinned client image, for the tests' client containers.
	client compose.Image
}

// openRig builds the relay image from the static binary, creates the networks, and opens the topology,
// with tune applied to its configuration first. The client image is pinned first: an engine pins
// nothing after its first container, and the verifier is one.
func openRig(t *testing.T, engine dockertest.Engine, layout topology.Layout, tune func(*topology.Config)) rig {
	t.Helper()

	eng := openEngine(t)

	client, err := eng.ResolveImage(t.Context(), clientImage, "")
	if err != nil {
		t.Fatalf("ResolveImage(%s) error = %v", clientImage, err)
	}

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

	// Registered after the engine's close, so it runs first, as a check's teardown does: the relays and
	// the verifier go, then the listeners, then the engine removes whatever else the test left.
	t.Cleanup(func() {
		if err := topo.Close(context.Background()); err != nil {
			t.Errorf("close the topology: %v", err)
		}
	})

	return rig{eng: eng, topo: topo, docker: engine.Docker(t), networks: networks, image: image, client: client}
}

// relayedRig is a rig verified and with its relays up.
func relayedRig(t *testing.T, engine dockertest.Engine, layout topology.Layout) rig {
	t.Helper()

	checked := openRig(t, engine, layout, nil)

	if err := checked.topo.Verify(t.Context()); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	if err := checked.topo.Relays(t.Context()); err != nil {
		t.Fatalf("Relays() error = %v", err)
	}

	return checked
}

// runClient creates and starts a client container running script, attached as attach says, and waits
// for the line it prints starting with RESULT. The container is removed when the test ends.
func (r rig) runClient(
	t *testing.T,
	kind rules.Kind,
	script string,
	dns netip.Addr,
	attach provision.NetworkAttach,
) (*provision.Container, string) {
	t.Helper()

	client, err := r.eng.CreateContainer(t.Context(), provision.ContainerSpec{
		Kind:     kind,
		Service:  selfAlias,
		DNS:      dns,
		Networks: []provision.NetworkAttach{attach},
		Spec: compose.Spec{
			Service:  selfAlias,
			Image:    r.client.ID,
			Hostname: r.topo.Placement().Hostname,
			Cmd:      []string{"sh", "-c", script},
			CmdSet:   true,
		},
	})
	if err != nil {
		t.Fatalf("create the client: %v", err)
	}

	// A removal that fails leaves the client for the engine's close, which reports it.
	t.Cleanup(func() { _ = r.eng.Remove(context.Background(), client) }) //nolint:errcheck // see above.

	if startErr := r.eng.Start(t.Context(), client); startErr != nil {
		t.Fatalf("start the client: %v", startErr)
	}

	ctx, cancel := context.WithTimeout(t.Context(), resultWait)
	defer cancel()

	line, err := r.eng.Output(ctx, client, "RESULT")
	if err != nil {
		t.Fatalf("the client printed no result: %v", err)
	}

	return client, strings.TrimSpace(strings.TrimPrefix(line, "RESULT"))
}

// target runs script as the check's target would run: on the service network with its self-aliases,
// its hostname, and the stub relay as its resolver.
func (r rig) target(t *testing.T, script string) (*provision.Container, string) {
	t.Helper()

	placement := r.topo.Placement()

	return r.runClient(t, rules.KindTarget, script, placement.DNS, placement.Attach)
}

// fields splits a client's result into its key=value pairs.
func fields(result string) map[string]string {
	pairs := map[string]string{}

	for field := range strings.FieldsSeq(result) {
		key, value, _ := strings.Cut(field, "=")
		pairs[key] = value
	}

	return pairs
}

// relayInfo is one relay container as the engine reports it.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type relayInfo struct {
	labels  map[string]string
	address netip.Addr
	id      string
	raw     json.RawMessage
}

// relaysOf reads every relay container of the check back from the engine, with its address on network:
// zero when it has none there.
func (r rig) relaysOf(t *testing.T, network string) []relayInfo {
	t.Helper()

	var found []relayInfo

	for _, id := range r.docker.Listing(t, rules.LabelCheck+"="+r.eng.CheckID()).Containers {
		raw := r.docker.Inspect(t, dockertest.ObjectContainer, id)

		var inspected inspectedContainer
		if err := json.Unmarshal(raw, &inspected); err != nil {
			t.Fatalf("decode container %s: %v", id, err)
		}

		if inspected.Config.Labels[rules.LabelKind] != string(rules.KindRelay) {
			continue
		}

		var address netip.Addr

		if parsed, err := netip.ParseAddr(inspected.NetworkSettings.Networks[network].IPAddress); err == nil {
			address = parsed
		}

		found = append(found, relayInfo{raw: raw, labels: inspected.Config.Labels, address: address, id: id})
	}

	return found
}

// inspectedContainer is the part of a container's inspect the tests read.
type inspectedContainer struct {
	Config          containerConfig   `json:"Config"`          //nolint:tagliatelle // the engine's own field name
	NetworkSettings containerNetworks `json:"NetworkSettings"` //nolint:tagliatelle // the engine's own field name
}

// containerConfig is the part of a container's configuration the tests read.
type containerConfig struct {
	Labels map[string]string `json:"Labels"` //nolint:tagliatelle // the engine's own field name
}

// containerNetworks is a container's networks, by name.
type containerNetworks struct {
	Networks map[string]containerEndpoint `json:"Networks"` //nolint:tagliatelle // the engine's own field name
}

// containerEndpoint is a container's attachment to one network.
type containerEndpoint struct {
	IPAddress string `json:"IPAddress"` //nolint:tagliatelle // the engine's own field name
}

// expectedMode is how containers reach this host, from the engine's own identity rather than from
// the topology's detection: Docker Desktop runs its engine in a VM of its own.
func expectedMode(eng *provision.Engine) harness.Mode {
	if strings.Contains(eng.Identity().Platform, "Docker Desktop") {
		return harness.ModeHostAlias
	}

	return harness.ModeGateway
}

// kinds counts the check's containers on the engine by kind, read back from the engine.
func (r rig) kinds(t *testing.T) map[string]int {
	t.Helper()

	counted := map[string]int{}

	for _, id := range r.docker.Listing(t, rules.LabelCheck+"="+r.eng.CheckID()).Containers {
		var inspected inspectedContainer

		if err := json.Unmarshal(r.docker.Inspect(t, dockertest.ObjectContainer, id), &inspected); err != nil {
			t.Fatalf("decode container %s: %v", id, err)
		}

		counted[inspected.Config.Labels[rules.LabelKind]]++
	}

	return counted
}
