//go:build linux

package provision

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
)

// publishedPort is the container port the publishing tests publish.
const publishedPort = 5432

// refusalLines are the engine's two answers to a start whose host port is taken: Docker Desktop's
// when a host process holds it, and the engine's own when another container does.
var refusalLines = []string{
	"Error response from daemon: ports are not available: exposing port TCP 127.0.0.1:%s -> 127.0.0.1:0: " +
		"/forwards/expose returned unexpected status: 500",
	"Error response from daemon: failed to set up container networking: driver failed programming external " +
		"connectivity on endpoint x: Bind for 127.0.0.1:%s failed: port is already allocated",
}

// publishingFixture is containerFixture's target publishing one port.
func publishingFixture(t *testing.T) (*Engine, *fakeEngine, ContainerSpec) {
	t.Helper()

	engine, fake, spec := containerFixture(t)
	spec.Publish = []uint16{publishedPort}

	return engine, fake, spec
}

// hostPortOf is the host port a created container was asked to publish on.
func hostPortOf(inspect *containerReport) string {
	if len(inspect.Bindings) == 0 {
		return ""
	}

	return inspect.Bindings[0].HostPort
}

// selectedPorts are the host ports every create asked for, in order.
func selectedPorts(t *testing.T, fake *fakeEngine) []string {
	t.Helper()

	var ports []string

	for _, argv := range fake.verbCalls("create") {
		for _, value := range flagValues(argv, "-p") {
			host, port, found := strings.Cut(strings.TrimPrefix(value, loopbackHost+":"), ":")
			if !found || port != strconv.Itoa(publishedPort)+"/tcp" {
				t.Fatalf("create published %q, not a host port for %d", value, publishedPort)
			}

			ports = append(ports, host)
		}
	}

	return ports
}

// TestAPublishedPortIsOnTheHostPortSelectedForIt: the create names a loopback host port the selector
// reserved, and the engine's read-back is held to exactly that port.
func TestAPublishedPortIsOnTheHostPortSelectedForIt(t *testing.T) {
	t.Parallel()

	engine, fake, spec := publishingFixture(t)

	container, err := engine.CreateContainer(t.Context(), spec)
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	ports := selectedPorts(t, fake)
	if len(ports) != 1 {
		t.Fatalf("selected %q, want one host port", ports)
	}

	port, err := strconv.ParseUint(ports[0], 10, 16)
	if err != nil || !reservedHostPort(uint16(port)) {
		t.Errorf("host port %s is not a reserved selection (%v)", ports[0], err)
	}

	if startErr := engine.Start(t.Context(), container); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	published, err := engine.Published(t.Context(), container, publishedPort)
	if err != nil || strconv.Itoa(int(published.Port())) != ports[0] {
		t.Errorf("Published = %s, %v; want the selected port %s", published, err, ports[0])
	}

	if err := engine.Remove(t.Context(), container); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if reservedHostPort(uint16(port)) {
		t.Errorf("host port %d is still reserved after its container was removed", port)
	}
}

// TestAContainerOnAHostPortNotSelectedIsRemoved: a binding on any host port but the selected one is
// not what was asked for.
func TestAContainerOnAHostPortNotSelectedIsRemoved(t *testing.T) {
	t.Parallel()

	engine, fake, spec := publishingFixture(t)
	fake.inspectHook = func(r *containerReport) { r.Bindings[0].HostPort = "8080" }

	if _, err := engine.CreateContainer(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "8080") {
		t.Errorf("CreateContainer = %v, want a failure naming host port 8080", err)
	}

	if len(fake.verbCalls(removeVerb)) == 0 {
		t.Error("the container on the wrong host port was left in place")
	}
}

// TestAStartRefusedForItsHostPortIsRetriedOnAnother: the refused container is removed, a new one is
// created on another host port, what was copied into the first is copied into it, and it starts. The
// published port is read after Start: the one chosen at create is gone with its container.
func TestAStartRefusedForItsHostPortIsRetriedOnAnother(t *testing.T) {
	t.Parallel()

	engine, fake, spec := publishingFixture(t)

	starts := 0
	fake.refuseStart = func(r *containerReport) string {
		starts++
		if starts == 1 {
			return fmt.Sprintf(refusalLines[0], hostPortOf(r))
		}

		return ""
	}

	container, err := engine.CreateContainer(t.Context(), spec)
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	refusedID := container.ID()

	token := []File{{Path: "token", Data: []byte("kept")}}
	if copyErr := engine.CopyIn(t.Context(), container, "/etc", token); copyErr != nil {
		t.Fatalf("CopyIn: %v", copyErr)
	}

	copied := fake.find(ResourceContainer, refusedID).copied

	if startErr := engine.Start(t.Context(), container); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	if container.ID() == refusedID || fake.find(ResourceContainer, refusedID) != nil {
		t.Errorf("the refused container %s is still the one in use or still on the engine", refusedID)
	}

	out, err := engine.CopyOut(t.Context(), container, "/etc/token")
	if err != nil {
		t.Fatalf("CopyOut: %v", err)
	}

	defer func() { _ = out.Close() }()

	replayed, err := io.ReadAll(out)
	if err != nil || !bytes.Equal(replayed, copied) {
		t.Error("the re-created container does not hold what was copied into the refused one")
	}

	ports := selectedPorts(t, fake)
	published, err := engine.Published(t.Context(), container, publishedPort)

	if len(ports) != 2 || ports[0] == ports[1] || err != nil || strconv.Itoa(int(published.Port())) != ports[1] {
		t.Errorf("selected %q and published %s (%v); want a second, different port, read after Start", ports,
			published, err)
	}
}

// TestAStartRefusedOnEveryHostPortEndsNamingEachOne: past the bound the start fails loudly, naming
// every port tried and what the engine said, and never loops.
func TestAStartRefusedOnEveryHostPortEndsNamingEachOne(t *testing.T) {
	t.Parallel()

	engine, fake, spec := publishingFixture(t)
	fake.refuseStart = func(r *containerReport) string { return fmt.Sprintf(refusalLines[1], hostPortOf(r)) }

	container, err := engine.CreateContainer(t.Context(), spec)
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	err = engine.Start(t.Context(), container)
	if !errors.Is(err, ErrPortTaken) {
		t.Fatalf("Start = %v, want ErrPortTaken", err)
	}

	ports := selectedPorts(t, fake)
	if len(ports) != startAttempts {
		t.Errorf("tried %d host ports, want %d", len(ports), startAttempts)
	}

	for _, port := range ports {
		if !strings.Contains(err.Error(), port) {
			t.Errorf("ErrPortTaken does not name host port %s: %v", port, err)
		}
	}

	if !strings.Contains(err.Error(), "port is already allocated") {
		t.Errorf("ErrPortTaken does not quote the engine: %v", err)
	}

	t.Logf("%v", err)
}

// TestARecreatedContainerIsVerifiedLikeAnyOther: the re-create is a create, ledgered and verified in
// full, so a replacement the engine did not make as asked is removed before it ever starts.
func TestARecreatedContainerIsVerifiedLikeAnyOther(t *testing.T) {
	t.Parallel()

	engine, fake, spec := publishingFixture(t)

	creates := 0
	fake.inspectHook = func(r *containerReport) {
		creates++
		if creates > 1 {
			r.Privileged = true
		}
	}

	starts := 0
	fake.refuseStart = func(r *containerReport) string {
		starts++
		if starts == 1 {
			return fmt.Sprintf(refusalLines[0], hostPortOf(r))
		}

		return ""
	}

	container, err := engine.CreateContainer(t.Context(), spec)
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	err = engine.Start(t.Context(), container)
	if err == nil || !strings.Contains(err.Error(), "privileged") {
		t.Fatalf("Start = %v, want the replacement refused as privileged", err)
	}

	if starts != 1 {
		t.Errorf("%d starts, want only the refused one: the replacement ran", starts)
	}

	if removed := len(fake.verbCalls(removeVerb)); removed != 2 {
		t.Errorf("%d removals, want the refused container and its unverified replacement", removed)
	}
}
