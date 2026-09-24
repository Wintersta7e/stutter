package topologydocker_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// inspection is one engine inspect, read by field path.
type inspection struct {
	value any
}

func inspect(t *testing.T, raw json.RawMessage) inspection {
	t.Helper()

	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode the inspect: %v", err)
	}

	return inspection{value: value}
}

// at is the value at path; absent is nil.
func (i inspection) at(path ...string) any {
	value := i.value

	for _, key := range path {
		object, isObject := value.(map[string]any)
		if !isObject {
			return nil
		}

		value = object[key]
	}

	return value
}

// list is the list at path; absent, or not a list, is empty.
func (i inspection) list(path ...string) []any {
	if listed, isList := i.at(path...).([]any); isList {
		return listed
	}

	return nil
}

// object is the object at path; absent, or not an object, is empty.
func (i inspection) object(path ...string) map[string]any {
	if found, isObject := i.at(path...).(map[string]any); isObject {
		return found
	}

	return nil
}

// is reports whether the boolean at path is present and equal to want.
func (i inspection) is(want bool, path ...string) bool {
	set, known := i.at(path...).(bool)

	return known && set == want
}

// empty reports whether a value holds nothing: absent, null, or an empty list or object.
func empty(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case []any:
		return len(typed) == 0
	case map[string]any:
		return len(typed) == 0
	default:
		return false
	}
}

// property is one thing the test reads back, what it was, and whether it is what it must be.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type property struct {
	got   any
	name  string
	holds bool
}

// TestRelayImageAndContainerReadBack reads the relay image and every relay container back from the
// engine: an image of one layer that runs the relay as an unprivileged user and carries nothing else,
// and containers with no capability, a read-only root, no new privileges, nothing mounted or
// published, and no forwarding between their two networks.
func TestRelayImageAndContainerReadBack(t *testing.T) {
	t.Parallel()

	checked := relayedRig(t, requireEngine(t), testLayout())
	image := inspect(t, checked.docker.Inspect(t, dockertest.ObjectImage, checked.image.ID))

	// The image's four, its five absent fields, and about ten for each of the three relays.
	const expected = 40

	properties := append(make([]property, 0, expected), []property{
		{name: "image User", got: image.at("Config", "User"), holds: image.at("Config", "User") == "65534:65534"},
		{
			name: "image Entrypoint", got: image.at("Config", "Entrypoint"),
			holds: fmt.Sprint(image.at("Config", "Entrypoint")) == "[/stutter relay]",
		},
		{name: "image layers", got: image.at("RootFS", "Layers"), holds: len(image.list("RootFS", "Layers")) == 1},
		{
			name: "image check label", got: image.at("Config", "Labels", rules.LabelCheck),
			holds: image.at("Config", "Labels", rules.LabelCheck) == checked.eng.CheckID(),
		},
	}...)

	for _, absent := range []string{"Cmd", "Volumes", "ExposedPorts", "Healthcheck", "Env"} {
		value := image.at("Config", absent)
		properties = append(properties, property{name: "image " + absent, got: value, holds: empty(value)})
	}

	relays := checked.relaysOf(t, checked.networks.Service.Name())
	if len(relays) != 3 {
		t.Fatalf("%d relay containers, want 3: the dependency, the bus and the stub", len(relays))
	}

	for _, each := range relays {
		properties = append(properties, relayProperties(t, each, checked)...)
	}

	for _, checkedProperty := range properties {
		if !checkedProperty.holds {
			t.Errorf("%s = %v", checkedProperty.name, checkedProperty.got)
		}
	}

	t.Logf("properties checked: %d", len(properties))
}

// relayProperties are one relay container's settings, read back.
func relayProperties(t *testing.T, each relayInfo, checked rig) []property {
	t.Helper()

	container := inspect(t, each.raw)
	isStub := each.labels[rules.LabelService] == ""

	name := "relay " + each.labels[rules.LabelService]
	if isStub {
		name = "relay stub"
	}

	sysctls := container.object("HostConfig", "Sysctls")
	properties := []property{
		{
			name: name + " CapDrop", got: container.at("HostConfig", "CapDrop"),
			holds: slices.Contains(container.list("HostConfig", "CapDrop"), "ALL"),
		},
		{
			name: name + " ReadonlyRootfs", got: container.at("HostConfig", "ReadonlyRootfs"),
			holds: container.is(true, "HostConfig", "ReadonlyRootfs"),
		},
		{
			name: name + " SecurityOpt", got: container.at("HostConfig", "SecurityOpt"),
			holds: slices.Contains(container.list("HostConfig", "SecurityOpt"), "no-new-privileges"),
		},
		{
			name: name + " restart", got: container.at("HostConfig", "RestartPolicy", "Name"),
			holds: container.at("HostConfig", "RestartPolicy", "Name") == "no",
		},
		{name: name + " mounts", got: container.at("Mounts"), holds: empty(container.at("Mounts"))},
		{
			name: name + " port bindings", got: container.at("HostConfig", "PortBindings"),
			holds: empty(container.at("HostConfig", "PortBindings")),
		},
		{
			name: name + " sysctl net.ipv4.ip_forward", got: sysctls["net.ipv4.ip_forward"],
			holds: sysctls["net.ipv4.ip_forward"] == "0",
		},
	}

	wantSysctls := 1

	if isStub {
		wantSysctls = 3

		properties = append(properties,
			property{
				name: name + " sysctl ip_local_port_range", got: sysctls["net.ipv4.ip_local_port_range"],
				holds: sysctls["net.ipv4.ip_local_port_range"] == "64512 65535",
			},
			property{
				name: name + " ready address", got: each.address,
				holds: each.address == checked.topo.Placement().DNS,
			},
		)
	}

	return append(properties, property{name: name + " sysctls", got: sysctls, holds: len(sysctls) == wantSysctls})
}
