package provision

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// errNotAsAsked means a created container differs from what its create asked for in a way that
// matters: it is removed before it ever starts.
var errNotAsAsked = errors.New("the created container is not what was asked for")

// expectation is what a created container must be, beyond carrying this check's labels.
type expectation struct {
	// binds are the bind sources the gate validated.
	binds map[string]bool
	// named maps each named volume the spec mounts to whether it was asked read-only.
	named map[string]bool
	// anonymous are the anonymous volumes found carrying this check's labels.
	anonymous map[string]bool
	// networks are this check's networks, by name.
	networks  map[string]bool
	noNetwork bool
}

// verifyContainer is the post-create verification, over what the engine reports: restart, log
// driver and removal settings; no privilege, device, borrowed volume, refused capability or shared
// namespace; only storage the check owns, with every bind recursively read-only; only the check's
// networks; ports only on loopback with an engine-assigned host port.
func verifyContainer(r containerReport, x expectation) error {
	for _, check := range []func() error{
		func() error { return verifySettings(r) },
		func() error { return verifyPrivileges(r) },
		func() error { return verifyMounts(r, x) },
		func() error { return verifyNetworks(r, x) },
		func() error { return verifyPorts(r) },
	} {
		if err := check(); err != nil {
			return err
		}
	}

	return nil
}

func verifySettings(r containerReport) error {
	switch {
	case r.Restart != "no":
		return fmt.Errorf("%w: restart policy %q, not no", errNotAsAsked, r.Restart)
	case r.AutoRemove:
		return fmt.Errorf("%w: it removes itself", errNotAsAsked)
	case r.LogDriver != "local":
		return fmt.Errorf("%w: log driver %q, not local", errNotAsAsked, r.LogDriver)
	default:
		return nil
	}
}

func verifyPrivileges(r containerReport) error {
	switch {
	case r.Privileged:
		return fmt.Errorf("%w: it is privileged", errNotAsAsked)
	case len(r.Devices) > 0:
		return fmt.Errorf("%w: device %s is mapped", errNotAsAsked, r.Devices[0])
	case len(r.DeviceCgroupRules) > 0:
		return fmt.Errorf("%w: it carries device cgroup rules", errNotAsAsked)
	case len(r.VolumesFrom) > 0:
		return fmt.Errorf("%w: it takes volumes from %s", errNotAsAsked, r.VolumesFrom[0])
	}

	for _, capability := range r.CapAdd {
		if compose.RefusedCapability(capability) {
			return fmt.Errorf("%w: it holds capability %s", errNotAsAsked, capability)
		}
	}

	// The cgroup namespace's default differs by host; the others' do not.
	if key, shared := compose.SharedNamespace(r.PidMode, r.IpcMode, r.UTSMode, r.UsernsMode, ""); shared {
		return fmt.Errorf("%w: it shares the %s namespace", errNotAsAsked, key)
	}

	return nil
}

func verifyMounts(r containerReport, x expectation) error {
	for _, m := range r.Mounts {
		switch m.Type {
		case mountBind:
			if m.RW || !r.recursive(m.Destination) || !x.binds[filepath.Clean(m.Source)] {
				return fmt.Errorf("%w: bind %s at %s is not a recursively read-only validated source",
					errNotAsAsked, m.Source, m.Destination)
			}
		case mountVolume:
			if err := verifyVolume(m, x); err != nil {
				return err
			}
		case mountTmpfs:
		default:
			return fmt.Errorf("%w: mount type %q at %s", errNotAsAsked, m.Type, m.Destination)
		}
	}

	return nil
}

func verifyVolume(m mountReport, x expectation) error {
	if readOnly, named := x.named[m.Name]; named {
		if m.RW == readOnly {
			return fmt.Errorf("%w: volume %s at %s is not mounted as asked (read-only %v)", errNotAsAsked, m.Name,
				m.Destination, readOnly)
		}

		return nil
	}

	if !x.anonymous[m.Name] {
		return fmt.Errorf("%w: at %s", ErrUnlabelledVolume, m.Destination)
	}

	return nil
}

func verifyNetworks(r containerReport, x expectation) error {
	if x.noNetwork {
		if len(r.Networks) != 1 || r.Networks[0].Name != "none" {
			return fmt.Errorf("%w: a container with no network is attached to %v", errNotAsAsked, r.Networks)
		}

		return nil
	}

	for _, n := range r.Networks {
		if !x.networks[n.Name] {
			return fmt.Errorf("%w: network %s is not this check's", errNotAsAsked, n.Name)
		}
	}

	return nil
}

func verifyPorts(r containerReport) error {
	if r.PublishAll {
		return fmt.Errorf("%w: it publishes every port", errNotAsAsked)
	}

	for _, b := range r.Bindings {
		if b.HostIP != "127.0.0.1" || b.HostPort != "" {
			return fmt.Errorf("%w: port %s is bound on %s:%s, not loopback with an engine-assigned port",
				errNotAsAsked, b.Port, b.HostIP, b.HostPort)
		}
	}

	return nil
}
