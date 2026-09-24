package compose

import (
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"
)

// Audit compares a created container, as the engine reports it, with the spec it was created from.
// It returns the properties checked and the properties that differ; any difference is an error
// wrapping ErrAudit naming each property and key — never a value. The expectations come from the
// compose environment, the image and Stutter's constants, not from the values Spec computed, so a
// defect in Spec is caught here rather than trusted.
//
//nolint:nonamedreturns // two counts of the same type are told apart by their names alone.
func Audit(s Spec, img Image, got Inspected) (checked, differ int, err error) {
	var a auditor

	a.row("image", "", got.Image == img.ID)
	a.environment(s, img, got.Env)
	a.argv(s, img, got)
	a.row("user", "", got.User == s.User)
	a.row("working_dir", "", got.WorkingDir == s.WorkingDir)
	a.row("hostname", "", got.Hostname == s.Hostname)
	a.row("domainname", "", got.Domainname == s.Domainname)
	a.mounts(s, got.Mounts)
	a.fixed(got)
	a.labels(img, got.Labels)

	if len(a.rows) == 0 {
		return 0, 0, fmt.Errorf("%w: service %s: nothing was checked", ErrAudit, s.Service)
	}

	var differences []string

	for _, row := range a.rows {
		if !row.matches {
			differences = append(differences, strings.TrimSpace(row.property+" "+row.key))
		}
	}

	if len(differences) > 0 {
		return len(a.rows), len(differences), fmt.Errorf("%w: service %s: %s", ErrAudit, s.Service,
			strings.Join(differences, ", "))
	}

	return len(a.rows), 0, nil
}

// auditRow is one property checked, with its key when the property has several.
type auditRow struct {
	property string
	key      string
	matches  bool
}

// auditor collects the properties checked.
type auditor struct {
	rows []auditRow
}

func (a *auditor) row(property, key string, matches bool) {
	a.rows = append(a.rows, auditRow{property: property, key: key, matches: matches})
}

// environment checks `Config.Env`: the image's environment overlaid by compose's, unescaped; no
// proxy variable with a value, a bare unset entry allowed; the CA variables with Stutter's value
// on a target container; nothing else.
func (a *auditor) environment(s Spec, img Image, entries []string) {
	want := imageEnvironment(img)
	for key, value := range s.raw {
		want[key] = unescape(value)
	}

	got := map[string]string{}
	bare := map[string]bool{}

	for _, entry := range entries {
		key, value, set := strings.Cut(entry, "=")
		if set {
			got[key] = value
		} else {
			bare[key] = true
		}
	}

	a.fixedEnvironment(s, want, got)

	for _, key := range slices.Sorted(maps.Keys(want)) {
		value, ok := got[key]
		a.row("env", key, ok && value == want[key])
	}

	for _, key := range slices.Sorted(maps.Keys(got)) {
		if _, ok := want[key]; !ok {
			a.row("env", key, false)
		}
	}

	for _, key := range slices.Sorted(maps.Keys(bare)) {
		if !slices.Contains(proxyVariables, key) {
			a.row("env", key, false)
		}
	}
}

// fixedEnvironment checks the variables Stutter decides whatever compose says — no proxy variable
// with a value; the CA variables on a target container — and takes them out of want and got.
func (a *auditor) fixedEnvironment(s Spec, want, got map[string]string) {
	for _, name := range proxyVariables {
		_, valued := got[name]
		a.row("env", name, !valued)

		delete(want, name)
		delete(got, name)
	}

	if !s.target {
		return
	}

	for _, name := range CAVariables() {
		a.row("env", name, got[name] == CAMountPath)

		delete(want, name)
		delete(got, name)
	}
}

// argv checks the process's whole argument vector, not the entrypoint field: an entrypoint of more
// than one element is created split across the entrypoint and the command.
func (a *auditor) argv(s Spec, img Image, got Inspected) {
	want := img.Entrypoint
	if s.EntrypointSet {
		want = s.Entrypoint
	}

	switch {
	case s.CmdSet:
		want = slices.Concat(want, s.Cmd)
	case !s.EntrypointSet:
		want = slices.Concat(want, img.Cmd)
	default:
	}

	a.row("argv", "", slices.Equal(slices.Concat(got.Entrypoint, got.Cmd), want))
}

// mounts checks the container's mounts: every bind read-only, a volume at every fresh target, a
// tmpfs at every tmpfs target, the CA bound read-only on a target container, nothing else.
func (a *auditor) mounts(s Spec, got []InspectedMount) {
	want := make(map[string]string, len(s.Mounts)+1)

	for _, mount := range s.Mounts {
		want[path.Clean(mount.Target)] = map[MountKind]string{
			MountBind: volumeTypeBind, MountFresh: volumeTypeVolume, MountTmpfs: volumeTypeTmpfs,
		}[mount.Kind]
	}

	if s.target {
		want[CAMountPath] = volumeTypeBind
	}

	seen := make(map[string]bool, len(got))

	for _, mount := range got {
		target := path.Clean(mount.Destination)
		kind, expected := want[target]
		seen[target] = true

		a.row("mounts", target, expected && kind == mount.Type && (mount.Type != volumeTypeBind || !mount.RW))
	}

	for _, target := range slices.Sorted(maps.Keys(want)) {
		if !seen[target] {
			a.row("mounts", target, false)
		}
	}
}

// fixed checks what Stutter sets on every container whatever compose asks.
func (a *auditor) fixed(got Inspected) {
	namespace, shared := SharedNamespace(got.PidMode, got.IpcMode, got.UTSMode, got.UsernsMode, got.CgroupnsMode)

	a.row("restart", "", got.RestartPolicy == "no")
	a.row("healthcheck", "", slices.Equal(got.Healthcheck, []string{"NONE"}))
	a.row("ports", "", got.PortBindings == 0)
	a.row("publish_all_ports", "", !got.PublishAllPorts)
	a.row("extra_hosts", "", len(got.ExtraHosts) == 0)
	a.row("privileged", "", !got.Privileged)
	a.row("cap_add", "", !slices.ContainsFunc(got.CapAdd, RefusedCapability))
	a.row("namespace", namespace, !shared)
	a.row("devices", "", len(got.Devices)+len(got.DeviceCgroupRules)+got.DeviceRequests == 0)
	a.row("runtime", "", got.Runtime == "" || got.Runtime == got.DefaultRuntime)
	a.row("volumes_from", "", len(got.VolumesFrom) == 0)
	a.row("log_driver", "", got.LogDriver == "local")
}

// labels checks that every compose label on the container came from the image: a container the
// user's own compose commands would take for theirs is refused, one merely built by compose is not.
func (a *auditor) labels(img Image, got map[string]string) {
	for _, key := range slices.Sorted(maps.Keys(got)) {
		if strings.HasPrefix(key, "com.docker.compose.") {
			value, inherited := img.Labels[key]
			a.row("labels", key, inherited && value == got[key])
		}
	}
}
