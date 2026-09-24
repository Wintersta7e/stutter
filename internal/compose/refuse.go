package compose

import (
	"cmp"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
)

// BindSources returns every host path those services mount: bind sources and `file:` config and
// secret sources. Deduplicated, sorted — the paths the start and end fingerprints walk.
func (m *Model) BindSources(services []string) []string {
	var sources []string

	for _, name := range services {
		svc, ok := m.typed.Services[name]
		if !ok {
			continue
		}

		for _, volume := range svc.Volumes {
			if volume.Type == "bind" && volume.Source != "" {
				sources = append(sources, volume.Source)
			}
		}

		for _, file := range m.usedFiles(svc) {
			if file.def.File != "" {
				sources = append(sources, file.def.File)
			}
		}
	}

	slices.Sort(sources)

	return slices.Compact(sources)
}

// Refusals refuses the first key, in service then key order, that asks for something no container
// Stutter creates from those services may have (classes K1–K13). labelNS is the label namespace
// Stutter's own resources carry; prints is the fingerprint of the services' BindSources. It runs
// before the first thing is created.
func (m *Model) Refusals(started []string, labelNS string, prints Prints) error {
	for _, name := range slices.Sorted(slices.Values(started)) {
		svc, ok := m.typed.Services[name]
		if !ok {
			continue
		}

		raw := m.rawService(name)
		found := slices.Concat(
			privilegeRefusals(svc),
			presenceRefusals(raw),
			m.mountRefusals(svc, prints),
			m.fileRefusals(svc, prints),
			labelRefusals(svc, raw, labelNS),
		)

		if len(found) > 0 {
			first := slices.MinFunc(found, func(a, b *Refusal) int { return cmp.Compare(a.Key, b.Key) })
			first.Service = name

			return first
		}
	}

	return nil
}

// rawService returns a service as compose printed it.
func (m *Model) rawService(name string) map[string]any {
	services, ok := m.root["services"].(map[string]any)
	if !ok {
		return nil
	}

	body, ok := services[name].(map[string]any)
	if !ok {
		return nil
	}

	return body
}

// present reports a key that is set to something: not absent, null, or an empty list or map.
func present(body map[string]any, keys ...string) bool {
	var value any = body

	for _, key := range keys {
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}

		value = object[key]
	}

	switch typed := value.(type) {
	case nil:
		return false
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	case string:
		return typed != ""
	default:
		return true
	}
}

// privilegeRefusals covers privilege, capabilities, network mode and namespaces (K1–K4).
func privilegeRefusals(svc *composeService) []*Refusal {
	var found []*Refusal

	if svc.Privileged {
		found = append(found, &Refusal{Key: "privileged", Class: K1})
	}

	if slices.ContainsFunc(svc.CapAdd, RefusedCapability) {
		found = append(found, &Refusal{Key: "cap_add", Class: K2})
	}

	if mode := svc.NetworkMode; mode == "none" || mode == "host" ||
		strings.HasPrefix(mode, "container:") || strings.HasPrefix(mode, "service:") {
		found = append(found, &Refusal{Key: keyNetworkMode, Class: K3})
	}

	if key, shared := SharedNamespace(svc.Pid, svc.IPC, svc.UTS, svc.UsernsMode, svc.Cgroup); shared {
		found = append(found, &Refusal{Key: key, Class: K4})
	}

	if svc.Runtime != "" {
		found = append(found, &Refusal{Key: "runtime", Class: K6})
	}

	if svc.Isolation != "" && svc.Isolation != "default" {
		found = append(found, &Refusal{Key: "isolation", Class: K6})
	}

	if svc.UseAPISocket {
		found = append(found, &Refusal{Key: "use_api_socket", Class: K7})
	}

	return found
}

// refusedWhenSet are the keys refused whatever they are set to, by their path in the model.
//
//nolint:gochecknoglobals // a fixed table, not mutable state.
var refusedWhenSet = []struct {
	path  []string
	class Class
}{
	{path: []string{keyDevices}, class: K5},
	{path: []string{"device_cgroup_rules"}, class: K5},
	{path: []string{"gpus"}, class: K5},
	{path: []string{keyDeploy, "resources", "reservations", keyDevices}, class: K5},
	{path: []string{keyDeploy, "resources", "reservations", "generic_resources"}, class: K5},
	{path: []string{"credential_spec"}, class: K6},
	{path: []string{"volumes_from"}, class: K7},
	{path: []string{"provider"}, class: K7},
	{path: []string{keyModels}, class: K7},
	{path: []string{"pre_start"}, class: K8},
	{path: []string{"post_start"}, class: K8},
	{path: []string{"pre_stop"}, class: K8},
}

// presenceRefusals covers devices, credential specs, other containers' resources and lifecycle
// hooks (K5–K8): keys refused whatever they are set to.
func presenceRefusals(raw map[string]any) []*Refusal {
	var found []*Refusal

	for _, entry := range refusedWhenSet {
		if present(raw, entry.path...) {
			found = append(found, &Refusal{Key: strings.Join(entry.path, "."), Class: entry.class})
		}
	}

	return found
}

// mountTarget is one path something is mounted at, with the key that mounts it.
type mountTarget struct {
	key    string
	target string
}

// mountTargets returns every mount target of a service: `volumes` entries and `tmpfs` paths.
func mountTargets(svc *composeService) []mountTarget {
	targets := make([]mountTarget, 0, len(svc.Volumes)+len(svc.Tmpfs))

	for index, volume := range svc.Volumes {
		targets = append(targets, mountTarget{key: "volumes[" + strconv.Itoa(index) + "]", target: volume.Target})
	}

	for index, entry := range svc.Tmpfs {
		target, _, _ := strings.Cut(entry, ":")
		targets = append(targets, mountTarget{key: "tmpfs[" + strconv.Itoa(index) + "]", target: target})
	}

	return targets
}

// mountRefusals covers mount types and options (K9), bind sources (K10) and mounts at or above the
// CA path (K13).
func (m *Model) mountRefusals(svc *composeService, prints Prints) []*Refusal {
	var found []*Refusal

	for index, volume := range svc.Volumes {
		if refusal := m.volumeRefusal("volumes["+strconv.Itoa(index)+"]", volume); refusal != nil {
			found = append(found, refusal)
		}

		if volume.Type == "bind" {
			found = append(found, sourceRefusal(volume.Source, prints)...)
		}
	}

	for _, mount := range mountTargets(svc) {
		key := mount.key
		if strings.HasPrefix(key, "volumes[") {
			key += ".target"
		}

		if coversCA(mount.target) {
			found = append(found, &Refusal{Key: key, Class: K13})
		}
	}

	return found
}

// volumeRefusal refuses a mount type or option Stutter cannot make safe (K9).
func (m *Model) volumeRefusal(key string, volume serviceVolume) *Refusal {
	switch {
	case volume.Type == "cluster" || volume.Type == "npipe" || volume.Type == "image":
		return &Refusal{Key: key + ".type", Class: K9}
	case volume.Volume != nil && volume.Volume.Subpath != "":
		return &Refusal{Key: key + ".volume.subpath", Class: K9}
	case volume.Bind != nil && volume.Bind.SELinux != "":
		return &Refusal{Key: key + ".bind.selinux", Class: K9}
	case volume.Type == "volume" && m.externalVolume(volume.Source):
		return &Refusal{Key: "volumes." + volume.Source + ".external", Class: K9}
	default:
		return nil
	}
}

func (m *Model) externalVolume(name string) bool {
	def := m.typed.Volumes[name]

	return def != nil && declaredExternal(def.External)
}

// declaredExternal reads an `external` value: `true`, or a map naming the external resource.
func declaredExternal(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	default:
		return true
	}
}

func sourceRefusal(source string, prints Prints) []*Refusal {
	if err := CheckSource(source, prints); err != nil {
		return []*Refusal{{Key: source, Class: K10}}
	}

	return nil
}

// coversCA reports a container path equal to the CA's mount path or above it: a mount there would
// hide the CA, and a copy there would replace it.
func coversCA(target string) bool {
	cleaned := path.Clean(target)

	return cleaned == CAMountPath || cleaned == "/" || strings.HasPrefix(CAMountPath, cleaned+"/")
}

// serviceFileRef is one config or secret a service uses, with its top-level definition.
type serviceFileRef struct {
	def    *topFile
	kind   string
	source string
	target string
}

// usedFiles returns every config and secret a service uses, each target defaulted as compose defaults
// it: `/<source>` for a config, `/run/secrets/<source>` for a secret.
func (m *Model) usedFiles(svc *composeService) []serviceFileRef {
	refs := make([]serviceFileRef, 0, len(svc.Configs)+len(svc.Secrets))

	for _, set := range []struct {
		defs   map[string]*topFile
		kind   string
		prefix string
		used   []serviceFile
	}{
		{defs: m.typed.Configs, kind: "configs", prefix: "/", used: svc.Configs},
		{defs: m.typed.Secrets, kind: "secrets", prefix: "/run/secrets/", used: svc.Secrets},
	} {
		for _, use := range set.used {
			def := set.defs[use.Source]
			if def == nil {
				def = &topFile{}
			}

			target := use.Target
			if target == "" {
				target = set.prefix + use.Source
			}

			refs = append(refs, serviceFileRef{def: def, kind: set.kind, source: use.Source, target: target})
		}
	}

	return refs
}

// fileRefusals covers configs and secrets: a copy Stutter cannot place faithfully or a definition
// Stutter cannot resolve (K11), a `file:` source (K10), and a target at or above the CA path (K13).
func (m *Model) fileRefusals(svc *composeService, prints Prints) []*Refusal {
	var found []*Refusal

	for _, file := range m.usedFiles(svc) {
		name := file.kind + "." + file.source

		for key, set := range map[string]bool{
			"external": declaredExternal(file.def.External), "driver": file.def.Driver != "",
			"template_driver": file.def.TemplateDriver != "",
		} {
			if set {
				found = append(found, &Refusal{Key: name + "." + key, Class: K11})
			}
		}

		if coversCA(file.target) {
			found = append(found, &Refusal{Key: name + ".target", Class: K13})
		}

		if file.def.File != "" {
			found = append(found, sourceRefusal(file.def.File, prints)...)
		}

		if file.def.Content == nil && file.def.Environment == "" {
			continue
		}

		if svc.ReadOnly {
			found = append(found, &Refusal{Key: name + " with read_only", Class: K11})
		}

		for _, mount := range mountTargets(svc) {
			if under(file.target, mount.target) {
				found = append(found, &Refusal{Key: name + ".target under " + mount.key, Class: K11})
			}
		}
	}

	return found
}

// under reports a container path at or beneath a mount target.
func under(target, mount string) bool {
	target, mount = path.Clean(target), path.Clean(mount)

	return target == mount || mount == "/" || strings.HasPrefix(target, mount+"/")
}

// labelRefusals covers labels in Stutter's own namespace, on the service or its build (K12).
func labelRefusals(svc *composeService, raw map[string]any, labelNS string) []*Refusal {
	if labelNS == "" {
		return nil
	}

	var found []*Refusal

	var buildLabels map[string]any

	if build, ok := raw["build"].(map[string]any); ok {
		if labels, ok := build["labels"].(map[string]any); ok {
			buildLabels = labels
		}
	}

	for prefix, keys := range map[string][]string{
		"labels":       slices.Collect(maps.Keys(svc.Labels)),
		"build.labels": slices.Collect(maps.Keys(buildLabels)),
	} {
		for _, key := range keys {
			if strings.HasPrefix(key, labelNS+".") {
				found = append(found, &Refusal{Key: prefix + "." + key, Class: K12})
			}
		}
	}

	return found
}
