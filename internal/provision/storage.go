package provision

import (
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// Where a dependency's writable paths land, decided by its path before anything is created. A commit
// silently omits whatever a volume holds, so every writable path of a seed lands either in the
// container layer the snapshot commits, on a tmpfs nothing keeps, or on a template volume the check
// owns and every restore copies.

// writableBind is how the compose model records a bind it declared writable and Stutter mounts
// read-only: the key of the `volumes` entry, and this text.
const writableBind = "mounted read-only"

// errStorage means a service's mounts cannot be planned.
var errStorage = errors.New("cannot plan a dependency's storage")

// storageInput is what one service's storage is planned from.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type storageInput struct {
	// isDir reports whether a host path is a directory; a bind of one is copied, a bind of a file is not.
	isDir func(path string) (bool, error)
	// shared are the model volumes two started dependencies mount: each gets a private template.
	shared map[string]bool
	// imageVolumes are the image's VOLUME paths.
	imageVolumes []string
	// spec is the service's container spec with only compose's own mounts: built against an image
	// with no VOLUME, so every mount in it is one compose declared.
	spec compose.Spec
}

// templateMount is one path backed by a template volume.
type templateMount struct {
	// target is the path in the container.
	target string
	// volume is the model volume it replaces; empty for an anonymous one or an image VOLUME.
	volume string
	// copyFrom is a host directory copied into it once per check, for a writable directory bind.
	copyFrom string
	// noCopy mounts it without the image's content: compose's volume.nocopy, and every restore.
	noCopy bool
}

// storagePlan is where each writable path of one container lands.
type storagePlan struct {
	// mounts are the mounts the container gets as they are: tmpfs, and binds, every one read-only.
	mounts []compose.Mount
	// templates are the paths backed by template volumes.
	templates []templateMount
	// verdicts are the seed record's `<target> <verdict>` entries.
	verdicts []string
	// pgdata moves the cluster to pgdataPath.
	pgdata bool
}

// seedPlan plans a seed's storage on its path.
func seedPlan(in storageInput, onPath string) (storagePlan, error) {
	writable, err := writableBinds(in.spec)
	if err != nil {
		return storagePlan{}, err
	}

	if onPath == PathPostgres {
		return postgresPlan(in, writable), nil
	}

	return otherPlan(in, writable)
}

// postgresPlan: each image VOLUME is a tmpfs, so no volume is ever made for one; a compose mount at or
// under a VOLUME or a compose-set PGDATA is dropped, since the cluster no longer lives there; every
// other compose volume is a template; every bind is read-only.
func postgresPlan(in storageInput, writable map[int]bool) storagePlan {
	plan := storagePlan{pgdata: true}

	dropped := slices.Clone(in.imageVolumes)

	if composed := in.spec.Env["PGDATA"]; composed != "" {
		plan.verdicts = append(plan.verdicts, "PGDATA replaced")
		dropped = append(dropped, composed)
	}

	for _, volume := range in.imageVolumes {
		plan.mounts = append(plan.mounts, compose.Mount{Kind: compose.MountTmpfs, Target: path.Clean(volume)})
		plan.verdicts = append(plan.verdicts, path.Clean(volume)+" tmpfs")
	}

	for index, m := range in.spec.Mounts {
		switch {
		case underAny(m.Target, dropped):
			plan.verdicts = append(plan.verdicts, m.Target+" dropped")
		case m.Kind == compose.MountFresh:
			plan.addTemplate(in, templateMount{target: m.Target, volume: m.Volume, noCopy: m.NoCopy})
		case writable[index]:
			plan.keepReadOnly(m)
		default:
			plan.keep(m)
		}
	}

	return plan
}

// otherPlan: each image VOLUME and compose volume is a template; a writable directory bind is copied
// into one; a writable file bind is read-only, since a volume cannot mount a file; tmpfs is kept.
func otherPlan(in storageInput, writable map[int]bool) (storagePlan, error) {
	var plan storagePlan

	for _, volume := range uncoveredVolumes(in) {
		plan.addTemplate(in, templateMount{target: volume})
	}

	for index, m := range in.spec.Mounts {
		switch {
		case m.Kind == compose.MountFresh:
			plan.addTemplate(in, templateMount{target: m.Target, volume: m.Volume, noCopy: m.NoCopy})
		case m.Kind == compose.MountBind && writable[index]:
			dir, err := in.isDir(m.Source)
			if err != nil {
				return storagePlan{}, fmt.Errorf("%w: the bind at %s: %w", errStorage, m.Target, err)
			}

			if !dir {
				plan.keepReadOnly(m)

				continue
			}

			plan.templates = append(plan.templates, templateMount{target: m.Target, copyFrom: m.Source})
			plan.verdicts = append(plan.verdicts, m.Target+" copied")
		default:
			plan.keep(m)
		}
	}

	return plan, nil
}

// classificationPlan keeps a classification container off every volume: a tmpfs wherever the other
// path would put a template — every image VOLUME, compose volume and writable directory bind — and
// file binds read-only. It runs once, only to be asked a question, and leaves nothing behind.
func classificationPlan(in storageInput) (storagePlan, error) {
	writable, err := writableBinds(in.spec)
	if err != nil {
		return storagePlan{}, err
	}

	seed, err := otherPlan(in, writable)
	if err != nil {
		return storagePlan{}, err
	}

	plan := storagePlan{mounts: seed.mounts}
	for _, tmpl := range seed.templates {
		plan.mounts = append(plan.mounts, compose.Mount{Kind: compose.MountTmpfs, Target: tmpl.target})
	}

	return plan, nil
}

// jobVolumes mounts a job's model volume that a started dependency holds as a template as that
// template, at the job's own target: what the job writes there is what the snapshot holds. Every other
// mount keeps its verdict.
func jobVolumes(job compose.Spec, templates map[string]*Volume) ([]compose.Mount, []VolumeMount) {
	var (
		mounts  []compose.Mount
		volumes []VolumeMount
	)

	for _, m := range job.Mounts {
		if tmpl, ok := templates[m.Volume]; ok && m.Kind == compose.MountFresh && m.Volume != "" {
			volumes = append(volumes, VolumeMount{
				Volume: tmpl, Target: m.Target, NoCopy: m.NoCopy, ReadOnly: m.ReadOnly,
			})

			continue
		}

		mounts = append(mounts, m)
	}

	return mounts, volumes
}

// addTemplate backs a path with a template volume, private when two started dependencies mount it.
func (p *storagePlan) addTemplate(in storageInput, tmpl templateMount) {
	p.templates = append(p.templates, tmpl)

	verdict := " template"
	if tmpl.volume != "" && in.shared[tmpl.volume] {
		verdict = " private"
	}

	p.verdicts = append(p.verdicts, tmpl.target+verdict)
}

// keep mounts a bind or tmpfs as it is. A bind is always read-only.
func (p *storagePlan) keep(m compose.Mount) {
	p.mounts = append(p.mounts, m)
}

// keepReadOnly mounts a bind compose declared writable, read-only, and names it.
func (p *storagePlan) keepReadOnly(m compose.Mount) {
	p.keep(m)
	p.verdicts = append(p.verdicts, m.Target+" read-only")
}

// uncoveredVolumes are the image's VOLUME paths no compose mount targets exactly: each needs storage of
// its own.
func uncoveredVolumes(in storageInput) []string {
	var out []string

	for _, volume := range in.imageVolumes {
		target := path.Clean(volume)
		if !slices.ContainsFunc(in.spec.Mounts, func(m compose.Mount) bool { return path.Clean(m.Target) == target }) {
			out = append(out, target)
		}
	}

	return out
}

// writableBinds are the indexes of the binds compose declared writable. The model mounts every bind
// read-only and records each writable one it overrode by its `volumes` key; the service's volumes come
// first in its mounts, one each, so the key's index is the mount's.
func writableBinds(spec compose.Spec) (map[int]bool, error) {
	writable := map[int]bool{}

	for _, replaced := range spec.Replaced {
		index, ok := volumeIndex(replaced.Key)
		if !ok || replaced.What != writableBind {
			continue
		}

		if index >= len(spec.Mounts) || spec.Mounts[index].Kind != compose.MountBind {
			return nil, fmt.Errorf("%w: service %s: %s is not a bind", errStorage, spec.Service, replaced.Key)
		}

		writable[index] = true
	}

	return writable, nil
}

// volumeIndex reads the index out of a `volumes[i]` key.
func volumeIndex(key string) (int, bool) {
	inside, ok := strings.CutPrefix(key, "volumes[")
	if !ok {
		return 0, false
	}

	inside, ok = strings.CutSuffix(inside, "]")
	if !ok {
		return 0, false
	}

	index, err := strconv.Atoi(inside)

	return index, err == nil && index >= 0
}

// underAny reports a path at or beneath any of roots.
func underAny(p string, roots []string) bool {
	cleaned := make([]string, 0, len(roots))
	for _, root := range roots {
		cleaned = append(cleaned, path.Clean(root))
	}

	return under(path.Clean(p), cleaned)
}

// healthcheckOf is the engine healthcheck a compose healthcheck asks for. A service that sets none has
// its image's; one that disables it has none. An exec-form test is passed as compose wrote it: the
// driver renders it as the one shell command the engine accepts, splitting back into the same argv.
func healthcheckOf(check compose.Healthcheck, set bool) *Healthcheck {
	switch {
	case !set:
		return &Healthcheck{Inherit: true}
	case check.Disabled:
		return nil
	default:
		return &Healthcheck{
			Test: slices.Clone(check.Test), Interval: check.Interval, Timeout: check.Timeout,
			StartPeriod: check.StartPeriod, StartInterval: check.StartInterval, Retries: check.Retries,
			Inherit: len(check.Test) == 0,
		}
	}
}

// isHostDir reports whether a host path is a directory, without following a final symlink.
func isHostDir(p string) (bool, error) {
	info, err := os.Lstat(p)
	if err != nil {
		return false, fmt.Errorf("%w: %w", errStorage, err)
	}

	return info.IsDir(), nil
}
