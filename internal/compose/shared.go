package compose

import (
	"path/filepath"
	"slices"
	"strings"
)

// sharedMounts pairs the target with each started dependency or job that mounts the same host
// source — equal, or one inside the other, compared after resolving symlinks — or the same model
// volume. Under Stutter the target reads the original read-only while the other side gets its own
// copy or fresh volume, so the channel between them is cut; the report says so.
func (c *classifier) sharedMounts(viewers []viewer) []Shared {
	targetSources := c.resolvedSources(c.model.service)
	targetVolumes := modelVolumes(c.model.typed.Services[c.model.service])

	var out []Shared

	for _, current := range viewers {
		if current.name == c.model.service {
			continue
		}

		for _, source := range c.resolvedSources(current.name) {
			for _, mine := range targetSources {
				if overlap, shared := overlapping(mine, source); shared {
					out = append(out, Shared{Service: current.name, Path: overlap})
				}
			}
		}

		for _, volume := range modelVolumes(c.model.typed.Services[current.name]) {
			if slices.Contains(targetVolumes, volume) {
				out = append(out, Shared{Service: current.name, Path: volume, Volume: true})
			}
		}
	}

	slices.SortFunc(out, func(a, b Shared) int {
		return strings.Compare(a.Service+"\x00"+a.Path, b.Service+"\x00"+b.Path)
	})

	return slices.Compact(out)
}

// resolvedSources returns a service's bind sources with symlinks resolved; one that does not
// resolve is kept as written.
func (c *classifier) resolvedSources(service string) []string {
	sources := c.model.BindSources([]string{service})

	for index, source := range sources {
		if resolved, err := filepath.EvalSymlinks(source); err == nil {
			sources[index] = resolved
		}
	}

	return sources
}

// overlapping reports two host paths that are the same or one inside the other, and the deeper.
func overlapping(first, second string) (string, bool) {
	first, second = filepath.Clean(first), filepath.Clean(second)

	switch {
	case first == second:
		return first, true
	case strings.HasPrefix(second, first+string(filepath.Separator)):
		return second, true
	case strings.HasPrefix(first, second+string(filepath.Separator)):
		return first, true
	default:
		return "", false
	}
}

// modelVolumes returns the named model volumes a service mounts.
func modelVolumes(svc *composeService) []string {
	var out []string

	for _, volume := range svc.Volumes {
		if volume.Type == volumeTypeVolume && volume.Source != "" {
			out = append(out, volume.Source)
		}
	}

	return out
}
