package compose

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// NamesFrom returns every name viewer can dial dep by in the user's own project: over each model
// network both join, dep's service name, its container names, its hostname and its aliases on
// those networks, plus viewer's own `links` aliases for it. Sorted, unique; empty when the two
// share no network — a name that never resolved in the user's world is not one.
func NamesFrom(m *Model, dep, viewer string) []string {
	depService, ok := m.typed.Services[dep]
	if !ok {
		return nil
	}

	viewing, ok := m.typed.Services[viewer]
	if !ok {
		return nil
	}

	shared := sharedNetworks(depService, viewing)
	if len(shared) == 0 {
		return nil
	}

	names := append([]string{dep}, m.containerNames(dep)...)
	if depService.Hostname != "" {
		names = append(names, depService.Hostname)
	}

	for _, network := range shared {
		if attach := depService.Networks[network]; attach != nil {
			names = append(names, attach.Aliases...)
		}
	}

	names = append(names, linkAliases(viewing.Links, dep)...)
	slices.Sort(names)

	return slices.Compact(names)
}

// sharedNetworks returns the model networks both services join. A `network_mode` service joins
// none.
func sharedNetworks(a, b *composeService) []string {
	if a.NetworkMode != "" || b.NetworkMode != "" {
		return nil
	}

	var shared []string

	for _, network := range slices.Sorted(maps.Keys(a.Networks)) {
		if _, ok := b.Networks[network]; ok {
			shared = append(shared, network)
		}
	}

	return shared
}

// containerNames returns a service's container_name, else compose's default name for each replica.
func (m *Model) containerNames(service string) []string {
	svc := m.typed.Services[service]
	if svc.ContainerName != "" {
		return []string{svc.ContainerName}
	}

	replicas := int64(1)

	switch {
	case svc.Deploy != nil && svc.Deploy.Replicas != nil:
		replicas = int64(*svc.Deploy.Replicas)
	case svc.Scale != nil:
		replicas = int64(*svc.Scale)
	default:
	}

	names := make([]string, 0, max(replicas, 0))
	for n := int64(1); n <= replicas; n++ {
		names = append(names, m.typed.Name+"-"+service+"-"+strconv.FormatInt(n, 10))
	}

	return names
}

// linkAliases returns the aliases a `links` list gives service: `service[:alias]` entries.
func linkAliases(links []string, service string) []string {
	var aliases []string

	for _, link := range links {
		name, alias, found := strings.Cut(link, ":")
		if name == service && found && alias != "" {
			aliases = append(aliases, alias)
		}
	}

	return aliases
}

// externalLinkNames returns the names a service's `external_links` give: each alias, else the
// container name.
func externalLinkNames(svc *composeService) []string {
	names := make([]string, 0, len(svc.ExternalLinks))

	for _, link := range svc.ExternalLinks {
		container, alias, found := strings.Cut(link, ":")
		if found && alias != "" {
			names = append(names, alias)
		} else {
			names = append(names, container)
		}
	}

	return names
}

// selfAliases returns the names the target answers to: its service name, its container name and
// its aliases on every network it joins.
func (m *Model) selfAliases() []string {
	svc, ok := m.typed.Services[m.service]
	if !ok {
		return nil
	}

	names := append([]string{m.service}, m.containerNames(m.service)...)

	for _, attach := range svc.Networks {
		if attach != nil {
			names = append(names, attach.Aliases...)
		}
	}

	slices.Sort(names)

	return slices.Compact(names)
}

// targetViews returns, for each of services, the names the target dials it by.
func (m *Model) targetViews(services []string) map[string][]string {
	views := make(map[string][]string, len(services))
	for _, service := range services {
		views[service] = NamesFrom(m, service, m.service)
	}

	return views
}

// dependencyNames returns the names the viewers other than dep dial dep by.
func (m *Model) dependencyNames(dep string, viewers []string) []string {
	var names []string

	for _, viewer := range viewers {
		if viewer != dep {
			names = append(names, NamesFrom(m, dep, viewer)...)
		}
	}

	slices.Sort(names)

	return slices.Compact(names)
}

// viewCollision refuses a name the target could dial two services by, or one service by while it
// is also one of the target's own names: the relay could answer for only one of them. Two bus
// services sharing a name are one bus.
func viewCollision(target string, views map[string][]string, self []string, bus map[string]bool) error {
	owner := make(map[string]string, len(self))
	for _, name := range self {
		owner[name] = target
	}

	for _, service := range slices.Sorted(maps.Keys(views)) {
		for _, name := range views[service] {
			claimant, claimed := owner[name]

			switch {
			case !claimed:
				owner[name] = service
			case claimant != target && bus[claimant] && bus[service]:
			default:
				return collision(name, claimant, service)
			}
		}
	}

	return nil
}

// networkCollision refuses a name two started dependencies are both dialled by on the dependency
// network.
func networkCollision(sets map[string][]string) error {
	return viewCollision("", sets, nil, nil)
}

func collision(name, first, second string) error {
	return fmt.Errorf("%w: the name %q is claimed by both %s and %s", ErrModel, name, first, second)
}
