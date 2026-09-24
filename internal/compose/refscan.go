package compose

import (
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// datastoreSchemes are the URL schemes a datastore is reached by, beside the Postgres forms.
//
//nolint:gochecknoglobals // a fixed list, not mutable state.
var datastoreSchemes = []string{"mysql", "mariadb", "mongodb", "mongodb+srv", "redis", schemeRediss}

// viewer is a started service whose configuration is scanned, with the names it can dial.
type viewer struct {
	name    string
	refs    []ref
	visible []string
}

// viewers returns the target, the jobs it depends on and every started dependency, each with its
// references and the model names it can see.
func (c *classifier) viewers(cls Classification) []viewer {
	names := []string{c.model.service}

	for _, dep := range cls.Deps {
		if (dep.Role == RoleJob && dep.InClosure) || dep.Role == RoleDatastore || dep.Role == RoleOther {
			names = append(names, dep.Service)
		}
	}

	slices.Sort(names)

	out := make([]viewer, 0, len(names))

	for _, name := range names {
		var visible []string

		for _, other := range c.model.Services() {
			if other != name {
				visible = append(visible, NamesFrom(c.model, other, name)...)
			}
		}

		out = append(out, viewer{name: name, refs: serviceReferences(c.model.typed.Services[name]), visible: visible})
	}

	return out
}

// datastoreReference reports a reference to a datastore: a Postgres form, a datastore URL scheme,
// or any `jdbc:` URL.
func datastoreReference(reference ref) bool {
	return reference.KeywordPG || pgScheme(reference.Scheme) || reference.JDBC ||
		slices.Contains(datastoreSchemes, reference.Scheme)
}

// externalDatastores refuses a datastore reference, from any started service, whose host is not a
// model name that service can see: Stutter cannot capture it, and a job would migrate it for real.
func externalDatastores(viewers []viewer) error {
	for _, current := range viewers {
		for _, reference := range current.refs {
			if datastoreReference(reference) && !slices.Contains(current.visible, reference.Host) {
				host := reference.Host
				if host == "" {
					host = "a local socket"
				}

				return fmt.Errorf("%w: service %s names a datastore at %s, outside the compose model",
					ErrModel, current.name, host)
			}
		}
	}

	return nil
}

// busIdentity refuses a model with no bus, or with more than one: hosts in one `nats://` seed list
// are one bus, and lists naming the same host or the same bus service are one too. The target's
// references are counted; when it names no bus, the model's bus services are.
func (c *classifier) busIdentity(cls Classification) error {
	var groups [][]string

	for _, reference := range c.targetRefs {
		if reference.Scheme != schemeNATS {
			continue
		}

		key := reference.Key + "#" + strconv.Itoa(reference.List)
		groups = append(groups, []string{key, busOf(cls, reference.Host)})
	}

	if len(groups) == 0 {
		for _, dep := range cls.Deps {
			if dep.Role == RoleBus {
				groups = append(groups, []string{dep.Service})
			}
		}
	}

	identities := mergeIdentities(groups)

	switch len(identities) {
	case 0:
		return fmt.Errorf("%w: no bus identity: the service names no nats:// host and the model has no bus", ErrModel)
	case 1:
		return nil
	default:
		return fmt.Errorf("%w: several bus identities: %s", ErrModel, strings.Join(identities, "; "))
	}
}

// busOf names the bus service a host reaches from the target, else the host itself.
func busOf(cls Classification, host string) string {
	for _, dep := range cls.Deps {
		if dep.Role == RoleBus && slices.Contains(dep.Names, host) {
			return "service " + dep.Service
		}
	}

	return host
}

// mergeIdentities joins groups that share a member and names each identity by its members.
func mergeIdentities(groups [][]string) []string {
	var merged [][]string

	for _, group := range groups {
		joined := slices.Clone(group)

		var kept [][]string

		for _, existing := range merged {
			if slices.ContainsFunc(existing, func(member string) bool { return slices.Contains(joined, member) }) {
				joined = append(joined, existing...)
			} else {
				kept = append(kept, existing)
			}
		}

		merged = append(kept, joined) //nolint:gocritic // kept is a fresh slice each round, never merged.
	}

	names := make([]string, 0, len(merged))
	for _, identity := range merged {
		var members []string

		for _, member := range identity {
			if !strings.Contains(member, "#") {
				members = append(members, member)
			}
		}

		slices.Sort(members)
		names = append(names, strings.Join(slices.Compact(members), ", "))
	}

	slices.Sort(names)

	return names
}

// localHost reports a host that bypasses name resolution: an IP literal, localhost or a socket.
func localHost(reference ref) bool {
	host := strings.ToLower(reference.Host)
	_, err := netip.ParseAddr(host)

	return reference.Socket || err == nil || host == "localhost" || strings.HasSuffix(host, ".localhost")
}

// disclose fills what the report discloses about hosts: the target's local hosts, the hosts jobs
// and started dependencies reach outside the model, and the single-label names the target dials
// that nothing answers to.
func (c *classifier) disclose(cls *Classification, viewers []viewer) {
	for _, reference := range c.targetRefs {
		if localHost(reference) {
			cls.Disclosed = appendHost(cls.Disclosed, Host{
				Service: c.model.service, Key: reference.Key,
				Host: reference.Host,
			})
		}
	}

	bus := slices.Concat(cls.BusNames, cls.SetupBusNames)

	for _, current := range viewers {
		if current.name == c.model.service {
			continue
		}

		for _, reference := range current.refs {
			if reference.Host != "" && !slices.Contains(current.visible, reference.Host) &&
				!slices.Contains(bus, reference.Host) {
				cls.SetupEgress = appendHost(cls.SetupEgress, Host{
					Service: current.name, Key: reference.Key,
					Host: reference.Host,
				})
			}
		}
	}

	cls.Dangling = c.dangling(*cls)
}

func appendHost(hosts []Host, host Host) []Host {
	if slices.Contains(hosts, host) {
		return hosts
	}

	return append(hosts, host)
}

// dangling returns the target's single-label names that no service, self-alias or bus answers to,
// and its `external_links` names: none gets a relay, so each reaches the stub.
func (c *classifier) dangling(cls Classification) []string {
	known := slices.Concat(cls.SelfAliases, cls.BusNames)
	for _, name := range c.model.Services() {
		known = append(known, NamesFrom(c.model, name, c.model.service)...)
	}

	var out []string

	for _, reference := range c.targetRefs {
		host := reference.Host
		if host == "" || strings.Contains(host, ".") || localHost(reference) || slices.Contains(known, host) {
			continue
		}

		out = append(out, host)
	}

	out = append(out, externalLinkNames(c.model.typed.Services[c.model.service])...)
	slices.Sort(out)

	return slices.Compact(out)
}

// setupBusNames returns the names jobs and started dependencies dial the bus by: every name of a
// bus service they can see, and every `nats://` host they name that no service answers to.
func (c *classifier) setupBusNames(cls Classification, viewers []viewer) []string {
	var names []string

	for _, current := range viewers {
		if current.name == c.model.service {
			continue
		}

		for _, dep := range cls.Deps {
			if dep.Role == RoleBus {
				names = append(names, NamesFrom(c.model, dep.Service, current.name)...)
			}
		}

		for _, reference := range current.refs {
			if reference.Scheme == schemeNATS && !slices.Contains(current.visible, reference.Host) {
				names = append(names, reference.Host)
			}
		}
	}

	slices.Sort(names)

	return slices.Compact(names)
}
