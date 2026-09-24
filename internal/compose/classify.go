package compose

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// errClassification means Classify lost track of a service: an internal fault, never the model's.
var errClassification = errors.New("compose classification")

// classifier holds one Classify call's inputs and what it has derived so far.
type classifier struct {
	model      *Model
	images     map[string]Image
	answers    map[string]map[uint16]pg.Answer
	closure    map[string]bool
	overrides  Overrides
	targetRefs []ref
	busNames   []string
	pgPort     uint16
}

// Classify gives every compose service other than the target exactly one role, and every port of
// it a protocol. It is pure, and it is called three times, all before any listener exists:
//
//   - pass 1 with the images present locally;
//   - pass 2 with those plus the images resolved for pass 1's Started — so a service not started in
//     pass 1 has the same image evidence in pass 2 and stays unstarted;
//   - pass 3 with the Postgres handshake answers of pass 2's Candidates as well: a `pg` answer
//     promotes an opaque endpoint, and one answered otherwise on a port already pg is refused.
func Classify(m *Model, images map[string]Image, answers map[string]map[uint16]pg.Answer) (Classification, error) {
	target, ok := m.typed.Services[m.service]
	if !ok {
		return Classification{}, fmt.Errorf("%w: the service under test is not in the model", errClassification)
	}

	c := &classifier{
		model: m, images: images, answers: answers, closure: dependsOnClosure(m), overrides: m.Overrides(),
		targetRefs: serviceReferences(target),
	}

	for _, reference := range c.targetRefs {
		if reference.Key == "PGPORT" {
			c.pgPort = reference.Port
		}
	}

	deps, err := c.dependencies()
	if err != nil {
		return Classification{}, err
	}

	if len(deps)+1 != len(m.Services()) {
		return Classification{}, fmt.Errorf("%w: classified %d of %d services", errClassification, len(deps)+1,
			len(m.Services()))
	}

	cls := Classification{
		Deps: deps, SelfAliases: m.selfAliases(), BusNames: c.busNames, Declarations: declarations(c.overrides),
		target: m.service,
	}

	if err = refuseClassification(cls); err != nil {
		return Classification{}, err
	}

	if cls.DependencyNames, err = c.collisions(cls); err != nil {
		return Classification{}, err
	}

	return cls, nil
}

// dependencies classifies every service but the target: endpoints for all, then the bus names,
// then roles.
func (c *classifier) dependencies() ([]Dependency, error) {
	names := slices.DeleteFunc(c.model.Services(), func(name string) bool { return name == c.model.service })
	sets := make(map[string]endpointSet, len(names))
	lineages := make(map[string]string, len(names))

	for _, name := range names {
		sets[name], lineages[name] = c.endpoints(name, c.model.typed.Services[name])

		if err := c.applyAnswers(name, sets[name]); err != nil {
			return nil, err
		}

		if role, declared := c.overrides.Roles[name]; declared {
			ensureRolePort(sets[name], role, evidenceDeclared)
		}
	}

	c.busNames = c.findBusNames(names, sets)

	deps := make([]Dependency, 0, len(names))

	for _, name := range names {
		deps = append(deps, c.dependency(name, sets[name], lineages[name]))
	}

	return deps, nil
}

// dependency classifies one service.
func (c *classifier) dependency(name string, set endpointSet, lineage string) Dependency {
	svc := c.model.typed.Services[name]
	role, evidence := c.role(name, svc, set.sorted())

	declaredRole, declared := c.overrides.Roles[name]
	if declared {
		role, evidence = declaredRole, []string{evidenceDeclared}
	} else {
		ensureRolePort(set, role, evidenceDefault)
	}

	return Dependency{
		Answers:   maps.Clone(c.answers[name]),
		Service:   name,
		Lineage:   lineage,
		Role:      role,
		Names:     NamesFrom(c.model, name, c.model.service),
		Evidence:  evidence,
		Endpoints: set.sorted(),
		Declared:  declared,
		Reachable: c.reachable(svc),
		InClosure: c.closure[name],
	}
}

// applyAnswers applies the Postgres handshake's answers: `pg` promotes an opaque endpoint of a
// service with no declared role, and anything else on a port classified pg is a contradiction.
func (c *classifier) applyAnswers(name string, set endpointSet) error {
	_, declaredRole := c.overrides.Roles[name]

	for _, port := range slices.Sorted(maps.Keys(c.answers[name])) {
		found, ok := set[portKey{port: port}]
		if !ok {
			continue
		}

		switch answer := c.answers[name][port]; {
		case answer == pg.AnswerOther && found.Protocol == ProtocolPG:
			return fmt.Errorf("%w: service %s port %d is classified pg but did not answer as Postgres",
				ErrHandshakeContradiction, name, port)
		case answer == pg.AnswerPostgres && !declaredRole && found.Evidence != evidenceDeclared &&
			(found.Protocol == "" || found.Protocol == ProtocolOpaque):
			found.Protocol, found.Evidence = ProtocolPG, evidenceHandshake
		default:
		}
	}

	return nil
}

// ensureRolePort gives a bus or a datastore its role's default port with the role's protocol, when
// no endpoint carries that protocol yet.
func ensureRolePort(set endpointSet, role Role, evidence string) {
	protocol, port := ProtocolPG, postgresPort
	if role == RoleBus {
		protocol, port = ProtocolNATS, natsPort
	} else if role != RoleDatastore {
		return
	}

	for _, found := range set {
		if found.Protocol == protocol {
			return
		}
	}

	found := set.at(portKey{port: port})
	found.Protocol, found.Evidence = protocol, evidence
}

// findBusNames returns the names the target dials the bus by: every name of a service with a nats
// endpoint, and every `nats://` host the target names that no service answers to.
func (c *classifier) findBusNames(names []string, sets map[string]endpointSet) []string {
	var (
		bus   []string
		known []string
	)

	for _, name := range names {
		view := NamesFrom(c.model, name, c.model.service)
		known = append(known, view...)

		if slices.ContainsFunc(endpointList(sets[name]), hasProtocol(ProtocolNATS)) {
			bus = append(bus, view...)
		}
	}

	known = append(known, c.model.selfAliases()...)

	for _, reference := range c.targetRefs {
		if reference.Scheme == schemeNATS && !slices.Contains(known, reference.Host) {
			bus = append(bus, reference.Host)
		}
	}

	slices.Sort(bus)

	return slices.Compact(bus)
}

// endpointList returns a set's endpoints as they stand, undecided ones included.
func endpointList(endpoints endpointSet) []Endpoint {
	out := make([]Endpoint, 0, len(endpoints))
	for _, found := range endpoints {
		out = append(out, *found)
	}

	return out
}

// refuseClassification raises what classification alone refuses: a service in the target's namespace, and an
// encrypted endpoint on a started service Stutter could only compare as bytes.
func refuseClassification(cls Classification) error {
	for _, dep := range cls.Deps {
		if dep.Role == RoleAttached {
			return fmt.Errorf("%w: service %s shares the target's network namespace, which Stutter cannot isolate",
				ErrModel, dep.Service)
		}
	}

	for _, dep := range cls.Deps {
		if dep.Role != RoleDatastore && dep.Role != RoleOther {
			continue
		}

		for _, found := range dep.Endpoints {
			if found.TLS && (found.Protocol == ProtocolOpaque || found.Protocol == ProtocolHTTP) {
				return fmt.Errorf("%w: service %s port %d (%s): Stutter cannot compare encrypted traffic",
					ErrEncryptedEndpoint, dep.Service, found.Port, found.Evidence)
			}
		}
	}

	return nil
}

// collisions refuses a name that would reach two services, and returns the names jobs and started
// dependencies dial each started dependency by.
func (c *classifier) collisions(cls Classification) (map[string][]string, error) {
	bus := map[string]bool{}
	others := make([]string, 0, len(cls.Deps))

	for _, dep := range cls.Deps {
		others = append(others, dep.Service)
		bus[dep.Service] = dep.Role == RoleBus
	}

	if err := viewCollision(c.model.service, c.model.targetViews(others), cls.SelfAliases, bus); err != nil {
		return nil, err
	}

	var viewers, started []string

	for _, dep := range cls.Deps {
		switch {
		case dep.Role == RoleJob && dep.InClosure:
			viewers = append(viewers, dep.Service)
		case dep.Role == RoleDatastore || dep.Role == RoleOther:
			viewers = append(viewers, dep.Service)
			started = append(started, dep.Service)
		default:
		}
	}

	names := make(map[string][]string, len(started))
	for _, dep := range started {
		names[dep] = c.model.dependencyNames(dep, viewers)
	}

	if err := networkCollision(names); err != nil {
		return nil, err
	}

	return names, nil
}

// declarations lists every `x-stutter` entry applied, with its file.
func declarations(overrides Overrides) []Declaration {
	var out []Declaration

	for _, service := range slices.Sorted(maps.Keys(overrides.Roles)) {
		out = append(out, Declaration{
			File: overrides.File, Service: service, Key: "x-stutter.roles." + service,
			Value: string(overrides.Roles[service]),
		})
	}

	for _, service := range slices.Sorted(maps.Keys(overrides.Endpoints)) {
		for _, port := range slices.Sorted(maps.Keys(overrides.Endpoints[service])) {
			out = append(out, Declaration{
				File: overrides.File, Service: service,
				Key:   "x-stutter.endpoints." + service + "." + strconv.Quote(strconv.Itoa(int(port))),
				Value: string(overrides.Endpoints[service][port]),
			})
		}
	}

	return out
}

// Started returns the services Stutter starts: the target, the jobs it depends on, and every
// datastore and other dependency. Sorted.
func (c Classification) Started() []string {
	started := []string{c.target}

	for _, dep := range c.Deps {
		if (dep.Role == RoleJob && dep.InClosure) || dep.Role == RoleDatastore || dep.Role == RoleOther {
			started = append(started, dep.Service)
		}
	}

	slices.Sort(started)

	return started
}

// Candidates returns the TCP ports still opaque on each datastore and other dependency with no
// declared role: the ports the Postgres handshake asks.
func (c Classification) Candidates() map[string][]uint16 {
	out := map[string][]uint16{}

	for _, dep := range c.Deps {
		if dep.Declared || (dep.Role != RoleDatastore && dep.Role != RoleOther) {
			continue
		}

		for _, found := range dep.Endpoints {
			if found.Protocol == ProtocolOpaque {
				out[dep.Service] = append(out[dep.Service], found.Port)
			}
		}
	}

	return out
}
