package compose

import (
	"maps"
	"slices"
	"strings"
)

// completedSuccessfully is the `depends_on` condition that makes a service a job.
const completedSuccessfully = "service_completed_successfully"

// role derives a service's role, first rule that matches: attached, job, bus, datastore, sibling,
// other, unused. It returns the role and what decided it.
func (c *classifier) role(name string, svc *composeService, endpoints []Endpoint) (Role, []string) {
	switch {
	case c.attached(name, svc):
		return RoleAttached, []string{"shares the target's network namespace"}
	case c.job(name):
		return RoleJob, []string{"depends_on " + completedSuccessfully}
	case slices.ContainsFunc(endpoints, hasProtocol(ProtocolNATS)):
		return RoleBus, []string{"nats endpoint"}
	case slices.ContainsFunc(endpoints, hasProtocol(ProtocolPG)):
		return RoleDatastore, []string{"pg endpoint"}
	default:
	}

	if evidence := c.sibling(svc); evidence != "" {
		return RoleSibling, []string{evidence}
	}

	var evidence []string

	if c.reachable(svc) {
		evidence = append(evidence, "shares a network with the target")
	}

	if c.closure[name] {
		evidence = append(evidence, "in the target's depends_on closure")
	}

	if len(evidence) > 0 {
		return RoleOther, evidence
	}

	return RoleUnused, []string{"unreachable from the target"}
}

func hasProtocol(protocol Protocol) func(Endpoint) bool {
	return func(found Endpoint) bool { return found.Protocol == protocol }
}

// attached reports a service in the target's network namespace, or the target in its.
func (c *classifier) attached(name string, svc *composeService) bool {
	target := c.model.typed.Services[c.model.service]
	if target.NetworkMode == "service:"+name {
		return true
	}

	if svc.NetworkMode == "service:"+c.model.service {
		return true
	}

	container, found := strings.CutPrefix(svc.NetworkMode, "container:")

	return found && slices.Contains(c.model.containerNames(c.model.service), container)
}

// job reports a service some service waits on to complete successfully.
func (c *classifier) job(name string) bool {
	for _, svc := range c.model.typed.Services {
		if dep, ok := svc.DependsOn[name]; ok && dep.Condition == completedSuccessfully {
			return true
		}
	}

	return false
}

// sibling reports why a service is another consumer of the same bus, or "": it builds, it runs the
// target's image or build, or its own configuration names the bus.
func (c *classifier) sibling(svc *composeService) string {
	target := c.model.typed.Services[c.model.service]

	switch {
	case svc.Build != nil:
		return "has a build"
	case svc.Image != "" && svc.Image == target.Image:
		return "runs the target's image"
	default:
	}

	for _, reference := range serviceReferences(svc) {
		if slices.Contains(c.busNames, reference.Host) {
			return "references the bus as " + reference.Key
		}
	}

	return ""
}

// reachable reports a service sharing a model network with the target.
func (c *classifier) reachable(svc *composeService) bool {
	return len(sharedNetworks(svc, c.model.typed.Services[c.model.service])) > 0
}

// dependsOnClosure returns every service the target transitively depends on.
func dependsOnClosure(m *Model) map[string]bool {
	closure := map[string]bool{}
	queue := []string{m.service}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		svc, ok := m.typed.Services[current]
		if !ok {
			continue
		}

		for _, name := range slices.Sorted(maps.Keys(svc.DependsOn)) {
			if !closure[name] {
				closure[name] = true
				queue = append(queue, name)
			}
		}
	}

	return closure
}
