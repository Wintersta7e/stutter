package compose

import (
	"path"
	"slices"
	"strconv"
	"strings"
)

// Ports a role or a lineage defaults to.
const (
	postgresPort uint16 = 5432
	natsPort     uint16 = 4222
)

// Evidence strings that are not a reference.
const (
	evidenceDeclared  = "x-stutter"
	evidenceHandshake = "handshake"
	evidenceDefault   = "role default"
	evidenceNone      = "no evidence"
)

// WebSocket schemes, which name a port but no protocol.
const (
	schemeWS  = "ws"
	schemeWSS = "wss"
)

// schemePorts is the port a URL scheme implies when a reference names none. `tls` and an unknown
// scheme imply no port.
//
//nolint:gochecknoglobals,mnd // a fixed table of well-known ports, not mutable state.
var schemePorts = map[string]uint16{
	schemePostgres: postgresPort, schemePostgresql: postgresPort, schemeNATS: natsPort,
	schemeHTTP: 80, schemeWS: 80, schemeHTTPS: 443, schemeWSS: 443,
	"redis": 6379, schemeRediss: 6379, "amqp": 5672, "amqps": 5671, "mqtt": 1883, "mqtts": 8883,
	"ldap": 389, "ldaps": 636, "smtp": 25, "smtps": 465, "mysql": 3306, "mariadb": 3306, "mongodb": 27017,
}

// tlsSchemes are the schemes whose connection is encrypted.
//
//nolint:gochecknoglobals // a fixed list, not mutable state.
var tlsSchemes = []string{schemeHTTPS, schemeWSS, "tls", schemeRediss, "amqps", "mqtts", "ldaps", "smtps"}

// portKey is one port of a service, TCP or UDP.
type portKey struct {
	port uint16
	udp  bool
}

// endpointSet collects one service's endpoints while the evidence ladder decides each.
type endpointSet map[portKey]*Endpoint

// at returns the endpoint for a port, adding it undecided.
func (e endpointSet) at(key portKey) *Endpoint {
	if found, ok := e[key]; ok {
		return found
	}

	found := &Endpoint{Port: key.port}
	if key.udp {
		found.Protocol, found.Evidence = ProtocolUDP, string(ProtocolUDP)
	}

	e[key] = found

	return found
}

// decide sets an undecided endpoint's protocol, with the evidence that decided it.
func (e endpointSet) decide(key portKey, protocol Protocol, evidence string) {
	if found := e.at(key); found.Protocol == "" {
		found.Protocol, found.Evidence = protocol, evidence
	}
}

// sorted returns the endpoints by port, every undecided one opaque.
func (e endpointSet) sorted() []Endpoint {
	out := make([]Endpoint, 0, len(e))

	for _, found := range e {
		if found.Protocol == "" {
			found.Protocol, found.Evidence = ProtocolOpaque, evidenceNone
		}

		out = append(out, *found)
	}

	slices.SortFunc(out, func(a, b Endpoint) int {
		if a.Port != b.Port {
			return int(a.Port) - int(b.Port)
		}

		return strings.Compare(string(a.Protocol), string(b.Protocol))
	})

	return out
}

// endpoints derives a service's endpoints, strongest evidence first: an `x-stutter` declaration;
// the target's own references; the image's lineage; 5432 or 4222 in compose's `expose` or `ports`;
// else opaque. The handshake, rank 2, is applied afterwards by answers. It returns the lineage that
// spoke, if any.
func (c *classifier) endpoints(name string, svc *composeService) (endpointSet, string) {
	found := endpointSet{}

	for port, protocol := range c.overrides.Endpoints[name] {
		found.decide(portKey{port: port}, protocol, evidenceDeclared)
	}

	c.referenced(found, NamesFrom(c.model, name, c.model.service))

	lineage, protocol, port := imageLineage(c.images[name])
	if lineage != "" {
		found.decide(portKey{port: port}, protocol, "lineage "+lineage)
	}

	composePorts := declaredPorts(svc)
	for _, key := range composePorts {
		switch {
		case key.udp:
			found.at(key)
		case key.port == postgresPort:
			found.decide(key, ProtocolPG, "compose port 5432")
		case key.port == natsPort:
			found.decide(key, ProtocolNATS, "compose port 4222")
		default:
			found.at(key)
		}
	}

	for _, key := range exposedPorts(c.images[name]) {
		found.at(key)
	}

	return found, lineage
}

// referenced adds the ports the target's own references dial a service on, by any of its names.
func (c *classifier) referenced(found endpointSet, names []string) {
	for _, reference := range c.targetRefs {
		if !slices.Contains(names, reference.Host) {
			continue
		}

		port := c.referencePort(reference)
		if port == 0 {
			continue
		}

		key := portKey{port: port}
		found.at(key).TLS = found.at(key).TLS || referenceTLS(reference)

		if protocol, speaks := referenceProtocol(reference); speaks {
			found.decide(key, protocol, reference.Key+" "+referenceScheme(reference)+" "+strconv.Itoa(int(port)))
		}
	}
}

// referencePort returns the port a reference dials: its own, else the one its form implies.
func (c *classifier) referencePort(reference ref) uint16 {
	switch {
	case reference.Port != 0:
		return reference.Port
	case reference.KeywordPG && c.pgPort != 0:
		return c.pgPort
	case reference.KeywordPG:
		return postgresPort
	default:
		return schemePorts[reference.Scheme]
	}
}

// referenceProtocol maps a reference's form to a protocol. A bare `host:port`, and `tls`, `ws` and
// `wss`, name a port but no protocol.
func referenceProtocol(reference ref) (Protocol, bool) {
	switch {
	case reference.KeywordPG || pgScheme(reference.Scheme):
		return ProtocolPG, true
	case reference.Scheme == schemeNATS:
		return ProtocolNATS, true
	case reference.Scheme == schemeHTTP || reference.Scheme == schemeHTTPS:
		return ProtocolHTTP, true
	case reference.Scheme == "" || reference.Scheme == "tls" || reference.Scheme == schemeWS ||
		reference.Scheme == schemeWSS:
		return "", false
	default:
		return ProtocolOpaque, true
	}
}

// referenceScheme names a reference's form in evidence.
func referenceScheme(reference ref) string {
	switch {
	case reference.Scheme != "":
		return reference.Scheme
	case reference.KeywordPG:
		return "libpq"
	default:
		return "host:port"
	}
}

func referenceTLS(reference ref) bool {
	return reference.TLS || slices.Contains(tlsSchemes, reference.Scheme)
}

// imageLineage reads an image's Postgres or NATS lineage: the name of the evidence, the protocol
// and the port it implies.
func imageLineage(img Image) (string, Protocol, uint16) {
	env := imageEnvironment(img)

	for _, name := range []string{"PG_MAJOR", "PG_VERSION"} {
		if _, ok := env[name]; ok {
			return name, ProtocolPG, postgresPort
		}
	}

	if _, ok := env["NATS_SERVER"]; ok {
		return "NATS_SERVER", ProtocolNATS, natsPort
	}

	for _, argv := range [][]string{img.Entrypoint, img.Cmd} {
		if len(argv) == 0 {
			continue
		}

		switch path.Base(argv[0]) {
		case "postgres":
			return "postgres", ProtocolPG, postgresPort
		case "nats-server":
			return "nats-server", ProtocolNATS, natsPort
		default:
		}
	}

	return "", "", 0
}

// declaredPorts returns the ports compose's `expose` and `ports` name: ranges expanded, `/udp`
// kept apart, `ports` by their container port.
func declaredPorts(svc *composeService) []portKey {
	var out []portKey

	for _, entry := range svc.Expose {
		out = append(out, portRange(string(entry))...)
	}

	for _, published := range svc.Ports {
		if port, ok := parsePort(strconv.FormatInt(int64(published.Target), 10)); ok {
			out = append(out, portKey{port: port, udp: published.Protocol == "udp"})
		}
	}

	return out
}

// exposedPorts returns an image's `EXPOSE` entries.
func exposedPorts(img Image) []portKey {
	out := make([]portKey, 0, len(img.ExposedPorts))
	for _, entry := range img.ExposedPorts {
		out = append(out, portRange(entry)...)
	}

	return out
}

// portRange reads `port[-port][/protocol]`.
func portRange(entry string) []portKey {
	ports, protocol, _ := strings.Cut(entry, "/")
	low, high, isRange := strings.Cut(ports, "-")

	first, ok := parsePort(low)
	if !ok {
		return nil
	}

	last := first
	if isRange {
		if last, ok = parsePort(high); !ok || last < first {
			return nil
		}
	}

	out := make([]portKey, 0, int(last-first)+1)
	for port := int(first); port <= int(last); port++ {
		out = append(out, portKey{port: uint16(port), udp: protocol == "udp"}) //nolint:gosec // bounded by last.
	}

	return out
}
