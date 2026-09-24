package compose

import (
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// xStutterLine finds a top-level `x-stutter` key in a compose file's bytes.
var xStutterLine = regexp.MustCompile(`(?m)^["']?x-stutter["']?[ \t]*:`)

//nolint:gochecknoglobals // fixed vocabularies, not mutable state.
var (
	// declarableRoles are the roles `x-stutter` may set. `job` and `attached` are facts of the
	// compose graph, never declared.
	declarableRoles = []Role{RoleBus, RoleDatastore, RoleSibling, RoleOther, RoleUnused}
	// declarableProtocols are the protocols `x-stutter` may set on a port.
	declarableProtocols = []Protocol{ProtocolPG, ProtocolNATS, ProtocolHTTP, ProtocolOpaque}
)

// Overrides returns the `x-stutter` declaration, with the file that declares it.
func (m *Model) Overrides() Overrides {
	out := Overrides{File: m.overrides.File, Roles: maps.Clone(m.overrides.Roles)}

	if m.overrides.Endpoints != nil {
		out.Endpoints = make(map[string]map[uint16]Protocol, len(m.overrides.Endpoints))
		for service, ports := range m.overrides.Endpoints {
			out.Endpoints[service] = maps.Clone(ports)
		}
	}

	return out
}

// declaringFile returns the one compose file that declares `x-stutter` as a top-level key, or "".
// Two are refused: one compose release keeps only the last file's key while later ones merge them,
// so the same files would mean different declarations on different supported releases.
func declaringFile(files []string) (string, error) {
	var found []string

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("%w: a compose file cannot be read: %w", ErrModel, err)
		}

		if xStutterLine.Match(data) {
			found = append(found, file)
		}
	}

	if len(found) > 1 {
		return "", fmt.Errorf("%w: x-stutter is declared in both %s and %s; declare it in one --compose file",
			ErrModel, found[0], found[1])
	}

	if len(found) == 0 {
		return "", nil
	}

	return found[0], nil
}

// readOverrides reads the model's `x-stutter` value against its grammar.
func readOverrides(model *Model, files []string) (Overrides, error) {
	file, err := declaringFile(files)
	if err != nil {
		return Overrides{}, err
	}

	value, declared := model.root["x-stutter"]
	if !declared || value == nil {
		return Overrides{}, nil
	}

	if file == "" {
		return Overrides{}, fmt.Errorf("%w: the model carries x-stutter, but no --compose file declares it; "+
			"declare it as a top-level YAML key in one --compose file", ErrModel)
	}

	body, ok := value.(map[string]any)
	if !ok {
		return Overrides{}, declarationError("x-stutter", "is not a map")
	}

	out := Overrides{File: file}

	for _, key := range slices.Sorted(maps.Keys(body)) {
		switch key {
		case "roles":
			out.Roles, err = model.declaredRoles(body[key])
		case "endpoints":
			out.Endpoints, err = model.declaredEndpoints(body[key])
		default:
			err = declarationError("x-stutter."+key, "is not a key x-stutter has (roles, endpoints)")
		}

		if err != nil {
			return Overrides{}, err
		}
	}

	return out, nil
}

func (m *Model) declaredRoles(value any) (map[string]Role, error) {
	body, ok := value.(map[string]any)
	if !ok {
		return nil, declarationError("x-stutter.roles", "is not a map")
	}

	out := make(map[string]Role, len(body))

	for _, service := range slices.Sorted(maps.Keys(body)) {
		path := "x-stutter.roles." + service
		if err := m.declarable(path, service); err != nil {
			return nil, err
		}

		role, ok := body[service].(string)
		if !ok || !slices.Contains(declarableRoles, Role(role)) {
			return nil, declarationError(path, "is not one of bus, datastore, sibling, other, unused "+
				"(job and attached come from the compose graph)")
		}

		out[service] = Role(role)
	}

	return out, nil
}

func (m *Model) declaredEndpoints(value any) (map[string]map[uint16]Protocol, error) {
	body, ok := value.(map[string]any)
	if !ok {
		return nil, declarationError("x-stutter.endpoints", "is not a map")
	}

	out := make(map[string]map[uint16]Protocol, len(body))

	for _, service := range slices.Sorted(maps.Keys(body)) {
		path := "x-stutter.endpoints." + service
		if err := m.declarable(path, service); err != nil {
			return nil, err
		}

		ports, ok := body[service].(map[string]any)
		if !ok {
			return nil, declarationError(path, "is not a map of port to protocol")
		}

		out[service] = make(map[uint16]Protocol, len(ports))

		for _, text := range slices.Sorted(maps.Keys(ports)) {
			portPath := path + "." + strconv.Quote(text)

			port, err := strconv.ParseUint(text, 10, 16)
			if err != nil || port == 0 || strings.TrimLeft(text, "0123456789") != "" {
				return nil, declarationError(portPath, "is not a decimal port 1-65535")
			}

			protocol, ok := ports[text].(string)
			if !ok || !slices.Contains(declarableProtocols, Protocol(protocol)) {
				return nil, declarationError(portPath, "is not one of pg, nats, http, opaque")
			}

			out[service][uint16(port)] = Protocol(protocol)
		}
	}

	return out, nil
}

// declarable refuses a declaration about a service the model lacks, or about the target.
func (m *Model) declarable(path, service string) error {
	if _, ok := m.typed.Services[service]; !ok {
		return declarationError(path, "names a service the model does not have")
	}

	if service == m.service {
		return declarationError(path, "names the service under test, which has no role to declare")
	}

	return nil
}

func declarationError(path, problem string) error {
	return fmt.Errorf("%w: %s %s", ErrModel, path, problem)
}
