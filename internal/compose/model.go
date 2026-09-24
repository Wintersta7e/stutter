package compose

import (
	"fmt"
	"maps"
	"slices"
)

// Model is one compose project, parsed once per check and held only in memory. It carries
// interpolated secrets, so it is never written, logged or printed: Format shows its shape alone.
type Model struct {
	// root is the model exactly as compose printed it, numbers kept as json.Number: the verbatim
	// source a build is handed.
	root map[string]any
	// environ holds the value of each variable an `environment:` config or secret names that compose
	// resolved; a named variable missing here is absent.
	environ map[string]string
	typed   composeProject
	// service is the service under test.
	service   string
	reproduce string
	files     []string
	// sourced names every variable an `environment:` config or secret of some service reads.
	sourced     []string
	composeVars []string
	stderrLines int
	dotEnv      bool
}

// Project returns the project name compose resolved.
func (m *Model) Project() string {
	return m.typed.Name
}

// Files returns the compose files, in the order given.
func (m *Model) Files() []string {
	return slices.Clone(m.files)
}

// Profiles returns every profile a model service carries, with the services carrying it. Both are
// read from the model, never from a variable's value.
func (m *Model) Profiles() []Profile {
	gated := map[string][]string{}

	for _, name := range m.Services() {
		for _, profile := range m.typed.Services[name].Profiles {
			gated[profile] = append(gated[profile], name)
		}
	}

	out := make([]Profile, 0, len(gated))
	for _, name := range slices.Sorted(maps.Keys(gated)) {
		out = append(out, Profile{Name: name, Services: gated[name]})
	}

	return out
}

// ComposeVars names every `COMPOSE_*` variable set in Stutter's environment or as a key of the
// project's `.env`. Values are never read.
func (m *Model) ComposeVars() []string {
	return slices.Clone(m.composeVars)
}

// DotEnv reports whether the project directory has a `.env`.
func (m *Model) DotEnv() bool {
	return m.dotEnv
}

// StderrLines counts the lines compose printed on stderr while producing the model. Their text is
// never kept.
func (m *Model) StderrLines() int {
	return m.stderrLines
}

// Reproduce is the command that prints the model, for a user to run: files and flags only.
func (m *Model) Reproduce() string {
	return m.reproduce
}

// Services returns every service in the model, sorted.
func (m *Model) Services() []string {
	return slices.Sorted(maps.Keys(m.typed.Services))
}

// DependsOn maps each service that service depends on to the condition it waits for. An unknown
// service depends on nothing.
func (m *Model) DependsOn(service string) map[string]string {
	svc, ok := m.typed.Services[service]
	if !ok {
		return nil
	}

	out := make(map[string]string, len(svc.DependsOn))
	for name, dep := range svc.DependsOn {
		out[name] = dep.Condition
	}

	return out
}

// Format prints the project name, the number of files and the service names for every verb —
// never a value from the model.
func (m *Model) Format(f fmt.State, _ rune) {
	fmt.Fprintf(f, "Model{project=%s files=%d services=%v}", m.typed.Name, len(m.files), m.Services())
}

// serviceOf returns one service's typed form.
func (m *Model) serviceOf(name string) (*composeService, error) {
	svc, ok := m.typed.Services[name]
	if !ok {
		return nil, fmt.Errorf("%w: service %s is not in the compose model", ErrModel, name)
	}

	return svc, nil
}
