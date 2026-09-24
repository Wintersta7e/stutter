package provision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// Templates for the three precondition reads. Each builds a snake_case JSON object inside the
// template, so nothing decodes the engine's own field names.
const (
	contextTemplate = `{"host":{{json .Endpoints.docker.Host}},"name":{{json .Name}}}`
	versionTemplate = `{"cli_version":{{json .Client.Version}},"server_version":{{json .Server.Version}},` +
		`"api_version":{{json .Server.APIVersion}},"platform":{{json .Server.Platform.Name}},` +
		`"os":{{json .Server.Os}},"arch":{{json .Server.Arch}},"components":[` +
		`{{range $i, $c := .Server.Components}}{{if $i}},{{end}}{{json $c.Name}}{{end}}]}`
	infoTemplate = `{"id":{{json .ID}},"security_options":{{json .SecurityOptions}},` +
		`"default_runtime":{{json .DefaultRuntime}},"plugins":[` +
		`{{range $i, $p := .ClientInfo.Plugins}}{{if $i}},{{end}}` +
		`{"name":{{json $p.Name}},"version":{{json $p.Version}}}{{end}}]}`
)

// Identity is the engine a check runs against: recorded in the ledger header and rendered with the
// report.
type Identity struct {
	// CLIPath is the docker CLI's absolute path.
	CLIPath string
	// CLIVersion is the docker CLI's version.
	CLIVersion string
	// ServerVersion is the engine's version.
	ServerVersion string
	// APIVersion is the engine's API version.
	APIVersion string
	// Platform is the engine's platform name.
	Platform string
	// OS is the engine's operating system.
	OS string
	// Arch is the engine's architecture, in Go's naming.
	Arch string
	// Endpoint is the pinned endpoint every call carries.
	Endpoint string
	// Context is the name of the docker context the endpoint was read from.
	Context string
	// EngineID is the engine's own ID.
	EngineID string
	// Compose is the compose plugin's version.
	Compose string
	// DefaultRuntime is the engine's default container runtime.
	DefaultRuntime string
}

// Preconditions decides whether this host and engine can run a compose check, and pins the
// engine: every refusal wraps ErrPrecondition and names what was required and what was found. It
// creates nothing.
func Preconditions(ctx context.Context) (Identity, error) {
	identity, _, err := preconditionsIn(ctx, os.Environ(), string(filepath.Separator))

	return identity, err
}

// preconditionsIn runs the checks in order against env and the filesystem under root, and returns
// the runner pinned to the engine it accepted. The runner is still read-only.
func preconditionsIn(ctx context.Context, env []string, root string) (Identity, *execRunner, error) {
	if err := hostRefusal(runtime.GOOS); err != nil {
		return Identity{}, nil, err
	}

	if err := containerRefusal(root); err != nil {
		return Identity{}, nil, err
	}

	runner := newRunner(env, nil)
	if runner.docker == "" {
		return Identity{}, nil, fmt.Errorf("%w: the docker CLI is required, and none was found on PATH",
			ErrPrecondition)
	}

	identity := Identity{CLIPath: runner.docker}

	if err := readEndpoint(ctx, runner, &identity); err != nil {
		return Identity{}, nil, err
	}

	if err := readVersion(ctx, runner, &identity); err != nil {
		return Identity{}, nil, err
	}

	if err := readInfo(ctx, runner, &identity); err != nil {
		return Identity{}, nil, err
	}

	return identity, runner, nil
}

// hostRefusal refuses every host but Linux.
func hostRefusal(goos string) error {
	if goos == "linux" {
		return nil
	}

	return fmt.Errorf("%w: --compose needs a Linux host; Windows runs it inside WSL2 (this build is for %s)",
		ErrPrecondition, goos)
}

// containerRefusal refuses a Stutter that is itself running inside a container: every bind it
// validated would name a path in the wrong filesystem.
func containerRefusal(root string) error {
	for _, marker := range []string{".dockerenv", filepath.Join("run", ".containerenv")} {
		path := filepath.Join(root, marker)

		_, err := os.Lstat(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}

		return fmt.Errorf("%w: --compose cannot run inside a container, and %s exists", ErrPrecondition, path)
	}

	return nil
}

// readEndpoint reads the endpoint the user's CLI resolves, refuses one that is not a local socket,
// and pins it.
func readEndpoint(ctx context.Context, runner *execRunner, identity *Identity) error {
	var found struct {
		Host string `json:"host"`
		Name string `json:"name"`
	}

	if err := readTemplate(ctx, runner, verbContext, contextTemplate, &found); err != nil {
		return fmt.Errorf("%w: the docker context could not be read: %w", ErrPrecondition, err)
	}

	if !strings.HasPrefix(found.Host, "unix://") {
		return fmt.Errorf("%w: the engine must be local (a unix:// endpoint); the %q context points at %q",
			ErrPrecondition, found.Name, found.Host)
	}

	runner.pin = found.Host
	identity.Endpoint, identity.Context = found.Host, found.Name

	return nil
}

// readVersion refuses an engine that does not answer or is not Docker's.
func readVersion(ctx context.Context, runner *execRunner, identity *Identity) error {
	var found struct {
		CLIVersion    string   `json:"cli_version"`
		ServerVersion string   `json:"server_version"`
		APIVersion    string   `json:"api_version"`
		Platform      string   `json:"platform"`
		OS            string   `json:"os"`
		Arch          string   `json:"arch"`
		Components    []string `json:"components"`
	}

	if err := readTemplate(ctx, runner, verbVersion, versionTemplate, &found); err != nil {
		return fmt.Errorf("%w: the engine at %s did not answer: %w", ErrPrecondition, identity.Endpoint, err)
	}

	if !slices.Contains(found.Components, "Engine") {
		return fmt.Errorf("%w: the engine must report a component named %q; it reports %q",
			ErrPrecondition, "Engine", found.Components)
	}

	identity.CLIVersion, identity.ServerVersion, identity.APIVersion = found.CLIVersion, found.ServerVersion,
		found.APIVersion
	identity.Platform, identity.OS, identity.Arch = found.Platform, found.OS, found.Arch

	return nil
}

// readInfo refuses a rootless engine and a compose plugin below the supported floor.
func readInfo(ctx context.Context, runner *execRunner, identity *Identity) error {
	var found struct {
		ID              string         `json:"id"`
		DefaultRuntime  string         `json:"default_runtime"`
		SecurityOptions []string       `json:"security_options"`
		Plugins         []pluginReport `json:"plugins"`
	}

	if err := readTemplate(ctx, runner, verbInfo, infoTemplate, &found); err != nil {
		return fmt.Errorf("%w: the engine at %s did not report its info: %w", ErrPrecondition, identity.Endpoint, err)
	}

	for _, option := range found.SecurityOptions {
		if slices.Contains(strings.Split(option, ","), "name=rootless") {
			return fmt.Errorf("%w: a rootful engine is required; the engine at %s runs rootless",
				ErrPrecondition, identity.Endpoint)
		}
	}

	for _, plugin := range found.Plugins {
		if plugin.Name == "compose" {
			identity.Compose = plugin.Version
		}
	}

	if err := compose.CheckVersion(identity.Compose); err != nil {
		return fmt.Errorf("%w: %w", ErrPrecondition, err)
	}

	identity.EngineID, identity.DefaultRuntime = found.ID, found.DefaultRuntime

	return nil
}

// pluginReport is one CLI plugin as the info template prints it.
type pluginReport struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// readTemplate runs one read-only template call and decodes its JSON.
func readTemplate(ctx context.Context, c caller, v verb, template string, into any) error {
	res, err := c.call(ctx, request{verb: v, args: []arg{{val: template}}})
	if err != nil {
		return err
	}

	return decodeJSON(res.out, into)
}

// decodeJSON decodes the one JSON object a template call printed.
func decodeJSON(out []byte, into any) error {
	if err := json.Unmarshal(bytes.TrimSpace(out), into); err != nil {
		return fmt.Errorf("%w: a template call printed something other than its template: %w", ErrEngine, err)
	}

	return nil
}

// WSLNetworkingMode returns the WSL networking mode this host runs under, as wslinfo reports it.
// The error wraps ErrEngine when wslinfo is absent or fails.
func WSLNetworkingMode(ctx context.Context) (string, error) {
	res, err := newRunner(os.Environ(), nil).call(ctx, request{verb: verbWSLInfo})
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(res.out)), nil
}
