//go:build linux

package provision

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// Answers a healthy engine gives to the three precondition templates, in the shapes the templates
// build. The values are invented; only their shapes were measured.
const (
	healthyContext = `{"host":"unix:///var/run/docker.sock","name":"default"}`
	healthyVersion = `{"cli_version":"29.6.2","server_version":"29.6.2","api_version":"1.55",` +
		`"platform":"Test Platform 1.0","os":"linux","arch":"amd64",` +
		`"components":["Engine","containerd","runc","docker-init"]}`
	healthyInfo = `{"id":"00000000-0000-4000-8000-000000000001",` +
		`"security_options":["name=seccomp,profile=builtin","name=cgroupns"],"default_runtime":"runc",` +
		`"plugins":[{"name":"buildx","version":"v0.35.0"},{"name":"compose","version":"v5.3.1"}]}`
	// argvLogVar names the file every shim appends its argv to.
	argvLogVar = "STUTTER_SHIM_ARGV"
)

// engineAnswers is what a docker shim prints for each precondition call. extra holds further case
// arms, for the calls a test makes after the preconditions.
type engineAnswers struct {
	context, version, info, extra string
	versionExit                   int
}

func healthyAnswers() engineAnswers {
	return engineAnswers{context: healthyContext, version: healthyVersion, info: healthyInfo}
}

// script renders a docker shim that logs its argv and answers the three precondition calls. Any
// other call exits 64, so a precondition that tried one would fail loudly as well as be counted.
func (a engineAnswers) script() string {
	return "#!/bin/sh\n" +
		`printf '%s\n' "$*" >> "$` + argvLogVar + `"` + "\n" +
		"case \"$1\" in\n" +
		"context) printf '%s\\n' '" + a.context + "' ;;\n" +
		"version) printf '%s\\n' '" + a.version + "'; exit " + strconv.Itoa(a.versionExit) + " ;;\n" +
		"info) printf '%s\\n' '" + a.info + "' ;;\n" +
		a.extra +
		"*) exit 64 ;;\n" +
		"esac\n"
}

// writeShim writes an executable `docker` script into dir. It holds the fork lock while the file is
// open for writing: a child another parallel test forks meanwhile would inherit the descriptor until
// it execs, and executing the shim then fails with "text file busy".
func writeShim(t *testing.T, dir, script string) {
	t.Helper()

	syscall.ForkLock.Lock()
	//nolint:gosec // a test shim must be executable to stand in for the docker CLI.
	err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755)
	syscall.ForkLock.Unlock()

	if err != nil {
		t.Fatalf("write shim: %v", err)
	}
}

// Every precondition refuses by name, and none of them ever reaches past the three read-only calls.
// A refusal that did not name what was required and what was found would leave the user guessing
// which of six conditions failed; a precondition that issued anything else would mutate an engine
// the check has not yet accepted.
func TestEachPreconditionRefusesByName(t *testing.T) {
	t.Parallel()

	withContext := func(host string) engineAnswers {
		a := healthyAnswers()
		a.context = `{"host":"` + host + `","name":"remote"}`

		return a
	}
	withVersion := func(version string, exit int) engineAnswers {
		a := healthyAnswers()
		a.version, a.versionExit = version, exit

		return a
	}
	withInfo := func(info string) engineAnswers {
		a := healthyAnswers()
		a.info = info

		return a
	}

	cases := []struct {
		name      string
		want      []string
		answers   engineAnswers
		noDocker  bool
		container bool
	}{
		{
			name: "remote endpoint", answers: withContext("tcp://10.0.0.1:2375"),
			want: []string{"unix://", "tcp://10.0.0.1:2375"},
		},
		{
			name: "unreachable engine", answers: withVersion(`{}`, 1),
			want: []string{"engine", "did not answer"},
		},
		{
			name: "no Engine component", answers: withVersion(strings.Replace(healthyVersion,
				`["Engine","containerd","runc","docker-init"]`, `["Compatible Engine","runc"]`, 1), 0),
			want: []string{`named "Engine"`, "Compatible Engine"},
		},
		{
			name: "rootless", answers: withInfo(strings.Replace(healthyInfo,
				`["name=seccomp,profile=builtin","name=cgroupns"]`, `["name=rootless"]`, 1)),
			want: []string{"rootless"},
		},
		{
			name: "no compose plugin", answers: withInfo(strings.Replace(healthyInfo,
				`,{"name":"compose","version":"v5.3.1"}`, ``, 1)),
			want: []string{"no compose plugin"},
		},
		{
			name: "compose below the floor", answers: withInfo(strings.Replace(healthyInfo,
				`"v5.3.1"`, `"v2.29.6"`, 1)),
			want: []string{"compose", "2.29.6"},
		},
		{name: "no docker on PATH", answers: healthyAnswers(), noDocker: true, want: []string{"docker", "PATH"}},
		{name: "inside a container", answers: healthyAnswers(), container: true, want: []string{"container"}},
	}

	calls, mutating := 0, 0

	for _, tc := range cases {
		shimDir, root := t.TempDir(), t.TempDir()
		argvLog := filepath.Join(t.TempDir(), "argv")

		if !tc.noDocker {
			writeShim(t, shimDir, tc.answers.script())
		}

		if tc.container {
			if err := os.WriteFile(filepath.Join(root, ".dockerenv"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}

		env := []string{"PATH=" + shimDir, argvLogVar + "=" + argvLog}

		_, _, err := preconditionsIn(t.Context(), env, root)
		if !errors.Is(err, ErrPrecondition) {
			t.Errorf("%s: want ErrPrecondition, got %v", tc.name, err)

			continue
		}

		for _, want := range tc.want {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: refusal %q does not name %q", tc.name, err, want)
			}
		}

		for _, line := range readLines(t, argvLog) {
			calls++

			switch strings.Fields(line)[0] {
			case "context", "version", "info":
			default:
				mutating++

				t.Errorf("%s: a precondition issued %q", tc.name, line)
			}
		}
	}

	t.Logf("cases=%d calls=%d mutating=%d", len(cases), calls, mutating)

	if calls == 0 {
		t.Fatal("no precondition call reached a shim")
	}
}

// A darwin or native-windows build refuses the compose path and says where it does run.
func TestTheHostRefusalNamesLinux(t *testing.T) {
	t.Parallel()

	for _, goos := range []string{"darwin", "windows"} {
		err := hostRefusal(goos)
		if !errors.Is(err, ErrPrecondition) {
			t.Fatalf("hostRefusal(%q) = %v, want ErrPrecondition", goos, err)
		}

		for _, want := range []string{"Linux", "WSL2", goos} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("hostRefusal(%q) = %q, does not name %q", goos, err, want)
			}
		}
	}

	if err := hostRefusal("linux"); err != nil {
		t.Errorf("hostRefusal(linux) = %v, want nil", err)
	}
}

// A healthy engine yields every identity field, and every call after the endpoint read is pinned to
// that endpoint.
func TestAHealthyEngineYieldsItsIdentity(t *testing.T) {
	t.Parallel()

	shimDir := t.TempDir()
	writeShim(t, shimDir, healthyAnswers().script())

	env := []string{"PATH=" + shimDir, argvLogVar + "=" + filepath.Join(t.TempDir(), "argv")}

	identity, runner, err := preconditionsIn(t.Context(), env, t.TempDir())
	if err != nil {
		t.Fatalf("preconditions: %v", err)
	}

	fields := map[string]string{
		"CLIPath": identity.CLIPath, "CLIVersion": identity.CLIVersion, "ServerVersion": identity.ServerVersion,
		"APIVersion": identity.APIVersion, "Platform": identity.Platform, "OS": identity.OS, "Arch": identity.Arch,
		"Endpoint": identity.Endpoint, "Context": identity.Context, "EngineID": identity.EngineID,
		"Compose": identity.Compose, "DefaultRuntime": identity.DefaultRuntime,
	}
	for name, value := range fields {
		if value == "" {
			t.Errorf("Identity.%s is empty", name)
		}
	}

	if identity.CLIPath != filepath.Join(shimDir, "docker") {
		t.Errorf("CLIPath = %q, want the shim resolved from PATH", identity.CLIPath)
	}

	if runner.pin != identity.Endpoint {
		t.Errorf("pin = %q, want the endpoint %q", runner.pin, identity.Endpoint)
	}
}

// readLines returns the lines of path, or none when it does not exist.
func readLines(t *testing.T, path string) []string {
	t.Helper()

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		t.Fatal(err)
	}

	return strings.FieldsFunc(string(data), func(r rune) bool { return r == '\n' })
}
