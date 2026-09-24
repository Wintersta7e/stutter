package compose_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

const debugProfile = "debug"

const minimalModel = `{"name": "shop", "services": {"api": {"image": "example.test/api:1"}}}`

// exitError is a runner failure carrying compose's exit status.
type exitError struct {
	text string
	code int
}

func (e *exitError) Error() string { return e.text }

func (e *exitError) ExitCode() int { return e.code }

func TestParseRunsTheWholeProjectOnce(t *testing.T) {
	t.Parallel()

	first := project(t, "compose.yaml")
	second := project(t, "override.yaml")
	run := &fakeRun{whole: func([]string) ([]byte, int, error) { return []byte(minimalModel), 0, nil }}

	_, err := compose.Parse(t.Context(), run.run, compose.Inputs{
		Service: target, Files: []string{first, second}, Profiles: []string{debugProfile},
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	calls := run.seen()
	if len(calls) != 1 {
		t.Fatalf("%d compose calls, want 1: %+v", len(calls), calls)
	}

	want := []string{"-f", first, "-f", second, "--profile", debugProfile}
	if !slices.Equal(calls[0].args, want) || calls[0].read != (compose.ConfigRead{}) {
		t.Errorf("call = %+v, want args %v and the whole-project read", calls[0], want)
	}

	if calls[0].dir != filepath.Dir(first) {
		t.Errorf("ran in %s, want the first file's directory %s", calls[0].dir, filepath.Dir(first))
	}

	for _, flag := range []string{"--project-directory", "--env-file", "-p"} {
		if slices.Contains(calls[0].args, flag) {
			t.Errorf("args %v carry %s", calls[0].args, flag)
		}
	}
}

func TestParseRefusesIncompleteInputs(t *testing.T) {
	t.Parallel()

	file := project(t, "compose.yaml")
	run := &fakeRun{whole: func([]string) ([]byte, int, error) { return []byte(minimalModel), 0, nil }}

	for _, in := range []compose.Inputs{
		{Files: []string{file}},
		{Service: target},
		{Service: target, Files: []string{"compose.yaml"}},
	} {
		if _, err := compose.Parse(t.Context(), run.run, in); err == nil {
			t.Errorf("Parse(%+v) = nil error, want the inputs refused", in)
		}
	}

	if calls := run.seen(); len(calls) != 0 {
		t.Errorf("compose ran %d times for refused inputs", len(calls))
	}
}

func TestAnAbsentServiceIsReadNarrowlyForItsProfiles(t *testing.T) {
	t.Parallel()

	file := project(t, "compose.yaml")
	gated := `{"name": "shop", "services": {"api": {"image": "a"}, "tools": {"image": "t", "profiles": ["debug"]}}}`
	run := &fakeRun{
		whole: func(profiles []string) ([]byte, int, error) {
			if slices.Contains(profiles, debugProfile) {
				return []byte(gated), 0, nil
			}

			return []byte(minimalModel), 0, nil
		},
		narrow: func(service string) ([]byte, error) {
			return []byte(`{"services": {"` + service + `": {"image": "t", "profiles": ["debug"]}}}`), nil
		},
	}

	model, err := compose.Parse(t.Context(), run.run, compose.Inputs{Service: toolsService, Files: []string{file}})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	calls := run.seen()
	reads := make([]compose.ConfigRead, 0, len(calls))

	for _, c := range calls {
		reads = append(reads, c.read)
	}

	want := []compose.ConfigRead{{}, {Service: toolsService}, {}}
	if !slices.Equal(reads, want) {
		t.Fatalf("reads = %+v, want whole, narrow, whole", reads)
	}

	if got := profilesIn(calls[2].args); !slices.Equal(got, []string{debugProfile}) {
		t.Errorf("second whole read profiles = %v, want [debug]", got)
	}

	if !slices.Contains(model.Services(), toolsService) {
		t.Errorf("services = %v, want tools", model.Services())
	}

	if !strings.Contains(model.Reproduce(), "--profile debug") {
		t.Errorf("reproduce = %q, want the added profile", model.Reproduce())
	}
}

func TestAServiceAbsentEvenWithItsProfilesIsRefused(t *testing.T) {
	t.Parallel()

	file := project(t, "compose.yaml")
	run := &fakeRun{
		whole:  func([]string) ([]byte, int, error) { return []byte(minimalModel), 0, nil },
		narrow: func(string) ([]byte, error) { return nil, &exitError{text: "no such service", code: 1} },
	}

	_, err := compose.Parse(t.Context(), run.run, compose.Inputs{Service: absentService, Files: []string{file}})
	if !errors.Is(err, compose.ErrModel) || !strings.Contains(err.Error(), absentService) {
		t.Fatalf("Parse = %v, want ErrModel naming the service", err)
	}

	if strings.Contains(err.Error(), "no such service") {
		t.Errorf("error %q relays compose's own text", err)
	}
}

func TestAParseFailureNamesTheCommandNotTheOutput(t *testing.T) {
	t.Parallel()

	sentinel := rand.Text()
	file := project(t, "compose.yaml")
	run := &fakeRun{whole: func([]string) ([]byte, int, error) {
		return nil, 3, fmt.Errorf("compose config: %w", &exitError{text: "invalid value " + sentinel, code: 15})
	}}

	_, err := compose.Parse(t.Context(), run.run, compose.Inputs{Service: target, Files: []string{file}})
	if !errors.Is(err, compose.ErrModel) {
		t.Fatalf("Parse = %v, want ErrModel", err)
	}

	text := err.Error()
	for _, want := range []string{"15", "docker compose -f " + file + " config --format json"} {
		if !strings.Contains(text, want) {
			t.Errorf("error %q does not name %q", text, want)
		}
	}

	if strings.Contains(text, sentinel) {
		t.Errorf("error %q carries compose's output", text)
	}
}

func TestParseReportsInputsForTheHeader(t *testing.T) {
	t.Parallel()

	file := project(t, "compose.yaml")
	if err := os.WriteFile(filepath.Join(filepath.Dir(file), ".env"), []byte("A=1\n"), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	model := `{"name": "shop", "services": {
		"api": {"image": "a"},
		"tools": {"image": "t", "profiles": ["debug", "ops"]},
		"shell": {"image": "s", "profiles": ["debug"]}
	}}`
	run := &fakeRun{whole: func([]string) ([]byte, int, error) { return []byte(model), 2, nil }}

	got, err := compose.Parse(t.Context(), run.run, compose.Inputs{
		Service: target, Files: []string{file}, Profiles: []string{debugProfile, "ops"},
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	wantProfiles := []compose.Profile{
		{Name: debugProfile, Services: []string{"shell", toolsService}},
		{Name: "ops", Services: []string{toolsService}},
	}

	switch {
	case got.Project() != "shop":
		t.Errorf("project = %q", got.Project())
	case !slices.Equal(got.Files(), []string{file}):
		t.Errorf("files = %v", got.Files())
	case !slices.EqualFunc(got.Profiles(), wantProfiles, func(a, b compose.Profile) bool {
		return a.Name == b.Name && slices.Equal(a.Services, b.Services)
	}):
		t.Errorf("profiles = %+v, want %+v", got.Profiles(), wantProfiles)
	case !got.DotEnv():
		t.Error("DotEnv() = false with a .env present")
	case got.StderrLines() != 2:
		t.Errorf("stderr lines = %d, want 2", got.StderrLines())
	case !slices.Equal(got.Services(), []string{target, "shell", toolsService}):
		t.Errorf("services = %v", got.Services())
	default:
	}

	if printed := fmt.Sprintf("%+v", got); !strings.Contains(printed, "shop") {
		t.Errorf("%%+v of the model = %q, want the project named", printed)
	}
}

func TestAnUnreadableDotEnvIsNotAbsent(t *testing.T) {
	t.Parallel()

	file := project(t, "compose.yaml")
	dotEnv := filepath.Join(filepath.Dir(file), ".env")

	if err := os.Mkdir(dotEnv, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	run := &fakeRun{whole: func([]string) ([]byte, int, error) { return []byte(minimalModel), 0, nil }}

	_, err := compose.Parse(t.Context(), run.run, compose.Inputs{Service: target, Files: []string{file}})
	if err == nil || !strings.Contains(err.Error(), dotEnv) {
		t.Fatalf("Parse = %v, want an error naming %s", err, dotEnv)
	}

	absent := modelFrom(t, minimalModel)
	if absent.DotEnv() {
		t.Error("DotEnv() = true with no .env")
	}
}

func TestDependsOnReadsConditions(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, string(golden(t, "5.5.1")))

	got := model.DependsOn(target)
	want := map[string]string{"db": "service_healthy", "migrate": "service_completed_successfully"}

	if len(got) != len(want) || got["db"] != want["db"] || got["migrate"] != want["migrate"] {
		t.Errorf("DependsOn(api) = %v, want %v", got, want)
	}

	if got := model.DependsOn("db"); len(got) != 0 {
		t.Errorf("DependsOn(db) = %v, want none", got)
	}
}
