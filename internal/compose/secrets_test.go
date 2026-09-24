package compose_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// surfaces collects everything the package prints or returns as text, to be searched for a secret.
type surfaces struct {
	texts []string
}

// values records every verb's rendering of each value.
func (s *surfaces) values(values ...any) {
	for _, value := range values {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			s.texts = append(s.texts, fmt.Sprintf(verb, value))
		}
	}
}

// failure records an error's text; a nil error records nothing.
func (s *surfaces) failure(err error) {
	if err != nil {
		s.texts = append(s.texts, err.Error())
	}
}

// secretModel carries the sentinel in an environment value, a build argument, a content config,
// an environment-sourced secret and a DSN password.
//
//nolint:gosec // the password is a placeholder each run replaces with a random sentinel.
const secretModel = `{"name": "shop", "services": {
	"api": {
		"build": {"context": "/project", "args": {"TOKEN": "SENTINEL"}},
		"image": "example.test/api:1",
		"environment": {"SECRET": "SENTINEL", "DATABASE_URL": "postgres://app:SENTINEL@db:5432/app",
			"NATS_URL": "nats://bus:4222"},
		"configs": [{"source": "inline", "target": "/etc/app/inline.conf"}],
		"secrets": [{"source": "token", "target": "/run/secrets/token"}],
		"depends_on": {"migrate": {"condition": "service_completed_successfully"}},
		"networks": {"default": null}
	},
	"migrate": {"image": "example.test/api:1", "healthcheck": {"test": ["CMD", "true"], "interval": "5s"},
		"stop_signal": "SIGINT", "networks": {"default": null}},
	"db": {"networks": {"default": null}},
	"bus": {"expose": ["4222"], "networks": {"default": null}},
	"sidecar": {"build": {"context": "/project"}, "privileged": true, "networks": {"default": null}}
}, "configs": {"inline": {"content": "SENTINEL"}}, "secrets": {"token": {"environment": "TOKEN_VAR"}}}`

func TestNoModelValueIsEverPrinted(t *testing.T) {
	t.Parallel()

	sentinel := rand.Text()
	model := strings.ReplaceAll(secretModel, "SENTINEL", sentinel)
	dir := t.TempDir()
	file := filepath.Join(dir, "compose.yaml")
	dotEnv := "COMPOSE_PROFILES=" + sentinel + "\nOTHER=" + sentinel + "\n"

	for name, content := range map[string]string{file: "services: {}\n", filepath.Join(dir, ".env"): dotEnv} {
		if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	var got surfaces

	parsed := parseSecretModel(t, &got, file, model, sentinel)
	recordRefusedModels(t, &got, model, sentinel)

	img := compose.Image{ID: "sha256:secret", Env: []string{"PATH=/bin"}}

	got.values(parsed, parsed.Project(), parsed.Files(), parsed.Profiles(), parsed.ComposeVars(), parsed.DotEnv(),
		parsed.StderrLines(), parsed.Reproduce(), parsed.Services(), parsed.DependsOn(target), parsed.Overrides(),
		parsed.BindSources(parsed.Services()), parsed.Images(parsed.Services()),
		compose.NamesFrom(parsed, "db", target))
	recordLifecycle(&got, parsed)
	recordSpecs(t, &got, parsed, img)
	recordClassification(&got, parsed)

	_, err := parsed.BuildModel(map[string]string{"db": "t:1"}, map[string]map[string]string{"db": {}})
	got.failure(err)

	prints, err := compose.Fingerprint(t.Context(), []string{filepath.Join(dir, "absent")})
	got.failure(err)
	got.failure(parsed.Refusals(parsed.Services(), labelNS, prints))
	got.failure(compose.CheckVersion("v1.0.0"))

	hits, size := 0, 0

	for _, text := range got.texts {
		size += len(text)

		if strings.Contains(text, sentinel) {
			hits++

			t.Errorf("a value leaked: %s", text)
		}
	}

	t.Logf("surfaces=%d bytes=%d sentinel-hits=%d", len(got.texts), size, hits)

	if len(got.texts) == 0 || size == 0 {
		t.Fatalf("surfaces=%d bytes=%d: nothing was scanned", len(got.texts), size)
	}
}

// parseSecretModel records a failed parse whose compose error echoes the secret, and returns a
// successful parse of model.
func parseSecretModel(t *testing.T, got *surfaces, file, model, sentinel string) *compose.Model {
	t.Helper()

	failing := &fakeRun{whole: func([]string) ([]byte, int, error) {
		return nil, 1, &exitError{text: "invalid interpolation " + sentinel, code: 15}
	}}

	_, err := compose.Parse(t.Context(), failing.run, compose.Inputs{Service: target, Files: []string{file}})
	got.failure(err)

	unknown := &fakeRun{whole: func([]string) ([]byte, int, error) {
		return []byte(`{"services": {"api": {"future": "` + sentinel + `"}}}`), 0, nil
	}}

	_, err = compose.Parse(t.Context(), unknown.run, compose.Inputs{Service: target, Files: []string{file}})
	got.failure(err)

	run := &fakeRun{
		whole:       func([]string) ([]byte, int, error) { return []byte(model), 1, nil },
		environment: []byte("TOKEN_VAR=" + sentinel + "\n"),
	}

	parsed, err := compose.Parse(t.Context(), run.run, compose.Inputs{Service: target, Files: []string{file}})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	unset := &fakeRun{whole: func([]string) ([]byte, int, error) { return []byte(model), 0, nil }}

	withoutEnv, err := compose.Parse(t.Context(), unset.run, compose.Inputs{Service: target, Files: []string{file}})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	_, err = withoutEnv.Spec(target, compose.Image{ID: "sha256:secret"}, compose.CAEnvironment())
	got.failure(err)

	return parsed
}

// recordRefusedModels records the refusals a model's values can reach: a datastore outside the
// model, an encrypted endpoint, and an x-stutter value outside its grammar.
func recordRefusedModels(t *testing.T, got *surfaces, model, sentinel string) {
	t.Helper()

	const bus = `"NATS_URL": "nats://bus:4222"`

	variants := []string{
		strings.Replace(model, bus, bus+`, "EXT": "postgres://u:`+sentinel+`@ext.test:5432/app"`, 1),
		strings.Replace(model, bus, bus+`, "CACHE": "rediss://:`+sentinel+`@db:6380/0"`, 1),
		strings.Replace(model, `"secrets": {`, `"x-stutter": {"roles": {"db": "`+sentinel+`"}}, "secrets": {`, 1),
	}

	for _, variant := range variants {
		run := &fakeRun{
			whole:       func([]string) ([]byte, int, error) { return []byte(variant), 0, nil },
			environment: []byte("TOKEN_VAR=x\n"),
		}

		parsed, err := compose.Parse(t.Context(), run.run, compose.Inputs{
			Service: target, Files: []string{declaring(t, t.TempDir(), "compose.yaml")},
		})
		got.failure(err)

		if err == nil {
			_, err = compose.Classify(parsed, nil, nil)
			got.failure(err)
		}

		if err == nil {
			t.Error("a refused variant was accepted")
		}
	}
}

func recordLifecycle(got *surfaces, parsed *compose.Model) {
	for _, service := range append(parsed.Services(), absentService) {
		check, set, err := parsed.Healthcheck(service)
		got.values(check, set)
		got.failure(err)

		stop, err := parsed.Stop(service)
		got.values(stop)
		got.failure(err)
	}
}

func recordSpecs(t *testing.T, got *surfaces, parsed *compose.Model, img compose.Image) {
	t.Helper()

	spec, err := parsed.Spec(target, img, compose.CAEnvironment())
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}

	got.values(spec, &spec, spec.CopyIn, spec.Mounts, spec.Replaced, spec.ProxyRemoved, spec.CAOverridden)

	job, err := parsed.Spec(jobService, img, nil)
	got.values(job)
	got.failure(err)

	_, err = parsed.Spec(absentService, img, nil)
	got.failure(err)

	broken := faithful(spec, img)
	broken.Env = append(broken.Env, "SECRET=changed", "EXTRA=1")
	broken.Privileged = true

	checked, differ, err := compose.Audit(spec, img, broken)
	got.values(checked, differ)
	got.failure(err)
}

func recordClassification(got *surfaces, parsed *compose.Model) {
	cls, err := compose.Classify(parsed, nil, nil)
	got.values(cls, cls.Started(), cls.Candidates(), cls.Deps, cls.Disclosed, cls.SetupEgress)
	got.failure(err)

	_, err = compose.Classify(parsed, nil, map[string]map[uint16]pg.Answer{"db": {5432: pg.AnswerOther}})
	if !errors.Is(err, compose.ErrHandshakeContradiction) {
		got.texts = append(got.texts, "unexpected: "+fmt.Sprint(err))
	}

	got.failure(err)
}
