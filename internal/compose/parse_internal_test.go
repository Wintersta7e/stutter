package compose

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// staticRun answers the whole-project read with model and the environment read with environ.
func staticRun(model, environ string) ConfigFunc {
	return func(_ context.Context, _ string, _ []string, read ConfigRead) ([]byte, int, error) {
		if read.Environment {
			return []byte(environ), 0, nil
		}

		return []byte(model), 0, nil
	}
}

// parseIn parses model from a compose file in dir, which must be absolute.
func parseIn(t *testing.T, dir, model, environ string) *Model {
	t.Helper()

	file := filepath.Join(dir, "compose.yaml")
	if err := os.WriteFile(file, []byte("services: {}\n"), 0o600); err != nil {
		t.Fatalf("write compose file: %v", err)
	}

	parsed, err := Parse(t.Context(), staticRun(model, environ), Inputs{Service: "api", Files: []string{file}})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	return parsed
}

func readGolden(t *testing.T, release string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "model", "kitchen-"+release+".json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}

	return string(data)
}

func TestParseDecodesBothComposeShapes(t *testing.T) {
	t.Parallel()

	floor := parseIn(t, t.TempDir(), readGolden(t, "2.29.7"), "")
	newest := parseIn(t, t.TempDir(), readGolden(t, "5.5.1"), "")

	api := floor.typed.Services["api"]

	if api.MemLimit != 536870912 || api.ShmSize != 67108864 {
		t.Errorf("mem_limit %d shm_size %d, want 536870912 and 67108864", api.MemLimit, api.ShmSize)
	}

	if mode := api.Configs[2].Mode; mode == nil || *mode != 0o400 {
		t.Errorf("config mode = %v, want 0400", mode)
	}

	if mode := api.Secrets[0].Mode; mode == nil || *mode != 0o400 {
		t.Errorf("secret mode = %v, want 0400", mode)
	}

	if bind := api.Volumes[0]; bind.Type != "bind" || !bind.ReadOnly || bind.Source != "/project/conf" {
		t.Errorf("short bind = %+v, want a read-only bind of /project/conf", bind)
	}

	if tmpfs := api.Volumes[4].Tmpfs; tmpfs == nil || tmpfs.Size != 1048576 || tmpfs.Mode != 0o1777 {
		t.Errorf("tmpfs = %+v, want size 1048576 mode 01777", tmpfs)
	}

	if limits := api.Ulimits; limits["nofile"] != [2]int64{1024, 2048} || limits["nproc"] != [2]int64{512, 512} {
		t.Errorf("ulimits = %v", limits)
	}

	worker := floor.typed.Services["worker"].Deploy.Resources
	if worker.Limits.Memory != 268435456 || worker.Limits.CPUs != 0.25 || worker.Reservations.Memory != 67108864 {
		t.Errorf("worker resources = %+v", worker)
	}

	if !reflect.DeepEqual(floor.typed, newest.typed) {
		t.Errorf("the two compose shapes decode differently:\n%#v\n%#v", floor.typed, newest.typed)
	}
}

func TestEnvironmentSourcedValuesAreReadFromCompose(t *testing.T) {
	t.Parallel()

	environ := "HOME=/home/x\nCONFIG_VALUE=first\nsecond\nOTHER=kept-out\n"
	model := parseIn(t, t.TempDir(), readGolden(t, "5.5.1"), environ)

	if want := map[string]string{"CONFIG_VALUE": "first\nsecond"}; !reflect.DeepEqual(model.environ, want) {
		t.Errorf("environment values = %q, want %q", model.environ, want)
	}

	if want := []string{"CONFIG_VALUE", "SECRET_VALUE"}; !slices.Equal(model.sourced, want) {
		t.Errorf("sourced = %v, want %v", model.sourced, want)
	}
}

func TestComposeVarsReadsKeysOnly(t *testing.T) {
	t.Parallel()

	const profiles = "COMPOSE_PROFILES"

	got := composeVars(
		[]string{"COMPOSE_FILE=a.yaml", "PATH=/bin", "COMPOSE_PROJECT_NAME=p", "NOEQUALS"},
		[]string{profiles, "OTHER", "COMPOSE_FILE"},
	)
	if want := []string{"COMPOSE_FILE", profiles, "COMPOSE_PROJECT_NAME"}; !slices.Equal(got, want) {
		t.Errorf("composeVars = %v, want %v", got, want)
	}

	sentinel := rand.Text()
	dir := t.TempDir()
	dotEnv := "# a comment\n\nCOMPOSE_PROFILES=" + sentinel + "\nexport COMPOSE_EXTRA=1\n  SPACED = x\n"

	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(dotEnv), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	keys, present, err := dotEnvKeys(dir)
	if err != nil || !present {
		t.Fatalf("dotEnvKeys = %v, %v, %v", keys, present, err)
	}

	if want := []string{"COMPOSE_PROFILES", "COMPOSE_EXTRA", "SPACED"}; !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}

	model := parseIn(t, dir, `{"name": "p", "services": {"api": {"image": "a"}}}`, "")

	if !slices.Contains(model.ComposeVars(), "COMPOSE_PROFILES") {
		t.Errorf("ComposeVars = %v, want COMPOSE_PROFILES", model.ComposeVars())
	}

	surfaces := []string{strings.Join(keys, " "), strings.Join(model.ComposeVars(), " "), fmt.Sprintf("%+v", model)}
	for _, surface := range surfaces {
		if strings.Contains(surface, sentinel) {
			t.Errorf("a .env value leaked: %s", surface)
		}
	}
}
