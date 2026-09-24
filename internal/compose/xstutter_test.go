package compose_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// declaring writes a compose file holding the x-stutter line and returns its path.
func declaring(t *testing.T, dir, name string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("x-stutter:\n  roles: {}\n"), 0o600); err != nil {
		t.Fatalf("write compose file: %v", err)
	}

	return path
}

// parseDeclared parses a model whose x-stutter value is declaration.
func parseDeclared(t *testing.T, declaration string, files ...string) (*compose.Model, error) {
	t.Helper()

	model := `{"name": "shop", "services": {"api": {"image": "a"}, "db": {"image": "d"}, "cache": {"image": "c"},
		"bus": {"image": "b"}, "twin": {"image": "a"}, "mock": {"image": "m"}, "idle": {"image": "i"}},
		"x-stutter": ` + declaration + `}`
	run := &fakeRun{whole: func([]string) ([]byte, int, error) { return []byte(model), 0, nil }}

	if len(files) == 0 {
		files = []string{declaring(t, t.TempDir(), "compose.yaml")}
	}

	return compose.Parse(t.Context(), run.run, compose.Inputs{Service: target, Files: files})
}

func TestXStutterGrammar(t *testing.T) {
	const cacheRole = "x-stutter.roles.cache"

	t.Parallel()

	failures := []struct{ declaration, key string }{
		{declaration: `[]`, key: "x-stutter"},
		{declaration: `{"seed": {}}`, key: "x-stutter.seed"},
		{declaration: `{"roles": []}`, key: "x-stutter.roles"},
		{declaration: `{"roles": {"ghost": "other"}}`, key: "x-stutter.roles.ghost"},
		{declaration: `{"roles": {"api": "other"}}`, key: "x-stutter.roles.api"},
		{declaration: `{"roles": {"cache": "job"}}`, key: cacheRole},
		{declaration: `{"roles": {"cache": "attached"}}`, key: cacheRole},
		{declaration: `{"roles": {"cache": "server"}}`, key: cacheRole},
		{declaration: `{"roles": {"cache": 1}}`, key: cacheRole},
		{declaration: `{"endpoints": "db"}`, key: "x-stutter.endpoints"},
		{declaration: `{"endpoints": {"ghost": {"5432": "pg"}}}`, key: "x-stutter.endpoints.ghost"},
		{declaration: `{"endpoints": {"api": {"5432": "pg"}}}`, key: "x-stutter.endpoints.api"},
		{declaration: `{"endpoints": {"db": ["5432"]}}`, key: "x-stutter.endpoints.db"},
		{declaration: `{"endpoints": {"db": {"70000": "pg"}}}`, key: `x-stutter.endpoints.db."70000"`},
		{declaration: `{"endpoints": {"db": {"0": "pg"}}}`, key: `x-stutter.endpoints.db."0"`},
		{declaration: `{"endpoints": {"db": {"54x": "pg"}}}`, key: `x-stutter.endpoints.db."54x"`},
		{declaration: `{"endpoints": {"db": {"+5432": "pg"}}}`, key: `x-stutter.endpoints.db."+5432"`},
		{declaration: `{"endpoints": {"db": {"5432": "mysql"}}}`, key: `x-stutter.endpoints.db."5432"`},
		{declaration: `{"endpoints": {"db": {"5432": true}}}`, key: `x-stutter.endpoints.db."5432"`},
	}

	for _, tc := range failures {
		_, err := parseDeclared(t, tc.declaration)
		if !errors.Is(err, compose.ErrModel) || !strings.Contains(err.Error(), tc.key) {
			t.Errorf("%s: Parse = %v, want ErrModel naming %s", tc.declaration, err, tc.key)
		}
	}

	valid := `{
		"roles": {"db": "datastore", "bus": "bus", "twin": "sibling", "mock": "other", "idle": "unused"},
		"endpoints": {"db": {"5432": "pg"}, "bus": {"4222": "nats"}, "mock": {"80": "http", "9000": "opaque"}}
	}`
	file := declaring(t, t.TempDir(), "override.yaml")

	model, err := parseDeclared(t, valid, file)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	want := compose.Overrides{
		Roles: map[string]compose.Role{
			"db": compose.RoleDatastore, "bus": compose.RoleBus, "twin": compose.RoleSibling,
			"mock": compose.RoleOther, "idle": compose.RoleUnused,
		},
		Endpoints: map[string]map[uint16]compose.Protocol{
			"db":   {5432: compose.ProtocolPG},
			"bus":  {4222: compose.ProtocolNATS},
			"mock": {80: compose.ProtocolHTTP, 9000: compose.ProtocolOpaque},
		},
		File: file,
	}

	if got := model.Overrides(); !reflect.DeepEqual(got, want) {
		t.Errorf("Overrides = %+v, want %+v", got, want)
	}

	if got := modelFrom(t, minimalModel).Overrides(); got.File != "" || len(got.Roles)+len(got.Endpoints) != 0 {
		t.Errorf("Overrides of an undeclared model = %+v, want none", got)
	}
}

func TestXStutterInTwoFilesIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first := declaring(t, dir, "compose.yaml")
	second := declaring(t, dir, "override.yaml")

	_, err := parseDeclared(t, `{"roles": {"cache": "other"}}`, first, second)
	if !errors.Is(err, compose.ErrModel) || !strings.Contains(err.Error(), first) ||
		!strings.Contains(err.Error(), second) {
		t.Fatalf("Parse = %v, want ErrModel naming %s and %s", err, first, second)
	}

	quoted := filepath.Join(dir, "quoted.yaml")
	if err := os.WriteFile(quoted, []byte("services: {}\n\"x-stutter\" :\n  roles: {}\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := parseDeclared(t, `{}`, first, quoted); err == nil || !strings.Contains(err.Error(), quoted) {
		t.Errorf("Parse = %v, want a quoted key found in %s", err, quoted)
	}
}

func TestXStutterWithNoMatchingFileIsRefused(t *testing.T) {
	t.Parallel()

	nested := project(t, "compose.yaml")
	if err := os.WriteFile(nested, []byte("services:\n  api:\n    x-stutter: {}\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := parseDeclared(t, `{"roles": {"cache": "other"}}`, nested)
	if !errors.Is(err, compose.ErrModel) || !strings.Contains(err.Error(), "top-level") {
		t.Errorf("Parse = %v, want ErrModel asking for a top-level key in one file", err)
	}

	missing := filepath.Join(t.TempDir(), "gone.yaml")

	_, err = parseDeclared(t, `{}`, declaring(t, t.TempDir(), "compose.yaml"), missing)
	if err == nil || !strings.Contains(err.Error(), missing) {
		t.Errorf("Parse = %v, want an error naming the unreadable %s", err, missing)
	}
}
